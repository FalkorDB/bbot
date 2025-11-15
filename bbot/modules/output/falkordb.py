import json
import logging
from contextlib import suppress

from bbot.modules.output.base import BaseOutputModule


# silence annoying redis logger
logging.getLogger("redis").setLevel(logging.CRITICAL)


class falkordb(BaseOutputModule):
    """
    # start FalkorDB in the background with docker
    docker run -d -p 6379:6379 -p 3000:3000 -v "$(pwd)/falkordb/:/data/" --name falkordb falkordb/falkordb

    # view all running docker containers
    > docker ps

    # view all docker containers
    > docker ps -a

    # stop a docker container
    > docker stop <CONTAINER_ID>

    # remove a docker container
    > docker remove <CONTAINER_ID>

    # start a stopped container
    > docker start <CONTAINER_ID>
    """

    watched_events = ["*"]
    meta = {"description": "Output to FalkorDB", "created_date": "2025-11-15", "author": "@FalkorDB"}
    options = {"host": "localhost", "port": 6379, "graph": "bbot"}
    options_desc = {
        "host": "FalkorDB server host",
        "port": "FalkorDB server port",
        "graph": "FalkorDB graph name",
    }
    deps_pip = ["falkordb"]
    _batch_size = 500
    _preserve_graph = True

    async def setup(self):
        try:
            from falkordb.asyncio import FalkorDB
            from redis.asyncio import BlockingConnectionPool

            # Create connection pool
            self.pool = BlockingConnectionPool(
                host=self.config.get("host", self.options["host"]),
                port=self.config.get("port", self.options["port"]),
                max_connections=16,
                timeout=None,
                decode_responses=True,
            )
            self.db = FalkorDB(connection_pool=self.pool)
            self.graph = self.db.select_graph(self.config.get("graph", self.options["graph"]))
            # Test connection with simple query
            await self.graph.query("MATCH () RETURN 1 LIMIT 1")
        except Exception as e:
            return False, f"Error setting up FalkorDB: {e}"
        return True

    async def handle_batch(self, *all_events):
        # group events by type, since cypher doesn't allow dynamic labels
        events_by_type = {}
        parents_by_type = {}
        relationships = []
        for event in all_events:
            parent = event.get_parent()
            try:
                events_by_type[event.type].append(event)
            except KeyError:
                events_by_type[event.type] = [event]
            try:
                parents_by_type[parent.type].append(parent)
            except KeyError:
                parents_by_type[parent.type] = [parent]

            module = str(event.module)
            timestamp = event.timestamp
            relationships.append((parent, module, timestamp, event))

        all_ids = {}
        for event_type, events in events_by_type.items():
            self.debug(f"{len(events):,} events of type {event_type}")
            all_ids.update(await self.merge_events(events, event_type))
        for event_type, parents in parents_by_type.items():
            self.debug(f"{len(parents):,} parents of type {event_type}")
            all_ids.update(await self.merge_events(parents, event_type, id_only=True))

        rel_ids = []
        for parent, module, timestamp, event in relationships:
            try:
                src_id = all_ids[parent.id]
                dst_id = all_ids[event.id]
            except KeyError as e:
                self.error(f'Error "{e}" correlating {parent.id}:{parent.data} --> {event.id}:{event.data}')
                continue
            rel_ids.append((src_id, module, timestamp, dst_id))

        await self.merge_relationships(rel_ids)

    async def merge_events(self, events, event_type, id_only=False):
        if id_only:
            insert_data = [{"data": str(e.data), "type": e.type, "id": e.id} for e in events]
        else:
            insert_data = []
            for e in events:
                event_json = e.json(mode="graph")
                # we pop the timestamp because it belongs on the relationship
                event_json.pop("timestamp")
                # nested data types aren't supported in graph databases
                for key in ("dns_children", "discovery_path"):
                    if key in event_json:
                        event_json[key] = json.dumps(event_json[key])
                insert_data.append(event_json)

        cypher = f"""UNWIND $events AS event
        MERGE (_:{event_type} {{ id: event.id }})
        SET _ += properties(event)
        RETURN event.data as event_data, event.id as event_id, id(_) as falkordb_id"""
        falkordb_ids = {}
        # insert events
        try:
            result = await self.graph.query(cypher, {"events": insert_data})
            # get FalkorDB ids
            for row in result.result_set:
                event_id = row[1]
                falkordb_id = row[2]
                falkordb_ids[event_id] = falkordb_id
        except Exception as e:
            self.error(f"Error inserting FalkorDB nodes (label:{event_type}): {e}")
            self.trace(insert_data)
            self.trace(cypher)
        return falkordb_ids

    async def merge_relationships(self, relationships):
        rels_by_module = {}
        # group by module
        for src_id, module, timestamp, dst_id in relationships:
            data = {"src_id": src_id, "timestamp": timestamp, "dst_id": dst_id}
            try:
                rels_by_module[module].append(data)
            except KeyError:
                rels_by_module[module] = [data]

        for module, rels in rels_by_module.items():
            self.debug(f"{len(rels):,} relationships of type {module}")
            cypher = f"""
            UNWIND $rels AS rel
            MATCH (a) WHERE id(a) = rel.src_id
            MATCH (b) WHERE id(b) = rel.dst_id
            MERGE (a)-[_:{module}]->(b)
            SET _.timestamp = rel.timestamp"""
            try:
                await self.graph.query(cypher, {"rels": rels})
            except Exception as e:
                self.error(f"Error inserting FalkorDB relationship (label:{module}): {e}")
                self.trace(cypher)

    async def cleanup(self):
        with suppress(Exception):
            await self.pool.aclose()
