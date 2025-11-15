from .base import ModuleTestBase


class TestFalkordb(ModuleTestBase):
    config_overrides = {"modules": {"falkordb": {"host": "127.0.0.1", "port": 11111}}}

    async def setup_before_prep(self, module_test):
        self.falkordb_used = False

        class MockResult:
            def __init__(self):
                self.result_set = [["evilcorp.com", "DNS_NAME:c8fab50640cb87f8712d1998ecc78caf92b90f71", 115]]

        class MockGraph:
            async def query(s, *args, **kwargs):
                self.falkordb_used = True
                return MockResult()

        class MockDB:
            def select_graph(self, *args, **kwargs):
                return MockGraph()

        class MockPool:
            def __init__(self, *args, **kwargs):
                pass

            async def aclose(self):
                pass

        def mock_falkordb(*args, **kwargs):
            return MockDB()

        def mock_pool(*args, **kwargs):
            return MockPool(*args, **kwargs)

        module_test.monkeypatch.setattr("falkordb.asyncio.FalkorDB", mock_falkordb)
        module_test.monkeypatch.setattr("redis.asyncio.BlockingConnectionPool", mock_pool)

    def check(self, module_test, events):
        assert self.falkordb_used is True
