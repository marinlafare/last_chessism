"""HTTP validation and auth checks with a fake graph reader; no database access."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from fastapi import FastAPI, Depends, HTTPException
from fastapi.testclient import TestClient
from chessism_api.routers import graph_explorer as api


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(api.router, prefix='/graph-explorer')
        self.client = TestClient(self.app)

    def test_validation_rejects_unbounded_and_malformed_queries(self):
        with patch.object(api, 'query', AsyncMock()) as query:
            for path in ('/positions?offset=1000000', '/positions?color=orange',
                         '/position?key=anything', '/position?key=chess:1&outgoing=-1',
                         '/map?a=bad%20player', '/map?b=', '/games?key=chess:1&max_ply=100',
                         '/ply-map?first=32', '/ply-map?first=7&last=2', '/ply-map?players=x',
                         '/ply-map?players=1,2,3,4,5,6,7,8,9', '/ply-map?offset=999999',
                         '/games?key=chess:1&move_key=arbitrary', '/games?key=chess:1&offset=100000'):
                self.assertEqual(self.client.get('/graph-explorer' + path).status_code, 422)
            query.assert_not_called()

    def test_ply_map_read_does_not_trigger_source_preparation(self):
        session=AsyncMock()
        factory=AsyncMock()
        factory.__aenter__.return_value=session
        with patch.object(api,'AsyncDBSession',return_value=factory), \
                patch.object(api.ply_map_reader,'snapshot',AsyncMock(return_value=({'pending':True},None))), \
                patch('chessism_api.database.neo4j.ply_map_source.read',AsyncMock()) as source:
            result=self.client.get('/graph-explorer/ply-map')
            self.assertEqual(result.status_code,200)
            self.assertIsNone(result.json()['snapshot'])
            source.assert_not_called()
            self.assertEqual(str(session.execute.call_args_list[0].args[0]),'SET TRANSACTION READ ONLY')

    def test_map_normalizes_names_and_returns_versioned_sample(self):
        with patch.object(api, 'query', AsyncMock(return_value=({'revision':'123','nodes':[]},2))) as query:
            result = self.client.get('/graph-explorer/map?a=LAFareto&b=HIKARU').json()
            self.assertEqual(result['revision'], '123')
            self.assertEqual(result['cache_seconds'], 300)
            self.assertEqual(query.call_args.args[-2:], ('lafareto','hikaru'))

    def test_pagination_reuses_the_scope_ranking_and_normalizes_color(self):
        data = {'rows':list(range(21)), 'coverage':{'games':3}}
        with patch.object(api, 'query', AsyncMock(return_value=(data,2))) as query:
            result = self.client.get('/graph-explorer/positions?offset=10&color=black').json()
            self.assertEqual(result['rows'], list(range(10,20)))
            self.assertTrue(result['has_more'])
            self.assertEqual(query.call_args.args[-2:], ('','all'))

    def test_routes_can_be_protected_without_opening_a_graph_connection(self):
        def deny():
            raise HTTPException(401, 'Authentication required')
        app = FastAPI()
        app.include_router(api.router, prefix='/graph-explorer', dependencies=[Depends(deny)])
        with patch.object(api, 'query', AsyncMock()) as query:
            self.assertEqual(TestClient(app).get('/graph-explorer/positions').status_code, 401)
            query.assert_not_called()


class CacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_results_are_bounded_cached_and_failures_are_not_cached(self):
        api._cache.clear()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(graph_explorer_driver=object())))
        with patch.object(api.GraphConfig,'from_env',return_value=SimpleNamespace(database='test')):
            with patch.object(api.explorer,'read',AsyncMock(return_value={'rows':[]})) as read:
                await api.query(request,api.explorer.ranking,'lafareto','all')
                await api.query(request,api.explorer.ranking,'lafareto','all')
                self.assertEqual(read.await_count,1)
                for i in range(40):
                    await api.query(request,api.explorer.players,str(i))
                self.assertLessEqual(len(api._cache),api.MAX_CACHE)
            with patch.object(api.explorer,'read',AsyncMock(side_effect=asyncio.TimeoutError())):
                with self.assertRaises(HTTPException) as failure:
                    await api.query(request,api.explorer.players,'uncached')
                self.assertEqual(failure.exception.status_code,503)
                self.assertNotIn(('players','uncached'),api._cache)
        api._cache.clear()
