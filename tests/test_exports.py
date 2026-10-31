"""Export contract, compiler, HTTP boundary and Parquet round-trip tests."""
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
import httpx
from fastapi import FastAPI
from pydantic import ValidationError
import pyarrow.parquet as pq

from chessism_api.operations.exports import catalog, config, files, service
from chessism_api.operations.exports.auth import require_client
from chessism_api.operations.exports.query import ExportSpec, compile_query, effective_order
from chessism_api.operations.exports.writer import ParquetOutput
from chessism_api.routers.exports import router
from chessism_api.redis_client import get_redis_pool

TABLES = {key: f'"public"."{key}"' for key in ('game','player','fen','game_mode','moves','game_player','game_fen_association')}


class QueryTests(unittest.TestCase):
    def test_bound_values_and_only_required_joins(self):
        attack = "hikaru'; DROP TABLE game; --"
        spec = ExportSpec(source='games', select=['game_id', 'white_rating'], where={'field':'white_player','op':'eq','value':attack})
        sql, params = compile_query(spec, TABLES)
        self.assertNotIn(attack, str(sql)); self.assertEqual(params['filter_0'], attack)
        self.assertIn('wp ON', str(sql)); self.assertNotIn(' bp ON', str(sql))
        self.assertNotIn('fen', str(sql)); self.assertNotIn('COUNT', str(sql))
        self.assertEqual(params['export_limit'], 100001)
        self.assertEqual(effective_order(spec), [{'field':'game_id','direction':'asc'}])

    def test_nested_groups_membership_and_stable_half_move_order(self):
        spec = ExportSpec(source='moves', select=['game_id','san','fen_id'], where={'all':[
            {'any':[{'field':'white_player','op':'eq','value':'hikaru'}, {'field':'black_player','op':'eq','value':'hikaru'}]},
            {'field':'mode','op':'in','value':['blitz','rapid']}, {'field':'score_cp','op':'is_not_null'}]},
            order_by=[{'field':'played_at','direction':'asc'}])
        sql, params = compile_query(spec, TABLES)
        self.assertIn(' OR ', str(sql)); self.assertIn(' AND ', str(sql))
        self.assertIn('a ON', str(sql)); self.assertIn('f ON', str(sql))
        self.assertEqual(params['filter_2'], ['blitz','rapid'])
        self.assertEqual(effective_order(spec)[-1], {'field':'is_white','direction':'desc'})
        self.assertIn("s.san <> '--'", str(sql))

    def test_strict_invalid_requests_fail_before_database_access(self):
        base = {'source':'games','select':['game_id']}
        invalid = [dict(base, sql='SELECT * FROM account'), dict(base, source='account'),
            dict(base, select=['game_id; DROP TABLE game']), dict(base, select=['game_id','game_id']),
            dict(base, limit=True), dict(base, limit=5000001),
            dict(base, where={'field':'game_id','op':'eq','value':1.2}),
            dict(base, where={'field':'game_id','op':'eq','value':2**63}),
            dict(base, where={'field':'white_rating','op':'eq','value':None}),
            dict(base, where={'field':'white_rating','op':[],'value':0}),
            dict(base, where={'field':'average_rating','op':'eq','value':10**1000}),
            dict(base, where={'field':'played_at','op':'gte','value':'2026-01-01'}),
            dict(base, where={'field':'game_id','op':'in','value':[]}),
            dict(base, where={'field':'game_id','op':'is_null','value':None}),
            dict(base, where={'all':[]}), dict(base, order_by=[{'field':'random()'}])]
        node = {'field':'game_id','op':'eq','value':1}
        for _ in range(8): node = {'all':[node]}
        invalid.append(dict(base, where=node))
        for value in invalid:
            with self.subTest(value=str(value)[:120]), self.assertRaises(ValidationError):
                ExportSpec.model_validate(value)

    def test_timestamp_values_remain_aware_and_column_types_are_explicit(self):
        spec=ExportSpec(source='games',select=['played_at'],where={'field':'played_at','op':'gte','value':'2026-01-01T01:00:00+01:00'})
        _,params=compile_query(spec,TABLES)
        self.assertEqual(params['filter_0'],datetime(2026,1,1,tzinfo=timezone.utc))
        self.assertEqual(catalog.SOURCES['positions'].columns['score_cp'].dtype, 'int32')
        self.assertEqual(catalog.public_catalog(['games'], 100)['sources'][0]['name'], 'games')
        self.assertEqual(len(catalog.public_catalog(['games'], 100)['sources']), 1)


class ParquetTests(unittest.TestCase):
    def test_nulls_empty_file_integer_identity_timestamp_and_checksum(self):
        spec=ExportSpec(source='games',select=['game_id','played_at','average_rating'])
        with TemporaryDirectory() as directory, patch.object(config,'ROOT',Path(directory)), patch.object(config,'FREE_FLOOR',0):
            path=Path(directory)/'test.parquet'
            writer=ParquetOutput(path,spec)
            writer.write([{'game_id':2**53+1,'played_at':None,'average_rating':None}])
            stamp=datetime(2026,10,14,1,2,3,123456,tzinfo=timezone.utc)
            writer.write([{'game_id':7,'played_at':stamp,'average_rating':1234.5}]);writer.close()
            rows=pq.read_table(path).to_pylist()
            self.assertEqual(rows[0]['game_id'],2**53+1)
            self.assertIsNone(rows[0]['played_at']);self.assertEqual(rows[1]['played_at'],stamp)
            self.assertEqual(len(files.sha256(path)),64)
            empty=Path(directory)/'empty.parquet';writer=ParquetOutput(empty,spec);writer.close()
            self.assertEqual(pq.read_table(empty).num_rows,0)
            self.assertEqual(pq.read_schema(path),pq.read_schema(empty))

    def test_file_namespace_and_disk_size_guards(self):
        with TemporaryDirectory() as directory, patch.object(config,'ROOT',Path(directory)), patch.object(config,'FREE_FLOOR',0):
            for invalid in ('..','/','../a','../../','not-an-id'):
                with self.assertRaises(ValueError):files.job_dir(invalid)
            with patch.object(config,'FREE_FLOOR',2**62),self.assertRaises(ValueError): files.ensure_space()
            writer=ParquetOutput(Path(directory)/'test.parquet',ExportSpec(source='players',select=['player_name']))
            try:
                with patch.object(config,'MAX_BYTES',0),self.assertRaises(ValueError):writer.write([{'player_name':'hikaru'}])
            finally:writer.close()


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app=FastAPI();self.app.include_router(router,prefix='/exports/v1')
        self.redis=AsyncMock(); self.redis.set.return_value=True
        self.app.dependency_overrides[get_redis_pool]=lambda:self.redis
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url='http://test')

    async def asyncTearDown(self): await self.client.aclose()

    def authenticate(self, sources=('games',), max_rows=100000):
        self.app.dependency_overrides[require_client]=lambda:SimpleNamespace(id='client',allowed_sources=list(sources),max_rows=max_rows)

    async def test_no_cookie_or_arbitrary_bearer_can_use_exports(self):
        for headers in ({}, {'Cookie':'chessism_session=browser-token'}, {'Authorization':'Bearer admin'}):
            self.assertEqual((await self.client.get('/exports/v1/catalog',headers=headers)).status_code,401)

    async def test_capabilities_validation_size_and_preview_are_bounded(self):
        self.authenticate()
        data=(await self.client.get('/exports/v1/catalog')).json()
        self.assertEqual([s['name'] for s in data['sources']],['games'])
        response=await self.client.post('/exports/v1/preview',json={'source':'players','select':['player_id']})
        self.assertEqual(response.status_code,403)
        with patch('chessism_api.routers.exports.preview_rows', AsyncMock(return_value={'rows':[]})) as read:
            response=await self.client.post('/exports/v1/preview',json={'source':'games','select':['game_id'],'where':{'field':'game_id','op':'eq','value':1}})
            self.assertEqual(response.status_code,200); read.assert_awaited_once()
            self.assertEqual(response.headers['cache-control'],'private, no-store')
        response=await self.client.post('/exports/v1',json={'source':'games','select':['game_id']})
        self.assertEqual(response.status_code,422) # idempotency key required
        response=await self.client.post('/exports/v1/preview',content=b' '*131073)
        self.assertEqual(response.status_code,413)
        self.redis.set.return_value=False
        response=await self.client.post('/exports/v1/preview',json={'source':'games','select':['game_id']})
        self.assertEqual(response.status_code,429)

    async def test_create_delegates_validated_request_and_status_is_owner_scoped(self):
        self.authenticate()
        with patch.object(service,'submit',AsyncMock(return_value={'id':'job','status':'queued'})) as submit:
            response=await self.client.post('/exports/v1',headers={'Idempotency-Key':'request-001'},json={'source':'games','select':['game_id']})
            self.assertEqual(response.status_code,202);self.assertEqual(submit.call_args.args[2],'request-001')


if __name__=='__main__':unittest.main()
