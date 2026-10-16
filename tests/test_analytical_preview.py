"""Preview bounds, JSON types, authorization, failure and lease contracts."""
import asyncio
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from pydantic import ValidationError
import pyarrow as pa
from redis.exceptions import ConnectionError as RedisConnectionError

from chessism_api.operations.exports.analytical_reader import AnalyticalError
from chessism_api.operations.exports.analytical_spec import AnalyticalPreviewSpec, SQLExportSpec
from chessism_api.operations.exports.analytical_query import ColumnSelection
from chessism_api.operations.exports.auth import require_client
from chessism_api.operations.exports.writer import preview_value
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers.exports import router


class PreviewTests(unittest.TestCase):
    def test_strict_contract_requires_known_design_and_bounded_row_count(self):
        design = {'mode': 'sql', 'sql': 'SELECT 0 AS score', 'exclude_null_columns': True}
        request = AnalyticalPreviewSpec(design=design)
        self.assertEqual(request.preview_rows, 100)
        self.assertTrue(request.design.exclude_null_columns)
        for change in ({'preview_rows': 0}, {'preview_rows': 101}, {'preview_rows': True},
                       {'preview_rows': '20'}, {'extra': True}, {'exclude_null_columns': True},
                       {'design': {'mode': 'sql', 'sql': 'SELECT pg_sleep(20)'}},
                       {'design': dict(design, exclude_null_columns='yes')},
                       {'design': {'mode': 'unknown'}}):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                AnalyticalPreviewSpec.model_validate({'design': design, **change})

    def test_sample_is_explicit_matrix_only_and_preserves_columns(self):
        design = {'mode':'move_matrix','player':'hikaru','require_complete_scores':True}
        request = AnalyticalPreviewSpec(design=design,preview_mode='sample',preview_rows=10)
        self.assertEqual(request.preview_mode,'sample')
        self.assertEqual(request.design.max_games,1000)
        self.assertEqual(AnalyticalPreviewSpec(design=design).preview_mode,'design')
        for body in (
            {'design':design,'preview_mode':'unknown'},
            {'design':{'mode':'sql','sql':'SELECT 1'},'preview_mode':'sample'},
            {'design':dict(design,require_complete_scores=False,exclude_null_columns=True),'preview_mode':'sample'}):
            with self.subTest(body=body),self.assertRaises(ValidationError):AnalyticalPreviewSpec.model_validate(body)

    def test_null_selection_never_drops_zero_false_or_rows(self):
        schema = pa.schema([pa.field('score', pa.int32()), pa.field('flag', pa.bool_()), pa.field('missing', pa.string())])
        columns = ColumnSelection(schema, True)
        columns.observe([{'score': 0, 'flag': False, 'missing': ''}])
        columns.observe([{'score': 3, 'flag': True, 'missing': None}])
        self.assertEqual(columns.finish().names, ['score', 'flag'])
        self.assertTrue(all(not field.nullable for field in columns.finish()))
        self.assertEqual(columns.metadata()['dropped_columns'], ['missing'])
        empty = ColumnSelection(schema, True)
        self.assertEqual(empty.finish().names, schema.names)
        columns.observe([{'score': None, 'flag': None, 'missing': None}])
        with self.assertRaises(AnalyticalError) as error: columns.finish()
        self.assertEqual(error.exception.code, 'NO_NON_NULL_COLUMNS')
        unchanged = ColumnSelection(schema, False)
        unchanged.observe([{'score': None, 'flag': None, 'missing': None}])
        self.assertEqual(unchanged.finish(), schema)

    def test_json_dates_naive_timestamps_and_exact_identifiers(self):
        self.assertEqual(preview_value(2**53+1), '9007199254740993')
        self.assertEqual(preview_value(0), 0)
        self.assertIs(preview_value(False), False)
        self.assertIsNone(preview_value(None))
        self.assertEqual(preview_value(date(2026, 1, 1)), '2026-01-01')
        self.assertEqual(preview_value(datetime(2026, 1, 1)), '2026-01-01T00:00:00')
        self.assertEqual(preview_value(datetime(2026, 1, 1, tzinfo=timezone.utc)), '2026-01-01T00:00:00+00:00')


class PreviewHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app = FastAPI(); app.include_router(router, prefix='/exports/v1')
        self.integration = SimpleNamespace(id='preview-client', allowed_sources=['games'], max_rows=1000, allow_analytics=True)
        self.redis = AsyncMock(); self.redis.set.return_value = True
        app.dependency_overrides[require_client] = lambda: self.integration
        app.dependency_overrides[get_redis_pool] = lambda: self.redis
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test')
        self.body = {'design': {'mode': 'sql', 'sql': 'SELECT game_id FROM exports_data_v1.games', 'max_result_rows': 1000}, 'preview_rows': 10}
        self.result = {'kind':'analytical_preview', 'mode':'sql', 'snapshot_at':'2026-01-01T00:00:00+00:00',
            'preview_rows':10, 'columns':[{'name':'n','type':'int32','nullable':True}], 'rows':[[0]],
            'row_count':1, 'has_more':False, 'total_row_count':1, 'entire_result_checked':True,
            'column_selection':{'exclude_null_columns':False,'checked_scope':'not_checked','dropped_columns':[]},
            'selection':{}, 'score_semantics':{}, 'json_integer_encoding':'decimal string when outside JavaScript safe range'}

    async def asyncTearDown(self): await self.http.aclose()

    async def test_auth_and_validation_fail_before_query(self):
        with patch('chessism_api.routers.exports.analytical_preview_rows', AsyncMock()) as run:
            self.integration.allow_analytics = False
            self.assertEqual((await self.http.post('/exports/v1/analytical-preview', json=self.body)).status_code, 403)
            self.integration.allow_analytics = True; self.integration.allowed_sources = ['players']
            self.assertEqual((await self.http.post('/exports/v1/analytical-preview', json=self.body)).status_code, 403)
            self.integration.allowed_sources = ['games']; self.integration.max_rows = 10
            self.assertEqual((await self.http.post('/exports/v1/analytical-preview', json=self.body)).status_code, 422)
            run.assert_not_awaited()
        self.redis.set.assert_not_awaited()

    async def test_success_and_errors_release_lease_with_clear_codes(self):
        with patch('chessism_api.routers.exports.analytical_preview_rows', AsyncMock(return_value=self.result)) as run:
            response = await self.http.post('/exports/v1/analytical-preview', json=self.body)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.headers['cache-control'], 'private, no-store')
            self.assertEqual(run.call_args.args[0].preview_rows, 10)
        self.redis.eval.assert_awaited_once()
        self.assertEqual(self.redis.set.call_args.args[0], 'export-preview:preview-client')
        for error, status, code in (
            (AnalyticalError('NO_NON_NULL_COLUMNS', 'No eligible columns.'), 422, 'NO_NON_NULL_COLUMNS'),
            (AnalyticalError('UNSAFE_QUERY_ROLE', 'Unsafe.'), 503, 'UNSAFE_QUERY_ROLE'),
            (AnalyticalError('PREVIEW_TIMEOUT', 'Too slow.'), 504, 'PREVIEW_TIMEOUT'),
            (asyncio.TimeoutError(), 504, 'PREVIEW_TIMEOUT'),
            (ConnectionError('private host/credential must not leak'), 503, 'PREVIEW_UNAVAILABLE'),
            (ValueError('private credential must not leak'), 503, 'PREVIEW_UNAVAILABLE')):
            self.redis.eval.reset_mock()
            with patch('chessism_api.routers.exports.analytical_preview_rows', AsyncMock(side_effect=error)):
                response = await self.http.post('/exports/v1/analytical-preview', json=self.body)
            self.assertEqual(response.status_code, status, response.text)
            self.assertEqual(response.json()['detail']['code'], code)
            self.assertNotIn('credential must not leak', response.text)
            self.redis.eval.assert_awaited_once()

    async def test_busy_redis_outage_and_release_failure(self):
        self.redis.set.return_value = False
        response = await self.http.post('/exports/v1/analytical-preview', json=self.body)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers['retry-after'], '3')
        self.redis.eval.assert_not_awaited()
        self.redis.set.side_effect = RedisConnectionError('private data')
        response = await self.http.post('/exports/v1/analytical-preview', json=self.body)
        self.assertEqual(response.status_code, 503)
        self.redis.set.side_effect = None; self.redis.set.return_value = True
        self.redis.eval.side_effect = RedisConnectionError('private data')
        with patch('chessism_api.routers.exports.analytical_preview_rows', AsyncMock(return_value=self.result)):
            response = await self.http.post('/exports/v1/analytical-preview', json=self.body)
        self.assertEqual(response.status_code, 200)

    async def test_sample_http_marks_unknown_totals_and_uses_shorter_deadline(self):
        self.integration.allowed_sources=['games','players','moves']
        body={'design':{'mode':'move_matrix','player':'hikaru','max_games':1000,
            'require_complete_scores':True},'preview_mode':'sample','preview_rows':10}
        result=dict(self.result,kind='analytical_sample',preview_mode='sample',mode='move_matrix',
            has_more=None,total_row_count=None,entire_result_checked=False)
        with patch('chessism_api.routers.exports.analytical_preview_rows',AsyncMock(return_value=result)), \
             patch('chessism_api.routers.exports.asyncio.wait_for',wraps=asyncio.wait_for) as wait:
            response=await self.http.post('/exports/v1/analytical-preview',json=body)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json()['kind'],'analytical_sample')
        self.assertIsNone(response.json()['has_more']);self.assertIsNone(response.json()['total_row_count'])
        self.assertEqual(wait.call_args.kwargs['timeout'],5)
