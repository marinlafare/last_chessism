import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

for key, value in {'AUTH_COOKIE_NAME': 'test_session', 'AUTH_GATE_COOKIE_NAME': 'test_gate',
    'AUTH_SESSION_TTL_DAYS': '1', 'AUTH_GATE_TTL_MINUTES': '1', 'AUTH_COOKIE_SECURE': 'false',
    'SUPERADMIN_GATE_CODE': 'test-only', 'DATABASE_URL': 'postgresql+asyncpg://test:test@localhost/test'}.items():
    os.environ.setdefault(key, value)

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from chessism_api.auth import get_current_account
from chessism_api.database.models import AccountRole
from chessism_api.operations import database_preview as operation
from chessism_api.routers import database_catalog as router


def relation(**extra):
    return dict(oid=1, schema='public', name='game', kind='r', reltuples=50000000, relpages=800000, **extra)


def result(rows):
    return SimpleNamespace(all=lambda: rows, mappings=lambda: SimpleNamespace(all=lambda: rows, one_or_none=lambda: rows[0] if rows else None))


class PreviewTests(unittest.TestCase):
    def client(self, role='admin'):
        app = FastAPI(); app.include_router(router.router, prefix='/database')
        async def account():
            if role is None: raise HTTPException(401, 'Authentication required')
            return SimpleNamespace(id='test', role=AccountRole(role))
        app.dependency_overrides[get_current_account] = account
        return TestClient(app)

    def test_admin_only_get_only_no_cache_fixed_endpoint_and_sanitized_errors(self):
        with patch.object(router, 'read_database_preview', AsyncMock(return_value={'rows': []})) as read:
            for role, status in [(None, 401), ('user', 403)]:
                self.assertEqual(self.client(role).get('/database/preview?table=public.game').status_code, status)
            read.assert_not_awaited()
            response = self.client().get('/database/preview?table=public.game')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'no-store')
            read.assert_awaited_once_with('public.game')
            self.assertEqual(self.client().post('/database/preview', json={'sql': 'DROP TABLE game'}).status_code, 405)
            self.assertEqual(self.client().get('/database/preview').status_code, 422)
            self.assertEqual(self.client().get('/database/preview', params={'table': 'x' * 513}).status_code, 422)
            for error in (SQLAlchemyError('private-driver-query-and-credentials'), asyncio.TimeoutError()):
                read.side_effect = error
                response = self.client().get('/database/preview?table=public.game')
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.headers['cache-control'], 'no-store')
                self.assertNotIn('private-driver', response.text)
            read.side_effect = operation.PreviewNotFound('Table unavailable')
            self.assertEqual(self.client().get('/database/preview?table=missing').status_code, 404)

    def test_projection_redacts_credentials_quotes_identifiers_and_bounds_sort(self):
        columns = [{'name': name, 'data_type': kind, 'redacted': operation.protected_column(name)}
                   for name, kind in [('id', 'bigint'), ('password_hash', 'text'), ('token_hash', 'text'),
                                      ('some"column', 'text'), ('bytes', 'bytea')]]
        sql = str(operation.preview_statement({**relation(), 'name': 'strange"name'}, columns, True))
        self.assertIn('FROM "public"."strange""name" TABLESAMPLE SYSTEM (:percentage)', sql)
        self.assertIn('left("some""column"::text, :cell_limit + 1)', sql)
        self.assertNotIn('password_hash', sql)
        self.assertNotIn('token_hash', sql)
        self.assertEqual(sql.count("'[redacted]'"), 2)
        self.assertIn('[binary]', sql)
        self.assertIn('MATERIALIZED', sql)
        self.assertLess(sql.index('LIMIT :candidate_limit'), sql.index('ORDER BY random()'))
        for name in ('api_key', 'privateKey', 'access_token', 'Authorization', 'cookie', 'db_credentials'):
            self.assertTrue(operation.protected_column(name))
        self.assertFalse(operation.protected_column('game_id'))

    def test_sampling_small_unknown_and_huge_tables(self):
        self.assertLess(operation.sampling_percent(relation()), .01)
        for rows in (0, -1, 10, float('nan')):
            self.assertEqual(operation.sampling_percent({**relation(), 'reltuples': rows}), 100)


class PreviewSessionTests(unittest.IsolatedAsyncioTestCase):
    def session(self, relation_rows, columns, *samples):
        session = AsyncMock()
        session.__aenter__.return_value = session
        session.begin = MagicMock(return_value=AsyncMock())
        session.execute.side_effect = [None, None, None, result(relation_rows), result(columns), *[result(rows) for rows in samples]]
        return session

    async def test_readonly_fixed_limits_nulls_precision_and_truncation(self):
        session = self.session([relation()], [{'name': 'id', 'data_type': 'bigint'}, {'name': 'pgn', 'data_type': 'text'}],
                               [('9007199254740999', None)] + [('1', 'x' * 513)] * 49)
        with patch.object(operation, 'AsyncDBSession', return_value=session):
            payload = await operation.read_database_preview('public.game')
        self.assertEqual(payload['row_count'], 50)
        self.assertEqual(payload['rows'][0], ['9007199254740999', None])
        self.assertEqual(len(payload['rows'][1][1]), 513)
        self.assertTrue(payload['rows'][1][1].endswith('…'))
        self.assertEqual(payload['truncated_cells'], 49)
        self.assertEqual(payload['sample_method'], 'random_blocks')
        calls = session.execute.await_args_list
        self.assertIn('READ ONLY', str(calls[0].args[0]))
        self.assertIn('statement_timeout', str(calls[1].args[0]))
        self.assertIn('lock_timeout', str(calls[2].args[0]))
        self.assertEqual(calls[3].args[1], {'table_id': 'public.game'})
        self.assertEqual(calls[5].args[1]['row_limit'], 50)
        self.assertEqual(calls[5].args[1]['candidate_limit'], 1000)

    async def test_sparse_block_retries_and_short_or_empty_table(self):
        for final in ([], [('1',), ('2',)]):
            session = self.session([relation()], [{'name': 'id', 'data_type': 'int'}], [], [('1',)], final)
            with patch.object(operation, 'AsyncDBSession', return_value=session):
                payload = await operation.read_database_preview('public.game')
            self.assertEqual(payload['row_count'], len(final))
            self.assertEqual(payload['sample_method'], 'bounded_candidates')
            self.assertEqual(session.execute.await_args_list[-1].args[1]['percentage'], 100)
            self.assertEqual(session.execute.await_count, 8)

    async def test_views_use_bounded_pool_and_wide_tables_have_column_limit(self):
        columns = [{'name': 'col_' + str(i), 'data_type': 'text'} for i in range(130)]
        session = self.session([{**relation(), 'kind': 'v'}], columns, [tuple(['x'] * 128)])
        with patch.object(operation, 'AsyncDBSession', return_value=session):
            payload = await operation.read_database_preview('public.report')
        self.assertEqual(len(payload['columns']), 128)
        self.assertEqual(payload['omitted_columns'], 2)
        self.assertNotIn('TABLESAMPLE', str(session.execute.await_args_list[-1].args[0]))
        self.assertEqual(session.execute.await_count, 6)

    async def test_invalid_identifier_never_becomes_sql(self):
        session = self.session([], [])
        malicious = 'public.game; DROP TABLE game; --'
        with patch.object(operation, 'AsyncDBSession', return_value=session):
            with self.assertRaises(operation.PreviewNotFound):
                await operation.read_database_preview(malicious)
        self.assertEqual(session.execute.await_count, 4)
        call = session.execute.await_args_list[-1]
        self.assertEqual(call.args[1], {'table_id': malicious})
        self.assertNotIn('DROP TABLE', str(call.args[0]))


if __name__ == '__main__': unittest.main()
