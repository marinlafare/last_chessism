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
from chessism_api.operations import database_catalog as operation
from chessism_api.routers import database_catalog as router


def relation(oid, name, **extra):
    return {'oid': oid, 'id': f'public.{name}', 'schema': 'public', 'name': name,
            'kind': 'r', 'is_partition': False, 'estimated_rows': 10, **extra}


def column(oid, name, **extra):
    return {'table_oid': oid, 'name': name, 'data_type': 'bigint', 'nullable': False, 'primary_key': False, **extra}


def foreign(oid=3, source=1, target=2, col='game_id', ref='id'):
    return {'oid': oid, 'name': 'declared_fk', 'source_oid': source, 'target_oid': target,
            'source_column': col, 'target_column': ref}


class CatalogTests(unittest.TestCase):
    def test_payload_composite_self_keys_and_no_record_values(self):
        payload = operation.catalog_payload(
            [relation(1, 'game_fen', password='not-returned'), relation(2, 'game'), relation(9, 'report', kind='v', estimated_rows=None)],
            [column(1, 'game_id', primary_key=True), column(1, 'move'), column(2, 'id', primary_key=True), column(2, 'move')],
            [foreign(), foreign(col='move', ref='move'), foreign(4, 2, 2, 'id', 'id'), foreign(5, 1, 999)],
        )
        self.assertEqual(len(payload['relationships']), 2)
        self.assertEqual(payload['relationships'][0]['source_columns'], ['game_id', 'move'])
        self.assertEqual(payload['relationships'][0]['target_columns'], ['id', 'move'])
        self.assertTrue(payload['tables'][0]['columns'][0]['primary_key'])
        self.assertTrue(payload['tables'][0]['columns'][0]['foreign_key'])
        self.assertEqual(payload['tables'][2]['kind'], 'view')
        self.assertNotIn('password', str(payload))
        self.assertNotIn('source_oid', str(payload))
        self.assertEqual(operation.catalog_payload([], [], [])['tables'], [])

    def client(self, role='admin'):
        app = FastAPI(); app.include_router(router.router, prefix='/database')
        async def account():
            if role is None: raise HTTPException(401, 'Authentication required')
            return SimpleNamespace(id='test', role=AccountRole(role))
        app.dependency_overrides[get_current_account] = account
        return TestClient(app)

    def test_endpoint_is_admin_only_get_only_and_sanitizes_errors(self):
        with patch.object(router, 'read_database_catalog', AsyncMock(return_value={'tables': [], 'relationships': []})) as read:
            self.assertEqual(self.client(None).get('/database/schema').status_code, 401)
            self.assertEqual(self.client('user').get('/database/schema').status_code, 403)
            read.assert_not_awaited()
            response = self.client().get('/database/schema')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'no-store')
            self.assertEqual(self.client().post('/database/schema', json={'sql': 'DROP TABLE game'}).status_code, 405)
            self.assertEqual(read.await_count, 1)
            read.side_effect = SQLAlchemyError('credentials-should-not-appear')
            response = self.client().get('/database/schema')
            self.assertEqual(response.status_code, 503)
            self.assertNotIn('credentials', response.text)

    def test_catalog_queries_never_count_or_fetch_application_rows(self):
        for statement in (operation.RELATIONS_SQL, operation.COLUMNS_SQL, operation.FOREIGN_KEYS_SQL):
            sql = str(statement).lower()
            self.assertTrue(sql.strip().startswith('select'))
            self.assertIn('pg_catalog.', sql)
            self.assertNotIn('count(', sql)
            self.assertNotIn('select *', sql)
            self.assertNotIn('pg_get_expr', sql)
            self.assertNotIn('pg_get_viewdef', sql)


class CatalogSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_only_transaction_bounded_queries_and_empty_database(self):
        session = AsyncMock()
        session.__aenter__.return_value = session
        session.begin = MagicMock(return_value=AsyncMock())
        def result(rows):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
        session.execute.side_effect = [None, None, None, result([relation(1, 'game')]), result([column(1, 'id')]), result([])]
        with patch.object(operation, 'AsyncDBSession', return_value=session):
            payload = await operation.read_database_catalog()
        self.assertEqual(payload['tables'][0]['id'], 'public.game')
        calls = session.execute.await_args_list
        self.assertIn('READ ONLY', str(calls[0].args[0]))
        self.assertIn('statement_timeout', str(calls[1].args[0]))
        self.assertIn('lock_timeout', str(calls[2].args[0]))
        self.assertEqual(calls[3].args[1]['limit'], operation.MAX_TABLES + 1)
        self.assertEqual(calls[4].args[1]['oids'], [1])
        session.execute.reset_mock(side_effect=True)
        session.execute.side_effect = [None, None, None, result([])]
        with patch.object(operation, 'AsyncDBSession', return_value=session):
            self.assertEqual((await operation.read_database_catalog())['tables'], [])
        self.assertEqual(session.execute.await_count, 4)


if __name__ == '__main__': unittest.main()
