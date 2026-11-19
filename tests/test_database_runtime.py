import ast
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
from contextlib import asynccontextmanager

from chessism_api.database.postgresql import runtime

ROOT = Path(__file__).resolve().parents[1]


class StartupWiringTests(unittest.TestCase):
    def test_services_use_shared_initializers(self):
        for path, initializer in (
            ('main.py','init_runtime_db'), ('worker.py','init_runtime_db'),
            ('chessism_api/operations/research_algorithms/worker.py','init_runtime_db'),
            ('chessism_api/operations/salience_backfill_cli.py','init_runtime_db'),
            ('chessism_api/operations/cloud_analysis/__main__.py','bind_runtime_engine'),
            ('chessism_api/operations/matrix_constructor/storage_cli.py','bind_runtime_engine'),
            ('chessism_api/operations/exports/jobs.py','bind_runtime_engine'),
            ('chessism_api/operations/exports/cli.py','bind_runtime_engine'),
        ):
            tree = ast.parse((ROOT/path).read_text())
            calls = [node.func.id for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)]
            self.assertIn(initializer, calls, path)
            self.assertNotIn('create_async_engine', calls, path)

    def test_compose_has_no_retired_storage_switch(self):
        self.assertNotIn('CHESSISM_DATABASE_PROFILE', (ROOT/'docker-compose.yml').read_text())


class RuntimeStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_cloud_does_not_create_client_when_database_startup_is_refused(self):
        from chessism_api.operations.cloud_analysis import __main__ as daemon
        with patch.dict(os.environ,{'DATABASE_URL':'postgresql+asyncpg://unused'}), \
             patch.object(daemon,'bind_runtime_engine',new_callable=AsyncMock,side_effect=RuntimeError('unapproved')), \
             patch.object(daemon,'Client') as client:
            with self.assertRaisesRegex(RuntimeError,'unapproved'):
                await daemon.serve(SimpleNamespace(database_connect_args={'ssl':False}))
            client.assert_not_called()

    async def test_api_and_arq_use_the_same_validated_runtime_binding(self):
        with patch.object(runtime, 'bind_runtime_engine', AsyncMock()) as bind:
            result = await runtime.init_runtime_db('unused')
        bind.assert_awaited_once_with('unused')
        self.assertIs(result, bind.return_value)

    async def test_ssl_timeouts_preserved_and_conflicting_search_path_refused(self):
        engine = Mock(); engine.dispose = AsyncMock()
        options = {'ssl': False, 'server_settings': {'statement_timeout': '1000'}}
        @asynccontextmanager
        async def transaction(_factory): yield object()
        with patch.object(runtime, 'create_async_engine', return_value=engine) as create, \
             patch.object(runtime, 'read_transaction', transaction), \
             patch.object(runtime, 'driver_connection', AsyncMock()), \
             patch.object(runtime.canonical, 'validate', AsyncMock()), \
             patch.object(runtime, 'AsyncDBSession') as factory:
            self.assertIs(await runtime.bind_runtime_engine('unused', connect_args=options), engine)
            self.assertEqual(create.call_args.kwargs['connect_args'],
                {'ssl': False, 'server_settings': {'statement_timeout': '1000', 'search_path': 'public'}})
            self.assertEqual(options, {'ssl': False, 'server_settings': {'statement_timeout': '1000'}})
            factory.configure.assert_called_once_with(bind=engine, info={})
            with self.assertRaisesRegex(ValueError, 'search_path'):
                await runtime.bind_runtime_engine('unused', connect_args={'server_settings': {'search_path': 'legacy'}})
            self.assertEqual(create.call_count, 1)
