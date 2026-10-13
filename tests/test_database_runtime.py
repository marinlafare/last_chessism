import ast
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from chessism_api.database import runtime
from chessism_api.database.relational_storage import create_relational_engine

ROOT = Path(__file__).resolve().parents[1]


class ProfileTests(unittest.TestCase):
    def test_explicit_profile_defaults_legacy_and_rejects_typos(self):
        self.assertIsNone(runtime.configured_storage({}))
        self.assertIsNone(runtime.configured_storage({runtime.PROFILE_ENV: 'legacy'}))
        self.assertEqual(runtime.configured_storage({runtime.PROFILE_ENV: 'relational_v2'}), runtime.CANONICAL)
        for value in ('', 'auto', 'RELATIONAL_V2', 'public', 'relational_v2 '):
            with self.assertRaises(ValueError):
                runtime.configured_storage({runtime.PROFILE_ENV: value})

    def test_all_compose_database_consumers_share_one_default_legacy_switch(self):
        # Inspect just the uniform two-line env declaration; Docker Compose
        # validates YAML separately, without adding a runtime YAML dependency.
        lines = (ROOT/'docker-compose.yml').read_text().splitlines()
        consumers = [i for i,line in enumerate(lines) if line.strip().startswith(('- DATABASE_URL=', 'DATABASE_URL:'))]
        for index in consumers:
            expected = ('      - CHESSISM_DATABASE_PROFILE=${CHESSISM_DATABASE_PROFILE:-legacy}'
                        if lines[index].strip().startswith('-') else
                        '      CHESSISM_DATABASE_PROFILE: ${CHESSISM_DATABASE_PROFILE:-legacy}')
            self.assertEqual(lines[index+1],expected)
        self.assertEqual(len(consumers),12)

    def test_services_use_shared_initializers(self):
        for path, initializer in (
            ('main.py','init_runtime_db'), ('worker.py','init_runtime_db'),
            ('chessism_api/operations/research_algorithms/worker.py','init_runtime_db'),
            ('chessism_api/operations/salience_backfill_cli.py','init_runtime_db'),
            ('chessism_api/operations/cloud_analysis/__main__.py','bind_runtime_engine'),
            ('chessism_api/operations/matrix_constructor/storage_cli.py','bind_runtime_engine'),
        ):
            tree = ast.parse((ROOT/path).read_text())
            calls = [node.func.id for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)]
            self.assertIn(initializer, calls, path)
            self.assertNotIn('create_async_engine', calls, path)


class RuntimeStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_initialization_passes_same_explicit_layout(self):
        with patch.dict(os.environ,{runtime.PROFILE_ENV:'relational_v2'}), \
             patch('chessism_api.database.engine.init_db',new_callable=AsyncMock) as init:
            await runtime.init_runtime_db('unused')
            init.assert_awaited_once_with('unused', relational_storage=runtime.CANONICAL)

    async def test_ssl_settings_preserved_and_conflicting_search_path_refused(self):
        engine=Mock()
        connection=AsyncMock()
        connection.scalar.side_effect=['release',True]
        engine.connect.return_value.__aenter__=AsyncMock(return_value=connection)
        engine.connect.return_value.__aexit__=AsyncMock(return_value=None)
        options={'ssl':False,'server_settings':{'statement_timeout':'1000'}}
        with patch('sqlalchemy.ext.asyncio.create_async_engine',return_value=engine) as create, \
             patch('chessism_api.database.relational_storage.require_storage_contract',new_callable=AsyncMock):
            self.assertIs(await create_relational_engine('unused',runtime.CANONICAL,connect_args=options,pool_pre_ping=True),engine)
            args=create.call_args.kwargs
            self.assertFalse(args['connect_args']['ssl'])
            self.assertEqual(args['connect_args']['server_settings'],
                {'search_path':runtime.CANONICAL.search_path,'statement_timeout':'1000'})
            self.assertEqual(options,{'ssl':False,'server_settings':{'statement_timeout':'1000'}})
            with self.assertRaises(ValueError):
                await create_relational_engine('unused',runtime.CANONICAL,connect_args={'server_settings':{'search_path':'public'}})

    async def test_cloud_does_not_create_client_when_database_startup_is_refused(self):
        from chessism_api.operations.cloud_analysis import __main__ as daemon
        with patch.dict(os.environ,{'DATABASE_URL':'postgresql+asyncpg://unused'}), \
             patch.object(daemon,'bind_runtime_engine',new_callable=AsyncMock,side_effect=RuntimeError('unapproved')), \
             patch.object(daemon,'Client') as client:
            with self.assertRaisesRegex(RuntimeError,'unapproved'):
                await daemon.serve(SimpleNamespace(database_connect_args={'ssl':False}))
            client.assert_not_called()
