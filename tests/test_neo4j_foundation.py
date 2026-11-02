from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from chessism_api.database.neo4j.config import GraphConfig
from chessism_api.database.neo4j.manage import create_secret
from chessism_api.database.neo4j.runtime import check_connection, connect


class GraphConfigTests(unittest.TestCase):
    def test_explicit_configuration_and_credentials_required(self):
        with self.assertRaises(ValueError):
            GraphConfig.from_env({})
        for uri in ('http://neo4j:7474', 'bolt://user:password@neo4j', 'bolt://neo4j?password=secret'):
            with self.assertRaises(ValueError):
                GraphConfig.from_env({'CHESSISM_NEO4J_URI': uri, 'CHESSISM_NEO4J_AUTH_FILE': '/unused'})
        config = GraphConfig.from_env({'CHESSISM_NEO4J_URI': 'bolt://neo4j:7687', 'CHESSISM_NEO4J_AUTH_FILE': '/unused'})
        self.assertEqual(config.database, 'neo4j')
        with self.assertRaises(ValueError):
            config.credentials()

    def test_secret_is_private_random_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path = create_secret(Path(directory) / 'private' / 'auth')
            config = GraphConfig('bolt://neo4j:7687', 'neo4j', path)
            username, password = config.credentials()
            self.assertEqual(username, 'neo4j')
            self.assertGreaterEqual(len(password), 32)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            original = path.read_bytes()
            with self.assertRaises(FileExistsError):
                create_secret(path)
            self.assertEqual(path.read_bytes(), original)
            other = create_secret(Path(directory) / 'other')
            self.assertNotEqual(path.read_bytes(), other.read_bytes())

    def test_invalid_secret_never_echoed_in_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'auth'
            path.write_text('secret-that-must-not-be-logged')
            with self.assertRaises(ValueError) as failure:
                GraphConfig('bolt://neo4j:7687', 'neo4j', path).credentials()
            self.assertNotIn('secret-that-must-not-be-logged', str(failure.exception))


class GraphRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_connection_failure_closes_driver(self):
        driver = AsyncMock()
        driver.verify_connectivity.side_effect = RuntimeError('unavailable')
        with patch.object(GraphConfig, 'credentials', return_value=('neo4j', 'test-password')), \
                patch('neo4j.AsyncGraphDatabase.driver', return_value=driver) as create:
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                async with connect(GraphConfig('bolt://neo4j:7687', 'neo4j', Path('/unused'))):
                    self.fail('Unavailable connection must not be returned')
        driver.close.assert_awaited_once()
        self.assertEqual(create.call_args.kwargs['max_connection_pool_size'], 4)
        self.assertTrue(create.call_args.kwargs['telemetry_disabled'])

    async def test_read_only_check_never_creates_schema(self):
        driver, session, result = AsyncMock(), AsyncMock(), AsyncMock()
        result.single.return_value = {'ok': 1}
        session.run.return_value = result
        driver.session = Mock()
        driver.session.return_value.__aenter__ = AsyncMock(return_value=session)
        driver.session.return_value.__aexit__ = AsyncMock(return_value=None)
        with patch.object(GraphConfig, 'credentials', return_value=('neo4j', 'test-password')), \
                patch('neo4j.AsyncGraphDatabase.driver', return_value=driver):
            status = await check_connection(GraphConfig('bolt://neo4j:7687', 'neo4j', Path('/unused')))
        self.assertEqual(status['status'], 'ready')
        driver.session.assert_called_once_with(database='neo4j', default_access_mode='READ')
        self.assertEqual(session.run.call_args.args[0].text, 'RETURN 1 AS ok')
        driver.close.assert_awaited_once()
