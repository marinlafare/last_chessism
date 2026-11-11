"""Fast offline tests for the initializer's public safety boundary."""
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from chessism_api.database.postgresql import canonical
from scripts.init_canonical_db import isolated_target


class CanonicalBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_initializer_refuses_nested_transaction_before_any_query(self):
        connection = AsyncMock()
        connection.is_in_transaction = lambda: True
        with self.assertRaisesRegex(ValueError,'active transaction'):
            await canonical.initialize(connection)
        connection.execute.assert_not_awaited()

    async def test_validation_of_absent_schema_does_not_initialize(self):
        connection = AsyncMock()
        connection.fetchval.return_value = None
        with self.assertRaisesRegex(canonical.SchemaMismatch,'not been initialized'):
            await canonical.validate(connection)
        connection.execute.assert_not_awaited()

    def test_explicit_isolated_target_required(self):
        for socket,database in [('/var/run/postgresql','chessism_db'),
                                ('/tmp/chessism-canonical-a/../../var','chessism_canonical_test'),
                                ('/tmp/chessism-canonical-test','chessism_db'),
                                ('/tmp/chessism-canonical-test','chessism_canonical_x;drop database x')]:
            with self.subTest(socket=socket,database=database),self.assertRaises(ValueError):
                isolated_target(socket,database)
        self.assertEqual(isolated_target('/tmp/chessism-canonical-test/pg','chessism_canonical_test'),
                         '/tmp/chessism-canonical-test/pg')

    def test_schema_digest_changes_with_sql_artifact(self):
        original = canonical.schema_digest()
        with patch.object(canonical,'schema_sql',return_value=canonical.schema_sql()+'\n-- changed'):
            self.assertNotEqual(original,canonical.schema_digest())

    def test_schema_artifacts_do_not_import_historical_layouts(self):
        sql = canonical.schema_sql()
        for token in ['chessism_reads','chessism_archive','relational_storage_release','CREATE DATABASE','DROP TABLE']:
            self.assertNotIn(token,sql)
        self.assertTrue(Path(canonical.__file__).with_name('support.sql').is_file())


if __name__ == '__main__':
    unittest.main()
