import unittest
from unittest.mock import AsyncMock, patch


class ReferenceWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_restore_accepts_old_backups_and_verifies_new_marker(self):
        from chessism_api.operations.database_restore_tests import _validate_fen_identity
        connection = AsyncMock()
        with patch('chessism_api.operations.database_restore_tests.asyncpg.connect', AsyncMock(return_value=connection)), \
             patch('chessism_api.database.postgresql.fen_identity.validate_identity', AsyncMock(return_value={'version':'numeric_v2'})), \
             patch('chessism_api.database.postgresql.fen_reference_identity.validate_references', AsyncMock(return_value={'version':'text_v1'})) as references:
            result = await _validate_fen_identity('/unused', {'schema_version':5,'fen_identity_version':'numeric_v2'})
            self.assertEqual(result['references']['version'],'text_v1')
            self.assertTrue(references.await_args.kwargs['allow_legacy'])
            with self.assertRaisesRegex(RuntimeError,'differ'):
                await _validate_fen_identity('/unused', {'schema_version':6,'fen_identity_version':'numeric_v2','fen_reference_version':'numeric_v1'})
            with self.assertRaisesRegex(RuntimeError,'manifest'):
                await _validate_fen_identity('/unused', {'schema_version':6,'fen_identity_version':'numeric_v2'})
            references.return_value = {'version':'numeric_v1'}
            result = await _validate_fen_identity('/unused', {'schema_version':6,'fen_identity_version':'numeric_v2','fen_reference_version':'numeric_v1'})
            self.assertEqual(result['references']['version'],'numeric_v1')
            self.assertFalse(references.await_args.kwargs['allow_legacy'])
