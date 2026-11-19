"""Old archive validation is read-only and cannot select application storage."""
from copy import deepcopy
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from chessism_api.database.postgresql import legacy_backup_validation as legacy


class LegacyBackupValidationTests(unittest.IsolatedAsyncioTestCase):
    def expected(self):
        return dict(version='relational_v3',core='public',archive='chessism_archive',reads='chessism_reads',
            columns=[{'column_name':'fen_id','data_type':'bigint'}],constraints=[],indexes=[],views=[])

    async def test_invalid_manifest_rejected_before_opening_database(self):
        for expected in (None,{},dict(version='canonical_v1'),{**self.expected(),'core':'public; DROP DATABASE x'}):
            with self.subTest(expected=expected),patch('sqlalchemy.ext.asyncio.create_async_engine') as create:
                with self.assertRaises((ValueError,RuntimeError)):await legacy.validate_backup_contract('synthetic',expected)
                create.assert_not_called()

    async def test_matching_old_manifest_uses_read_only_connection_and_closes(self):
        engine=MagicMock();engine.dispose=AsyncMock();expected=self.expected()
        with patch('sqlalchemy.ext.asyncio.create_async_engine',return_value=engine) as create, \
             patch.object(legacy,'describe_contract',AsyncMock(return_value=expected)):
            result=await legacy.validate_backup_contract('synthetic',expected)
        self.assertEqual(result,{'version':'relational_v3','schema_matches':True})
        settings=create.call_args.kwargs['connect_args']['server_settings']
        self.assertEqual(settings['default_transaction_read_only'],'on')
        self.assertEqual(settings['search_path'],'"chessism_reads", "public", public')
        engine.dispose.assert_awaited_once()

    async def test_schema_drift_or_read_failure_is_not_success(self):
        expected=self.expected();changed=deepcopy(expected);changed['columns'][0]['data_type']='text'
        for actual in (changed,RuntimeError('unreadable restored schema')):
            engine=MagicMock();engine.dispose=AsyncMock()
            reader=AsyncMock(side_effect=actual) if isinstance(actual,Exception) else AsyncMock(return_value=actual)
            with patch('sqlalchemy.ext.asyncio.create_async_engine',return_value=engine),patch.object(legacy,'describe_contract',reader):
                with self.assertRaises(RuntimeError):await legacy.validate_backup_contract('synthetic',expected)
            engine.dispose.assert_awaited_once()
