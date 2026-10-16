import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch

os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.fen_references import association_rows
from chessism_api.database.fen_reference_identity import require_numeric_reference_schema,schema_version
from chessism_api.database.models import GameFenAssociation,PlayerPositionFrequency
from chessism_api.database.db_interface import DBInterface


class ReferenceWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_restore_accepts_old_backups_and_verifies_new_marker(self):
        from chessism_api.operations.database_restore_tests import _validate_fen_identity
        connection = AsyncMock()
        with patch('chessism_api.operations.database_restore_tests.asyncpg.connect', AsyncMock(return_value=connection)), \
             patch('chessism_api.database.fen_identity.validate_identity', AsyncMock(return_value={'version':'numeric_v2'})), \
             patch('chessism_api.database.fen_reference_identity.validate_references', AsyncMock(return_value={'version':'text_v1'})) as references:
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

    async def test_portable_input_becomes_numeric_without_mutating_payload(self):
        session=AsyncMock()
        rows=[{'game_link':10,'fen_fen':'one','n_move':1,'move_color':'white'}]
        with patch('chessism_api.database.fen_references.resolve_fen_ids',AsyncMock(return_value={'one':7})):
            prepared=await association_rows(session,rows)
            await DBInterface(GameFenAssociation).create_all_with_session(session,rows)
        self.assertEqual(prepared[0]['fen_id'],7)
        self.assertNotIn('fen_fen',prepared[0])
        self.assertEqual(rows[0]['fen_fen'],'one')
        self.assertEqual(session.execute.await_args.args[1],prepared)
        self.assertIn('fen_id',str(session.execute.await_args.args[0]))
        self.assertNotIn('fen_fen',str(session.execute.await_args.args[0]))

    async def test_missing_mismatched_or_empty_identity_fails_before_insert(self):
        session=AsyncMock()
        for row in ({'fen_fen':'missing'},{'fen_fen':'one','fen_id':8},{}):
            with patch('chessism_api.database.fen_references.resolve_fen_ids',AsyncMock(return_value={'one':7})):
                with self.assertRaises(ValueError): await association_rows(session,[row])
        session.execute.assert_not_awaited()

    async def test_startup_rejects_legacy_or_half_migration(self):
        tables=('game_fen_association','player_position_frequency')
        legacy=[{'table_name':t,'column_name':'fen_fen'} for t in tables]
        numeric=[{'table_name':t,'column_name':'fen_id','data_type':'bigint','not_null':True} for t in tables]
        session=AsyncMock()
        for rows in (legacy,legacy[:1]+numeric[1:]):
            session.execute.return_value=SimpleNamespace(mappings=lambda:SimpleNamespace(all=lambda:rows))
            with self.assertRaisesRegex(RuntimeError,'explicit'): await require_numeric_reference_schema(session)
        self.assertEqual(schema_version(numeric),'numeric_v1')
        self.assertEqual(schema_version([]),'empty')
        for model in (GameFenAssociation,PlayerPositionFrequency):
            self.assertIn('fen_id',model.__table__.columns)
            self.assertNotIn('fen_fen',model.__table__.columns)
