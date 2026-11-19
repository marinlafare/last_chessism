"""Canonical backup contracts on isolated PostgreSQL; no real archives or data."""
from unittest.mock import patch, AsyncMock
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.canonical import backup
from chessism_api.database.postgresql.transactions import read_transaction
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed


class CanonicalBackupTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp();await canonical.initialize(self.db);await seed(self.db)

    async def test_manifest_matches_read_only_snapshot_and_preserves_old_version_identity(self):
        async with self.db.transaction(readonly=True):
            expected=await backup.describe(self.db)
            result=await backup.validate_restored(self.db,expected)
            self.assertEqual(result['version'],'canonical_v1')
            self.assertGreater(result['sequences_checked'],5)
            with patch.object(canonical,'VERSION',canonical.VERSION+1):
                self.assertEqual(await backup.validate_restored(self.db,expected),result)
                with self.assertRaises(canonical.SchemaMismatch):await backup.describe(self.db)

    async def test_mismatched_marker_catalog_and_disabled_fk_are_rejected(self):
        expected=await backup.describe(self.db)
        for corrupt in ({},None,{**expected,'ddl_sha256':'x'},{**expected,'version':expected['version']+1}):
            with self.subTest(corrupt=corrupt),self.assertRaises(canonical.SchemaMismatch):await backup.validate_restored(self.db,corrupt)
        async with self.db.transaction():
            await self.db.execute('ALTER TABLE game DISABLE TRIGGER ALL')
            with self.assertRaises(canonical.SchemaMismatch):await backup.validate_restored(self.db,expected)

    async def test_sequence_behind_saved_ids_is_rejected_without_repair(self):
        expected=await backup.describe(self.db)
        await self.db.execute("SELECT setval(pg_get_serial_sequence('public.fen','fen_id'),1,false)")
        with self.assertRaisesRegex(canonical.SchemaMismatch,'behind stored values'):await backup.validate_restored(self.db,expected)
        self.assertFalse(await self.db.fetchval('SELECT is_called FROM fen_fen_id_seq'))

    async def test_snapshot_metadata_and_restore_dispatch_use_canonical_contract(self):
        from chessism_api.operations import database_snapshot as snapshot, database_restore_tests as restore
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,port=55432,query={'host':self.socket}))
        self.addAsyncCleanup(engine.dispose)
        factory=async_sessionmaker(engine)
        with patch('chessism_api.operations.matrix_constructor.definition_backups.definition_backup_manifest',AsyncMock(return_value={})), \
             patch('chessism_api.operations.research_algorithms.backups.algorithm_backup_manifest',AsyncMock(return_value={})), \
             patch('chessism_api.operations.matrix_backups.backup_completed_matrices',AsyncMock(return_value={})), \
             patch('chessism_api.operations.exports.backup_roles.role_manifest',AsyncMock(return_value=[])):
            async with read_transaction(factory) as session:metadata=await snapshot.snapshot_metadata(session,AsyncMock())
        self.assertEqual(metadata['schema_version'],9)
        self.assertEqual(metadata['fen_reference_version'],'canonical_v1')
        self.assertNotIn('relational_storage',metadata)
        with patch.object(restore,'RESTORE_USER','test'),patch.object(restore,'RESTORE_DATABASE',self.name):
            result=await restore._validate_fen_identity(self.socket,metadata)
            self.assertTrue(result['references']['schema_matches'])
            for changes in ({'fen_reference_version':'relational_v3'},{'fen_identity_version':'text_v1'},{'schema_version':10}):
                with self.subTest(changes=changes),self.assertRaises(RuntimeError):
                    await restore._validate_fen_identity(self.socket,{**metadata,**changes})
        self.assertEqual(engine.pool.checkedout(),0)
