"""Opt-in recipe checks; writes use temporary tables and always roll back."""

import os
from types import SimpleNamespace
import unittest
import uuid

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.matrix_definitions import import_snapshot_definitions
from chessism_api.database.models import MatrixArtifact, MatrixDefinition
from chessism_api.operations.matrix_constructor.definition_backups import (
    definition_backup_manifest, validate_definition_backup,
)
from chessism_api.operations.matrix_constructor.live_preview import preview_definition
from chessism_api.routers.matrices import definitions as routes
from tests.test_matrix_definitions import CONFIG


@unittest.skipUnless(os.getenv("CHESSISM_DB_INTEGRATION") == "1", "opt-in database test")
class DefinitionDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.old_bind = AsyncDBSession.kw.get("bind")
        self.old_join = AsyncDBSession.kw.get("join_transaction_mode", "conditional_savepoint")
        self.engine = create_async_engine(os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://", 1))

    async def asyncTearDown(self):
        AsyncDBSession.configure(bind=self.old_bind, join_transaction_mode=self.old_join)
        await self.engine.dispose()

    async def test_recipe_migration_crud_and_restore_fingerprint(self):
        async with self.engine.connect() as connection:
            transaction = await connection.begin()
            try:
                for name in ("matrix_artifact", "matrix_definition"):
                    await connection.execute(text(
                        f"CREATE TEMPORARY TABLE {name} (LIKE public.{name} INCLUDING ALL) ON COMMIT DROP"
                    ))
                AsyncDBSession.configure(bind=connection, join_transaction_mode="create_savepoint")
                artifact_id = str(uuid.uuid4())
                await connection.execute(MatrixArtifact.__table__.insert().values(
                    id=artifact_id, name=CONFIG["name"], row_type=CONFIG["row_type"],
                    status="complete", config=CONFIG,
                ))
                await import_snapshot_definitions(connection)
                await import_snapshot_definitions(connection)
                self.assertEqual(await connection.scalar(select(func.count()).select_from(MatrixDefinition)), 1)
                saved = await routes.save_matrix_definition(routes.MatrixRequest(**CONFIG), SimpleNamespace(id=None))
                self.assertEqual(saved["status"], "saved")
                self.assertIsNotNone(saved["created_at"])
                async with AsyncDBSession() as session:
                    manifest = await definition_backup_manifest(session)
                    # The same serialization is used when inspecting restored PostgreSQL.
                    restored = await session.scalar(text("""
                        SELECT COALESCE(json_agg(row_to_json(d)), '[]'::json)
                        FROM (SELECT id, name, row_type, config FROM matrix_definition ORDER BY id) d
                    """))
                    self.assertEqual(validate_definition_backup(manifest, restored)["count"], 2)
                await routes.delete_matrix_definition(uuid.UUID(artifact_id))
                self.assertEqual(await connection.scalar(select(func.count()).select_from(MatrixDefinition)), 1)
                self.assertEqual(await connection.scalar(select(func.count()).select_from(MatrixArtifact)), 1)
            finally:
                await transaction.rollback()

    async def test_live_preview_reads_only_a_small_source_sample(self):
        AsyncDBSession.configure(bind=self.engine)
        preview = await preview_definition(CONFIG, limit=5)
        self.assertGreater(len(preview["rows"]), 0)
        self.assertLessEqual(len(preview["rows"]), 5)
        self.assertFalse(preview["materialized"])
        self.assertEqual(len(preview["columns"]), 3)

    async def test_column_projection_preserves_source_scope_and_row_keys(self):
        AsyncDBSession.configure(bind=self.engine)
        full = await preview_definition(CONFIG, limit=3)
        features = await preview_definition(CONFIG, limit=3, role="features")
        labels = await preview_definition(CONFIG, limit=3, role="labels")
        self.assertEqual(full["row_keys"], features["row_keys"])
        self.assertEqual(full["row_keys"], labels["row_keys"])
        self.assertEqual(full["rows"], [left + right for left, right in zip(features["rows"], labels["rows"])])
        empty = await preview_definition({**CONFIG, "label_columns": []}, limit=3, role="labels")
        self.assertEqual(empty["row_keys"], full["row_keys"])
        self.assertEqual(empty["rows"], [[], [], []])
