"""Opt-in advisory-lock test; writes no production rows or schema."""

import os
import unittest

from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.matrix_constructor.storage import MatrixCatalogBusy, matrix_catalog_lock


@unittest.skipUnless(os.getenv("CHESSISM_DB_INTEGRATION") == "1", "opt-in database test")
class MatrixLockTests(unittest.IsolatedAsyncioTestCase):
    async def test_backup_freezes_deletion_and_releases_on_failure(self):
        old_bind = AsyncDBSession.kw.get("bind")
        engine = create_async_engine(os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://", 1))
        AsyncDBSession.configure(bind=engine)
        try:
            with self.assertRaisesRegex(RuntimeError, "simulated failure"):
                async with matrix_catalog_lock():
                    with self.assertRaises(MatrixCatalogBusy):
                        async with matrix_catalog_lock(wait=False):
                            self.fail("Concurrent deletion acquired the backup lock")
                    raise RuntimeError("simulated failure")
            async with matrix_catalog_lock(wait=False):
                pass  # No persistent lock or lease after rollback.
        finally:
            AsyncDBSession.configure(bind=old_bind)
            await engine.dispose()
