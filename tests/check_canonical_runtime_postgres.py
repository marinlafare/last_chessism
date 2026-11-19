"""Runtime admission against private PostgreSQL; startup never initializes it."""
import asyncio
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from chessism_api.database.postgresql import canonical, runtime
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalRuntimeTests(CanonicalDatabaseTestCase):
    def url(self):
        return URL.create('postgresql+asyncpg', username='test', database=self.name,
                          port=55432, query={'host': self.socket})

    async def test_valid_schema_binds_with_no_profile_and_preserves_schema_and_rows(self):
        await canonical.initialize(self.db)
        await self.db.execute("INSERT INTO player(player_name) VALUES('retained')")
        before = await canonical.catalog_digest(self.db)
        factory = async_sessionmaker(expire_on_commit=False)
        with patch.object(runtime, 'AsyncDBSession', factory):
            engine = await runtime.init_runtime_db(self.url())
            self.addAsyncCleanup(engine.dispose)
            async with factory() as session:
                self.assertEqual(await session.scalar(text('SELECT player_name FROM player')), 'retained')
                self.assertEqual(await session.scalar(text('SHOW search_path')), 'public')
                self.assertEqual(session.info, {})
        self.assertEqual(before, await canonical.catalog_digest(self.db))
        self.assertEqual(engine.pool.checkedout(), 0)

    async def test_empty_database_is_rejected_without_creating_objects(self):
        engine = create_async_engine(self.url())
        with patch.object(runtime, 'create_async_engine', return_value=engine), patch.object(runtime, 'AsyncDBSession') as factory:
            with self.assertRaises(canonical.SchemaMismatch): await runtime.bind_runtime_engine(self.url())
            factory.configure.assert_not_called()
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM pg_tables WHERE schemaname='public'"), 0)
        self.assertEqual(engine.pool.checkedout(), 0)

    async def test_old_version_or_disabled_foreign_keys_never_bind_or_repair(self):
        await canonical.initialize(self.db)
        for mutation in ('UPDATE canonical_schema_version SET version=1', 'ALTER TABLE game DISABLE TRIGGER ALL'):
            await self.db.execute(mutation)
            before = await canonical.catalog_digest(self.db)
            with patch.object(runtime, 'AsyncDBSession') as factory:
                with self.assertRaises(canonical.SchemaMismatch): await runtime.bind_runtime_engine(self.url())
                factory.configure.assert_not_called()
            self.assertEqual(before, await canonical.catalog_digest(self.db))
            await self.db.execute('UPDATE canonical_schema_version SET version=$1', canonical.VERSION)
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM pg_trigger WHERE tgrelid='game'::regclass AND tgenabled='D'") > 0, True)

    async def test_repeated_cancellation_finishes_disposal_before_propagating(self):
        await canonical.initialize(self.db)
        engine = create_async_engine(self.url()); actual_dispose = engine.dispose
        validating, disposing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def blocked_validation(_connection):
            validating.set()
            await asyncio.Event().wait()
        async def blocked_disposal(_engine, close=True):
            disposing.set()
            await release.wait()
            await actual_dispose()
        with patch.object(runtime, 'create_async_engine', return_value=engine), \
             patch.object(runtime, 'AsyncDBSession') as factory, \
             patch.object(runtime.canonical, 'validate', blocked_validation), \
             patch.object(type(engine), 'dispose', blocked_disposal):
            task = asyncio.create_task(runtime.bind_runtime_engine(self.url()))
            try:
                await asyncio.wait_for(validating.wait(), 5); task.cancel()
                await asyncio.wait_for(disposing.wait(), 5)
                for _ in range(3):
                    task.cancel(); await asyncio.sleep(0)
                    self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError): await asyncio.wait_for(task, 5)
                factory.configure.assert_not_called()
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                await actual_dispose()
        self.assertEqual(engine.pool.checkedout(), 0)
