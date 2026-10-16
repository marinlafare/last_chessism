"""Requires an isolated PostgreSQL test DB; never runs against the app DB."""
import os
import unittest
from uuid import uuid4
from unittest.mock import AsyncMock

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
import asyncpg
from chessism_api.database.fen_identity import validate_identity
from chessism_api.database.fen_identity_migration import migrate, compare_continuations

URL = os.getenv('CHESSISM_FEN_ID_TEST_URL', '')


@unittest.skipUnless(URL, 'requires isolated chessism_fen_id_test database')
class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not URL.endswith('/chessism_fen_id_test'):
            raise RuntimeError('Refusing non-test database')
        self.conn = await asyncpg.connect(URL)
        self.schema = 'fen_identity_test_' + uuid4().hex
        await self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        await self.conn.execute(f'SET search_path="{self.schema}"')
        await self.conn.execute("""
          CREATE TABLE fen (fen VARCHAR PRIMARY KEY, score DOUBLE PRECISION);
          CREATE TABLE fen_continuation (id SERIAL PRIMARY KEY, fen_fen VARCHAR NOT NULL REFERENCES fen(fen),
            rank INTEGER NOT NULL, move VARCHAR NOT NULL, score DOUBLE PRECISION NOT NULL, UNIQUE(fen_fen,rank));
          INSERT INTO fen VALUES ('position-a',12.5),('position-b',-19),('position-c',NULL);
          INSERT INTO fen_continuation (id,fen_fen,rank,move,score)
          VALUES (10,'position-a',2,'e2e4',12.5),(11,'position-a',3,'g1f3',-10000),(15,'position-b',2,'d7d5',-19);
        """)

    async def asyncTearDown(self):
        await self.conn.execute('SET search_path=public')
        await self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        await self.conn.close()

    async def test_exact_copy_keeps_old_table_sequences_constraints_and_is_idempotent(self):
        self.assertEqual((await validate_identity(self.conn, allow_legacy=True))['version'], 'text_v1')
        report = await migrate(self.conn, AsyncMock())
        self.assertEqual(report['comparison'], {'original_rows': 3, 'numeric_rows': 3, 'mismatches': 0})
        self.assertEqual(await self.conn.fetchval('SELECT count(*) FROM fen_continuation_legacy_text'), 3)
        self.assertEqual(await self.conn.fetchval('SELECT score FROM fen WHERE fen=$1', 'position-a'), 12.5)
        identifier = await self.conn.fetchval("INSERT INTO fen (fen) VALUES ('new') RETURNING fen_id")
        self.assertGreater(identifier, report['identity']['fen']['maximum_id'])
        row_id = await self.conn.fetchval("INSERT INTO fen_continuation (fen_id,rank,move,score) VALUES ($1,2,'a1a2',0) RETURNING id", identifier)
        self.assertGreater(row_id, 15)
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.conn.execute("INSERT INTO fen_continuation (fen_id,rank,move,score) VALUES ($1,2,'a1b1',1)", identifier)
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.conn.execute("INSERT INTO fen_continuation (fen_id,rank,move,score) VALUES (999,2,'a1b1',1)")
        self.assertTrue((await migrate(self.conn, AsyncMock()))['already_migrated'])

    async def test_interruption_keeps_text_table_active_and_retry_uses_existing_ids(self):
        async def fail(phase, **kwargs):
            if phase == 'indexing_numeric_continuations':
                raise RuntimeError('injected interruption')
        with self.assertRaisesRegex(RuntimeError, 'injected'):
            await migrate(self.conn, fail)
        self.assertEqual(await self.conn.fetchval('SELECT count(*) FROM fen_continuation WHERE fen_fen IS NOT NULL'), 3)
        self.assertIsNone(await self.conn.fetchval("SELECT to_regclass('fen_continuation_numeric_v2')"))
        ids = await self.conn.fetch('SELECT fen,fen_id FROM fen ORDER BY fen')
        await migrate(self.conn, AsyncMock())
        self.assertEqual(ids, await self.conn.fetch('SELECT fen,fen_id FROM fen ORDER BY fen'))

    async def test_empty_database_and_sequence_validation(self):
        await self.conn.execute('DELETE FROM fen_continuation; DELETE FROM fen')
        report = await migrate(self.conn, AsyncMock())
        self.assertEqual(report['fen_count'], 0)
        self.assertEqual(await self.conn.fetchval("INSERT INTO fen (fen) VALUES ('new') RETURNING fen_id"), 1)
        await self.conn.execute("SELECT setval(pg_get_serial_sequence('fen','fen_id'),1,false)")
        with self.assertRaisesRegex(RuntimeError, 'sequence'):
            await validate_identity(self.conn)

    async def test_unexpected_dependencies_fail_closed(self):
        await self.conn.execute('CREATE VIEW continuation_report AS SELECT * FROM fen_continuation')
        with self.assertRaisesRegex(RuntimeError, 'dependencies'):
            await migrate(self.conn, AsyncMock())
        self.assertEqual(await self.conn.fetchval('SELECT count(*) FROM fen_continuation'), 3)

    async def test_comparison_detects_corruption(self):
        await migrate(self.conn, AsyncMock())
        await self.conn.execute('UPDATE fen_continuation SET score=999 WHERE id=10')
        with self.assertRaisesRegex(RuntimeError, 'verification failed'):
            await compare_continuations(self.conn, 'fen_continuation_legacy_text', 'fen_continuation')

    async def test_restore_refuses_broken_child_sequence(self):
        await migrate(self.conn, AsyncMock())
        await self.conn.execute("SELECT setval(pg_get_serial_sequence('fen_continuation','id'),1,false)")
        with self.assertRaisesRegex(RuntimeError, 'sequence'):
            await validate_identity(self.conn)


if __name__ == '__main__': unittest.main()
