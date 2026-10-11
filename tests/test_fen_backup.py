"""Offline archive safety plus isolated PostgreSQL export/restore regressions."""
import asyncio
from datetime import datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation
from chessism_api.operations.fen_backup import archive, catalog, database, jobs
from chessism_api.operations.fen_backup.progress import Progress
from chessism_api.operations.backup_files import atomic_write_json


def record(fen='8/8/8/8/8/8/4k3/6K1 w - -', score=1, **extra):
    return {'type': 'fen_analysis', 'fen': fen, 'score': score, 'continuations': [], **extra}


def legacy(root, rows):
    path = root / 'fen-analysis-20260101T000000_000000Z.jsonl.gz'
    with gzip.open(path, 'wt') as output:
        output.write(json.dumps({'type': 'metadata', 'format': archive.FORMAT, 'version': 2, 'records': len(rows)}) + '\n')
        for row in rows:
            output.write(json.dumps(row) + '\n')
    meta = {'filename': path.name, 'created_at': '2026-01-01T00:00:00+00:00',
            'snapshot_cutoff': '2026-01-01T00:00:00+00:00', 'records': len(rows),
            'bytes': path.stat().st_size, 'sha256': archive.sha256(path)}
    atomic_write_json(path.with_name(path.name + '.meta.json'), meta)
    return path, meta


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_segments_are_bounded_round_trip_and_unpublished_parts_are_invisible(self):
        writer = archive.SegmentWriter(self.root, part_records=2)
        rows = [record(fen=f'fen-{i}', score=i) for i in range(5)]
        writer.append(rows)
        writer.close_part()
        self.assertEqual([p['records'] for p in writer.files], [2, 2, 1])
        self.assertEqual(catalog.listing(self.root), [])
        restored = []
        for part in writer.files:
            jobs.verify_part(self.root, part)
            restored.extend(archive.records(self.root / part['file']))
        self.assertEqual(restored, rows)

    def test_legacy_read_and_checksum_failure(self):
        path, meta = legacy(self.root, [record()])
        part = catalog.source_files(self.root, meta)[0]
        jobs.verify_part(self.root, part)
        path.write_bytes(path.read_bytes() + b'bad')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            jobs.verify_part(self.root, part)

    def test_paths_and_symlinks_fail_closed(self):
        for name in ('../secret', '/etc/passwd', 'fen-analysis-parts/../secret', 'random.json'):
            with self.assertRaises(ValueError):
                archive.safe_path(self.root, name)
        path, _ = legacy(self.root, [record()])
        other = self.root / 'fen-analysis-20260102T000000_000000Z.jsonl.gz'
        other.symlink_to(path)
        with self.assertRaisesRegex(ValueError, 'symbolic'):
            archive.safe_path(self.root, other.name)

    def test_manifest_refuses_missing_parts_and_wrong_counts(self):
        path, meta = legacy(self.root, [record()])
        manifest = {'format': archive.SET_FORMAT, 'version': 3, 'records': 1,
                    'bytes': path.stat().st_size, 'files': catalog.source_files(self.root, meta)}
        archive.validate_manifest(self.root, manifest)
        with self.assertRaisesRegex(ValueError, 'record count'):
            archive.validate_manifest(self.root, {**manifest, 'records': 2})
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'Missing'):
            archive.validate_manifest(self.root, manifest)


class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_while_no_database_rows_are_available(self):
        redis = AsyncMock()
        async with Progress({'redis': redis, 'job_id': 'test'}, 'fen_analysis_backup') as progress:
            await progress.change(phase='exporting')
            await asyncio.sleep(2.05)
        payloads = [json.loads(call.args[1]) for call in redis.set.call_args_list]
        self.assertGreaterEqual(len(payloads), 3)
        self.assertGreater(payloads[-1]['elapsed_seconds'], payloads[0]['elapsed_seconds'])
        self.assertEqual(payloads[-1]['processed'], 0)


TEST_URL = os.getenv('CHESSISM_FEN_BACKUP_TEST_URL', '')


@unittest.skipUnless(TEST_URL, 'requires isolated chessism_fen_backup_test database')
class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not TEST_URL.endswith('/chessism_fen_backup_test'):
            raise RuntimeError('Refusing non-test database')
        self.schema = 'fen_backup_test_' + uuid4().hex
        self.admin = create_async_engine(TEST_URL)
        async with self.admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        self.engine = create_async_engine(TEST_URL, connect_args={'server_settings': {'search_path': self.schema}})
        AsyncDBSession.configure(bind=self.engine)
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda c: Fen.__table__.create(c))
            await conn.run_sync(lambda c: FenContinuation.__table__.create(c))
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.a, self.b = 'fen-a', 'fen-b'
        async with AsyncDBSession() as session, session.begin():
            session.add_all([Fen(fen=fen, n_games=1, moves_counter='{}', score=i,
                                 analyzed_at=datetime.now(timezone.utc) - timedelta(days=1))
                             for i, fen in enumerate((self.a, self.b))])
            await session.flush()
            session.add(FenContinuation(fen_fen=self.a, rank=1, move='a1a2', score=0))

    async def asyncTearDown(self):
        await self.engine.dispose()
        async with self.admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        await self.admin.dispose()

    async def create(self):
        return await jobs.create({'job_id': uuid4().hex}, self.root)

    def effective(self, filename):
        manifest = catalog.load(self.root, filename)
        values = {}
        for part in manifest['files']:
            values.update((row['fen'], row) for row in archive.records(self.root / part['file']))
        return values

    async def change(self, fen, score):
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(Fen).where(Fen.fen == fen).values(score=score, analyzed_at=text('now()')))

    async def test_base_delta_no_changes_and_restore_scores_continuations(self):
        first = await self.create()
        original = {p['file']: archive.sha256(self.root / p['file']) for p in catalog.load(self.root, first['filename'])['files']}
        await self.change(self.a, 55)
        async with AsyncDBSession() as session, session.begin():
            session.add(Fen(fen='fen-new', n_games=1, moves_counter='{}', score=3, analyzed_at=text('now()')))
        second = await self.create()
        self.assertEqual(second['exported_records'], 2)
        self.assertEqual({name: archive.sha256(self.root / name) for name in original}, original)
        third = await self.create()
        self.assertEqual(third['exported_records'], 0)
        self.assertEqual(third['new_bytes'], 0)
        self.assertEqual(third['parts'], second['parts'])
        await self.change(self.a, -999)
        async with AsyncDBSession() as session, session.begin():
            await session.execute(text('DELETE FROM fen_continuation'))
        refresh = AsyncMock()
        result = await jobs.restore({}, self.root, third['filename'], refresh)
        self.assertEqual(result['missing'], 0)
        refresh.assert_awaited_once()
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(Fen, self.a)).score, 55)
            continuation = (await session.scalars(select(FenContinuation))).one()
            self.assertEqual((continuation.move, continuation.rank, continuation.score), ('a1a2', 1, 0))
        # Historical analyzed_at values restored above force a fresh base.
        fourth = await self.create()
        self.assertEqual(fourth['mode'], 'snapshot')
        self.assertEqual(fourth['exported_records'], 3)

    async def test_legacy_is_retained_and_latest_values_win(self):
        path, _ = legacy(self.root, [record(self.a, -1), record(self.b, -2)])
        checksum = archive.sha256(path)
        result = await self.create()
        self.assertEqual(archive.sha256(path), checksum)
        self.assertEqual(catalog.load(self.root, result['filename'])['files'][0]['file'], path.name)
        self.assertEqual(self.effective(result['filename'])[self.a]['score'], 0)

    async def test_late_transaction_commit_is_captured_on_next_update(self):
        first = await self.create()
        pending = AsyncDBSession()
        try:
            await pending.execute(update(Fen).where(Fen.fen == self.a).values(score=88, analyzed_at=text('now()')))
            second = await self.create()
            self.assertEqual(self.effective(second['filename'])[self.a]['score'], 0)
            await pending.commit()
            third = await self.create()
            self.assertEqual(self.effective(third['filename'])[self.a]['score'], 88)
        finally:
            await pending.close()

    async def test_snapshot_keeps_scores_and_continuations_consistent(self):
        async with AsyncDBSession() as snapshot, snapshot.begin():
            await snapshot.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await snapshot.execute(text('SELECT 1'))
            async with AsyncDBSession() as writer, writer.begin():
                await writer.execute(update(Fen).where(Fen.fen == self.a).values(score=9, analyzed_at=text('now()')))
                await writer.execute(update(FenContinuation).values(score=9))
            rows = [row async for batch in database.export_batches(snapshot) for row in batch]
            a = next(row for row in rows if row['fen'] == self.a)
            self.assertEqual((a['score'], a['continuations'][0]['score']), (0, 0))

    async def test_failure_before_publication_keeps_last_backup_and_retry_succeeds(self):
        first = await self.create()
        await self.change(self.a, 44)
        with patch.object(archive, 'publish', side_effect=OSError('interrupted')):
            with self.assertRaisesRegex(OSError, 'interrupted'):
                await self.create()
        self.assertEqual(catalog.listing(self.root)[0]['filename'], first['filename'])
        self.assertEqual(self.effective(first['filename'])[self.a]['score'], 0)
        retry = await self.create()
        self.assertEqual(self.effective(retry['filename'])[self.a]['score'], 44)

    async def test_corrupt_part_refuses_restore_before_any_database_writes(self):
        result = await self.create()
        part = catalog.load(self.root, result['filename'])['files'][0]
        path = self.root / part['file']
        raw = path.read_bytes()
        path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
        with patch.object(database, 'restore_batch', new_callable=AsyncMock) as restore:
            with self.assertRaisesRegex(ValueError, 'checksum'):
                await jobs.restore({}, self.root, result['filename'], AsyncMock())
            restore.assert_not_awaited()

    async def test_batched_export_does_not_accumulate_all_results(self):
        with patch.object(database, 'PAGE_SIZE', 1):
            async with AsyncDBSession() as session:
                sizes = [len(batch) async for batch in database.export_batches(session)]
        self.assertEqual(sizes, [1, 1])


if __name__ == '__main__':
    unittest.main()
