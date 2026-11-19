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
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.engine import URL
from chessism_api.database.postgresql import canonical
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import FENS
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import Fen, FenContinuation
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
    def test_tablebase_wdl_is_a_signed_numeric_code_not_text(self):
        for value in (None,-2,-1,0,1,2,2.0):
            archive.validate_record(record(tablebase_wdl=value))
        for value in ('win','2',True,0.5,3,float('nan'),float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                archive.validate_record(record(tablebase_wdl=value))

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


TEST_SOCKET = os.getenv('CHESSISM_CANONICAL_TEST_SOCKET', '')


@unittest.skipUnless(TEST_SOCKET, 'requires explicitly isolated canonical PostgreSQL')
class DatabaseTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host':self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        old = dict(AsyncDBSession.kw)
        AsyncDBSession.configure(bind=self.engine, info={})
        self.addCleanup(lambda: AsyncDBSession.configure(**old))
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.a, self.b = FENS[:2]
        for i, fen in enumerate((self.a, self.b)):
            await self.db.execute("INSERT INTO fen(fen,n_games,moves_counter,score,analyzed_at) VALUES($1,1,'{}',$2,now()-interval '1 day')", fen, i)
        await self.db.execute("INSERT INTO fen_continuation SELECT fen_id,1,'a1a2',0 FROM fen WHERE fen=$1", self.a)



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
            await session.execute(text("INSERT INTO fen(fen,n_games,score,analyzed_at) VALUES(:fen,1,3,now())"), {'fen':FENS[2]})
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
            self.assertEqual(await session.scalar(text('SELECT score FROM fen WHERE fen=:fen'),{'fen':self.a}),55)
            continuation = (await session.execute(text('SELECT * FROM fen_continuation'))).one()
            self.assertEqual((continuation.move, continuation.rank, continuation.score), ('a1a2', 1, 0))
        # Historical analyzed_at values restored above force a fresh base.
        fourth = await self.create()
        self.assertEqual(fourth['mode'], 'snapshot')
        self.assertEqual(fourth['exported_records'], 3)

    async def test_portable_backup_survives_different_numeric_ids(self):
        first = await self.create()
        old_id = None
        async with AsyncDBSession() as session, session.begin():
            old_id = await session.scalar(select(Fen.fen_id).where(Fen.fen == self.a))
            await session.execute(text('DELETE FROM fen_continuation'))
            await session.execute(text('DELETE FROM fen'))
            await session.execute(text("SELECT setval(pg_get_serial_sequence('public.fen','fen_id'),:next_id,false)"), {'next_id':old_id+1000})
            for fen in (self.a,self.b):
                await session.execute(text('INSERT INTO fen(fen,n_games) VALUES(:fen,1)'),{'fen':fen})
        records = self.effective(first['filename'])
        self.assertNotIn('fen_id', records[self.a])
        self.assertNotIn('_fen_id', records[self.a])
        self.assertNotIn('fen_id', records[self.a]['continuations'][0])
        result = await jobs.restore({}, self.root, first['filename'], AsyncMock())
        self.assertEqual(result['missing'], 0)
        async with AsyncDBSession() as session:
            continuation = (await session.execute(text('SELECT * FROM fen_continuation'))).one()
            self.assertEqual(continuation.fen_id, old_id + 1000)
            self.assertEqual((continuation.move, continuation.score), ('a1a2', 0))

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
                await session.begin()
                sizes = [len(batch) async for batch in database.export_batches(session)]
        self.assertEqual(sizes, [1, 1])


    async def test_unattributed_restore_preserves_original_metadata_and_archive_bytes(self):
        stamp = '2020-01-01T00:00:00+00:00'
        rows = [record(self.a, 24, continuations=[{'rank':2,'move':'e2e4','score':-9998}]),
                record(self.b, -9998, analysis_source='stockfish', analyzed_at=stamp)]
        path, _ = legacy(self.root, rows); original = path.read_bytes()
        result = await jobs.restore({}, self.root, path.name, AsyncMock())
        self.assertEqual((result['restored'],result['missing']), (2,0))
        self.assertEqual(path.read_bytes(),original)
        a = await self.db.fetchrow('SELECT score,analysis_source,analyzed_at FROM fen WHERE fen=$1',self.a)
        b = await self.db.fetchrow('SELECT score,analysis_source,analyzed_at FROM fen WHERE fen=$1',self.b)
        self.assertEqual(tuple(a),(24,None,None))
        self.assertEqual(tuple(b),(-9998,'stockfish',datetime.fromisoformat(stamp)))
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen_continuation'),-9998)
        from chessism_api.evaluations import score_kind
        self.assertEqual(score_kind(a['score'],a['analysis_source']),'unknown')
        self.assertEqual(score_kind(b['score'],b['analysis_source']),'mate')
        saved = await self.create()
        self.assertIsNone(self.effective(saved['filename'])[self.a]['analysis_source'])
        self.assertIsNone(self.effective(saved['filename'])[self.a]['analyzed_at'])

    async def test_missing_timestamp_is_always_included_in_incremental_export(self):
        await database.restore_batch([record(self.a, 24)])
        first = await self.create()
        original = {p['file']: (self.root/p['file']).read_bytes() for p in catalog.load(self.root,first['filename'])['files']}
        await database.restore_batch([record(self.a, 25)])
        second = await self.create()
        self.assertEqual(second['mode'],'incremental')
        self.assertEqual(second['exported_records'],1)
        self.assertEqual(self.effective(second['filename'])[self.a]['score'],25)
        self.assertIsNone(self.effective(second['filename'])[self.a]['analyzed_at'])
        third = await self.create()
        self.assertEqual(third['exported_records'],1)
        self.assertEqual({name:(self.root/name).read_bytes() for name in original},original)

    async def test_newest_duplicate_wins_and_missing_positions_are_not_created(self):
        missing = FENS[2]
        self.assertEqual(await database.restore_batch([record(self.a,4),record(missing,9),record(self.a,8)]),(2,1))
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',self.a),8)
        self.assertIsNone(await self.db.fetchval('SELECT fen FROM fen WHERE fen=$1',missing))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen_continuation'),0)

    async def test_invalid_values_never_round_or_partially_replace_existing_results(self):
        before = await self.db.fetch('SELECT * FROM fen ORDER BY fen_id')
        alternatives = await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank')
        for bad in ({'score':1.5},{'score':True},{'score':2147483648},
                    {'analysis_source':'stockfish'}, {'analysis_source':'invented'},
                    {'tablebase_wdl':1}, {'wdl_win':1000},
                    {'wdl_win':500,'wdl_draw':500,'wdl_loss':1},
                    {'continuations':[{'rank':1,'move':'e2e4','score':.5}]},
                    {'continuations':[{'rank':1,'move':'','score':0}]},
                    {'analyzed_at':'2020-01-01T00:00:00'}):
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                await database.restore_batch([record(self.a,900),record(self.b,4,**bad)] if 'score' not in bad
                    else [record(self.a,900),record(self.b,**bad)])
            self.assertEqual(await self.db.fetch('SELECT * FROM fen ORDER BY fen_id'),before)
            self.assertEqual(await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank'),alternatives)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_continuation_failure_rolls_back_score_and_previous_alternatives(self):
        before = await self.db.fetch('SELECT * FROM fen ORDER BY fen_id')
        alternatives = await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank')
        await self.db.execute("""CREATE FUNCTION reject_test_alternative() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN RAISE EXCEPTION 'synthetic alternative failure'; END $$;
            CREATE TRIGGER reject_test_alternative BEFORE INSERT ON fen_continuation
            FOR EACH ROW EXECUTE FUNCTION reject_test_alternative()""")
        with self.assertRaisesRegex(Exception,'synthetic alternative failure'):
            await database.restore_batch([record(self.a,999,continuations=[{'rank':1,'move':'e2e4','score':1}])])
        self.assertEqual(await self.db.fetch('SELECT * FROM fen ORDER BY fen_id'),before)
        self.assertEqual(await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank'),alternatives)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_late_incompatible_record_refuses_restore_before_any_batch(self):
        path,_ = legacy(self.root,[record(self.a,25)]*1001+[record(self.b,.5)])
        original = path.read_bytes()
        with patch.object(database,'restore_batch',AsyncMock()) as restore:
            with self.assertRaisesRegex(ValueError,'integer'):
                await jobs.restore({},self.root,path.name,AsyncMock())
            restore.assert_not_awaited()
        self.assertEqual(path.read_bytes(),original)

    async def test_repeated_cancellation_rolls_back_waiting_restore_and_keeps_prior_commit(self):
        await database.restore_batch([record(self.b,17)])
        before = await self.db.fetch('SELECT * FROM fen ORDER BY fen_id')
        alternatives = await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank')
        await self.db.execute("""CREATE FUNCTION wait_test_alternative() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN PERFORM pg_advisory_xact_lock(837294); RETURN NEW; END $$;
            CREATE TRIGGER wait_test_alternative BEFORE INSERT ON fen_continuation
            FOR EACH ROW EXECUTE FUNCTION wait_test_alternative()""")
        blocker = await self.connect()
        async with blocker.transaction():
            await blocker.execute('SELECT pg_advisory_xact_lock(837294)')
            task = asyncio.create_task(database.restore_batch([record(self.a,999,continuations=[{'rank':1,'move':'e2e4','score':1}])]))
            try:
                for _ in range(100):
                    waiting = await self.db.fetchval("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory')")
                    if waiting: break
                    await asyncio.sleep(.01)
                self.assertTrue(waiting,'restore must reach the continuation write before cancellation')
                task.cancel(); await asyncio.sleep(0); task.cancel()
                with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
            finally:
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertEqual(await self.db.fetch('SELECT * FROM fen ORDER BY fen_id'),before)
        self.assertEqual(await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank'),alternatives)
        self.assertEqual(self.engine.pool.checkedout(),0)


if __name__ == '__main__':
    unittest.main()
