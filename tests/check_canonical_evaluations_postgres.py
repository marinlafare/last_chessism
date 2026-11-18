"""Canonical publication and real receipt transactions; synthetic engine output only."""
import asyncio
import hashlib
from uuid import uuid4
from unittest.mock import patch

import asyncpg
import chess
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.evaluation_results import stage_fen_results
from chessism_api.database.postgresql.models import CloudAnalysisRun
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations.cloud_analysis import importer
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS
from cloud_job.launch import configuration, expected_contract
from stockfish_batch.checkpoints import BatchCheckpoints, encode


def result(fen=FENS[0], **values):
    return {'fen': fen, 'score': 24, 'next_moves': None, **values}


def continuation(fen=FENS[0], **values):
    return {'fen_fen': fen, 'rank': 2, 'move': 'd2d4', 'score': 10, **values}


def cloud_result(position, multipv, score=0):
    """Synthetic nonterminal output obeying the real worker's MultiPV contract."""
    moves = sorted(move.uci() for move in chess.Board(position['fen']).legal_moves)
    return {'fen': position['fen'], 'is_valid': True, 'analysis': [
        {'multipv': rank, 'nodes': 100, 'score': score - rank + 1,
         'wdl': [200, 700, 100], 'pv': [move]}
        for rank, move in enumerate(moves[:multipv], 1)]}


class EvaluationCase(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        await self.db.execute('''UPDATE database_summary SET n_positions=5,unscored_fens=5 WHERE id=1;
            UPDATE scored_position_summary SET total_positions=5,unscored_fens=5 WHERE id=1''')
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.tasks = []
        self.addAsyncCleanup(self.stop_tasks)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    def start(self, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        return task

    async def snapshot(self):
        return (await self.db.fetch('SELECT * FROM fen ORDER BY fen'),
                await self.db.fetch('SELECT * FROM fen_continuation ORDER BY fen_id,rank'),
                await self.db.fetch('SELECT * FROM database_summary'),
                await self.db.fetch('SELECT * FROM scored_position_summary'))

    async def publish(self, rows, moves=(), *, count_new=True):
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session, rows, moves, count_new=count_new)


class CanonicalEvaluationTests(EvaluationCase):
    async def test_requires_existing_transaction_and_empty_batch_does_no_work(self):
        async with self.factory() as session:
            await stage_fen_results(session, [], [])
            self.assertFalse(session.in_transaction())
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await stage_fen_results(session, [result()], [])
            self.assertFalse(session.in_transaction())

    async def test_scores_alternatives_and_totals_commit_together(self):
        before = await self.snapshot()
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session, [result(score=0.0)], [continuation(score=10.0)], count_new=True)
            self.assertTrue(session.in_transaction())
            self.assertEqual(await self.snapshot(), before)
        saved = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', FENS[0])
        self.assertEqual((saved['score'], saved['analysis_source'], saved['next_moves']), (0, 'stockfish', None))
        self.assertIsNotNone(saved['analyzed_at'])
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen_continuation'), 10)
        self.assertEqual(tuple(await self.db.fetchrow('''SELECT analyzed_fens,unscored_fens,
            scored_fens,nonzero_scored_fens FROM database_summary''')), (1, 4, 0, 0))

    async def test_application_failure_rolls_back_complete_unit_and_allows_retry(self):
        before = await self.snapshot()
        with self.assertRaisesRegex(RuntimeError, 'before commit'):
            async with write_transaction(self.factory) as session:
                await stage_fen_results(session, [result()], [continuation()], count_new=True)
                raise RuntimeError('before commit')
        self.assertEqual(await self.snapshot(), before)
        await self.publish([result()], [continuation()])
        committed = await self.snapshot()
        with self.assertRaisesRegex(ValueError, 'already scored'):
            await self.publish([result(score=999)], [])
        self.assertEqual(await self.snapshot(), committed)

    async def test_whole_batch_validation_and_missing_identity_preserve_prior_commits(self):
        await self.publish([result(FENS[4], score=-12)])
        before = await self.snapshot()
        invalid = [([result(), result()], []), ([result(), result(FENS[1], score=1.5)], []),
            ([result(), result('absent w - -')], []), ([result(piece_count=3.5)], []),
            ([result()], [continuation(fen=FENS[1])]), ([result()], [continuation(score=1.5)]),
            ([result()], [continuation(), continuation()]), ([result()], [continuation(move='')]),
            ([result(wdl_win=1, wdl_draw=2, wdl_loss=3)], [])]
        for rows, moves in invalid:
            with self.subTest(rows=rows, moves=moves), self.assertRaises(ValueError):
                await self.publish(rows, moves)
            self.assertEqual(await self.snapshot(), before)
        with self.assertRaisesRegex(ValueError, 'disappeared'):
            await self.publish([result(), result('absent w - -')], count_new=False)
        self.assertEqual(await self.snapshot(), before)

    async def test_missing_totals_rolls_back_scores_and_continuations(self):
        await self.db.execute('DELETE FROM database_summary')
        before = await self.snapshot()
        with self.assertRaisesRegex(ValueError, 'totals are missing'):
            await self.publish([result()], [continuation()])
        self.assertEqual(await self.snapshot(), before)

    async def test_continuation_failure_rolls_back_scores_and_old_alternatives(self):
        await self.publish([result()], [continuation()])
        before = await self.snapshot()
        await self.db.execute('''CREATE FUNCTION reject_alternative() RETURNS trigger LANGUAGE plpgsql
            AS $$ BEGIN RAISE EXCEPTION 'injected continuation failure'; END $$;
            CREATE TRIGGER reject_alternative BEFORE INSERT ON fen_continuation
            FOR EACH STATEMENT EXECUTE FUNCTION reject_alternative()''')
        with self.assertRaisesRegex(asyncpg.RaiseError, 'injected continuation'):
            await self.publish([result(score=99)], [continuation(score=88)], count_new=False)
        self.assertEqual(await self.snapshot(), before)

    async def test_reviewed_replacement_replaces_alternatives_without_incrementing_new_counts(self):
        await self.publish([result()], [continuation(), continuation(rank=3, move='g1f3')])
        totals = (await self.snapshot())[2]
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session, [result(score=-9998)], [continuation(score=-9999)])
            await stage_fen_results(session, [result(FENS[1], score=0)], [])
        self.assertEqual((await self.snapshot())[2], totals)
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1', FENS[0]), -9998)
        self.assertEqual([tuple(r) for r in await self.db.fetch('SELECT rank,score FROM fen_continuation')], [(2, -9999)])

    async def test_existing_piece_count_and_exact_wdl_are_preserved_and_tablebase_fields_cleared(self):
        await self.db.execute('''UPDATE fen SET piece_count=32,score=0,analysis_source='tablebase',
            tablebase_wdl=0,tablebase_dtz=0,analyzed_at=now() WHERE fen=$1''', FENS[0])
        await self.publish([result(piece_count=31, wdl_win=200.0, wdl_draw=700, wdl_loss=100)], count_new=False)
        row = await self.db.fetchrow('''SELECT piece_count,score,wdl_win,wdl_draw,wdl_loss,
            analysis_source,tablebase_wdl,tablebase_dtz FROM fen WHERE fen=$1''', FENS[0])
        self.assertEqual(tuple(row), (32, 24, 200, 700, 100, 'stockfish', None, None))

    async def test_concurrent_publishers_cannot_overwrite_or_double_count(self):
        async with write_transaction(self.factory) as first:
            await stage_fen_results(first, [result()], [], count_new=True)
            pid = await first.scalar(text('SELECT pg_backend_pid()'))
            second = self.start(self.publish([result(score=-99)]))
            async def blocked():
                while not await self.db.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                    WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))''', pid):
                    if second.done():
                        second.result()
                        self.fail('Second publisher did not wait for the first transaction')
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(blocked(), 2)
            self.assertFalse(second.done())
        with self.assertRaisesRegex(ValueError, 'already scored'):
            await asyncio.wait_for(second, 2)
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1', FENS[0]), 24)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 1)

    async def test_bounded_statements_include_typed_all_null_columns(self):
        rows = [result(f'synthetic-{index} w - -', score=index) for index in range(1001)]
        await self.db.executemany('INSERT INTO fen(fen) VALUES($1)', [(r['fen'],) for r in rows])
        await self.db.execute('''UPDATE database_summary SET unscored_fens=1006;
            UPDATE scored_position_summary SET unscored_fens=1006;
            CREATE TABLE evaluation_statements(n integer);
            CREATE FUNCTION count_evaluation_statement() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO evaluation_statements SELECT count(*) FROM changed; RETURN NULL; END $$;
            CREATE TRIGGER count_evaluations AFTER UPDATE ON fen REFERENCING NEW TABLE AS changed
            FOR EACH STATEMENT EXECUTE FUNCTION count_evaluation_statement()''')
        await self.publish(rows)
        self.assertEqual(sorted(r['n'] for r in await self.db.fetch('SELECT n FROM evaluation_statements')), [1, 1000])
        self.assertEqual(tuple(await self.db.fetchrow('''SELECT analyzed_fens,unscored_fens,
            nonzero_scored_fens FROM database_summary''')), (1001, 5, 1000))


class CanonicalCloudReceiptTests(EvaluationCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        patcher = patch.object(importer, 'AsyncDBSession', self.factory)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.run_id = uuid4().hex
        self.positions = [{'id': hashlib.sha256(fen.encode()).hexdigest(), 'fen': fen} for fen in FENS[:2]]
        config = configuration(self.run_id, 2, 1000000, 3600)
        self.contract = expected_contract(b''.join(encode(row) for row in self.positions), self.positions,
            config, {'engine_sha': 'a' * 64, 'worker_version': '1.4.0', 'chess_version': '1.11.2'})
        checkpoint = BatchCheckpoints(None, '', self.contract, self.positions, 500)
        records = [checkpoint.prepare(row, cloud_result(row, self.contract['settings']['multipv']), 1)
                   for row in self.positions]
        self.raw = encode({'fingerprint': self.contract['fingerprint'], 'records': records})
        async with write_transaction(self.factory) as session:
            session.add(CloudAnalysisRun(id=self.run_id, job_id='job-a', status='running',
                positions=self.positions, contract=self.contract, launch={}, receipts={}))
        await self.db.execute('INSERT INTO cloud_fen_claim(fen,run_id) SELECT unnest($1::text[]),$2',
                              list(FENS[:2]), self.run_id)

    async def receipt_snapshot(self):
        return (await self.snapshot(), await self.db.fetch('SELECT * FROM cloud_fen_claim ORDER BY fen'),
                await self.db.fetch('SELECT * FROM cloud_result_batch'),
                await self.db.fetchval("SELECT imported FROM cloud_analysis_job WHERE id='job-a'"),
                await self.db.fetchval('SELECT imported_count FROM cloud_analysis_run WHERE id=$1', self.run_id),
                await self.db.fetch('SELECT * FROM cloud_control_document ORDER BY id'))

    async def test_cloud_receipt_counts_claim_release_and_replay_are_atomic(self):
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0), 2)
        saved = await self.receipt_snapshot()
        self.assertEqual(saved[1], [])
        self.assertEqual(len(saved[2]), 1)
        self.assertEqual(saved[3:5], (2, 2))
        self.assertEqual(saved[2][0]['sha256'], hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 2)
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0), 0)
        self.assertEqual(await self.receipt_snapshot(), saved)
        with self.assertRaisesRegex(ValueError, 'checkpoint has changed'):
            await importer.import_batch(self.run_id, self.raw + b' ', 0)
        self.assertEqual(await self.receipt_snapshot(), saved)

    async def test_failed_cloud_publication_keeps_claims_and_no_receipt(self):
        before = await self.receipt_snapshot()
        original = importer.stage_fen_results
        async def fail_after_writes(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError('before receipt commit')
        with patch.object(importer, 'stage_fen_results', fail_after_writes):
            with self.assertRaisesRegex(RuntimeError, 'before receipt'):
                await importer.import_batch(self.run_id, self.raw, 0)
        self.assertEqual(await self.receipt_snapshot(), before)
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0), 2)

    async def test_receipt_insert_failure_rolls_back_scores_claims_and_counters(self):
        before = await self.receipt_snapshot()
        await self.db.execute('''CREATE FUNCTION reject_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected receipt failure'; END $$;
            CREATE TRIGGER reject_receipt BEFORE INSERT ON cloud_result_batch
            FOR EACH STATEMENT EXECUTE FUNCTION reject_receipt()''')
        with self.assertRaisesRegex(Exception, 'injected receipt failure'):
            await importer.import_batch(self.run_id, self.raw, 0)
        self.assertEqual(await self.receipt_snapshot(), before)

    async def test_missing_or_wrong_claim_refuses_publication(self):
        await self.db.execute('UPDATE cloud_fen_claim SET run_id=\'run-b\' WHERE fen=$1', FENS[0])
        before = await self.receipt_snapshot()
        with self.assertRaisesRegex(ValueError, 'Reserved FENs changed'):
            await importer.import_batch(self.run_id, self.raw, 0)
        self.assertEqual(await self.receipt_snapshot(), before)

    async def test_lost_acknowledgment_after_commit_retries_from_durable_receipt(self):
        created = []
        def factory():
            session = self.factory()
            created.append(session)
            if len(created) == 2:
                async def unavailable(*args, **kwargs):
                    raise OSError('timing write unavailable after result commit')
                session.execute = unavailable
            return session
        with patch.object(importer, 'AsyncDBSession', factory):
            with self.assertRaisesRegex(OSError, 'after result commit'):
                await importer.import_batch(self.run_id, self.raw, 0)
        committed = await self.receipt_snapshot()
        self.assertEqual(committed[3:5], (2, 2))
        self.assertEqual(committed[1], [])
        self.assertIsNone(committed[2][0]['transaction_seconds'])
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0), 0)
        self.assertEqual(await self.receipt_snapshot(), committed)

    async def test_concurrent_receipt_retries_commit_only_once(self):
        results = await asyncio.wait_for(asyncio.gather(
            importer.import_batch(self.run_id, self.raw, 0),
            importer.import_batch(self.run_id, self.raw, 0)), 3)
        self.assertEqual(sorted(results), [0, 2])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_result_batch'), 1)
        self.assertEqual(await self.db.fetchval("SELECT imported FROM cloud_analysis_job WHERE id='job-a'"), 2)

    async def test_prevalidated_batch_rechecks_authoritative_input_and_remains_reusable(self):
        from chessism_api.operations.cloud_analysis.batch_validation import prepare_batch
        prepared = prepare_batch(self.raw, self.positions, self.contract)
        before = await self.receipt_snapshot()
        with self.assertRaisesRegex(ValueError, 'Prevalidated batch'):
            await importer.import_batch(self.run_id, self.raw + b' ', 0, _prepared=prepared)
        self.assertEqual(await self.receipt_snapshot(), before)
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0, _prepared=prepared), 2)
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0, _prepared=prepared), 0)

    async def test_invalid_bytes_and_checkpoint_indices_leave_all_state_intact(self):
        before = await self.receipt_snapshot()
        for raw, index in [(b'not json', 0), (b'{}', 0), (self.raw, -1), (self.raw, 1)]:
            with self.subTest(raw=raw[:20], index=index), self.assertRaises((ValueError, KeyError)):
                await importer.import_batch(self.run_id, raw, index)
            self.assertEqual(await self.receipt_snapshot(), before)

    async def test_task_receipts_import_out_of_order_without_losing_prior_results(self):
        tasks, payloads = [], []
        for index, position in enumerate(self.positions):
            contract = expected_contract(encode(position), [position], configuration(self.run_id, 1, 1000000, 3600),
                {'engine_sha': 'a' * 64, 'worker_version': '1.4.0', 'chess_version': '1.11.2'})
            checkpoint = BatchCheckpoints(None, '', contract, [position], 500)
            record = checkpoint.prepare(position, cloud_result(position, contract['settings']['multipv'], index), 1)
            payloads.append(encode({'fingerprint': contract['fingerprint'], 'records': [record]}))
            tasks.append({'index': index, 'start': index, 'count': 1, 'contract': contract})
        async with write_transaction(self.factory) as session:
            run = await session.get(CloudAnalysisRun, self.run_id)
            run.launch = {'tasks': tasks}
        self.assertEqual(await importer.import_batch(self.run_id, payloads[1], 0, task_index=1), 1)
        first_commit = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', FENS[1])
        self.assertEqual(await importer.import_batch(self.run_id, payloads[1], 0, task_index=1), 0)
        self.assertEqual(await importer.import_batch(self.run_id, payloads[0], 0, task_index=0), 1)
        self.assertEqual(await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', FENS[1]), first_commit)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_result_batch'), 2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'), 0)
        self.assertEqual(await self.db.fetchval("SELECT imported FROM cloud_analysis_job WHERE id='job-a'"), 2)

    async def test_repeated_cancellation_finishes_real_rollback_and_session_close(self):
        before = await self.receipt_snapshot()
        for phase, rollback_failure in [('rollback', False), ('close', False), ('close', True)]:
            with self.subTest(phase=phase, rollback_failure=rollback_failure):
                staged, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
                closed = []
                original = importer.stage_fen_results
                async def stage_then_pause(*args, **kwargs):
                    await original(*args, **kwargs)
                    staged.set()
                    await asyncio.Future()
                def factory():
                    session = self.factory()
                    rollback, close = session.rollback, session.close
                    async def rollback_then_fail():
                        if phase == 'rollback':
                            cleaning.set()
                            await release.wait()
                        if rollback_failure:
                            raise OSError('injected rollback error')
                        await rollback()
                    async def close_then_record():
                        if phase == 'close':
                            cleaning.set()
                            await release.wait()
                        await close()
                        closed.append(True)
                    session.rollback, session.close = rollback_then_fail, close_then_record
                    return session
                with patch.object(importer, 'stage_fen_results', stage_then_pause), \
                     patch.object(importer, 'AsyncDBSession', factory):
                    task = self.start(importer.import_batch(self.run_id, self.raw, 0))
                    await asyncio.wait_for(staged.wait(), 2)
                    task.cancel()
                    await asyncio.wait_for(cleaning.wait(), 2)
                    try:
                        for _ in range(2):
                            task.cancel()
                            await asyncio.sleep(0)
                            self.assertFalse(task.done())
                        self.assertEqual(closed, [])
                    finally:
                        release.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task, 2)
                self.assertEqual(closed, [True])
                self.assertEqual(await self.receipt_snapshot(), before)
                # A new connection can acquire the rows after all cleanup ends.
                await self.db.fetch('SELECT fen FROM fen ORDER BY fen FOR UPDATE NOWAIT')
        self.assertEqual(await importer.import_batch(self.run_id, self.raw, 0), 2)
