"""Canonical bounded FEN workflow against an explicitly isolated PostgreSQL."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, game_imports, game_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations.ingestion_pipeline import fen_batches, fen_repository, fen_orchestrator
from chessism_api.operations.ingestion_pipeline.fen_bulk import persist_positions
from chessism_api.operations.ingestion_pipeline.fen_core import process_single_game_sync, aggregate_fen_data
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.check_canonical_imports_postgres import bundle


class CanonicalFenTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        for module in (fen_batches, fen_repository):
            patcher = patch.object(module, 'AsyncDBSession', self.factory)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.children = []
        self.addAsyncCleanup(self.drain)

    async def drain(self):
        for task in self.children:
            if not task.done():
                task.cancel()
        if self.children:
            await asyncio.gather(*self.children, return_exceptions=True)

    async def populate(self, repeated=False):
        games, moves, openings = bundle()
        if repeated:
            games[0]['n_moves'] = 4
            template = moves[0]
            moves = [m for m in moves if m['link'] != 101]
            moves += [{**template, 'n_move': i, 'white_move': w, 'black_move': b}
                for i, (w, b) in enumerate([('Nf3','Nf6'),('Ng1','Ng8')]*2, 1)]
            openings = [o for o in openings if o['link'] != 101]
        async with self.db.transaction():
            await game_imports.ingest_games(self.db, games, moves, openings)
        return moves

    async def positions(self, link=101):
        async with self.factory() as session:
            moves = await fen_repository.read_moves_for_games(session, [link])
        rows, errors = process_single_game_sync((link, moves[link]))
        self.assertEqual(errors, [])
        return aggregate_fen_data(rows)

    async def save(self, fens, links):
        async with write_transaction(self.factory) as session:
            return await persist_positions(session, fens, links)

    async def state(self):
        return [tuple(row) for row in await self.db.fetch(
            'SELECT link,fens_done,fens_processing FROM game ORDER BY link')]

    async def pipeline(self, count=2, workers=2):
        redis = AsyncMock()
        async def enqueue(name, **kwargs):
            self.assertEqual(name, 'run_fen_batch_job')
            self.assertEqual(kwargs.pop('_queue_name'), 'fen_queue')
            task = asyncio.create_task(fen_batches.run_fen_batch_job(
                {'redis': redis, 'job_id': f'child-{len(self.children)}'}, **kwargs))
            self.children.append(task)
            async def result(**_kwargs):
                return await task
            return SimpleNamespace(result=result)
        redis.enqueue_job.side_effect = enqueue
        return await fen_batches.run_bounded_pipeline(
            {'redis': redis, 'job_id': 'coordinator'}, count, 1, workers)

    async def test_empty_database_is_exhaustion_without_queueing(self):
        self.assertEqual(await fen_repository.count_pending_games(), 0)
        self.assertEqual(await fen_batches.run_fen_batch_job({}, 10), {
            'successful_game_links': [], 'failed_game_links': [],
            'positions': 0, 'new_positions': 0, 'new_links': 0})
        redis = AsyncMock()
        result = await fen_orchestrator._run_fen_pipeline({'redis': redis}, 10, 100, 3)
        self.assertEqual(result, {'claimed_games': 0, 'successful_games': 0, 'failed_games': 0})
        redis.enqueue_job.assert_not_awaited()

    async def test_claims_use_eligibility_stable_order_and_skip_locked(self):
        await self.populate()
        self.assertEqual(await fen_repository.count_pending_games(), 2)
        async with write_transaction(self.factory) as first:
            self.assertEqual(await fen_repository.claim_games_needing_fens(first, 1), [101])
            async with write_transaction(self.factory) as second:
                self.assertEqual(await fen_repository.claim_games_needing_fens(second, 10), [105])
                await fen_repository.set_game_fen_state(second, [105], processing=True)
            await fen_repository.set_game_fen_state(first, [101], processing=True)
        async with write_transaction(self.factory) as session:
            self.assertEqual(await fen_repository.claim_games_needing_fens(session, 10), [])
        self.assertEqual(await fen_repository.release_fen_processing_claims([105]), 1)
        async with write_transaction(self.factory) as session:
            self.assertEqual(await fen_repository.claim_games_needing_fens(session, 10), [105])
        self.assertEqual((await self.state())[0], (101, False, True))

    async def test_selection_failure_propagates_and_never_returns_empty_success(self):
        await self.db.execute('ALTER TABLE game RENAME TO unavailable_game')
        for operation in (fen_repository.count_pending_games,
                          lambda: fen_batches.run_fen_batch_job({}, 1)):
            with self.assertRaisesRegex(Exception, 'game.*does not exist'):
                await operation()

    async def test_repeated_positions_use_numeric_keys_and_exact_occurrence_counts(self):
        await self.populate(repeated=True)
        fens, links = await self.positions()
        self.assertEqual(await self.save(fens, links), {'new_positions': 4, 'new_links': 8})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association'), 8)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen'), 4)
        self.assertTrue(await self.db.fetchval('SELECT bool_and(n_games=2) FROM fen'))
        self.assertEqual(await self.db.fetchval('SELECT n_positions FROM database_summary'), 4)
        self.assertEqual(await self.db.fetchval('SELECT unscored_fens FROM scored_position_summary'), 4)
        self.assertEqual(await self.save(fens, links), {'new_positions': 0, 'new_links': 0})
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 8)

    async def test_concurrent_identical_retries_do_not_duplicate_links_or_counters(self):
        await self.populate()
        fens, links = await self.positions()
        results = await asyncio.wait_for(asyncio.gather(self.save(fens, links), self.save(fens, links)), 10)
        self.assertEqual(sum(r['new_positions'] for r in results), 2)
        self.assertEqual(sum(r['new_links'] for r in results), 2)
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 2)
        self.assertEqual(await self.db.fetchval('SELECT n_positions FROM database_summary'), 2)

    async def test_existing_evaluation_claims_and_continuations_survive_new_occurrences(self):
        await self.populate()
        fens, links = await self.positions()
        await self.save(fens, links)
        chosen = fens[0]['fen']
        await self.db.execute('''UPDATE fen SET score=25,next_moves='e7e5',analysis_source='stockfish',
            analyzed_at=now(),wdl_win=300,wdl_draw=500,wdl_loss=200 WHERE fen=$1''', chosen)
        await self.db.execute('''INSERT INTO fen_continuation SELECT fen_id,1,'e7e5',25 FROM fen WHERE fen=$1;
        ''', chosen)
        await self.db.execute("""INSERT INTO cloud_analysis_job(id,status,target,imported) VALUES('job','running',1,0);
            INSERT INTO cloud_analysis_run(id,job_id,status) VALUES('run','job','running')""")
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run')", chosen)
        before = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', chosen)
        fens, links = await self.positions(105)
        self.assertEqual(await self.save(fens, links), {'new_positions': 0, 'new_links': 2})
        after = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', chosen)
        self.assertEqual({**dict(before), 'n_games': 2}, dict(after))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'), 1)
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen_continuation'), 25)
        self.assertEqual(await self.db.fetchval('SELECT equal_appearances FROM scored_position_summary'), 1)

    async def test_conflicting_or_out_of_batch_associations_roll_back(self):
        await self.populate()
        fens, links = await self.positions()
        await self.save(fens, links)
        for invalid in ({**links[0], 'fen_fen': fens[1]['fen']},
                        {**links[0], 'fen_fen': 'missing'},
                        {**links[0], 'move_color': 'unknown'}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                await self.save(fens, [invalid])
        with self.assertRaises(ValueError):
            await self.save(fens[:1], [links[-1]])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association'), 2)
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 2)

    async def test_missing_summary_rolls_back_fens_links_counts_and_claims(self):
        await self.populate()
        await self.db.execute('DELETE FROM scored_position_summary')
        with self.assertRaisesRegex(RuntimeError, 'Missing scored_position_summary'):
            await fen_batches.run_fen_batch_job({}, 2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen'), 0)
        self.assertEqual(await self.db.fetchval('SELECT n_positions FROM database_summary'), 0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game WHERE fens_processing'), 0)

    async def test_failure_after_copy_rolls_back_only_uncommitted_batch(self):
        await self.populate()
        committed = await fen_batches.run_fen_batch_job({}, 1)
        self.assertEqual(committed['successful_game_links'], [101])
        async def fail(*args):
            await persist_positions(*args)
            raise RuntimeError('after COPY')
        with patch.object(fen_batches, 'persist_positions', fail), self.assertRaisesRegex(RuntimeError, 'after COPY'):
            await fen_batches.run_fen_batch_job({}, 1)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association'), 2)
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 2)
        self.assertEqual((await self.state())[-1], (105, False, False))

    async def test_cancellation_after_writes_rolls_back_and_releases_locks(self):
        await self.populate()
        written = asyncio.Event()
        async def interrupted(*args):
            await persist_positions(*args)
            written.set()
            await asyncio.Event().wait()
        with patch.object(fen_batches, 'persist_positions', interrupted):
            task = asyncio.create_task(fen_batches.run_fen_batch_job({}, 1))
            self.children.append(task)
            await asyncio.wait_for(written.wait(), 5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen'), 0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game WHERE fens_processing'), 0)
        async with self.db.transaction():
            self.assertEqual(await self.db.fetchval('SELECT link FROM game WHERE link=101 FOR UPDATE NOWAIT'), 101)

    async def test_rolled_back_contention_retries_without_counting_twice(self):
        await self.populate()
        calls = 0
        class SerializationFailure(Exception):
            sqlstate = '40001'
        async def fail_once(*args):
            nonlocal calls
            calls += 1
            result = await persist_positions(*args)
            if calls == 1:
                raise SerializationFailure('synthetic contention after writes')
            return result
        with patch.object(fen_batches, 'persist_positions', fail_once):
            result = await fen_batches.run_fen_batch_job({}, 1)
        self.assertEqual(calls, 2)
        self.assertEqual((result['positions'],result['new_positions'],result['new_links']), (2,2,2))
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 2)

    async def test_invalid_san_discards_partial_positions_and_does_not_hide_other_games(self):
        await self.populate()
        await self.db.execute("UPDATE moves SET black_move='invalid' WHERE link=101")
        result = await self.pipeline()
        self.assertEqual(result, {'claimed_games':2,'successful_games':1,'failed_games':1,'successful_game_links':[105]})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association WHERE game_link=101'), 0)
        self.assertEqual((await self.state())[0], (101,False,False))
        self.assertEqual((await self.state())[-1], (105,True,False))

    async def test_application_workers_finalize_coverage_states_and_pipeline_totals(self):
        await self.populate()
        result = await self.pipeline()
        self.assertEqual(result, {'claimed_games':2,'successful_games':2,'failed_games':0,'successful_game_links':[101,105]})
        self.assertEqual(await fen_repository.count_pending_games(), 0)
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 4)
        rows = await self.db.fetch('SELECT link,total_positions,unscored_positions,is_fully_analyzed FROM game_analysis_coverage ORDER BY link')
        self.assertEqual([tuple(r) for r in rows], [(101,2,2,False),(105,2,2,False)])
        totals = await self.db.fetchrow('SELECT parsed_games,analyzable_games,excluded_games,fen_extracted_games FROM fen_pipeline_summary')
        self.assertEqual(tuple(totals), (2,2,0,2))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game WHERE fens_processing'), 0)

    async def test_failed_finalization_preserves_committed_positions_and_allows_exact_retry(self):
        await self.populate()
        original = game_coverage.refresh_pipeline_summary
        async def fail(connection):
            await original(connection)
            raise RuntimeError('after final summary')
        with patch.object(fen_batches, 'refresh_pipeline_summary', fail), self.assertRaisesRegex(RuntimeError, 'after final summary'):
            await self.pipeline()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_analysis_coverage'), 0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game WHERE fens_done OR fens_processing'), 0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association'), 4)
        self.assertEqual(await self.db.fetchval('SELECT fen_extracted_games FROM fen_pipeline_summary'), 0)
        await self.pipeline()
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'), 4)
        self.assertEqual(await self.db.fetchval('SELECT n_positions FROM database_summary'), 2)
        self.assertEqual(await self.db.fetchval('SELECT fen_extracted_games FROM fen_pipeline_summary'), 2)

    async def test_coverage_replay_empty_scope_and_score_buckets(self):
        await self.populate()
        fens, links = await self.positions()
        await self.save(fens, links)
        await self.db.execute("UPDATE fen SET score=CASE WHEN fen=$1 THEN 25 ELSE -9000 END, analysis_source='stockfish',analyzed_at=now()", fens[0]['fen'])
        async with write_transaction(self.factory) as session:
            self.assertEqual(await game_coverage.refresh_coverage(session, []), [])
            self.assertEqual(await game_coverage.refresh_coverage(session, fens=[]), [])
            first = await game_coverage.refresh_coverage(session, [101,105])
            self.assertEqual([dict(r) for r in first], [{'link':101,'is_fully_analyzed':True}])
            self.assertEqual(await game_coverage.refresh_coverage(session, fens=[fens[0]['fen']]), first)
        row = await self.db.fetchrow('SELECT total_positions,analyzed_positions,unscored_positions,equal_positions,mate_positions FROM game_analysis_coverage')
        self.assertEqual(tuple(row), (2,2,0,1,1))
        async with write_transaction(self.factory) as session:
            self.assertEqual(await game_coverage.refresh_coverage(session), first)
        await self.db.execute('DELETE FROM game_fen_association WHERE game_link=101')
        async with write_transaction(self.factory) as session:
            self.assertEqual(await game_coverage.refresh_coverage(session, [101]), [])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_analysis_coverage'), 0)

    async def test_occurrence_repair_counts_composite_associations(self):
        await self.populate(repeated=True)
        fens, links = await self.positions()
        await self.save(fens, links)
        await self.db.execute('UPDATE fen SET n_games=999')
        await fen_repository.refresh_fen_occurrence_counts([row['fen'] for row in fens])
        self.assertTrue(await self.db.fetchval('SELECT bool_and(n_games=2) FROM fen'))

    async def test_persistence_and_coverage_require_caller_transaction(self):
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await persist_positions(session, [], [])
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await game_coverage.refresh_coverage(session, [])
        with self.assertRaisesRegex(ValueError, 'caller-owned'):
            await game_coverage.refresh_pipeline_summary(self.db)

    async def test_waiting_coverage_refresh_reads_committed_scores_after_game_lock(self):
        await self.populate()
        fens, links = await self.positions()
        await self.save(fens, links)
        pid_ready = asyncio.Event()
        waiter_pid = None
        async def refresh_waiter():
            nonlocal waiter_pid
            async with write_transaction(self.factory) as session:
                waiter_pid = await session.scalar(text('SELECT pg_backend_pid()'))
                pid_ready.set()
                return await game_coverage.refresh_coverage(session, [101])
        async def wait_for_lock():
            while await self.db.fetchval(
                    'SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1', waiter_pid) != 'Lock':
                await asyncio.sleep(0.01)
        async with write_transaction(self.factory) as first:
            await first.execute(text("UPDATE fen SET score=0,analysis_source='stockfish',analyzed_at=now()"))
            await game_coverage.refresh_coverage(first, [101])
            waiter = asyncio.create_task(refresh_waiter())
            self.children.append(waiter)
            await asyncio.wait_for(pid_ready.wait(), 2)
            await asyncio.wait_for(wait_for_lock(), 2)
            self.assertFalse(waiter.done())
        rows = await asyncio.wait_for(waiter, 5)
        self.assertEqual([dict(row) for row in rows], [{'link':101,'is_fully_analyzed':True}])
        self.assertEqual(await self.db.fetchval('SELECT analyzed_positions FROM game_analysis_coverage'), 2)
