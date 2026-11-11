"""Real canonical tablebase persistence with synthetic probes, never Syzygy files."""
import asyncio
from contextlib import asynccontextmanager, ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, tablebase_repository as repository
from chessism_api.database.postgresql.evaluation_results import increment_new_fen_counts
from chessism_api.database.postgresql.analysis_selection import release_analysis_session
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import tablebase
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS

ENDGAMES = (*FENS[3:], '8/8/3k4/8/8/8/2R5/3K4 w - -', '8/8/3k4/8/8/8/2R5/3K4 b - -')


def solved(fen=ENDGAMES[0], **values):
    return {'fen': fen, 'piece_count': 3, 'score': 0.0, 'next_moves': None,
            'wdl_win': 0.0, 'wdl_draw': 1000.0, 'wdl_loss': 0.0,
            'tablebase_wdl': 0, 'tablebase_dtz': 0, **values}


class CanonicalTablebaseTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        await seed(self.db)
        await self.db.executemany('INSERT INTO fen(fen,n_games) VALUES($1,$2)', [(ENDGAMES[2], 50), (ENDGAMES[3], 40)])
        self.ids = {r['fen']: r['fen_id'] for r in await self.db.fetch('SELECT fen,fen_id FROM fen')}
        await self.db.execute('''DELETE FROM game_fen_association;
            UPDATE database_summary SET n_positions=7,unscored_fens=7''')
        for game, fen, move, white in [(101, ENDGAMES[1], 0, True), (101, ENDGAMES[1], 1, False),
                                     (101, ENDGAMES[0], 2, True), (105, ENDGAMES[0], 0, True)]:
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)', game, self.ids[fen], move, white)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        patcher = patch.object(repository, 'AsyncDBSession', self.factory)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.leases, self.tasks = [], []
        self.addAsyncCleanup(self.release_resources)

    async def release_resources(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        for session in self.leases:
            await session.close()

    def start(self, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        return task

    async def lease(self, links=None, limit=20, **kwargs):
        session, rows = await repository.lease_tablebase_batch(links, limit, **kwargs)
        if session is not None:
            self.leases.append(session)
        return session, rows

    async def wait_for_lock(self, task, blocker_pid):
        async def blocked():
            while not await self.db.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))''', blocker_pid):
                if task.done():
                    task.result()
                    self.fail('Expected tablebase lease to wait for the row lock')
                await asyncio.sleep(.01)
        await asyncio.wait_for(blocked(), 2)

    async def snapshot(self):
        return (await self.db.fetch('SELECT * FROM fen ORDER BY fen'),
                await self.db.fetch('SELECT * FROM database_summary'),
                await self.db.fetch('SELECT * FROM cloud_fen_claim ORDER BY fen'))

    async def publish(self, rows, unavailable=()):
        async with write_transaction(self.factory) as session:
            await repository.stage_tablebase_results(session, rows, unavailable)
            await increment_new_fen_counts(session, rows)

    async def test_discovery_counts_scope_duplicates_empty_and_numeric_spool(self):
        self.assertEqual(await repository.count_tablebase_candidates(), 4)
        self.assertEqual(await repository.count_tablebase_candidates([101, 101, 105]), 2)
        self.assertEqual(await repository.count_tablebase_candidates([]), 0)
        self.assertFalse(await repository.has_tablebase_candidates([]))
        for links, limit, expected in [([101, 101, 105], None, 2), (None, 1, 1), ([], None, 0), (None, 0, 0)]:
            spool, advance = repository.CandidateSpool(), AsyncMock()
            try:
                await spool.discover(links, limit, advance)
                self.assertEqual(spool.count, expected)
                ids = spool.read(10)
                self.assertEqual(len(set(ids)), expected)
                self.assertTrue(set(ids) <= {self.ids[f] for f in ENDGAMES})
                self.assertEqual(spool.read(10), [])
                self.assertEqual(sum(c.args[0] for c in advance.await_args_list), expected)
                self.assertEqual(self.engine.pool.checkedout(), 0)
            finally:
                spool.close()
            self.assertTrue(spool.file.closed)

    async def test_all_candidate_paths_exclude_scores_claims_and_unavailable(self):
        await self.publish([solved()], [{'fen': ENDGAMES[2], 'piece_count': 3}])
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", ENDGAMES[1])
        self.assertEqual(await repository.count_tablebase_candidates(), 1)
        self.assertTrue(await repository.has_tablebase_candidates())
        self.assertFalse(await repository.has_tablebase_candidates([101, 105]))
        rows = await repository.read_tablebase_batch(list(self.ids.values()))
        self.assertEqual([r['fen'] for r in rows], [ENDGAMES[3]])
        session, rows = await self.lease()
        self.assertEqual([r['fen'] for r in rows], [ENDGAMES[3]])
        await release_analysis_session(session)
        self.assertIsNone(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1', ENDGAMES[2]))
        # Unavailable is still a legitimate Stockfish candidate, never a scored result.
        from chessism_api.database.postgresql import analysis_selection
        with patch.object(analysis_selection, 'AsyncDBSession', self.factory):
            session, fens = await analysis_selection.get_fens_for_analysis(20)
            self.leases.append(session)
            self.assertIn(ENDGAMES[2], fens)

    async def test_global_order_skip_locked_and_rollback_release(self):
        first, rows = await self.lease(limit=1)
        self.assertEqual([r['fen'] for r in rows], [ENDGAMES[0]])
        self.assertTrue(first.in_transaction())
        second, rest = await self.lease()
        self.assertEqual([r['fen'] for r in rest], list(ENDGAMES[1:]))
        await release_analysis_session(first)
        await release_analysis_session(second)
        again, rows = await self.lease(limit=1)
        self.assertEqual([r['fen'] for r in rows], [ENDGAMES[0]])

    async def test_scoped_order_deduplicates_repeated_occurrences(self):
        session, rows = await self.lease([101, 101])
        self.assertEqual([r['fen'] for r in rows], [ENDGAMES[1], ENDGAMES[0]])
        self.assertEqual(await self.lease([]), (None, []))

    async def test_read_releases_locks_before_probe_and_rechecks_later_score_or_claim(self):
        for kind in ('score', 'claim'):
            fen = ENDGAMES[2] if kind == 'score' else ENDGAMES[3]
            rows = await repository.read_tablebase_batch([self.ids[fen]])
            self.assertEqual(rows, [{'fen': fen, 'piece_count': 3}])
            self.assertEqual(self.engine.pool.checkedout(), 0)
            async with self.db.transaction():
                await self.db.fetch('SELECT fen FROM fen WHERE fen=$1 FOR UPDATE NOWAIT', fen)
                if kind == 'score':
                    await self.db.execute("UPDATE fen SET score=71,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1", fen)
                else:
                    await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", fen)
            self.assertEqual(await self.lease(candidate_ids=[self.ids[fen]]), (None, []))

    async def test_claim_committed_during_lock_wait_is_rejected_by_fresh_snapshot(self):
        fen = ENDGAMES[0]
        observed = []
        original = repository.unclaimed_locked_fens
        async def recheck(session, fens):
            observed.append(list(fens))
            return await original(session, fens)
        with patch.object(repository, 'unclaimed_locked_fens', recheck):
            async with write_transaction(self.factory) as reserver:
                await reserver.execute(text('SELECT fen FROM fen WHERE fen=:fen FOR UPDATE'), {'fen': fen})
                pid = await reserver.scalar(text('SELECT pg_backend_pid()'))
                waiting = self.start(self.lease(candidate_ids=[self.ids[fen]]))
                await self.wait_for_lock(waiting, pid)
                await reserver.execute(text("INSERT INTO cloud_fen_claim(fen,run_id) VALUES(:fen,'run-a')"), {'fen': fen})
            self.assertEqual(await asyncio.wait_for(waiting, 2), (None, []))
        self.assertEqual(observed, [[fen]])

    async def test_database_errors_propagate_and_close_sessions(self):
        await self.db.execute('ALTER TABLE fen RENAME TO missing_fen')
        sessions = []
        def factory():
            session = self.factory()
            session.close = AsyncMock(wraps=session.close)
            sessions.append(session)
            return session
        with patch.object(repository, 'AsyncDBSession', factory):
            for operation in (lambda: repository.count_tablebase_candidates(),
                              lambda: repository.has_tablebase_candidates(),
                              lambda: repository.read_tablebase_batch([1]),
                              lambda: repository.lease_tablebase_batch(None, 1)):
                with self.assertRaises(DBAPIError):
                    await operation()
        self.assertEqual(len(sessions), 4)
        for session in sessions:
            session.close.assert_awaited_once()
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_mixed_publication_and_counts_are_atomic_and_unavailable_is_unscored(self):
        before = await self.snapshot()
        rows, unavailable = [solved()], [{'fen': ENDGAMES[1], 'piece_count': 2}]
        async with write_transaction(self.factory) as session:
            await repository.stage_tablebase_results(session, rows, unavailable)
            await increment_new_fen_counts(session, rows)
            self.assertEqual(await self.snapshot(), before)
        saved = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', ENDGAMES[0])
        self.assertEqual((saved['score'], saved['analysis_source'], saved['wdl_draw'], saved['tablebase_dtz']), (0, 'tablebase', 1000, 0))
        self.assertIsNotNone(saved['analyzed_at'])
        missing = await self.db.fetchrow('SELECT * FROM fen WHERE fen=$1', ENDGAMES[1])
        self.assertEqual((missing['score'], missing['analysis_source'], missing['analyzed_at']), (None, 'tablebase_unavailable', None))
        self.assertEqual(tuple(await self.db.fetchrow('SELECT analyzed_fens,unscored_fens,nonzero_scored_fens FROM database_summary')), (1, 6, 0))

    async def test_validation_missing_scored_and_claimed_rows_cannot_partially_publish(self):
        await self.publish([solved(ENDGAMES[3])])
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", ENDGAMES[2])
        before = await self.snapshot()
        batches = [([solved(), solved()], []), ([solved(score=1.5)], []),
            ([solved()], [{'fen': ENDGAMES[0], 'piece_count': 3}]),
            ([], [{'fen': ENDGAMES[1], 'piece_count': 2}] * 2),
            ([solved(), solved('absent w - -')], []), ([solved(), solved(ENDGAMES[3])], []),
            ([solved(), solved(ENDGAMES[2])], []), ([solved(piece_count=6)], [])]
        for rows, unavailable in batches:
            with self.subTest(rows=rows, unavailable=unavailable), self.assertRaises(ValueError):
                await self.publish(rows, unavailable)
            self.assertEqual(await self.snapshot(), before)

    async def test_requires_existing_transaction_and_empty_is_noop(self):
        async with self.factory() as session:
            await repository.stage_tablebase_results(session, [], [])
            self.assertFalse(session.in_transaction())
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await repository.stage_tablebase_results(session, [solved()], [])
            self.assertFalse(session.in_transaction())

    async def test_partial_update_or_missing_totals_rolls_back_the_whole_batch(self):
        await self.db.execute('''CREATE FUNCTION skip_one_evaluation() RETURNS trigger LANGUAGE plpgsql
            AS $$ BEGIN IF NEW.fen LIKE '8/8/%' THEN RETURN NULL; END IF; RETURN NEW; END $$;
            CREATE TRIGGER skip_one BEFORE UPDATE ON fen FOR EACH ROW EXECUTE FUNCTION skip_one_evaluation()''')
        before = await self.snapshot()
        with self.assertRaisesRegex(ValueError, 'partial batch'):
            await self.publish([solved(), solved(ENDGAMES[1], piece_count=2)])
        self.assertEqual(await self.snapshot(), before)
        await self.db.execute('DROP TRIGGER skip_one ON fen; DELETE FROM database_summary')
        before = await self.snapshot()
        with self.assertRaisesRegex(ValueError, 'totals are missing'):
            await self.publish([solved()])
        self.assertEqual(await self.snapshot(), before)

    async def test_temp_input_can_be_reused_and_failed_application_transaction_retried(self):
        before = await self.snapshot()
        with self.assertRaisesRegex(RuntimeError, 'before commit'):
            async with write_transaction(self.factory) as session:
                await repository.stage_tablebase_results(session, [solved()], [])
                await repository.stage_tablebase_results(session, [], [{'fen': ENDGAMES[1], 'piece_count': 2}])
                raise RuntimeError('before commit')
        self.assertEqual(await self.snapshot(), before)
        await self.publish([solved()], [{'fen': ENDGAMES[1], 'piece_count': 2}])

    async def test_application_probes_without_database_connections_and_preserves_result_contract(self):
        async def probe(rows):
            self.assertEqual(self.engine.pool.checkedout(), 0)
            return ([solved(r['fen'], piece_count=r['piece_count']) for r in rows if r['fen'] != ENDGAMES[1]],
                    [r for r in rows if r['fen'] == ENDGAMES[1]])
        prober = SimpleNamespace(workers=2, probe=AsyncMock(side_effect=probe), close=AsyncMock())
        with patch.object(tablebase.os.path, 'isdir', return_value=True), \
             patch.object(tablebase, 'TablebaseProber', return_value=prober), \
             patch.object(tablebase, 'increment_scored_position_summary_for_scored_fens', AsyncMock()), \
             patch.object(tablebase, 'increment_game_analysis_summary_for_scored_fens', AsyncMock()):
            result = await tablebase.analyze_tablebase_positions({}, batch_size=2, refresh_projections=False)
        self.assertEqual(result, {'processed': 4, 'solved': 3, 'unavailable': 1, 'target': 4, 'skipped': 0})
        prober.close.assert_awaited_once_with(abort=False)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 3)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_reporting_failure_after_lease_releases_actual_database_locks(self):
        before = await self.snapshot()
        class Reporter:
            advance = AsyncMock()
            def goal(self, *args): pass
            @asynccontextmanager
            async def step(self, name, *args):
                yield
                if name == 'claim_endgames':
                    raise RuntimeError('reporting failed after lease')
        prober = SimpleNamespace(workers=2, probe=AsyncMock(return_value=([solved()], [])), close=AsyncMock())
        with patch.object(tablebase.os.path, 'isdir', return_value=True), \
             patch.object(tablebase, 'TablebaseProber', return_value=prober):
            with self.assertRaisesRegex(RuntimeError, 'after lease'):
                await tablebase._analyze_tablebase_positions({}, reporter=Reporter(), refresh_projections=False)
        self.assertEqual(await self.snapshot(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        await self.db.fetch('SELECT fen FROM fen FOR UPDATE NOWAIT')
        prober.close.assert_awaited_once_with(abort=True)

    async def test_cancelled_publication_finishes_rollback_and_close_after_repeated_cancellation(self):
        before = await self.snapshot()
        staged, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        closed = []
        def factory():
            session = self.factory()
            rollback, close = session.rollback, session.close
            async def slow_rollback():
                cleaning.set()
                await release.wait()
                await rollback()
            async def close_recorded():
                await close()
                closed.append(True)
            session.rollback, session.close = slow_rollback, close_recorded
            return session
        original = tablebase.stage_tablebase_results
        async def stage_then_wait(*args):
            await original(*args)
            staged.set()
            await asyncio.Future()
        prober = SimpleNamespace(workers=2, probe=AsyncMock(return_value=([solved()], [])), close=AsyncMock())
        with ExitStack() as stack:
            stack.enter_context(patch.object(repository, 'AsyncDBSession', factory))
            stack.enter_context(patch.object(tablebase.os.path, 'isdir', return_value=True))
            stack.enter_context(patch.object(tablebase, 'TablebaseProber', return_value=prober))
            stack.enter_context(patch.object(tablebase, 'stage_tablebase_results', stage_then_wait))
            task = self.start(tablebase.analyze_tablebase_positions({}, refresh_projections=False))
            await asyncio.wait_for(staged.wait(), 2)
            closed.clear()  # Completed discovery and read sessions precede the write lease.
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
        self.assertEqual(await self.snapshot(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        prober.close.assert_awaited_once_with(abort=True)

    async def test_copy_updates_once_for_a_bounded_batch_and_refuses_oversized_input(self):
        rows = [solved(f'synthetic-{index} w - -', piece_count=2) for index in range(1001)]
        await self.db.executemany('INSERT INTO fen(fen,piece_count) VALUES($1,2)', [(r['fen'],) for r in rows])
        await self.db.execute('''UPDATE database_summary SET unscored_fens=1008;
            CREATE TABLE evaluation_statements(n integer);
            CREATE FUNCTION count_tablebase_statement() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO evaluation_statements SELECT count(*) FROM changed; RETURN NULL; END $$;
            CREATE TRIGGER count_tablebase AFTER UPDATE ON fen REFERENCING NEW TABLE AS changed
            FOR EACH STATEMENT EXECUTE FUNCTION count_tablebase_statement()''')
        await self.publish(rows)
        self.assertEqual([r['n'] for r in await self.db.fetch('SELECT n FROM evaluation_statements')], [1001])
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 1001)
        before = await self.snapshot()
        with self.assertRaisesRegex(ValueError, 'bounded probe batch'):
            await self.publish([solved(f'oversize-{index} w - -') for index in range(5001)])
        self.assertEqual(await self.snapshot(), before)
