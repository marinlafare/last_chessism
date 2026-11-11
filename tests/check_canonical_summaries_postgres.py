"""Real canonical coverage/accuracy/recovery, with synthetic scores and no engines."""
import asyncio
import json
import math
from unittest.mock import Mock, patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, engine_summary
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import fen_summary_recovery as recovery
from chessism_api.operations import game_analysis_summary as summaries
from chessism_api.operations import player_game_scores
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS, GAME_SQL, PLAYED
from tests.test_cuda_accuracy import fixture as accuracy_fixture, oracle


class CanonicalSummaryTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
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

    async def score_seed(self):
        await self.db.execute('''UPDATE fen SET score=CASE fen WHEN $1 THEN 15 WHEN $2 THEN -40 ELSE 100 END,
            analysis_source='stockfish',analyzed_at=now() WHERE fen=ANY($3::text[])''', FENS[0], FENS[1], list(FENS[:3]))

    async def refresh(self, links=None):
        return await summaries.refresh_game_analysis_summary(links, factory=self.factory)

    async def saved(self):
        return [dict(r) for r in await self.db.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,is_white')]

    async def recover(self, factory=None):
        return await recovery.recover_batch(factory=factory or self.factory)

    async def test_empty_database_initialization_repeat_and_full_refresh(self):
        await self.db.execute('DELETE FROM game; DELETE FROM fen_summary_refresh')
        self.assertEqual(await self.refresh(), {'games': 0, 'scope': 'all'})
        self.assertEqual(await player_game_scores.refresh_game_player_engine_summaries(factory=self.factory),
                         {'games': 0, 'game_players': 0})
        self.assertEqual((await self.recover())['pending'], 0)
        self.assertEqual(await self.saved(), [])
        self.assertFalse(await canonical.initialize(self.db))
        self.assertEqual(await canonical.validate(self.db), 6)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_explicit_empty_scopes_do_not_open_sessions(self):
        factory = Mock(side_effect=AssertionError('Empty scope opened a session'))
        for links in ([], (), [None]):
            self.assertEqual(await summaries.refresh_game_analysis_summary(links, factory=factory),
                             {'games': 0, 'scope': 'links'})
        self.assertEqual(await summaries.increment_game_analysis_summary_for_scored_fens(
            [{'fen': None}, {'fen': ' '}], factory=factory), {'games': 0, 'fens': 0})
        self.assertEqual(await player_game_scores.refresh_game_player_engine_summaries((), factory=factory),
                         {'games': 0, 'game_players': 0})
        factory.assert_not_called()

    async def test_side_keys_foreign_keys_and_cascading_deletion(self):
        await self.score_seed()
        await self.refresh()
        self.assertEqual(len(await self.saved()), 4)
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO game_player_engine_summary SELECT * FROM game_player_engine_summary LIMIT 1')
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('UPDATE game_player_engine_summary SET game_link=9999 WHERE game_link=101')
        await self.db.execute('DELETE FROM game WHERE link=101')
        self.assertEqual({r['game_link'] for r in await self.saved()}, {105})
        result = await self.recover()
        self.assertIsNone(result['error'])
        self.assertEqual(result['pending'], 0)

    async def test_actual_default_recovery_commits_both_stages_and_acknowledges(self):
        await self.score_seed()
        result = await self.recover()
        self.assertEqual(result, {'processed': 2, 'source_only': 0, 'engine_refreshed': 2, 'pending': 0, 'error': None})
        self.assertEqual([(r['game_link'], r['is_white'], r['analyzed_player_moves']) for r in await self.saved()],
                         [(101, False, 1), (101, True, 2), (105, False, 1), (105, True, 2)])
        self.assertEqual(await self.db.fetchval('SELECT sum(analyzed_positions) FROM game_analysis_coverage'), 6)
        before = await self.saved()
        self.assertEqual((await self.recover())['processed'], 0)
        self.assertEqual(await self.saved(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_incremental_replay_deduplicates_fens_and_counts_occurrences(self):
        await self.score_seed()
        payload = [{'fen': ' '+FENS[0]+' ', 'score': 999}, {'fen': FENS[0]}, {'fen': 'absent w - -'}]
        counts = await self.db.fetch('SELECT fen,n_games FROM fen ORDER BY fen')
        for _ in range(2):
            result = await summaries.increment_game_analysis_summary_for_scored_fens(payload, factory=self.factory)
            self.assertEqual(result, {'games': 2, 'fully_analyzed_games': 2, 'fens': 2})
            self.assertEqual(await self.db.fetchval('SELECT sum(total_positions) FROM game_analysis_coverage'), 6)
        self.assertEqual(await self.db.fetch('SELECT fen,n_games FROM fen ORDER BY fen'), counts)
        self.assertEqual(len(await self.saved()), 4)
        self.assertEqual(await summaries.increment_game_analysis_summary_for_scored_fens(
            [{'fen': 'absent w - -'}], factory=self.factory), {'games': 0, 'fully_analyzed_games': 0, 'fens': 1})

    async def test_scopes_preserve_other_games_and_exclude_variants_and_zero_moves(self):
        await self.score_seed()
        self.assertEqual(await self.refresh([101, 101, 102, 103, 104, 9999]), {'games': 1, 'scope': 'links'})
        self.assertEqual({r['game_link'] for r in await self.saved()}, {101})
        self.assertEqual(await self.refresh(), {'games': 2, 'scope': 'all'})
        before = [r for r in await self.saved() if r['game_link'] == 105]
        await self.refresh([101])
        self.assertEqual([r for r in await self.saved() if r['game_link'] == 105], before)
        self.assertEqual({r['game_link'] for r in await self.saved()}, {101, 105})

    async def test_incomplete_and_removed_associations_remove_only_selected_projection(self):
        await self.score_seed()
        await self.refresh()
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,9,false)', self.ids['fens'][4])
        await self.refresh([101])
        self.assertEqual({r['game_link'] for r in await self.saved()}, {105})
        self.assertFalse(await self.db.fetchval('SELECT is_fully_analyzed FROM game_analysis_coverage WHERE link=101'))
        await self.db.execute('DELETE FROM game_fen_association WHERE game_link=105')
        await self.refresh([105])
        self.assertEqual(await self.saved(), [])
        self.assertIsNone(await self.db.fetchval('SELECT link FROM game_analysis_coverage WHERE link=105'))

    async def test_self_play_and_owner_switch_have_two_distinct_sides_without_duplicate_owner_storage(self):
        await self.score_seed()
        await self.refresh()
        query = '''SELECT e.is_white,p.player_name,e.result FROM game_player_engine_summary e
            JOIN game_player gp USING(game_link,is_white) JOIN player p ON p.id=gp.player_id
            WHERE e.game_link=105 ORDER BY e.is_white'''
        before = await self.db.fetch(query)
        self.assertEqual([tuple(r) for r in before], [(False, 'synthetic_white', 'loss'), (True, 'synthetic_white', 'win')])
        await self.db.execute("""UPDATE game SET black_player_id=$1,white_result=.5,black_result=.5,
            white_str_result='agreed',black_str_result='agreed' WHERE link=105""", self.ids['black'])
        self.assertIsNone((await self.recover())['error'])
        self.assertEqual([tuple(r) for r in await self.db.fetch(query)],
                         [(False, 'synthetic_black', 'draw'), (True, 'synthetic_white', 'draw')])
        self.assertTrue(all(r['end_by'] == 'agreement' for r in await self.saved() if r['game_link'] == 105))

    async def test_numerical_equivalence_to_existing_independent_scalar_oracle(self):
        raw, games = accuracy_fixture()
        for i, game in enumerate(games):
            link = 1000+i
            selected = raw[raw['game'] == i]
            await self.db.execute(GAME_SQL, link, self.ids['white'], self.ids['black'], PLAYED,
                                  (len(selected)+1)//2, 'chess', None)
            for r in selected:
                ply = int(r['ply']); tablebase = r['source'] == 2
                fen_id = await self.db.fetchval('''INSERT INTO fen(fen,score,analysis_source,analyzed_at,tablebase_wdl)
                    VALUES($1,$2,$3,now(),$4) RETURNING fen_id''', f'synthetic-{i}-{ply} w - -', int(r['score']),
                    'tablebase' if tablebase else 'stockfish', 0 if tablebase else None)
                await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',
                                      link, fen_id, (ply+1)//2, ply % 2 == 1)
        links = tuple(1000+i for i in range(len(games)))
        await self.refresh(links)
        actual = await self.saved()
        self.assertEqual(len(actual), 2*len(games))
        max_error = 0.0
        for row in actual:
            i = row['game_link']-1000
            expected = oracle(raw, games[i], i, row['is_white'])
            for field, value in expected.items():
                if value is None:
                    self.assertIsNone(row[field], (i, field))
                else:
                    max_error = max(max_error, abs(row[field]-value))
                    self.assertTrue(math.isclose(row[field], value, rel_tol=1e-11, abs_tol=1e-9),
                                    (i, row['is_white'], field, row[field], value))
            self.assertEqual((row['result'], row['end_by']), ('win' if row['is_white'] else 'loss', 'resignation'))
        sql = engine_summary.VALUES_SQL.replace(':all_games', '$1').replace(':game_links', '$2')
        plan = json.loads(await self.db.fetchval('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql, False, list(links)))
        print('CANONICAL_ACCURACY_PARITY '+json.dumps({'games': len(games), 'positions': len(raw),
              'max_numeric_error': max_error, 'scoped_select_execution_ms': plan[0]['Execution Time']}), flush=True)

    async def test_repeated_fen_occurrences_keep_each_move_and_both_sides(self):
        await self.score_seed()
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,9,false)', self.ids['fens'][0])
        await self.refresh([101])
        rows = await self.saved()
        self.assertEqual([(r['is_white'], r['analyzed_player_moves']) for r in rows], [(False, 2), (True, 2)])
        self.assertEqual(await self.db.fetchval('SELECT total_positions FROM game_analysis_coverage WHERE link=101'), 4)
        self.assertEqual(rows[0]['final_player_cp'], -15)
        self.assertEqual(rows[1]['final_player_cp'], 15)

    async def test_caller_owns_transaction_and_rollback_includes_deletes_and_upserts(self):
        await self.score_seed()
        await self.refresh()
        before = await self.saved()
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await engine_summary.refresh_engine_summaries(session, [101])
        with self.assertRaisesRegex(RuntimeError, 'rollback'):
            async with write_transaction(self.factory) as session:
                await session.execute(text('UPDATE fen SET score=score+10 WHERE fen=:fen'), {'fen': FENS[0]})
                await refresh_coverage(session, [101])
                result = await engine_summary.refresh_engine_summaries(session, [101])
                self.assertEqual(result, {'games': 1, 'game_players': 2})
                self.assertEqual(await self.saved(), before)
                raise RuntimeError('rollback')
        self.assertEqual(await self.saved(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_real_player_query_failure_preserves_committed_coverage_and_recovery(self):
        await self.score_seed()
        await self.db.execute('''CREATE FUNCTION reject_summary() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic accuracy write failure'; END $$;
            CREATE TRIGGER reject_summary BEFORE INSERT ON game_player_engine_summary
            FOR EACH STATEMENT EXECUTE FUNCTION reject_summary()''')
        with self.assertRaisesRegex(Exception, 'synthetic accuracy write failure'):
            await self.refresh()
        self.assertEqual(await self.db.fetchval('SELECT sum(analyzed_positions) FROM game_analysis_coverage'), 6)
        self.assertEqual(await self.saved(), [])
        result = await self.recover()
        self.assertEqual((result['processed'], result['pending']), (0, 2))
        self.assertIn('synthetic accuracy write failure', result['error'])
        self.assertTrue(await self.db.fetchval('SELECT bool_and(attempts=1 AND retry_after>now()) FROM fen_summary_refresh'))
        await self.db.execute("""DROP TRIGGER reject_summary ON game_player_engine_summary;
            UPDATE fen_summary_refresh SET retry_after=now()-interval '1 second'""")
        self.assertEqual((await self.recover())['pending'], 0)
        self.assertEqual(len(await self.saved()), 4)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_missing_control_is_failure_not_successful_empty_projection(self):
        await self.score_seed()
        await self.db.execute('DELETE FROM cuda_summary_control')
        with self.assertRaisesRegex(RuntimeError, 'Required cuda_summary_control'):
            await self.refresh()
        result = await self.recover()
        self.assertIn('Required cuda_summary_control', result['error'])
        self.assertEqual(result['pending'], 2)
        self.assertEqual(await self.saved(), [])

    async def test_manual_mode_preserves_saved_accuracy_without_accessing_its_table(self):
        await self.score_seed()
        await self.refresh()
        before = await self.saved()
        await self.db.execute('UPDATE cuda_summary_control SET manual=true; UPDATE fen SET score=score+15 WHERE score IS NOT NULL')
        async with write_transaction(self.factory) as blocker:
            await blocker.execute(text('LOCK game_player_engine_summary IN ACCESS EXCLUSIVE MODE'))
            self.assertEqual(await asyncio.wait_for(player_game_scores.refresh_game_player_engine_summaries(
                (101,), factory=self.factory), 2), {'games': 0, 'game_players': 0, 'deferred': True})
            result = await asyncio.wait_for(self.recover(), 2)
            self.assertIsNone(result['error'])
            self.assertEqual(result['pending'], 0)
        self.assertEqual(await self.saved(), before)

    async def test_control_activation_waits_until_current_cpu_transaction_finishes(self):
        await self.score_seed()
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session, [101])
            self.assertEqual((await engine_summary.refresh_engine_summaries(session, [101]))['game_players'], 2)
            with self.assertRaises(asyncpg.LockNotAvailableError):
                async with self.db.transaction():
                    await self.db.execute("SET LOCAL lock_timeout='150ms'")
                    await self.db.execute('UPDATE cuda_summary_control SET manual=true')
        await self.db.execute('UPDATE cuda_summary_control SET manual=true')
        self.assertTrue((await player_game_scores.refresh_game_player_engine_summaries((101,), factory=self.factory))['deferred'])

    async def test_concurrent_overlapping_refreshes_are_absolute(self):
        await self.score_seed()
        results = await asyncio.wait_for(asyncio.gather(self.refresh([105, 101]), self.refresh([101, 105])), 4)
        self.assertEqual(results, [{'games': 2, 'scope': 'links'}]*2)
        before = await self.saved()
        self.assertEqual(len(before), 4)
        await self.refresh()
        self.assertEqual(await self.saved(), before)
        self.assertEqual(await self.db.fetchval('SELECT sum(total_positions) FROM game_analysis_coverage'), 6)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_cancellation_after_coverage_commit_keeps_queue_and_finishes_repeated_cleanup(self):
        await self.score_seed()
        staged, rollback_started, release, closed = (asyncio.Event() for _ in range(4))
        sessions = []
        def factory():
            session = self.factory(); sessions.append(session)
            execute, rollback, close = session.execute, session.rollback, session.close
            async def pause_write(statement, *args, **kwargs):
                result = await execute(statement, *args, **kwargs)
                if 'INSERT INTO public.game_player_engine_summary' in str(statement):
                    staged.set()
                    await asyncio.Future()
                return result
            async def finish_rollback():
                rollback_started.set()
                await release.wait()
                await rollback()
            async def finish_close():
                try:
                    await close()
                finally:
                    closed.set()
            session.execute = pause_write
            session.rollback = finish_rollback
            session.close = finish_close
            return session
        task = self.start(recovery.recover_batch(factory=factory))
        try:
            await asyncio.wait_for(staged.wait(), 3)
            # Read/coverage sessions have closed; reset to observe accuracy closure.
            closed.clear()
            task.cancel()
            await asyncio.wait_for(rollback_started.wait(), 2)
            task.cancel(); task.cancel()
            self.assertFalse(task.done())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(closed.is_set())
        self.assertEqual(await self.saved(), [])
        self.assertEqual(await self.db.fetchval('SELECT sum(analyzed_positions) FROM game_analysis_coverage'), 6)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen_summary_refresh'), 2)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        self.assertEqual((await self.recover())['pending'], 0)
