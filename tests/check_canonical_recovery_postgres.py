"""Canonical recovery commits, exact revisions and trigger boundaries on real PG."""
import asyncio
import json
from unittest.mock import AsyncMock, patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, summary_refresh
from chessism_api.database.postgresql.evaluation_results import stage_fen_results
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.tablebase_repository import stage_tablebase_results
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction
from chessism_api.operations import fen_summary_recovery as recovery
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS, GAME_SQL, PLAYED


class CanonicalRecoveryTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        self.initial = await self.pending()
        await self.db.execute('DELETE FROM fen_summary_refresh; UPDATE database_summary SET n_positions=5,unscored_fens=5')
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

    async def pending(self):
        return [dict(row) for row in await self.db.fetch('SELECT * FROM fen_summary_refresh ORDER BY game_link')]

    async def publish(self, fen=FENS[0], score=24):
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session, [{'fen': fen, 'score': score, 'next_moves': None}], [], count_new=True)

    async def coverage(self, links, *, strict):
        self.assertTrue(strict)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session, links)

    async def recover(self, refresh=None):
        return await recovery.recover_batch(factory=self.factory, refresh=refresh or self.coverage)

    async def test_version_repeat_and_empty_operation_preserve_queue_and_catalog(self):
        self.assertEqual([r['game_link'] for r in self.initial], [101, 105])
        callback = AsyncMock()
        self.assertEqual(await self.recover(callback), {'processed': 0, 'source_only': 0,
            'engine_refreshed': 0, 'pending': 0, 'error': None})
        callback.assert_not_awaited()
        await self.publish()
        before = await self.pending()
        self.assertFalse(await canonical.initialize(self.db))
        self.assertEqual(await canonical.validate(self.db), 5)
        self.assertEqual(await self.pending(), before)

    async def test_local_scores_and_refresh_requests_commit_or_rollback_together(self):
        before = await self.pending()
        with self.assertRaisesRegex(RuntimeError, 'before commit'):
            async with write_transaction(self.factory) as session:
                await stage_fen_results(session, [{'fen': FENS[0], 'score': 24}], [], count_new=True)
                self.assertEqual(await self.pending(), before)
                self.assertEqual(await session.scalar(text('SELECT count(*) FROM fen_summary_refresh')), 2)
                raise RuntimeError('before commit')
        self.assertEqual(await self.pending(), before)
        self.assertIsNone(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1', FENS[0]))
        await self.publish()
        self.assertEqual([r['game_link'] for r in await self.pending()], [101, 105])

    async def test_cloud_claim_exclusion_survives_claim_deletion_in_same_commit(self):
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", FENS[0])
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session, [{'fen': FENS[0], 'score': 24}], [], count_new=True)
            await session.execute(text('DELETE FROM cloud_fen_claim WHERE fen=:fen'), {'fen': FENS[0]})
        self.assertEqual(await self.pending(), [])
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1', FENS[0]), 24)
        await self.publish(FENS[1])
        self.assertEqual([r['game_link'] for r in await self.pending()], [101, 105])

    async def test_tablebase_results_queue_but_unavailable_metadata_does_not(self):
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,3,true)', self.ids['fens'][3])
        await self.db.execute('DELETE FROM fen_summary_refresh')
        async with write_transaction(self.factory) as session:
            await stage_tablebase_results(session, [{'fen': FENS[3], 'piece_count': 3, 'score': 0,
                'tablebase_wdl': 0, 'tablebase_dtz': 0, 'next_moves': None,
                'wdl_win': 0, 'wdl_draw': 1000, 'wdl_loss': 0}], [{'fen': FENS[4], 'piece_count': 2}])
        self.assertEqual([r['game_link'] for r in await self.pending()], [101])
        self.assertEqual(await self.db.fetchval('SELECT analysis_source FROM fen WHERE fen=$1', FENS[4]), 'tablebase_unavailable')

    async def test_metadata_noop_and_empty_updates_do_not_even_read_associations(self):
        async with write_transaction(self.factory) as blocker:
            await blocker.execute(text('LOCK game_fen_association IN ACCESS EXCLUSIVE MODE'))
            await self.db.execute("SET lock_timeout='300ms'")
            try:
                for _ in range(8):
                    await self.db.execute('UPDATE fen SET n_games=n_games WHERE false')
                await self.db.execute('UPDATE fen SET n_games=n_games+1,moves_counter=moves_counter')
                await self.db.execute('UPDATE fen SET score=score,analysis_source=analysis_source')
                await self.db.execute("UPDATE fen SET analysis_source='tablebase_unavailable' WHERE fen=$1", FENS[0])
            finally:
                await self.db.execute("SET lock_timeout='0'")
        self.assertEqual(await self.pending(), [])

    async def test_known_evaluation_kind_change_queues_and_unknown_provenance_is_rejected(self):
        await self.publish(score=0)
        await self.db.execute('DELETE FROM fen_summary_refresh')
        await self.db.execute("UPDATE fen SET analysis_source='tablebase',tablebase_wdl=0 WHERE fen=$1", FENS[0])
        self.assertEqual([r['game_link'] for r in await self.pending()], [101, 105])
        before = await self.pending()
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute("UPDATE fen SET analysis_source='stockfish_backup',tablebase_wdl=NULL WHERE fen=$1", FENS[0])
        self.assertEqual(await self.pending(), before)

    async def test_new_associations_to_existing_scores_persist_recovery(self):
        await self.publish()
        await self.db.execute(GAME_SQL, 106, self.ids['white'], self.ids['black'], PLAYED, 2, 'chess', None)
        await self.db.execute('DELETE FROM fen_summary_refresh')
        await self.db.execute('INSERT INTO game_fen_association VALUES(106,$1,0,true)', self.ids['fens'][0])
        self.assertEqual([r['game_link'] for r in await self.pending()], [106])
        await self.recover()
        self.assertTrue(await self.db.fetchval('SELECT is_fully_analyzed FROM game_analysis_coverage WHERE link=106'))

    async def test_association_update_and_delete_queue_all_old_and_new_games(self):
        await self.db.execute('UPDATE game_fen_association SET game_link=105,n_move=7 WHERE game_link=101 AND n_move=0')
        self.assertEqual([r['game_link'] for r in await self.pending()], [101, 105])
        await self.db.execute('DELETE FROM fen_summary_refresh')
        await self.db.execute('DELETE FROM game_fen_association WHERE game_link=105 AND n_move=7')
        self.assertEqual([r['game_link'] for r in await self.pending()], [105])

    async def test_game_ownership_and_classification_queue_without_flag_update_noise(self):
        await self.db.execute('UPDATE game SET fens_processing=true WHERE link=101')
        self.assertEqual(await self.pending(), [])
        await self.db.execute('UPDATE game SET white_player_id=black_player_id WHERE link=101')
        self.assertEqual([r['game_link'] for r in await self.pending()], [101])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player WHERE game_link=101 AND player_id=$1', self.ids['black']), 2)
        await self.db.execute("UPDATE game SET rules='chess' WHERE link=102")
        await self.db.execute('UPDATE game SET n_moves=1 WHERE link=104')
        self.assertEqual([r['game_link'] for r in await self.pending()], [101, 102, 104])

    async def test_deleted_game_recovery_survives_cascading_association_deletion(self):
        await self.db.execute('DELETE FROM game WHERE link=105')
        self.assertEqual([r['game_link'] for r in await self.pending()], [105])
        result = await self.recover()
        self.assertEqual((result['processed'], result['pending']), (1, 0))

    async def test_success_refreshes_absolute_coverage_then_acknowledges(self):
        await self.publish()
        result = await self.recover()
        self.assertEqual(result, {'processed': 2, 'source_only': 0, 'engine_refreshed': 2, 'pending': 0, 'error': None})
        self.assertEqual([tuple(r) for r in await self.db.fetch('''SELECT total_positions,analyzed_positions,
            unscored_positions FROM game_analysis_coverage ORDER BY link''')], [(3, 1, 2), (3, 1, 2)])
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 1)
        self.assertEqual((await self.recover())['processed'], 0)

    async def test_failed_refresh_after_projection_commit_retains_requests_and_backoff(self):
        await self.publish()
        async def partial_refresh(links, *, strict):
            await self.coverage(links, strict=strict)
            raise RuntimeError('later player summary failed')
        result = await self.recover(partial_refresh)
        self.assertEqual((result['processed'], result['pending']), (0, 2))
        self.assertIn('later player summary failed', result['error'])
        self.assertTrue(all(r['attempts'] == 1 and r['error'] for r in await self.pending()))
        self.assertTrue(await self.db.fetchval('SELECT bool_and(retry_after>clock_timestamp()) FROM fen_summary_refresh'))
        callback = AsyncMock()
        self.assertEqual((await self.recover(callback))['pending'], 2)
        callback.assert_not_awaited()
        await self.db.execute("UPDATE fen_summary_refresh SET retry_after=now()-interval '1 second'")
        self.assertEqual((await self.recover())['pending'], 0)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 1)

    async def test_source_change_during_refresh_retains_newer_revision(self):
        await self.publish()
        before = {r['game_link']: r['revision'] for r in await self.pending()}
        async def racing_refresh(links, *, strict):
            await self.coverage(links, strict=strict)
            await self.db.execute('UPDATE fen SET score=score+1 WHERE fen=$1', FENS[0])
        result = await self.recover(racing_refresh)
        self.assertEqual((result['processed'], result['pending']), (2, 2))
        self.assertTrue(all(r['revision'] > before[r['game_link']] for r in await self.pending()))
        self.assertEqual((await self.recover())['pending'], 0)

    async def test_failure_cannot_postpone_newer_change_and_fresh_change_clears_prior_error(self):
        await self.publish()
        async def racing_failure(links, *, strict):
            await self.db.execute('UPDATE fen SET score=score+1 WHERE fen=$1', FENS[0])
            raise RuntimeError('stale refresh failed')
        await self.recover(racing_failure)
        self.assertTrue(all(r['attempts'] == 0 and r['error'] is None for r in await self.pending()))
        await self.recover(AsyncMock(side_effect=RuntimeError('failed current revision')))
        self.assertTrue(all(r['attempts'] == 1 for r in await self.pending()))
        await self.db.execute('UPDATE fen SET score=score+1 WHERE fen=$1', FENS[0])
        self.assertTrue(all(r['attempts'] == 0 and r['error'] is None for r in await self.pending()))
        self.assertEqual((await self.recover())['pending'], 0)

    async def test_concurrent_recovery_replays_absolute_coverage_without_double_counting(self):
        await self.publish()
        both = asyncio.Event()
        calls = 0
        async def refresh(links, *, strict):
            nonlocal calls
            calls += 1
            if calls == 2:
                both.set()
            await asyncio.wait_for(both.wait(), 2)
            # Another refresher may now own a connection; each operation still
            # performs the absolute refresh in its own transaction.
            self.assertTrue(strict)
            async with write_transaction(self.factory) as session:
                await refresh_coverage(session, links)
        results = await asyncio.wait_for(asyncio.gather(self.recover(refresh), self.recover(refresh)), 4)
        self.assertEqual([r['processed'] for r in results], [2, 2])
        self.assertEqual(await self.pending(), [])
        self.assertEqual(await self.db.fetchval('SELECT sum(analyzed_positions) FROM game_analysis_coverage'), 2)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary'), 1)

    async def test_cancellation_after_projection_commit_keeps_request_for_retry(self):
        await self.publish()
        before = await self.pending()
        entered = asyncio.Event()
        async def interrupted_refresh(links, *, strict):
            await self.coverage(links, strict=strict)
            entered.set()
            await asyncio.Future()
        task = self.start(self.recover(interrupted_refresh))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        self.assertEqual(await self.pending(), before)
        self.assertEqual((await self.recover())['pending'], 0)

    async def test_repeated_cancellation_during_acknowledgment_rolls_back_and_closes(self):
        await self.publish()
        before = await self.pending()
        staged, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        closed = []
        def factory():
            session = self.factory()
            rollback, close = session.rollback, session.close
            async def rollback_slowly():
                cleaning.set()
                await release.wait()
                await rollback()
            async def close_recorded():
                await close()
                closed.append(True)
            session.rollback, session.close = rollback_slowly, close_recorded
            return session
        original = summary_refresh.finish_batch
        async def ack_then_pause(*args):
            await original(*args)
            staged.set()
            await asyncio.Future()
        with patch.object(summary_refresh, 'finish_batch', ack_then_pause):
            task = self.start(recovery.recover_batch(factory=factory, refresh=self.coverage))
            await asyncio.wait_for(staged.wait(), 2)
            closed.clear()
            task.cancel()
            await asyncio.wait_for(cleaning.wait(), 2)
            try:
                for _ in range(2):
                    task.cancel()
                    await asyncio.sleep(0)
                    self.assertFalse(task.done())
            finally:
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
        self.assertEqual(closed, [True])
        self.assertEqual(await self.pending(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_acknowledgment_database_failure_propagates_and_keeps_requests(self):
        await self.publish()
        before = await self.pending()
        await self.db.execute('''CREATE FUNCTION reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected acknowledgment failure'; END $$;
            CREATE TRIGGER reject_ack BEFORE DELETE ON fen_summary_refresh
            FOR EACH STATEMENT EXECUTE FUNCTION reject_ack()''')
        with self.assertRaisesRegex(asyncpg.RaiseError, 'acknowledgment failure'):
            await self.recover()
        self.assertEqual(await self.pending(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_bounded_order_and_exact_revision_updates_require_transaction(self):
        await self.db.execute('''INSERT INTO fen_summary_refresh(game_link,queued_at) VALUES
            (1001,'2020-01-01'),(1002,'2020-01-02'),(1003,'2020-01-03');
            UPDATE fen_summary_refresh SET retry_after=now()+interval '1 day' WHERE game_link=1001''')
        with self.assertRaisesRegex(ValueError, 'caller-owned'):
            await summary_refresh.pending_batch(self.db, 2)
        async with write_transaction(self.factory) as session:
            connection = await driver_connection(session)
            rows = await summary_refresh.pending_batch(connection, 1)
            self.assertEqual([r['game_link'] for r in rows], [1002])
            for limit in (0, 501):
                with self.assertRaises(ValueError):
                    await summary_refresh.pending_batch(connection, limit)
            await summary_refresh.finish_batch(connection, [{**rows[0], 'revision': rows[0]['revision']-1}])
        self.assertEqual(len(await self.pending()), 3)
        with patch.object(recovery, 'BATCH_GAMES', 1):
            result = await self.recover()
        self.assertEqual((result['processed'], result['pending']), (1, 2))

    async def test_metadata_trigger_plan_on_reproducible_fanout_fixture(self):
        await self.db.execute('''INSERT INTO fen(fen,n_games)
            SELECT 'synthetic-'||i||' w - -',20 FROM generate_series(1,2000) i;
            INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white)
            SELECT g.link,f.fen_id,f.fen_id::int,true FROM game g CROSS JOIN fen f
            WHERE g.link IN (101,105) AND f.fen LIKE 'synthetic-%';
            DELETE FROM fen_summary_refresh;
            ANALYZE fen; ANALYZE game_fen_association''')
        for _ in range(8):
            await self.db.execute('UPDATE fen SET n_games=n_games WHERE false')
        plan = json.loads(await self.db.fetchval('''EXPLAIN (ANALYZE,FORMAT JSON)
            UPDATE fen SET n_games=n_games+1'''))[0]
        self.assertEqual(await self.pending(), [])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association'), 4006)
        print('CANONICAL_RECOVERY_PLAN '+json.dumps({'fens': 2005, 'associations': 4006,
            'execution_ms': plan['Execution Time'], 'triggers': plan.get('Triggers', [])}), flush=True)
