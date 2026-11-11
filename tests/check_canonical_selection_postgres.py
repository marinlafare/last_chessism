"""Actual canonical leases and durable game queues, with no engine/cloud calls."""
import asyncio
from unittest.mock import patch

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, analysis_selection as selection
from chessism_api.database.postgresql import fen_claims
from chessism_api.database.postgresql.fen_claims import locked_eligible_fens
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations.cloud_analysis import game_fen_queue as queue
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS, GAME_SQL, PLAYED


class CanonicalSelectionTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.identities = await seed(self.db)
        self.observer = await self.connect()
        self.engine = create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine,expire_on_commit=False)
        for module in (selection,queue):
            patcher = patch.object(module,'AsyncDBSession',self.factory)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.leases, self.tasks = [], []
        self.addAsyncCleanup(self.release_resources)

    async def release_resources(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks,return_exceptions=True)
        for session in self.leases:
            await session.close()

    async def lease(self, operation, *args, **kwargs):
        session, fens = await operation(*args, **kwargs)
        if session is not None:
            self.leases.append(session)
        return session, fens

    async def wait_for_lock(self, task, *, blocker_pid=None):
        # The lock holder can be in a transaction with a cached activity
        # snapshot. Observe from an independent connection in autocommit.
        async def wait():
            while not await self.observer.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                WHERE datname=current_database() AND wait_event_type='Lock'
                  AND ($1::int IS NULL OR $1=ANY(pg_blocking_pids(pid))))''',blocker_pid):
                if task.done():
                    task.result()  # Surface the actual SQL error, if any.
                    self.fail('Expected a blocked waiter, but it completed')
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(),2)
        self.assertFalse(task.done())

    def start(self, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        return task

    async def mark_scored(self, fens):
        await self.db.execute("""UPDATE fen SET score=0,analysis_source='stockfish',analyzed_at=now()
            WHERE fen=ANY($1::text[])""", fens)

    async def claim(self, fens, run='run-a'):
        await self.db.execute('INSERT INTO cloud_fen_claim(fen,run_id) SELECT unnest($1::text[]),$2',fens,run)

    async def ordered_fixture(self):
        # Canonical composite keys prohibit two FENs at the same game/ply.
        # Use a third game to create a genuine ordering tie across shared FENs.
        await self.db.execute(GAME_SQL,106,self.identities['white'],self.identities['black'],PLAYED,20,'chess',None)
        await self.db.execute('DELETE FROM game_fen_association; UPDATE game SET n_moves=20 WHERE link IN (101,105)')
        ids = self.identities['fens']
        for game,index,move,white in [(101,0,10,True),(101,1,11,True),(101,2,2,True),
                (101,3,2,False),(101,2,20,True),(105,0,1,True),(106,1,1,True)]:
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',game,ids[index],move,white)
        return sorted(FENS[:2]) + [FENS[2],FENS[3]]

    def selectors(self):
        return [(selection.get_fens_for_analysis,(10,)),
                (selection.get_player_fens_for_analysis,('synthetic_white',10)),
                (selection.get_game_set_fens_for_analysis,([101,105],10))]

    async def test_global_priority_and_handoff_preserve_transaction_and_skip_locked(self):
        first, fens = await self.lease(selection.get_fens_for_analysis,2)
        self.assertEqual(fens,list(FENS[:2]))
        self.assertTrue(first.in_transaction())
        second, rest = await self.lease(selection.get_fens_for_analysis,10)
        self.assertEqual(rest,list(FENS[2:]))
        await second.close()
        await first.rollback()
        await first.close()
        again, fens = await self.lease(selection.get_fens_for_analysis,2)
        self.assertEqual(fens,list(FENS[:2]))

    async def test_player_membership_deduplicates_games_and_self_play(self):
        for player in ('synthetic_white','synthetic_black'):
            session, fens = await self.lease(selection.get_player_fens_for_analysis,player,10)
            self.assertEqual(fens,list(FENS[:3]))
            await session.close()
        self.assertEqual(await selection.get_player_fens_for_analysis('absent',10),(None,None))

    async def test_concurrent_player_selectors_fill_limit_after_skipping_fen_locks(self):
        first, fens = await self.lease(selection.get_player_fens_for_analysis,'synthetic_white',2)
        second, rest = await self.lease(selection.get_player_fens_for_analysis,'synthetic_white',2)
        self.assertEqual(fens,list(FENS[:2]))
        self.assertEqual(rest,[FENS[2]])
        # Membership is only an EXISTS filter: participant/game rows are not leased.
        async with self.db.transaction():
            self.assertEqual(await self.db.fetchval('SELECT link FROM game WHERE link=101 FOR UPDATE NOWAIT'),101)

    async def test_frozen_games_preserve_independent_minima_duplicates_and_text_ties(self):
        expected = await self.ordered_fixture()
        session, fens = await self.lease(selection.get_game_set_fens_for_analysis,[101,None,105,106,101],50)
        self.assertEqual(fens,expected)
        await session.close()
        self.assertEqual(await selection.get_game_set_fens_for_analysis([],10),(None,None))
        self.assertEqual(await selection.get_game_set_fens_for_analysis([102,103,104],10),(None,None))

    async def test_all_selectors_exclude_committed_scores_and_cloud_claims(self):
        await self.mark_scored([FENS[0]])
        await self.claim([FENS[1]])
        for index,(operation,args) in enumerate(self.selectors()):
            with self.subTest(selector=operation.__name__):
                session, fens = await self.lease(operation,*args)
                self.assertEqual(fens,list(FENS[2:]) if index==0 else [FENS[2]])
                await session.close()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'),1)

    async def test_successful_exhaustion_releases_every_selector_connection(self):
        await self.mark_scored(list(FENS))
        for operation,args in self.selectors():
            self.assertEqual(await operation(*args),(None,None))
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_database_failures_propagate_and_release_sessions(self):
        await self.db.execute('ALTER TABLE fen RENAME TO unavailable_fen')
        for operation,args in self.selectors():
            with self.subTest(selector=operation.__name__), self.assertRaisesRegex(Exception,'fen.*does not exist'):
                await operation(*args)
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_recheck_sees_reservation_committed_while_initial_lock_waited(self):
        initial_rows = []
        recheck = fen_claims.unclaimed_locked_fens
        async def observe_recheck(session, fens):
            initial_rows.extend(fens)
            return await recheck(session,fens)
        async def waiter():
            async with write_transaction(self.factory) as session:
                return await locked_eligible_fens(session,text('''SELECT fen FROM public.fen
                    WHERE fen=:fen AND score IS NULL AND NOT EXISTS (
                        SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=public.fen.fen)
                    FOR UPDATE'''),{'fen':FENS[0]})
        with patch.object(fen_claims,'unclaimed_locked_fens',observe_recheck):
            async with self.db.transaction():
                blocker_pid = await self.db.fetchval('SELECT pg_backend_pid()')
                await self.db.execute('SELECT fen FROM fen WHERE fen=$1 FOR UPDATE',FENS[0])
                task = self.start(waiter())
                await self.wait_for_lock(task,blocker_pid=blocker_pid)
                await self.claim([FENS[0]])
            self.assertEqual(await asyncio.wait_for(task,5),[])
        self.assertEqual(initial_rows,[FENS[0]])
        self.assertEqual(await self.db.fetchval('SELECT run_id FROM cloud_fen_claim WHERE fen=$1',FENS[0]),'run-a')

    async def test_frozen_counts_and_completion_use_canonical_coverage(self):
        await self.claim([FENS[1]])
        self.assertEqual(await selection.count_game_set_fens_for_analysis([101,105,101]),3)
        await self.mark_scored([FENS[0]])
        self.assertEqual(await selection.count_game_set_fens_for_analysis([101,105]),2)
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session,[101,105])
        self.assertEqual(await selection.get_game_set_analysis_completion([101,105,101]),
            {'selected_games':2,'fully_analyzed_games':0,'incomplete_games':2})
        await self.mark_scored(list(FENS[:3]))
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session,[101,105])
        self.assertEqual(await selection.get_game_set_analysis_completion([101,105]),
            {'selected_games':2,'fully_analyzed_games':2,'incomplete_games':0})
        self.assertEqual(await selection.count_game_set_fens_for_analysis([]),0)
        self.assertEqual(await selection.get_game_set_analysis_completion([]),
            {'selected_games':0,'fully_analyzed_games':0,'incomplete_games':0})

    async def test_rejected_selection_does_not_undo_previously_committed_result(self):
        await self.mark_scored([FENS[0]])
        with self.assertRaises(Exception):
            await selection.get_fens_for_analysis(-1)
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]),0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_queue_order_rollback_atomic_claims_restart_and_exhaustion(self):
        expected = await self.ordered_fixture()
        links = [101,105,106,101]
        session, fens = await self.lease(queue.get_game_queue_fens,'job-a',links,2)
        self.assertEqual(fens,expected[:2])
        # Other connections still see uncommitted queue removals.
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),4)
        await session.rollback()
        await session.close()
        session, fens = await self.lease(queue.get_game_queue_fens,'job-a',links,2)
        await session.execute(text('INSERT INTO cloud_fen_claim SELECT unnest(CAST(:fens AS text[])),:run'),
            {'fens':fens,'run':'run-a'})
        await session.commit()
        await session.close()
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),2)
        restarted = async_sessionmaker(self.engine,expire_on_commit=False)
        with patch.object(queue,'AsyncDBSession',restarted):
            session, fens = await self.lease(queue.get_game_queue_fens,'job-a',links,10)
        self.assertEqual(fens,expected[2:])
        await session.commit()
        await session.close()
        self.assertEqual(await queue.get_game_queue_fens('job-a',links,10),(None,None))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'),2)

    async def test_queue_keeps_locked_candidates_available_for_next_reservation(self):
        expected = await self.ordered_fixture()
        links = [101,105,106]
        async with self.db.transaction():
            await self.db.execute('SELECT fen FROM fen WHERE fen=$1 FOR UPDATE',expected[0])
            session, fens = await self.lease(queue.get_game_queue_fens,'job-a',links,10)
            self.assertEqual(fens,expected[1:])
            await session.commit()
            await session.close()
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),1)
        session, fens = await self.lease(queue.get_game_queue_fens,'job-a',links,10)
        self.assertEqual(fens,[expected[0]])

    async def test_queue_rechecks_scores_and_claims_after_durable_build(self):
        await queue.ensure_queue('job-a',[101,105])
        await self.mark_scored([FENS[0]])
        await self.claim([FENS[1]],'run-b')
        session, fens = await self.lease(queue.get_game_queue_fens,'job-a',[101,105],10)
        self.assertEqual(fens,[FENS[2]])
        await session.commit()
        await session.close()
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),0)
        self.assertEqual(await self.db.fetchval('SELECT run_id FROM cloud_fen_claim'),'run-b')

    async def test_queue_rejects_changed_game_identity_without_mutation(self):
        await queue.ensure_queue('job-a',[101,105])
        before = await self.db.fetch("SELECT * FROM cloud_game_fen_candidate WHERE job_id='job-a' ORDER BY ordinal")
        with self.assertRaisesRegex(ValueError,'selection changed'):
            await queue.get_game_queue_fens('job-a',[105,101],10)
        self.assertEqual(await self.db.fetch("SELECT * FROM cloud_game_fen_candidate WHERE job_id='job-a' ORDER BY ordinal"),before)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_same_job_reservers_serialize_and_take_disjoint_pages(self):
        session, fens = await self.lease(queue.get_game_queue_fens,'job-a',[101,105],1)
        task = self.start(self.lease(queue.get_game_queue_fens,'job-a',[101,105],1))
        await self.wait_for_lock(task)
        self.assertFalse(task.done())
        await session.execute(text('INSERT INTO cloud_fen_claim(fen,run_id) VALUES(:fen,:run)'),
            {'fen':fens[0],'run':'run-a'})
        await session.commit()
        await session.close()
        second, following = await asyncio.wait_for(task,5)
        self.assertEqual(len(following),1)
        self.assertTrue(set(fens).isdisjoint(following))

    async def test_cancelled_queue_wait_releases_connection_without_consuming_page(self):
        first, fens = await self.lease(queue.get_game_queue_fens,'job-a',[101,105],1)
        task = self.start(queue.get_game_queue_fens('job-a',[101,105],1))
        await self.wait_for_lock(task)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task,5)
        await first.rollback()
        await first.close()
        self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),3)
        session, retry = await self.lease(queue.get_game_queue_fens,'job-a',[101,105],1)
        self.assertEqual(retry,fens)

    async def test_failed_queue_build_keeps_unbuilt_checkpoint_and_retries(self):
        with patch.object(queue,'candidate_sql',return_value='SELECT definitely_missing_function()'):
            with self.assertRaises(Exception):
                await queue.ensure_queue('job-a',[101,105])
        self.assertIsNone(await self.db.fetchval("SELECT built_at FROM cloud_game_fen_queue WHERE job_id='job-a'"))
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),0)
        await queue.ensure_queue('job-a',[101,105])
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),3)

    async def test_discard_and_empty_queue_are_scoped_to_one_job(self):
        await queue.ensure_queue('job-a',[101])
        await queue.ensure_queue('job-b',[105])
        async with write_transaction(self.factory) as session:
            await queue.discard_queue(session,'job-a')
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-a'"),0)
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id='job-b'"),3)
        self.assertEqual(await queue.get_game_queue_fens('job-a',[],10),(None,None))
