"""Homepage contracts on a private canonical database; no production configuration."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import AsyncMock, patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError, NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, home_summary as storage
from chessism_api.operations import home_summary as home
from chessism_api.routers import games
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL, PLAYED


class CanonicalHomeSummaryTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        fixture=await seed(self.db)
        self.ids={'a':fixture['white'],'b':fixture['black']}
        self.fens=fixture['fens']
        await self.db.execute("UPDATE player SET player_name='a',title='GM',joined=1 WHERE id=$1",self.ids['a'])
        await self.db.execute("UPDATE player SET player_name='b',title=' im ',joined=1 WHERE id=$1",self.ids['b'])
        for name,title,joined in (('c',None,1),('d','BOT',1),('deleted','GM',1),('no-games','WFM',1),('shell','GM',0)):
            self.ids[name]=await self.db.fetchval('INSERT INTO player(player_name,title,joined) VALUES($1,$2,$3) RETURNING id',name,title,joined)
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='deleted'")
        for link,w,b in ((106,'a','c'),(107,'c','d'),(108,'deleted','c'),(109,'shell','c')):
            await self.db.execute(GAME_SQL,link,self.ids[w],self.ids[b],PLAYED,2,'chess',None)
        await self.db.execute('DELETE FROM game_fen_association')
        for link,index,move,white in ((101,0,0,True),(101,0,0,False),(101,1,1,True),
            (105,0,0,True),(106,2,0,True),(107,0,0,True),(107,3,1,True),(108,3,0,True),(109,2,0,True)):
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',link,self.fens[index],move,white)
        # Mode-summary bars are independent of the exact all-mode live roster.
        for name,mode,count in (('a','blitz',9000),('c','bullet',3210),('c','blitz',1000),('d','rapid',7650),('deleted','blitz',99999)):
            await self.db.execute('INSERT INTO main_character_mode_summary(mode,player_id,n_games) VALUES($1,$2,$3)',mode,self.ids[name],count)
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,
            port=55432,query={'host':self.socket}),connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.redis=AsyncMock()
        self.tasks=[];self.addAsyncCleanup(self.stop_tasks)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)

    def start(self,coroutine):
        task=asyncio.create_task(coroutine);self.tasks.append(task);return task

    async def read(self):
        return await home.get_home_summary(self.redis,session_factory=self.factory)

    async def refresh(self):
        return await home.refresh_home_summary(session_factory=self.factory)

    async def revision(self):
        return await self.db.fetchval('SELECT revision FROM home_summary_source')

    async def snapshot(self):
        return dict(await self.db.fetchrow('SELECT * FROM home_titled_summary'))

    async def assert_unlocked(self):
        async with self.db.transaction():
            self.assertTrue(await self.db.fetchval('SELECT pg_try_advisory_xact_lock($1::bigint)',home.LOCK_ID))
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_empty_initialization_queues_once_and_refresh_publishes_zero_counts(self):
        await self.db.execute('DELETE FROM game;DELETE FROM fen;DELETE FROM months;DELETE FROM player')
        empty=await self.read()
        self.assertIsNone(empty['total_fens']);self.assertTrue(empty['stale'])
        self.assertEqual(empty['players'],[]);self.assertEqual(empty['untitled_players'],[])
        self.assertEqual(empty['game_players'],{'titled':[],'untitled':[]})
        self.redis.enqueue_job.assert_awaited_once_with('run_home_summary_refresh',_job_id=home.JOB_ID,_queue_name='analysis_queue')
        self.assertEqual(await self.refresh(),dict(status='complete',total_fens=0,titled_players=0,titled_games=0,titled_fens=0))
        saved=await self.read();self.assertFalse(saved['stale']);self.assertEqual(saved['total_fens'],0)
        before=await self.snapshot();self.assertFalse(await canonical.initialize(self.db));self.assertEqual(await self.snapshot(),before)

    async def test_distinct_unions_self_play_half_units_and_live_roster_match_oracle(self):
        self.assertEqual(await self.refresh(),dict(status='complete',total_fens=5,titled_players=4,titled_games=4,titled_fens=3))
        result=await self.read()
        self.assertEqual({p['player_name']:(p['games'],p['game_half_units']) for p in result['players']},
                         {'a':(4,5),'b':(1,1),'shell':(1,2),'no-games':(0,0)})
        self.assertEqual(sum(p['game_half_units'] for p in result['players']),2*result['titled_games'])
        self.assertEqual([(p['player_name'],p['games']) for p in result['game_players']['titled']],[('a',4),('b',1)])
        self.assertEqual([(p['player_name'],p['games']) for p in result['game_players']['untitled']],[('c',4),('d',1)])
        self.assertEqual([(p['player_name'],p['games']) for p in result['untitled_players']],[('d',7650),('c',4210)])
        self.assertFalse(result['stale']);self.assertIsNone(result['refresh_error'])
        self.assertTrue(all(set(p)=={'player_name','title','games','game_half_units','avatar'} for p in result['players']))

    async def test_avatar_is_live_without_invalidating_the_cached_snapshot(self):
        await self.refresh();before=await self.read();revision=await self.revision()
        await self.db.execute("UPDATE player SET avatar='https://images.example.invalid/a.jpg' WHERE player_name='a'")
        after=await self.read()
        self.assertEqual(await self.revision(),revision);self.assertFalse(after['stale'])
        self.assertEqual(after['refreshed_at'],before['refreshed_at'])
        self.assertEqual(after['players'][0]['avatar'],'https://images.example.invalid/a.jpg')
        self.assertEqual(after['game_players']['titled'][0]['avatar'],'https://images.example.invalid/a.jpg')

    async def test_scores_processing_flags_occurrences_and_joined_do_not_invalidate_union(self):
        await self.refresh();revision=await self.revision()
        await self.db.execute("UPDATE fen SET score=55,analysis_source='stockfish',analyzed_at=now();UPDATE game SET fens_done=true WHERE is_standard AND n_moves>0")
        await self.db.execute("UPDATE fen SET n_games=n_games+1;UPDATE player SET joined=0 WHERE player_name='a'")
        result=await self.read();self.assertEqual(await self.revision(),revision);self.assertFalse(result['stale'])
        self.assertNotIn('a',[p['player_name'] for p in result['game_players']['titled']])
        self.assertIn('a',[p['player_name'] for p in result['players']])

    async def test_structural_invalidation_rolls_back_with_source_changes(self):
        await self.refresh();revision=await self.revision()
        transaction=self.db.transaction();await transaction.start()
        await self.db.execute("UPDATE player SET title=NULL WHERE player_name='a'")
        self.assertGreater(await self.revision(),revision)
        await transaction.rollback()
        self.assertEqual(await self.revision(),revision);self.assertFalse((await self.read())['stale'])
        await self.db.execute("UPDATE player SET title=NULL WHERE player_name='a'")
        self.assertTrue((await self.read())['stale'])
        await self.refresh()
        result=await self.read();self.assertEqual((result['titled_games'],result['titled_fens']),(2,3))
        self.assertEqual({p['player_name']:p['game_half_units'] for p in result['players']},{'b':2,'shell':2,'no-games':0})

    async def test_eligibility_updates_invalidate_and_include_only_newly_active_games(self):
        await self.refresh()
        for sql in ("UPDATE game SET rules='chess' WHERE link=102",
                    "UPDATE game SET initial_setup=NULL WHERE link=103",
                    "UPDATE game SET n_moves=2 WHERE link=104"):
            revision=await self.revision();await self.db.execute(sql)
            self.assertGreater(await self.revision(),revision);self.assertTrue((await self.read())['stale'])
        await self.refresh();result=await self.read()
        self.assertEqual(result['titled_games'],7)
        self.assertEqual([(p['player_name'],p['games']) for p in result['game_players']['titled']],[('a',7),('b',4)])

    async def test_association_membership_changes_invalidate_the_distinct_fen_union(self):
        await self.refresh();revision=await self.revision()
        await self.db.execute('UPDATE game_fen_association SET fen_id=$1 WHERE game_link=101 AND n_move=1',self.fens[4])
        self.assertGreater(await self.revision(),revision);self.assertTrue((await self.read())['stale'])
        await self.refresh()
        self.assertEqual((await self.read())['titled_fens'],3)
        await self.db.execute('DELETE FROM game_fen_association WHERE game_link=106 OR game_link=109')
        await self.refresh();self.assertEqual((await self.read())['titled_fens'],2)

    async def test_physical_deletion_retains_complete_prior_snapshot_until_rebuilt(self):
        await self.refresh();before=await self.read();saved=await self.snapshot()
        async with self.db.transaction():
            await self.db.execute('DELETE FROM game WHERE white_player_id=$1 OR black_player_id=$1',self.ids['a'])
            await self.db.execute('DELETE FROM months WHERE player_id=$1',self.ids['a'])
            await self.db.execute('DELETE FROM player WHERE id=$1',self.ids['a'])
        stale=await self.read()
        self.assertEqual(await self.snapshot(),saved)
        self.assertEqual(stale['players'],before['players']);self.assertEqual(stale['titled_games'],before['titled_games'])
        self.assertTrue(stale['stale']);self.assertNotIn('a',[p['player_name'] for p in stale['game_players']['titled']])
        await self.refresh();self.assertNotIn('a',[p['player_name'] for p in (await self.read())['players']])

    async def test_live_new_game_roster_does_not_wait_for_fen_or_mode_summary(self):
        await self.refresh();before=await self.read()
        new_id=await self.db.fetchval("INSERT INTO player(player_name,title,joined) VALUES('new-pro',' gm ',1) RETURNING id")
        await self.db.execute(GAME_SQL,200,new_id,self.ids['c'],PLAYED,2,'chess',None)
        after=await self.read()
        self.assertEqual(after['players'],before['players']);self.assertEqual(after['refreshed_at'],before['refreshed_at'])
        self.assertIn({'player_name':'new-pro','title':'GM','avatar':None,'games':1},after['game_players']['titled'])
        self.assertTrue(after['stale'])

    async def test_snapshot_revision_marks_source_changes_during_publication_stale(self):
        original=storage.build_snapshot
        async def change_after_snapshot(session):
            snapshot=await original(session)
            await self.db.execute("UPDATE player SET title=NULL WHERE player_name='a'")
            return snapshot
        with patch.object(storage,'build_snapshot',change_after_snapshot):await self.refresh()
        result=await self.read();self.assertTrue(result['stale']);self.assertEqual(result['titled_games'],4)
        self.assertEqual(sum(p['game_half_units'] for p in result['players']),8)
        await self.refresh();self.assertFalse((await self.read())['stale'])

    async def test_missing_expensive_sources_fail_refresh_but_keep_cached_reads_available(self):
        await self.refresh();before=await self.read()
        await self.db.execute('ALTER TABLE fen RENAME TO hidden_fen;ALTER TABLE game_fen_association RENAME TO hidden_associations')
        with self.assertRaises(DBAPIError):await self.refresh()
        self.redis.enqueue_job.reset_mock()
        after=await self.read()
        for field in ('players','total_fens','titled_games','titled_fens','refreshed_at','game_players'):
            self.assertEqual(after[field],before[field])
        self.assertIsNotNone(after['refresh_error']);self.redis.enqueue_job.assert_not_awaited()
        await self.assert_unlocked()

    async def test_older_failure_metadata_cannot_overwrite_new_success(self):
        build,record=storage.build_snapshot,storage.record_failure
        first=True
        async def fail_once(session):
            nonlocal first
            value=await build(session)
            if first:first=False;raise RuntimeError('earlier attempt failed')
            return value
        async def late_record(session,attempted_at):
            await self.refresh()
            await record(session,attempted_at)
        with patch.object(storage,'build_snapshot',fail_once),patch.object(storage,'record_failure',late_record):
            with self.assertRaisesRegex(RuntimeError,'earlier attempt failed'):await self.refresh()
        result=await self.read();self.assertIsNone(result['refresh_error']);self.assertFalse(result['stale'])
        self.assertEqual(result['titled_games'],4);await self.assert_unlocked()

    async def test_failure_reporting_does_not_replace_original_exception(self):
        with patch.object(storage,'build_snapshot',AsyncMock(side_effect=ValueError('original failure'))), \
             patch.object(storage,'record_failure',AsyncMock(side_effect=OSError('report failed'))):
            with self.assertRaisesRegex(ValueError,'original failure'):await self.refresh()
        await self.assert_unlocked()

    async def test_concurrent_refresh_is_nonblocking_and_does_not_publish_twice(self):
        entered,release=asyncio.Event(),asyncio.Event();build=storage.build_snapshot
        async def pause(session):
            value=await build(session);entered.set();await release.wait();return value
        with patch.object(storage,'build_snapshot',pause):
            first=self.start(self.refresh())
            try:
                await asyncio.wait_for(entered.wait(),2)
                self.assertEqual(await asyncio.wait_for(self.refresh(),2),{'status':'already_refreshing'})
                self.assertIsNone((await self.snapshot())['refreshed_at'])
            finally:release.set()
            self.assertEqual((await asyncio.wait_for(first,5))['status'],'complete')
        await self.assert_unlocked()

    async def test_repeated_cancellation_rolls_back_snapshot_and_releases_advisory_lock(self):
        await self.refresh();before=await self.snapshot()
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4));publish=storage.publish
        async def pause(*args):
            await publish(*args);entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=closing;return session
        with patch.object(storage,'publish',pause):
            task=self.start(home.refresh_home_summary(session_factory=factory))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2)
                for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
        self.assertEqual(await self.snapshot(),before);await self.assert_unlocked()

    async def test_reads_are_readonly_and_missing_cache_is_not_implicit_success(self):
        original=storage.read_snapshot
        async def write_attempt(session):
            await original(session)
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            await session.execute(text('DELETE FROM public.home_titled_summary'))
        with patch.object(storage,'read_snapshot',write_attempt),self.assertRaises(DBAPIError):await self.read()
        self.redis.enqueue_job.assert_not_awaited()
        await self.db.execute('DELETE FROM home_titled_summary')
        with self.assertRaises(NoResultFound):await self.read()
        with self.assertRaises(NoResultFound):await self.refresh()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM home_titled_summary'),0)
        await self.assert_unlocked()

    async def test_missing_source_revision_prevents_untracked_structural_writes(self):
        await self.db.execute('DELETE FROM home_summary_source')
        with self.assertRaisesRegex(asyncpg.RaiseError,'home summary source is missing'):
            await self.db.execute("INSERT INTO player(player_name) VALUES('must-rollback')")
        self.assertFalse(await self.db.fetchval("SELECT EXISTS(SELECT 1 FROM player WHERE player_name='must-rollback')"))
        with self.assertRaises(NoResultFound):await self.read()
        self.redis.enqueue_job.assert_not_awaited()

    async def test_debounce_queue_failure_and_real_route_contract(self):
        now=datetime.now(timezone.utc)
        self.assertFalse(home.needs_refresh({'attempted_at':now,'last_error':'failed'},now))
        self.assertTrue(home.needs_refresh({'attempted_at':now-timedelta(minutes=6),'last_error':'failed'},now))
        self.redis.enqueue_job.side_effect=RuntimeError('queue unavailable')
        result=await self.read();self.assertIsNone(result['total_fens']);self.assertTrue(result['stale'])
        self.assertEqual(result['refresh_error'],'Summary refresh unavailable; try again later.')
        self.assertIsNone((await self.snapshot())['last_error'])
        self.redis.enqueue_job.side_effect=None;await self.refresh()
        with patch.object(home,'AsyncDBSession',self.factory):
            response=await games.api_get_home_summary(redis=self.redis)
        self.assertEqual(json.loads(response.body),await self.read())

    async def test_invalid_half_units_preserve_previous_snapshot(self):
        await self.refresh();before=await self.read();build=storage.build_snapshot
        async def broken(session):
            snapshot=await build(session);snapshot['players'][0]['game_half_units']+=1;return snapshot
        with patch.object(storage,'build_snapshot',broken),self.assertRaisesRegex(ValueError,'contributions'):
            await self.refresh()
        after=await self.read()
        self.assertEqual(after['players'],before['players']);self.assertEqual(after['titled_games'],before['titled_games'])
        self.assertIsNotNone(after['refresh_error']);await self.assert_unlocked()
