"""Canonical player metadata: real transactions, invalidation and atomic publication."""
import asyncio
from collections import Counter
from datetime import date, datetime, timezone
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_metadata as store
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations import player_analytics_summary as summaries
from chessism_api.operations.player_catalog_snapshot import catalog_snapshot_page
from chessism_api.routers import games
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL


# Independent source records: both sides, UTC times, modes, ratings, results, seconds.
GAMES = (
    (101,'alice','bob','blitz','2025-03-09T06:59:00+00:00',2003,2100,1,0,360),
    (105,'alice','alice','blitz','2025-03-09T07:01:00+00:00',2010,1999,.5,.5,540),
    (106,'bob','alice','bullet','2025-03-10T01:00:00+00:00',1900,2020,0,1,54),
    (107,'alice','bob','rapid','2025-03-10T03:00:00+00:00',2050,2000,0,1,18),
    (108,'alice','bob',None,'2025-03-11T04:00:00+00:00',2060,2200,1,0,60),
)


class Cache:
    def __init__(self):self.data={}
    async def get(self,key):return self.data.get(key)
    async def set(self,key,value,**kwargs):self.data[key]=value


class CanonicalPlayerMetadataTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        fixture=await seed(self.db)
        self.ids={'alice':fixture['white'],'bob':fixture['black']}
        for name,identity in self.ids.items():
            await self.db.execute("UPDATE player SET player_name=$2,joined=1,timezone='America/New_York',timezone_source='user' WHERE id=$1",identity,name)
        for name in ('empty','carol'):
            self.ids[name]=await self.db.fetchval("INSERT INTO player(player_name,joined,timezone) VALUES($1,1,'UTC') RETURNING id",name)
        self.mode_ids={mode:await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',mode) for mode in ('bullet','blitz','rapid')}
        for link,w,b,mode,stamp,wr,br,ws,bs,seconds in GAMES:
            at=datetime.fromisoformat(stamp)
            if link not in (101,105):await self.db.execute(GAME_SQL,link,self.ids[w],self.ids[b],at,2,'chess',None)
            await self.db.execute('''UPDATE game SET played_at=$2,mode_id=$3,white_elo=$4,black_elo=$5,
                white_result=$6,black_result=$7,time_elapsed=$8,eco='https://www.chess.com/openings/Test-Opening' WHERE link=$1''',
                link,at,self.mode_ids.get(mode),wr,br,ws,bs,seconds)
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,
            port=55432,query={'host':self.socket}),connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.enabled=patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'});self.enabled.start();self.addCleanup(self.enabled.stop)
        self.tasks=[];self.addAsyncCleanup(self.stop_tasks)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)

    def start(self,work):
        task=asyncio.create_task(work);self.tasks.append(task);return task

    async def refresh(self,player='alice',**kwargs):
        return await summaries.refresh_player(player,session_factory=self.factory,**kwargs)

    async def saved(self,section='hours',player='alice'):
        async with read_transaction(self.factory) as session:return await summaries.read_snapshot(session,player,section)

    async def state(self,player='alice'):
        return dict(await self.db.fetchrow('SELECT * FROM player_analytics_state WHERE player_id=$1',self.ids[player]))

    async def snapshots(self):
        return await self.db.fetch('SELECT * FROM player_analytics_snapshot ORDER BY player_id,section')

    async def build_all(self):
        for player in self.ids:await self.refresh(player)

    async def assert_unlocked(self,player='alice'):
        async with self.db.transaction():
            self.assertTrue(await self.db.fetchval('SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))','player-analytics:'+str(self.ids[player])))
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_empty_missing_and_repeated_refresh(self):
        result=await self.refresh('empty')
        self.assertEqual(result,{'status':'ready','days':0,'remaining':0})
        saved=await self.saved(player='empty');self.assertEqual(saved['modes'],{});self.assertFalse(saved['summary_stale'])
        before=await self.snapshots();self.assertEqual(await self.refresh('empty'),{'status':'ready'})
        self.assertEqual(await self.snapshots(),before);self.assertEqual(await self.refresh('missing'),{'status':'ready'})
        self.assertFalse(await canonical.initialize(self.db));self.assertEqual(await self.snapshots(),before)
        self.assertEqual(await summaries.get_latest_game('missing',session_factory=self.factory),{'player_name':'missing','played_at':None})

    async def test_sections_match_independent_counts_dst_and_selfplay_oracles(self):
        await self.refresh()
        hours=await self.saved();self.assertEqual(hours['modes'],{
            'blitz':{'games':3,'seconds':1440},'bullet':{'games':1,'seconds':54},
            'rapid':{'games':1,'seconds':18},'null':{'games':1,'seconds':60}})
        self.assertEqual(hours['latest_played_at'],'2025-03-11T04:00:00+00:00')
        activity=(await self.saved('activity_daily'))['rows']
        self.assertEqual(sum(r['games'] for r in activity),6)
        self.assertEqual({r['hour'] for r in activity if r['mode']=='blitz'},{1,3})
        self.assertEqual(sum(r['draws'] for r in activity),2)
        ratings=(await self.saved('ratings'))['rows']
        self.assertEqual({r['mode']:(r['date'],r['rating']) for r in ratings},
            {'blitz':('2025-03-09',1999),'bullet':('2025-03-09',2020),'rapid':('2025-03-09',2050)})
        distribution=(await self.saved('distribution'))['counts']
        self.assertEqual(Counter({(r['mode'],r['rating']):r['games'] for r in distribution}),
            Counter({('blitz',2003):1,('blitz',2010):1,('blitz',1999):1,('bullet',2020):1,('rapid',2050):1}))
        for section in ('opponents','openings'):
            rows=(await self.saved(section))['rows'];self.assertEqual(sum(r['games'] for r in rows),5)
        self.assertEqual({r['key'] for r in (await self.saved('opponents'))['rows']},{'alice','bob'})
        snapshots=await self.db.fetch('SELECT section,revision FROM player_analytics_snapshot WHERE player_id=$1',self.ids['alice'])
        self.assertEqual(len(snapshots),7);self.assertEqual(len({r['revision'] for r in snapshots}),1)

    async def test_bounded_batches_keep_previous_complete_snapshot(self):
        # Seed games originally lived on Jan 1, 2026. Moving them in setUp must
        # also rebuild that now-empty date; it cannot silently disappear.
        dirty=await self.db.fetch('SELECT day FROM player_analytics_dirty WHERE player_id=$1 ORDER BY day',self.ids['alice'])
        self.assertEqual([r['day'] for r in dirty],[date(2025,3,9),date(2025,3,10),date(2025,3,11),date(2026,1,1)])
        first=await self.refresh(batch_days=1);self.assertEqual(first,{'status':'building','days':1,'remaining':3})
        self.assertIsNone(await self.saved())
        self.assertEqual((await self.refresh(batch_days=1))['remaining'],2)
        self.assertIsNone(await self.saved())
        self.assertEqual((await self.refresh(batch_days=1))['remaining'],1)
        self.assertIsNone(await self.saved())
        self.assertEqual((await self.refresh(batch_days=1))['status'],'ready')
        old_day=json.loads(await self.db.fetchval('SELECT payload FROM player_analytics_partition WHERE player_id=$1 AND day=$2',self.ids['alice'],date(2026,1,1)))
        self.assertEqual(old_day['hours'],[])
        before=await self.snapshots()
        await self.db.execute("UPDATE player SET timezone='Asia/Kolkata' WHERE player_name='alice'")
        self.assertEqual((await self.refresh(batch_days=1))['status'],'building')
        self.assertEqual(await self.snapshots(),before);self.assertTrue((await self.saved())['summary_stale'])
        await self.refresh();self.assertFalse((await self.saved())['summary_stale'])
        self.assertEqual((await self.saved())['time_context']['timezone'],'Asia/Kolkata')

    async def test_incremental_correction_replaces_only_its_day(self):
        await self.refresh()
        before={r['day']:r['payload'] for r in await self.db.fetch('SELECT day,payload FROM player_analytics_partition WHERE player_id=$1',self.ids['alice'])}
        await self.db.execute('UPDATE game SET white_elo=2500,time_elapsed=720 WHERE link=101')
        self.assertEqual((await self.refresh())['days'],1)
        after={r['day']:r['payload'] for r in await self.db.fetch('SELECT day,payload FROM player_analytics_partition WHERE player_id=$1',self.ids['alice'])}
        self.assertEqual(sum(before[k]!=after[k] for k in before),1)
        self.assertEqual((await self.saved())['modes']['blitz']['seconds'],1800)
        self.assertIn(2500,[r['rating'] for r in (await self.saved('distribution'))['counts']])
        saved=await self.snapshots();await self.refresh();self.assertEqual(await self.snapshots(),saved)

    async def test_date_moves_and_deletion_remove_old_contributions(self):
        await self.refresh()
        await self.db.execute("UPDATE game SET played_at='2025-03-15T00:00:00Z' WHERE link=108")
        self.assertEqual((await self.refresh())['days'],2)
        self.assertEqual((await self.saved())['latest_played_at'],'2025-03-15T00:00:00+00:00')
        await self.db.execute('DELETE FROM game WHERE link=108')
        self.assertEqual((await self.refresh())['days'],1)
        saved=await self.saved();self.assertNotIn('null',saved['modes'])
        self.assertEqual(saved['latest_played_at'],'2025-03-10T03:00:00+00:00')

    async def test_eligibility_and_ownership_updates_invalidate_both_sides(self):
        await self.build_all()
        await self.db.execute('DELETE FROM game_fen_association WHERE game_link=105')
        await self.db.execute("UPDATE game SET n_moves=0 WHERE link=105;UPDATE game SET rules='chess' WHERE link=102")
        await self.db.execute('UPDATE game SET black_player_id=$1 WHERE link=106',self.ids['carol'])
        for player in ('alice','bob','carol'):self.assertTrue((await self.saved(player=player))['summary_stale'])
        await self.build_all()
        self.assertEqual((await self.saved())['modes']['blitz']['games'],1)
        self.assertNotIn('bullet',(await self.saved())['modes'])
        self.assertEqual((await self.saved(player='carol'))['modes']['bullet']['games'],1)
        self.assertEqual((await self.saved())['modes']['null']['games'],2)

    async def test_nonmetadata_updates_and_duplicate_inserts_do_not_dirty_state(self):
        await self.build_all();before=await self.db.fetch('SELECT * FROM player_analytics_state ORDER BY player_id')
        await self.db.execute("UPDATE game SET fens_done=true WHERE is_standard AND n_moves>0;UPDATE player SET avatar='changed',title='GM'")
        await self.db.execute("UPDATE fen SET score=0,analysis_source='stockfish',analyzed_at=now()")
        await self.db.execute(GAME_SQL+' ON CONFLICT DO NOTHING',101,self.ids['alice'],self.ids['bob'],
                              datetime(2025,3,9,tzinfo=timezone.utc),2,'chess',None)
        self.assertEqual(await self.db.fetch('SELECT * FROM player_analytics_state ORDER BY player_id'),before)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_analytics_dirty'),0)

    async def test_rollback_retains_state_and_mode_rename_invalidates(self):
        await self.build_all();before=await self.state()
        transaction=self.db.transaction();await transaction.start()
        await self.db.execute("UPDATE player SET timezone='UTC' WHERE player_name='alice';UPDATE game SET time_elapsed=1 WHERE link=101")
        self.assertGreater((await self.state())['revision'],before['revision']);await transaction.rollback()
        self.assertEqual(await self.state(),before)
        await self.db.execute("UPDATE game_mode SET name='daily' WHERE name='blitz'")
        self.assertTrue((await self.saved())['summary_stale']);await self.refresh()
        self.assertEqual((await self.saved())['modes']['daily']['games'],3)
        self.assertNotIn('blitz',{r['mode'] for r in (await self.saved('distribution'))['counts']})

    async def test_rename_retains_identity_and_rebuilds_opponent_catalog(self):
        await self.build_all()
        await self.db.execute("UPDATE player SET player_name='renamed' WHERE player_name='alice'")
        self.assertIsNone(await self.saved());saved=await self.saved(player='renamed')
        self.assertEqual(saved['_player_id'],self.ids['alice']);self.assertTrue(saved['summary_stale'])
        self.assertTrue((await self.saved(player='bob'))['summary_stale'])
        await self.refresh('bob');await self.refresh('renamed')
        self.assertEqual({r['key'] for r in (await self.saved('opponents','bob'))['rows']},{'renamed'})
        self.assertEqual({r['key'] for r in (await self.saved('opponents','renamed'))['rows']},{'renamed','bob'})

    async def test_demotion_and_physical_deletion_cannot_leave_owned_snapshots(self):
        await self.build_all()
        await self.db.execute("UPDATE player SET joined=0 WHERE player_name='alice'")
        self.assertIsNone(await self.saved());self.assertEqual(await self.refresh(),{'status':'ineligible'})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_analytics_snapshot WHERE player_id=$1',self.ids['alice']),0)
        await self.db.execute("UPDATE player SET joined=1 WHERE player_name='alice'");await self.refresh()
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='alice'")
        self.assertIsNone(await self.saved());self.assertEqual(await self.refresh(),{'status':'ineligible'})
        await self.db.execute("UPDATE player SET deleted_at=NULL WHERE player_name='alice'");await self.refresh()
        async with self.db.transaction():
            await self.db.execute('DELETE FROM game WHERE white_player_id=$1 OR black_player_id=$1',self.ids['alice'])
            await self.db.execute('DELETE FROM months WHERE player_id=$1',self.ids['alice'])
            await self.db.execute('DELETE FROM player WHERE id=$1',self.ids['alice'])
        for suffix in ('state','dirty','partition','snapshot'):
            self.assertEqual(await self.db.fetchval(f'SELECT count(*) FROM player_analytics_{suffix} WHERE player_id=$1',self.ids['alice']),0)
        self.assertTrue((await self.saved(player='bob'))['summary_stale'])

    async def test_foreign_keys_and_missing_required_state_fail_closed(self):
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute("INSERT INTO player_analytics_partition VALUES(999999,'2025-01-01','{}')")
        await self.db.execute('DELETE FROM player_analytics_state WHERE player_id=$1',self.ids['alice'])
        with self.assertRaisesRegex(RuntimeError,'state is missing'):await self.refresh()
        self.assertIsNone(await self.saved());await self.assert_unlocked()

    async def test_cached_hours_and_latest_routes_match_live_readonly_fallback(self):
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'0'}):
            live=await summaries.get_player_hours_played('alice',session_factory=self.factory)
            latest=await summaries.get_latest_game('alice',session_factory=self.factory)
        self.assertEqual(live,dict(player_name='alice',total_hours=.44,bullet_games=1,blitz_games=3,rapid_games=1,bullet_hours=.02,blitz_hours=.4,rapid_hours=.01))
        await self.refresh()
        saved=await summaries.get_player_hours_played('alice',session_factory=self.factory)
        self.assertEqual({k:saved[k] for k in live},live)
        self.assertEqual((await summaries.get_latest_game('alice',session_factory=self.factory))['played_at'],latest['played_at'])
        await self.db.execute('ALTER TABLE game RENAME TO hidden_games')
        with patch.object(summaries,'AsyncDBSession',self.factory):
            response=await games.api_get_player_hours_played('ALICE')
        self.assertEqual(json.loads(response.body),saved)
        self.assertEqual((await summaries.get_latest_game('alice',session_factory=self.factory))['played_at'],latest['played_at'])
        with self.assertRaises(DBAPIError):await self.refresh_after_dirty()
        retained=await summaries.get_player_hours_played('alice',session_factory=self.factory)
        self.assertEqual(retained,{**saved,'summary_stale':True})

    async def test_truncate_marks_old_partitions_dirty_and_publishes_empty_counts(self):
        await self.build_all();before=await self.snapshots()
        await self.db.execute('TRUNCATE game CASCADE')
        self.assertEqual(await self.snapshots(),before);self.assertTrue((await self.saved())['summary_stale'])
        await self.refresh();saved=await self.saved()
        self.assertEqual(saved['modes'],{});self.assertIsNone(saved['latest_played_at'])
        self.assertFalse(saved['summary_stale']);self.assertEqual((await self.saved('opponents'))['rows'],[])

    async def refresh_after_dirty(self):
        await self.db.execute('UPDATE player_analytics_state SET revision=revision+1,full_rebuild=true WHERE player_id=$1',self.ids['alice'])
        return await self.refresh()

    async def test_readonly_rejects_writes_and_missing_source_errors_propagate(self):
        original=store.live_hours
        async def write(session,player):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            await original(session,player)
            await session.execute(text('DELETE FROM player_analytics_state'))
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'0'}),patch.object(store,'live_hours',write):
            with self.assertRaises(DBAPIError):await summaries.get_player_hours_played('alice',session_factory=self.factory)
        await self.assert_unlocked()
        await self.db.execute('ALTER TABLE player_analytics_state RENAME TO hidden_state')
        with self.assertRaises(DBAPIError):await summaries.refresh_pending(session_factory=self.factory)

    async def test_concurrent_refresh_returns_busy_and_preserves_snapshot(self):
        entered,release=asyncio.Event(),asyncio.Event();original=store.day_rows
        async def pause(*args):
            rows=await original(*args);entered.set();await release.wait();return rows
        with patch.object(store,'day_rows',pause):
            task=self.start(self.refresh())
            try:
                await asyncio.wait_for(entered.wait(),2)
                self.assertEqual(await asyncio.wait_for(self.refresh(),2),{'status':'busy'})
                self.assertIsNone(await self.saved())
            finally:release.set()
            self.assertEqual((await asyncio.wait_for(task,5))['status'],'ready')
        await self.assert_unlocked()

    async def test_concurrent_invalidation_rejects_partial_publication_and_retries(self):
        await self.refresh();original=store.day_rows
        for rebuild in (False,True):
            with self.subTest(full_rebuild=rebuild):
                await self.db.execute('UPDATE game SET white_elo=white_elo+1 WHERE link=101')
                if rebuild:await self.db.execute("UPDATE player SET timezone='UTC' WHERE player_name='alice'")
                before=await self.snapshots();changed=False
                async def invalidate(*args):
                    nonlocal changed
                    rows=await original(*args)
                    if not changed:
                        changed=True
                        await self.db.execute('UPDATE game SET white_elo=white_elo+1 WHERE link=101')
                    return rows
                with patch.object(store,'day_rows',invalidate),self.assertRaises(DBAPIError) as caught:await self.refresh()
                self.assertEqual(caught.exception.orig.sqlstate,'40001')
                self.assertEqual(await self.snapshots(),before)
                self.assertTrue((await self.saved())['summary_stale'])
                await self.refresh();self.assertFalse((await self.saved())['summary_stale']);await self.assert_unlocked()

    async def test_writer_waiting_for_publication_retains_its_invalidation(self):
        await self.refresh();await self.db.execute('UPDATE game SET white_elo=2500 WHERE link=101')
        entered,release=asyncio.Event(),asyncio.Event();original=store.publish
        connection=await self.connect();pid=await connection.fetchval('SELECT pg_backend_pid()')
        async def pause(*args):await original(*args);entered.set();await release.wait()
        with patch.object(store,'publish',pause):
            refresh=self.start(self.refresh())
            try:
                await asyncio.wait_for(entered.wait(),2)
                writer=self.start(connection.execute('UPDATE game SET white_elo=2600 WHERE link=101'))
                async def blocked():
                    for _ in range(100):
                        if await self.db.fetchval("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",pid):return
                        await asyncio.sleep(.01)
                    self.fail('Writer never reached the publication lock')
                await asyncio.wait_for(blocked(),2);self.assertFalse(writer.done())
            finally:release.set()
            await asyncio.wait_for(refresh,5);await asyncio.wait_for(writer,5)
        self.assertTrue((await self.saved())['summary_stale']);await self.refresh()
        self.assertIn(2600,[r['rating'] for r in (await self.saved('distribution'))['counts']]);await self.assert_unlocked()

    async def test_publish_failure_rolls_back_all_sections_partitions_and_dirty_days(self):
        await self.refresh();await self.db.execute('UPDATE game SET white_elo=2500 WHERE link=101')
        before=await self.snapshots();state=await self.state()
        parts=await self.db.fetch('SELECT * FROM player_analytics_partition ORDER BY player_id,day')
        dirty=await self.db.fetch('SELECT * FROM player_analytics_dirty ORDER BY player_id,day');original=store.publish
        async def fail(*args):await original(*args);raise ValueError('publication failed')
        with patch.object(store,'publish',fail),self.assertRaisesRegex(ValueError,'publication failed'):await self.refresh()
        self.assertEqual(await self.snapshots(),before);self.assertEqual(await self.state(),state)
        self.assertEqual(await self.db.fetch('SELECT * FROM player_analytics_partition ORDER BY player_id,day'),parts)
        self.assertEqual(await self.db.fetch('SELECT * FROM player_analytics_dirty ORDER BY player_id,day'),dirty)
        await self.assert_unlocked()

    async def test_repeated_cancellation_drains_rollback_and_closure(self):
        await self.refresh();await self.db.execute('UPDATE game SET white_elo=2500 WHERE link=101')
        before=await self.snapshots();state=await self.state();original=store.publish
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        async def pause(*args):await original(*args);entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=closing;return session
        with patch.object(store,'publish',pause):
            task=self.start(summaries.refresh_player('alice',session_factory=factory))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
        self.assertEqual(await self.snapshots(),before);self.assertEqual(await self.state(),state);await self.assert_unlocked()

    async def test_pending_failure_backoff_and_late_failure_after_success(self):
        await self.build_all();await self.db.execute('UPDATE game SET white_elo=2500 WHERE link=101')
        # Remove Bob's pending work so the polling result has one exact identity.
        await self.refresh('bob');before=await self.snapshots()
        with patch.object(store,'day_rows',AsyncMock(side_effect=ValueError('failure'))):
            self.assertEqual(await summaries.refresh_pending(session_factory=self.factory),{'player':'alice','status':'retrying'})
        self.assertEqual(await self.snapshots(),before);self.assertIsNotNone((await self.state())['error'])
        self.assertIsNone(await summaries.refresh_pending(session_factory=self.factory))
        await self.db.execute("UPDATE player_analytics_state SET attempted_at=now()-interval '1 minute' WHERE player_id=$1",self.ids['alice'])
        original_refresh=summaries.refresh_player;original_record=store.record_failure
        async def late_record(session,pending):
            await original_refresh('alice',session_factory=self.factory)
            await original_record(session,pending)
        with patch.object(summaries,'refresh_player',AsyncMock(side_effect=RuntimeError('older attempt'))),patch.object(store,'record_failure',late_record):
            self.assertEqual(await summaries.refresh_pending(session_factory=self.factory),{'player':'alice','status':'retrying'})
        self.assertIsNone((await self.state())['error']);self.assertFalse((await self.saved())['summary_stale']);await self.assert_unlocked()

    async def test_manual_metadata_ownership_and_missing_snapshot_placeholder(self):
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cuda_summary_control'),1)
        async with read_transaction(self.factory) as session:
            self.assertFalse(await store.manual_metadata(session))
        # Match the existing metadata installer's extension of the initialized
        # control singleton; never replace the canonical table in this test.
        await self.db.execute('''ALTER TABLE cuda_summary_control ADD COLUMN metadata boolean NOT NULL DEFAULT false;
            UPDATE cuda_summary_control SET manual=true,metadata=true''')
        before=await self.state()
        self.assertEqual(await self.refresh(),{'status':'awaiting_cuda'})
        self.assertIsNone(await summaries.refresh_pending(session_factory=self.factory));self.assertEqual(await self.state(),before)
        saved=await self.saved();self.assertTrue(saved['manual']);self.assertTrue(saved['summary_pending']);self.assertEqual(saved['modes'],{})
        await self.db.execute('UPDATE cuda_summary_control SET manual=false');await self.refresh()
        self.assertFalse((await self.saved())['summary_stale']);await self.assert_unlocked()

    async def test_catalog_cache_identity_changes_after_delete_and_recreate(self):
        await self.refresh();old_revision=(await self.saved())['summary_revision'];cache=Cache()
        scope=SimpleNamespace(player='alice',response_base=lambda:{'player_name':'alice','mode':'all'})
        async def page():
            async with read_transaction(self.factory) as session:
                return await catalog_snapshot_page(session,cache,scope,kind='openings',mode='all',color='all',search='',page=1,limit=50)
        first=await page();self.assertEqual(first['total_games'],5);old_keys=set(cache.data)
        async with self.db.transaction():
            await self.db.execute('DELETE FROM game WHERE white_player_id=$1 OR black_player_id=$1',self.ids['alice'])
            await self.db.execute('DELETE FROM months WHERE player_id=$1',self.ids['alice']);await self.db.execute('DELETE FROM player WHERE id=$1',self.ids['alice'])
        new=await self.db.fetchval("INSERT INTO player(player_name,joined) VALUES('alice',1) RETURNING id")
        self.assertNotEqual(new,self.ids['alice'])
        # Same public name and revision; only canonical identity differentiates the cache.
        await self.db.execute('UPDATE player_analytics_state SET revision=$2 WHERE player_id=$1',new,old_revision)
        await self.refresh();second=await page();self.assertEqual(second['rows'],[])
        self.assertEqual(second['summary_revision'],first['summary_revision']);self.assertEqual(len(set(cache.data)-old_keys),1)

    async def test_batch_size_validation_does_not_open_a_session(self):
        factory=AsyncMock()
        for size in (0,-1,True,1.5):
            with self.assertRaises(ValueError):await summaries.refresh_player('alice',batch_days=size,session_factory=factory)
        factory.assert_not_called()
