"""Metadata charts checked against game records, using only disposable PostgreSQL."""
import asyncio
import base64
from datetime import date, datetime, timedelta
import json
import os
import re
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_activity_reads as reads
from chessism_api.operations import player_chart_games as chart, player_hero_analytics as hero
from chessism_api.operations import player_analytics_summary as snapshots, player_game_catalog as catalog
from chessism_api.operations.player_game_explorer import explore_player_games
from chessism_api.operations.player_timezone import ResolvedPlayerTimezone
from chessism_api.routers import players
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class Cache:
    async def get(self, key): return None
    async def set(self, *args, **kwargs): pass


class CanonicalActivityTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = {name: await self.db.fetchval(
            "INSERT INTO player(player_name,joined,timezone) VALUES($1,$2,'America/New_York') RETURNING id",
            name, 0 if name == 'shell' else 1) for name in ('alice', 'bob', 'carol', 'empty', 'shell')}
        self.modes = {name: await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id', name)
                      for name in ('bullet', 'blitz', 'rapid', 'daily')}
        self.records = []
        await self.add([
            self.game(1, stamp='2025-03-09T04:59:59+00:00'),
            self.game(2, white='bob', black='alice', wr=1800, br=1005, result=.5, stamp='2025-03-09T05:00:00+00:00'),
            self.game(3, black='alice', wr=1009, br=1019),
            self.game(4, black='carol', wr=1010, result=0, mode='rapid', eco='no_eco'),
            self.game(5, wr=0, result=.5, stamp='2025-03-10T03:59:59+00:00'),
            self.game(6, wr=-1, mode='bullet', stamp='2025-03-10T04:00:00+00:00'),
            self.game(7, mode='daily', wr=1300, stamp='2025-03-09T12:00:00+00:00'),
            self.game(8, white='bob', black='alice', br=1400, result=0, mode=None, stamp='2025-03-09T12:00:00+00:00'),
            self.game(9, rules='chess960'), self.game(10, moves=0),
            self.game(11, setup='8/8/8/8/8/4K3/8/6k1 w - -'),
            self.game(12, wr=1500, stamp='2025-11-02T05:30:00+00:00'),
            self.game(13, wr=1510, stamp='2025-11-02T06:30:00+00:00'),
            self.game(14, wr=1600, stamp='2025-03-11T12:00:00+00:00'),
            self.game(15, black='alice', wr=1605, br=1615, result=.5, stamp='2025-03-11T12:00:00+00:00'),
        ])
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test', database=self.name,
            port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        flag = patch.dict(os.environ, {'CHESSISM_PLAYER_SUMMARIES': '0'})
        flag.start(); self.addCleanup(flag.stop)
        self.tasks = []; self.addAsyncCleanup(self.stop_tasks)

    def game(self, link, *, white='alice', black='bob', wr=1000, br=2000, result=1, mode='blitz',
             rules='chess', moves=2, setup=None, stamp='2025-03-09T07:01:00+00:00',
             eco=' https://WWW.chess.com/openings/Test-Opening '):
        return dict(link=link, white=white, black=black, wr=wr, br=br, result=result, mode=mode,
                    rules=rules, moves=moves, setup=setup, stamp=stamp, eco=eco)

    async def add(self, games):
        columns = ('link', 'white_player_id', 'black_player_id', 'mode_id', 'played_at', 'white_elo', 'black_elo',
                   'white_result', 'black_result', 'white_str_result', 'black_str_result', 'time_control', 'eco',
                   'time_elapsed', 'n_moves', 'rules', 'initial_setup')
        await self.db.copy_records_to_table('game', columns=columns, records=[(
            g['link'], self.ids[g['white']], self.ids[g['black']], self.modes.get(g['mode']),
            datetime.fromisoformat(g['stamp']), g['wr'], g['br'], g['result'], 1-g['result'],
            'recorded', 'recorded', '600', g['eco'], 60, g['moves'], g['rules'], g['setup']) for g in games])
        self.records.extend(games)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def start(self, work):
        task = asyncio.create_task(work); self.tasks.append(task); return task

    def rows(self, mode='all', start=None, end=None, zone='America/New_York'):
        rows = []
        for g in self.records:
            if g['rules'] != 'chess' or g['setup'] or not g['moves']: continue
            utc = datetime.fromisoformat(g['stamp']); local = utc.astimezone(ZoneInfo(zone))
            if (mode != 'all' and mode != g['mode']) or (start and local.date() < start) or (end and local.date() > end): continue
            for owner, opponent, color, rating, opposite, score in (
                (g['white'], g['black'], 'white', g['wr'], g['br'], g['result']),
                (g['black'], g['white'], 'black', g['br'], g['wr'], 1-g['result'])):
                if owner != 'alice': continue
                opening = re.sub(r'^https?://(www\.)?chess\.com/openings/', '', g['eco'].strip(), flags=re.I)
                rows.append(dict(link=g['link'], color=color, mode=g['mode'], rating=rating, opponent=opponent,
                    opponent_rating=opposite, result=score, utc=utc, local=local, opening='' if opening == 'no_eco' else opening))
        return rows

    async def activity(self, **kwargs):
        return await hero.get_player_behavioural_activity(kwargs.pop('player', 'alice'), session_factory=kwargs.pop('session_factory', self.factory), **kwargs)

    async def ratings(self, **kwargs):
        return await hero.get_player_behavioural_ratings(kwargs.pop('player', 'alice'), session_factory=kwargs.pop('session_factory', self.factory), **kwargs)

    async def day(self, **kwargs):
        return await hero.get_player_behavioural_day(kwargs.pop('player', 'alice'), kwargs.pop('target_date', date(2025,3,9)),
            session_factory=kwargs.pop('session_factory', self.factory), **kwargs)

    async def page(self, **kwargs):
        defaults = dict(scope_kind='result', modes=['bullet','blitz','rapid'], rating_from=None, result='win',
                        timezone_name=None, limit=100, cursor=None, session_factory=self.factory)
        defaults.update(kwargs)
        return await chart.explore_chart_games(defaults.pop('player', 'alice'), **defaults)

    def assert_buckets(self, output, rows):
        self.assertEqual(output['total_games'], len(rows))
        for key, size in (('weekdays',7), ('hours',24), ('weekday_hours',168)):
            self.assertEqual(len(output[key]), size)
            for bucket in output[key]:
                selected = [r for r in rows if ('weekday' not in bucket or r['local'].isoweekday() == bucket['weekday'])
                            and ('hour' not in bucket or r['local'].hour == bucket['hour'])]
                self.assertEqual(bucket['games'], len(selected))
                self.assertEqual([bucket[n] for n in ('wins','draws','losses')],
                                 [sum(r['result'] == value for r in selected) for value in (1,.5,0)])
                self.assertEqual(bucket['proportion'], round(len(selected)/len(rows),8) if rows else 0)

    async def test_activity_all_modes_local_windows_and_eligibility(self):
        for mode in ('all','bullet','blitz','rapid'):
            for start, end in ((None,None),(date(2025,3,9),date(2025,3,9)),(date(2025,3,10),None),(None,date(2025,3,8))):
                with self.subTest(mode=mode, start=start, end=end):
                    output = await self.activity(mode=mode, date_from=start, date_to=end)
                    expected = self.rows(mode,start,end); self.assert_buckets(output, expected)
                    for selected in ('bullet','blitz','rapid'):
                        self.assert_buckets(output['by_mode'][selected], [r for r in expected if r['mode']==selected])
        self.assertGreater((await self.activity())['total_games'], sum(len(self.rows(m)) for m in ('bullet','blitz','rapid')))

    async def test_day_dst_boundaries_repeated_hours_and_selfplay_order(self):
        for target in (date(2025,3,9), date(2025,11,2), date(2025,3,11)):
            output = await self.day(target_date=target); rows = self.rows(start=target,end=target)
            self.assertEqual(output['games'],len(rows)); self.assertEqual(len(output['hours']),24)
            rows.sort(key=lambda r:(r['utc'],r['link'],r['color']=='black'))
            for bucket in output['hours']:
                selected = [r for r in rows if r['local'].hour == bucket['hour']]
                self.assertEqual(bucket['games'],len(selected))
                self.assertEqual([bucket[n] for n in ('wins','draws','losses')],
                                 [sum(r['result']==value for r in selected) for value in (1,.5,0)])
                self.assertEqual(bucket['ratings'], [dict(game_id=r['link'],mode=r['mode'] or 'unknown',rating=r['rating'],
                    played_at_utc=r['utc'].isoformat(),played_at_local=r['local'].isoformat()) for r in selected])
                self.assertEqual(bucket['last_rating'],selected[-1]['rating'] if selected else None)
            if target.month==11:
                self.assertEqual(output['hours'][1]['games'],2)
                self.assertNotEqual(output['hours'][1]['ratings'][0]['played_at_local'],output['hours'][1]['ratings'][1]['played_at_local'])
            elif target.day==9: self.assertEqual(output['hours'][2]['games'],0)
            else: self.assertEqual(output['hours'][8]['last_rating'],1615)

    async def test_rating_series_dense_gaps_mode_date_link_and_black_ties(self):
        start,end=date(2025,3,8),date(2025,3,12)
        for mode in ('all','bullet','blitz','rapid'):
            output=await self.ratings(mode=mode,date_from=start,date_to=end)
            self.assertEqual([s['mode'] for s in output['series']],list(('bullet','blitz','rapid') if mode=='all' else (mode,)))
            for series in output['series']:
                rows=self.rows(series['mode'],start,end)
                self.assertEqual(len(series['points']),5)
                for offset,point in enumerate(series['points']):
                    day=start+timedelta(days=offset);self.assertEqual(point['date'],day.isoformat())
                    chosen=max((r for r in rows if r['local'].date()==day),key=lambda r:(r['utc'],r['link'],r['color']=='black'),default=None)
                    self.assertEqual({k:v for k,v in point.items() if k!='date'}, dict(
                        last_rating=chosen['rating'] if chosen else None,game_id=chosen['link'] if chosen else None,
                        played_at_utc=chosen['utc'].isoformat() if chosen else None,played_at_local=chosen['local'].isoformat() if chosen else None))

    async def test_mean_country_zones_match_local_grouping_and_day_window(self):
        zones=('America/New_York','America/Chicago');target=date(2025,3,9)
        expected=[]
        for row in self.rows():
            left,right=(row['utc'].astimezone(ZoneInfo(z)).replace(tzinfo=None) for z in zones)
            local=left+(right-left)/2
            if local.date()==target:expected.append({**row,'local':local})
        with patch.object(hero,'resolve_player_timezone',return_value=ResolvedPlayerTimezone(zones,'country',estimated=True)):
            self.assert_buckets(await self.activity(date_from=target,date_to=target),expected)
            output=await self.day(target_date=target)
        self.assertEqual(output['games'],len(expected))
        observations=[r for bucket in output['hours'] for r in bucket['ratings']]
        self.assertEqual(sorted(r['played_at_local'] for r in observations),sorted(r['local'].isoformat() for r in expected))

    async def test_saved_activity_and_sparse_ratings_match_live_observations(self):
        await snapshots.refresh_player('alice',session_factory=self.factory)
        for mode in ('all','blitz','rapid'):
            for dates in ({},dict(date_from=date(2025,3,9),date_to=date(2025,3,11))):
                live_activity=await self.activity(mode=mode,**dates);live_ratings=await self.ratings(mode=mode,**dates)
                with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):
                    saved_activity=await self.activity(mode=mode,**dates);saved_ratings=await self.ratings(mode=mode,**dates)
                for key in ('total_games','weekdays','hours','weekday_hours','by_mode'):
                    self.assertEqual(saved_activity[key],live_activity[key])
                for live,saved in zip(live_ratings['series'],saved_ratings['series']):
                    self.assertEqual(saved['points'],[p for p in live['points'] if p['game_id'] is not None])
                self.assertFalse(saved_activity['summary_stale']);self.assertFalse(saved_ratings['summary_stale'])

    async def test_stale_snapshots_remain_visible_and_day_is_live(self):
        await snapshots.refresh_player('alice',session_factory=self.factory)
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):
            before=await self.activity();ratings=await self.ratings()
            await self.add([self.game(20,wr=1900,stamp='2025-03-09T20:00:00+00:00')])
            saved=await self.activity();after_ratings=await self.ratings()
            self.assertEqual(saved['total_games'],before['total_games']);self.assertTrue(saved['summary_stale'])
            self.assertEqual(after_ratings['series'],ratings['series']);self.assertTrue(after_ratings['summary_stale'])
            self.assertEqual((await self.day())['games'],len(self.rows(start=date(2025,3,9),end=date(2025,3,9))))
        self.assertEqual((await self.activity())['total_games'],before['total_games']+1)

    async def test_changed_timezone_falls_back_unless_manually_owned(self):
        await snapshots.refresh_player('alice',session_factory=self.factory)
        await self.db.execute("UPDATE player SET timezone='UTC' WHERE player_name='alice'")
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):
            self.assert_buckets(await self.activity(),self.rows(zone='UTC'))
            ratings=await self.ratings(date_from=date(2025,3,9),date_to=date(2025,3,9))
            self.assertEqual(ratings['series'][1]['points'][0]['last_rating'],1019)
            await self.db.execute('ALTER TABLE cuda_summary_control ADD COLUMN metadata boolean NOT NULL DEFAULT true; UPDATE cuda_summary_control SET manual=true')
            for output in (await self.activity(),await self.ratings()):
                self.assertTrue(output['manual']);self.assertTrue(output['summary_pending']);self.assertTrue(output['summary_stale'])
                if 'series' in output:self.assertTrue(all(not s['points'] for s in output['series']))
                else:self.assertEqual(output['total_games'],0)

    async def test_manual_missing_snapshots_never_run_live_queries(self):
        await self.db.execute('ALTER TABLE cuda_summary_control ADD COLUMN metadata boolean NOT NULL DEFAULT true; UPDATE cuda_summary_control SET manual=true')
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}), patch.object(reads,'activity',AsyncMock()) as a, patch.object(reads,'ratings',AsyncMock()) as r:
            output=await self.activity();self.assertEqual(output['total_games'],0);self.assertTrue(output['summary_pending'])
            output=await self.ratings();self.assertTrue(all(not s['points'] for s in output['series']));self.assertTrue(output['summary_pending'])
            a.assert_not_awaited();r.assert_not_awaited()

    async def test_empty_missing_deleted_and_shell_profiles(self):
        self.assertEqual((await self.activity(player='empty'))['total_games'],0)
        self.assertTrue(all(not s['points'] for s in (await self.ratings(player='empty'))['series']))
        self.assertEqual((await self.day(player='empty'))['games'],0)
        self.assertEqual((await self.page(player='empty'))['pagination'],dict(limit=100,returned_games=0,has_more=False,next_cursor=None))
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='carol'")
        for work in (self.activity,self.ratings,self.day,self.page):
            for player in ('absent','carol','shell'):
                with self.assertRaises(LookupError):await work(player=player)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_chart_filters_pagination_and_both_selfplay_sides(self):
        options=[dict(scope_kind='result',result=r) for r in ('win','draw','loss')]+[
            dict(scope_kind='rating',result=None,rating_from=1000),dict(scope_kind='rating',result=None,rating_from=0),
            dict(scope_kind='opponent',result=None,opponent=' ALICE '),dict(scope_kind='opponent',result=None,opponent='bob',color='black'),
            dict(scope_kind='opening',result=None,opening='Test-Opening'),dict(scope_kind='opening',result=None,opening='',color='white')]
        for opts in options:
            with self.subTest(opts=opts):
                expected=[r for r in self.rows() if r['mode'] in ('bullet','blitz','rapid')]
                if opts['scope_kind']=='result': expected=[r for r in expected if r['result']=={'win':1,'draw':.5,'loss':0}[opts['result']]]
                elif opts['scope_kind']=='rating':expected=[r for r in expected if r['rating']>0 and opts['rating_from']<=r['rating']<opts['rating_from']+10]
                elif opts['scope_kind']=='opponent':expected=[r for r in expected if r['opponent']==opts['opponent'].strip().lower()]
                else:expected=[r for r in expected if r['opening']==opts['opening']]
                if opts.get('color'):expected=[r for r in expected if r['color']==opts['color']]
                expected.sort(key=lambda r:(r['utc'],r['link'],r['color']),reverse=True)
                actual=[];cursor=None
                for _ in range(len(expected)+1):
                    page=await self.page(**opts,limit=1,cursor=cursor)
                    self.assertEqual(page['pagination']['returned_games'],len(page['games']))
                    self.assertLessEqual(len(page['games']),1);actual+=page['games']
                    cursor=page['pagination']['next_cursor']
                    self.assertEqual(page['pagination']['has_more'],cursor is not None)
                    if not cursor:break
                else:self.fail('Pagination did not terminate')
                self.assertEqual(actual,[dict(game_id=r['link'],player_color=r['color'],opponent_name=r['opponent'],mode=r['mode'],
                    player_rating=r['rating'],opponent_rating=r['opponent_rating'],played_at_utc=r['utc'].isoformat(),
                    played_at_local=r['local'].isoformat(),result={1:'win',.5:'draw',0:'loss'}[r['result']]) for r in expected])

    async def test_catalog_buckets_and_chart_membership_agree(self):
        with patch.object(catalog,'READ_SLOTS',asyncio.Semaphore(2)):
            for kind,scope in (('opponents','opponent'),('openings','opening')):
                for mode in ('all','blitz'):
                    output=await catalog.get_player_game_catalog('alice',Cache(),kind=kind,mode=mode,limit=50,session_factory=self.factory)
                    for bucket in output['rows']:
                        page=await self.page(scope_kind=scope,result=None,modes=['bullet','blitz','rapid'] if mode=='all' else [mode],**{scope:bucket['key']})
                        self.assertEqual(len(page['games']),bucket['games'])
                        self.assertEqual([sum(g['result']==r for g in page['games']) for r in ('win','draw','loss')],
                                         [bucket[k] for k in ('wins','draws','losses')])

    async def test_cursor_is_bound_to_scope_and_rejects_invalid_components(self):
        cursor=(await self.page(limit=1))['pagination']['next_cursor'];self.assertIsNotNone(cursor)
        for options in (dict(result='loss'),dict(modes=['rapid']),dict(player='bob'),dict(scope_kind='rating',result=None,rating_from=1000)):
            with self.assertRaisesRegex(ValueError,'pagination cursor'):await self.page(cursor=cursor,**options)
        data=json.loads(base64.urlsafe_b64decode(cursor+'='*(-len(cursor)%4)))
        for index,value in ((1,'2025-03-09'),(2,True),(2,0),(2,2**63),(3,'purple')):
            invalid=data.copy();invalid[index]=value
            token=base64.urlsafe_b64encode(json.dumps(invalid).encode()).decode()
            with self.assertRaisesRegex(ValueError,'pagination cursor'):await self.page(cursor=token)
        with self.assertRaisesRegex(ValueError,'pagination cursor'):await self.page(cursor='!invalid!')
        # Timezone changes only presentation; the same UTC keyset still resumes.
        page=await self.page(limit=1,cursor=cursor,timezone_name='UTC')
        self.assertEqual(page['time_context']['component_timezones'],['UTC'])

    async def test_invalid_selections_and_explorer_filters_are_rejected(self):
        factory=AsyncMock()
        for opts in (dict(scope_kind='rating',result=None,rating_from=True),dict(scope_kind='rating',result=None,rating_from=1001),
                     dict(scope_kind='opening',result=None,opening='x',color='red'),dict(scope_kind='opponent',result=None,opponent=''),
                     dict(scope_kind='opponent',result=None,opponent='bob',opening='x'),dict(result='unknown')):
            with self.assertRaises(ValueError):await self.page(**opts,session_factory=factory)
        factory.assert_not_called()
        for opts in (dict(analyzed_only=True),dict(analyzed_only=False,modes=['daily']),dict(analyzed_only=False,limit=0)):
            params=dict(scope_kind='result',modes=['blitz'],result='win');params.update(opts)
            with self.assertRaises(ValueError):await explore_player_games('alice',**params)
        for work in (self.activity,self.ratings):
            with self.assertRaises(ValueError):await work(date_from=date(2025,3,10),date_to=date(2025,3,9))

    async def test_profile_and_source_share_one_snapshot_during_writes(self):
        original=hero._analytics_scope
        async def concurrent(session,*args,**kwargs):
            scope=await original(session,*args,**kwargs)
            await self.db.execute("UPDATE player SET timezone='UTC' WHERE player_name='alice'")
            await self.add([self.game(20)])
            return scope
        before=len(self.rows())
        with patch.object(hero,'_analytics_scope',concurrent):output=await self.activity()
        self.assertEqual(output['total_games'],before)
        self.assertEqual(output['time_context']['component_timezones'],['America/New_York'])
        self.assert_buckets(await self.activity(),self.rows(zone='UTC'))

    async def test_readonly_and_database_failures_propagate_and_close(self):
        async def fail(session,*args):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            self.assertEqual(await session.scalar(text('SHOW transaction_isolation')),'repeatable read')
            await session.execute(text('DELETE FROM game'))
        before=await self.db.fetchval('SELECT count(*) FROM game')
        for method,work in (('activity',self.activity),('ratings',self.ratings),('day',self.day),('chart_games',self.page)):
            with patch.object(reads,method,fail),self.assertRaises(DBAPIError):await work()
            self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),before)

    async def test_statement_timeouts_remain_failures_not_empty_results(self):
        async def slow(session,*args):
            await session.execute(text("SET LOCAL statement_timeout='20ms'"))
            await session.execute(text('SELECT pg_sleep(0.2)'))
        for method,work in (('activity',self.activity),('ratings',self.ratings),('day',self.day),('chart_games',self.page)):
            with patch.object(reads,method,slow),self.assertRaises(DBAPIError) as caught:await work()
            self.assertEqual(caught.exception.orig.sqlstate,'57014');self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_relation_lock_timeout_releases_the_read_transaction(self):
        blocker=await self.connect();transaction=blocker.transaction();await transaction.start()
        try:
            await blocker.execute('LOCK TABLE game IN ACCESS EXCLUSIVE MODE')
            with self.assertRaises(DBAPIError) as caught:await self.activity()
            self.assertEqual(caught.exception.orig.sqlstate,'55P03');self.assertEqual(self.engine.pool.checkedout(),0)
        finally:await transaction.rollback()
        self.assertEqual((await self.activity())['total_games'],len(self.rows()))

    async def test_repeated_cancellation_drains_rollback_and_closure(self):
        for method,work in (('activity',self.activity),('ratings',self.ratings),('day',self.day),('chart_games',self.page)):
            entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
            async def pause(*args):entered.set();await asyncio.Future()
            def factory():
                session=self.factory();rollback,close=session.rollback,session.close
                async def delayed():rolling.set();await release.wait();await rollback()
                async def closing():
                    try:await close()
                    finally:closed.set()
                session.rollback=delayed;session.close=closing;return session
            with patch.object(reads,method,pause):
                task=self.start(work(session_factory=factory))
                try:
                    await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                    for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
                finally:
                    release.set()
                    if not task.done():task.cancel()
                    await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled());self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_real_routes_preserve_behavior_and_error_statuses(self):
        app=FastAPI();app.include_router(players.router,prefix='/players')
        with patch.object(hero,'AsyncDBSession',self.factory),patch.object(chart,'AsyncDBSession',self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                for path,key,value in (('activity','total_games',len(self.rows())),('days/2025-03-09','games',len(self.rows(start=date(2025,3,9),end=date(2025,3,9))))):
                    response=await client.get('/players/ALICE/analysis/behavioural/'+path)
                    self.assertEqual(response.status_code,200);self.assertEqual(response.json()[key],value)
                response=await client.get('/players/alice/analysis/behavioural/ratings?date_from=2025-03-11&date_to=2025-03-11&mode=blitz')
                self.assertEqual(response.status_code,200);self.assertEqual(response.json()['series'][0]['points'][0]['last_rating'],1615)
                response=await client.post('/players/alice/analysis/measures/explore-games',json=dict(scope='opponent',opponent='alice',modes=['blitz'],analyzed_only=False,limit=1))
                self.assertEqual(response.status_code,200);self.assertEqual(response.json()['games'][0]['player_color'],'white')
                self.assertTrue(response.json()['pagination']['has_more'])
                self.assertEqual((await client.get('/players/absent/analysis/behavioural/activity')).status_code,404)
                self.assertEqual((await client.get('/players/alice/analysis/behavioural/activity?date_from=2025-03-10&date_to=2025-03-09')).status_code,422)
