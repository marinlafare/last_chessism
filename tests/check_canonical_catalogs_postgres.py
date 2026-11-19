"""Catalog and rating contracts against independently enumerated canonical games."""
import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timezone
from fractions import Fraction
import json
import os
import re
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from redis.exceptions import RedisError
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_catalog_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations import player_game_catalog as catalog, player_rating_distribution as ppd
from chessism_api.operations import player_analytics_summary as summaries
from chessism_api.routers import players
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class Cache:
    def __init__(self):self.data={};self.writes=[]
    async def get(self,key):return self.data.get(key)
    async def set(self,key,value,**kwargs):self.data[key]=value;self.writes.append((key,kwargs))


class CanonicalCatalogTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp();await canonical.initialize(self.db)
        self.ids={}
        for name in ('alice','bob','carol','shell','empty'):
            self.ids[name]=await self.db.fetchval("INSERT INTO player(player_name,joined,timezone) VALUES($1,$2,'UTC') RETURNING id",name,0 if name=='shell' else 1)
        self.modes={name:await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',name) for name in ('bullet','blitz','rapid','daily')}
        self.records=[]
        await self.add([
            self.game(1,'alice','bob',2200,2300,1,' https://WWW.chess.com/openings/Test-Opening '),
            self.game(2,'bob','alice',2220,2210,.5,'Test-Opening'),
            self.game(3,'alice','alice',2209,2239,1,'no_eco'),
            self.game(4,'alice','carol',2300,2350,0,'Other-Opening',mode='rapid'),
            self.game(5,'alice','carol',0,1500,1,'Other-Opening',mode='bullet'),
            self.game(6,'alice','bob',1800,2100,1,'Daily',mode='daily'),
            self.game(7,'alice','shell',-1,2500,.5,'Single-Opening'),
            self.game(8,'alice','bob',1100,2200,1,'Variant',rules='chess960'),
            self.game(9,'bob','alice',1111,2222,1,'Empty',mode='bullet',moves=0),
            self.game(10,'alice','bob',1200,2300,1,'Custom',setup='8/8/8/8/8/4K3/8/6k1 w - -'),
            self.game(100,'bob','bob',2750,2600,1,'Latest',stamp='2025-05-01T00:00:00+00:00'),
            self.game(101,'bob','carol',9999,9998,1,'Variant',rules='chess960',stamp='2025-06-01T00:00:00+00:00'),
            self.game(102,'bob','carol',8888,8887,1,'Empty',moves=0,stamp='2025-07-01T00:00:00+00:00'),
        ])
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,
            port=55432,query={'host':self.socket}),connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose);self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.feature=patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'0'});self.feature.start();self.addCleanup(self.feature.stop)
        self.cache=Cache();self.tasks=[];self.addAsyncCleanup(self.stop_tasks)
        self.patches=[patch.object(module,'READ_SLOTS',asyncio.Semaphore(2)) for module in (catalog,ppd)]
        for p in self.patches:p.start();self.addCleanup(p.stop)

    def game(self,link,white,black,wr,br,result,eco,*,mode='blitz',rules='chess',moves=2,setup=None,stamp=None):
        return dict(link=link,white=white,black=black,wr=wr,br=br,result=result,eco=eco,mode=mode,rules=rules,
                    moves=moves,setup=setup,stamp=stamp or f'2025-01-{min(link,28):02d}T00:00:00+00:00')

    async def add(self,games):
        columns=('link','white_player_id','black_player_id','mode_id','played_at','white_elo','black_elo',
                 'white_result','black_result','white_str_result','black_str_result','time_control','eco','time_elapsed','n_moves','rules','initial_setup')
        rows=[(g['link'],self.ids[g['white']],self.ids[g['black']],self.modes[g['mode']],datetime.fromisoformat(g['stamp']),g['wr'],g['br'],
               g['result'],1-g['result'],'recorded','recorded','600',g['eco'],60,g['moves'],g['rules'],g['setup']) for g in games]
        await self.db.copy_records_to_table('game',columns=columns,records=rows);self.records.extend(games)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)

    def start(self,work):
        task=asyncio.create_task(work);self.tasks.append(task);return task

    def own_rows(self,player='alice'):
        rows=[]
        for g in self.records:
            if g['rules']!='chess' or g['setup'] or g['moves']==0 or g['mode']=='daily':continue
            for owner,opponent,color,rating,result in ((g['white'],g['black'],'white',g['wr'],g['result']),
                                                        (g['black'],g['white'],'black',g['br'],1-g['result'])):
                if owner!=player:continue
                opening=re.sub(r'^https?://(www\.)?chess\.com/openings/','',g['eco'].strip(),flags=re.I)
                rows.append(dict(mode=g['mode'],color=color,rating=rating,result=result,opponent=opponent,
                                 opening='' if opening=='no_eco' else opening))
        return rows

    def oracle(self,kind,mode='all',color='all',search='',sort_by='games',ratio_bin=None):
        grouped=defaultdict(list)
        for r in self.own_rows():
            if (mode=='all' or r['mode']==mode) and (color=='all' or r['color']==color):
                grouped[r['opponent' if kind=='opponents' else 'opening']].append(r['result'])
        rows=[]
        minimum=1 if kind=='opponents' and search else 2
        for key,results in grouped.items():
            if len(results)<minimum:continue
            row=dict(key=key,games=len(results),wins=results.count(1),draws=results.count(.5),losses=results.count(0),unknown=0)
            if sort_by!='games' and min(9,10*row[sort_by]//row['games'])!=ratio_bin:continue
            rows.append(row)
        rows.sort(key=lambda r:((-r['games'],r['key']) if sort_by=='games' else (-Fraction(r[sort_by],r['games']),-r['games'],r['key'])))
        result=[]
        for index,row in enumerate(rows,1):
            label=(row['key'].replace('-',' ') or 'Unknown opening') if kind=='openings' else (row['key'] or 'Unknown opponent')
            if not search or search in row['key'].lower() or search in label.lower():result.append({**row,'rank':index})
        return result

    async def page(self,kind='opponents',**params):
        return await catalog.get_player_game_catalog('alice',params.pop('cache',self.cache),kind=kind,session_factory=self.factory,**params)

    async def distribution(self,mode='all',**params):
        return await ppd.get_player_rating_distribution(params.pop('player','alice'),mode,params.pop('cache',self.cache),session_factory=self.factory)

    def counted_rows(self,result):
        return [{k:v for k,v in r.items() if k!='latest_ratings'} for r in result['rows']]

    async def test_catalog_counts_filters_search_and_pagination_match_records(self):
        for kind in ('opponents','openings'):
            for mode in ('all','bullet','blitz','rapid'):
                for color in ('all','white','black'):
                    with self.subTest(kind=kind,mode=mode,color=color):
                        expected=self.oracle(kind,mode,color)
                        result=await self.page(kind,mode=mode,color=color,limit=1)
                        self.assertEqual(self.counted_rows(result),expected[:1])
                        self.assertEqual(result['total_games'],sum(r['games'] for r in expected))
                        self.assertEqual(result['pagination']['total_groups'],len(expected))
                        beyond=await self.page(kind,mode=mode,color=color,page=20,limit=1)
                        self.assertEqual(beyond['rows'],[]);self.assertFalse(beyond['pagination']['has_more'])
                        self.assertEqual(beyond['pagination']['total_groups'],len(expected))
        result=await self.page('openings',search='test opening')
        self.assertEqual(self.counted_rows(result),self.oracle('openings',search='test opening'))
        self.assertEqual((await self.page('openings',search='unknown opening'))['rows'][0]['key'],'')

    async def test_single_opponent_search_does_not_relax_opening_or_browse_minimum(self):
        for enabled in ('0','1'):
            await summaries.refresh_player('alice',session_factory=self.factory)
            with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':enabled}):
                self.assertNotIn('shell',[r['key'] for r in (await self.page())['rows']])
                searched=await self.page(search=' SHELL ')
                self.assertEqual(searched['total_games'],1);self.assertEqual(searched['rows'][0]['games'],1)
                self.assertEqual(searched['rows'][0]['latest_ratings']['blitz'],2500)
                self.assertEqual((await self.page('openings',search='Single'))['rows'],[])

    async def test_all_outcome_bin_boundaries_live_and_saved(self):
        added=[]
        for family,outcome in enumerate(('wins','draws','losses')):
            for boundary in range(11):
                name=f'boundary-{outcome}-{boundary}'
                self.ids[name]=await self.db.fetchval('INSERT INTO player(player_name) VALUES($1) RETURNING id',name)
                for n in range(20):
                    selected={'wins':1,'draws':.5,'losses':0}[outcome]
                    score=selected if n<boundary*2 else (1 if selected==0 else 0)
                    added.append(self.game(1000+family*300+boundary*20+n,'alice',name,2100,2200,score,name))
        await self.add(added);await summaries.refresh_player('alice',session_factory=self.factory)
        for outcome in ('wins','draws','losses'):
            for ratio_bin in range(10):
                for kind in ('opponents','openings'):
                    options=dict(sort_by=outcome,ratio_bin=ratio_bin,search='boundary-'+outcome+'-',limit=50)
                    expected=self.oracle(kind,search=options['search'],sort_by=outcome,ratio_bin=ratio_bin)
                    with self.subTest(outcome=outcome,bin=ratio_bin,kind=kind):
                        live=await self.page(kind,**options)
                        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):saved=await self.page(kind,**options)
                        self.assertEqual(self.counted_rows(live),expected);self.assertEqual(self.counted_rows(saved),expected)
                        self.assertEqual(live['total_games'],sum(r['games'] for r in expected))
                        self.assertEqual(saved['total_games'],live['total_games'])
                        wanted=[10,9] if ratio_bin==9 else [ratio_bin]
                        self.assertEqual([int(r['key'].rsplit('-',1)[1]) for r in live['rows']],wanted)

    async def test_latest_opponent_ratings_resolve_selfplay_ties_and_ignore_archives(self):
        result=await self.page();bob=next(r for r in result['rows'] if r['key']=='bob')
        self.assertEqual(bob['latest_ratings'],{'bullet':None,'blitz':2600,'rapid':None})
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='bob'")
        result=await self.page(cache=Cache());bob=next(r for r in result['rows'] if r['key']=='bob')
        self.assertEqual(bob['latest_ratings'],dict.fromkeys(('bullet','blitz','rapid')))

    async def test_exact_rating_counts_and_percentages_include_both_selfplay_sides(self):
        async with read_transaction(self.factory) as session:
            rows=await reads.rating_counts(session,self.ids['alice'])
        expected=Counter((r['mode'],r['rating']) for r in self.own_rows())
        self.assertEqual({(r['mode'],r['rating']):r['games'] for r in rows},dict(expected))
        all_modes=await self.distribution();self.assertEqual(all_modes['games_count'],5)
        self.assertEqual(all_modes['excluded_games_count'],2);self.assertEqual(all_modes['total_games_count'],7)
        blitz=await self.distribution('blitz');self.assertEqual(blitz['x'],[2200,2210,2220,2230]);self.assertEqual(blitz['y'],[50,25,0,25])
        bullet=await self.distribution('bullet');self.assertEqual(bullet['x'],[]);self.assertEqual(bullet['excluded_games_count'],1)
        self.assertEqual({kw['ex'] for _,kw in self.cache.writes},{60})
        await summaries.refresh_player('alice',session_factory=self.factory)
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):saved=await self.distribution()
        for field in ('x','y','games_count','excluded_games_count','total_games_count'):self.assertEqual(saved[field],all_modes[field])

    async def test_saved_catalogs_match_live_for_each_mode_color_and_outcome(self):
        await summaries.refresh_player('alice',session_factory=self.factory)
        for kind in ('opponents','openings'):
            for mode in ('all','blitz','rapid','bullet'):
                for color in ('all','white','black'):
                    for outcome,ratio_bin in (('games',None),('wins',9),('draws',5),('losses',0)):
                        options=dict(mode=mode,color=color,sort_by=outcome,ratio_bin=ratio_bin,limit=50)
                        live=await self.page(kind,**options)
                        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):saved=await self.page(kind,**options)
                        self.assertEqual(saved['rows'],live['rows']);self.assertEqual(saved['total_games'],live['total_games'])
                        self.assertEqual(saved['pagination'],live['pagination'])

    async def test_empty_profiles_and_deleted_or_untracked_profiles(self):
        empty=await ppd.get_player_rating_distribution('empty','all',self.cache,session_factory=self.factory)
        self.assertEqual(empty['x'],[]);self.assertEqual(empty['total_games_count'],0)
        page=await catalog.get_player_game_catalog('empty',self.cache,kind='openings',session_factory=self.factory)
        self.assertEqual(page['rows'],[]);self.assertEqual(page['pagination']['total_pages'],0)
        await self.page();await self.distribution()
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='alice'")
        for name in ('alice','shell','missing'):
            with self.assertRaises(LookupError):await self.distribution(player=name)
            with self.assertRaises(LookupError):await catalog.get_player_game_catalog(name,self.cache,kind='opponents',session_factory=self.factory)

    async def test_live_cache_identity_is_not_reused_after_profile_recreation(self):
        self.assertTrue((await self.page())['rows']);self.assertGreater((await self.distribution())['games_count'],0)
        keys=set(self.cache.data)
        async with self.db.transaction():
            await self.db.execute('DELETE FROM game WHERE white_player_id=$1 OR black_player_id=$1',self.ids['alice'])
            await self.db.execute('DELETE FROM player WHERE id=$1',self.ids['alice'])
        new=await self.db.fetchval("INSERT INTO player(player_name,joined,timezone) VALUES('alice',1,'UTC') RETURNING id")
        self.assertNotEqual(new,self.ids['alice'])
        self.assertEqual((await self.page())['rows'],[]);self.assertEqual((await self.distribution())['games_count'],0)
        self.assertEqual(len(set(self.cache.data)-keys),2)

    async def test_redis_outage_and_malformed_cache_fall_back_to_database(self):
        broken=AsyncMock();broken.get.side_effect=RedisError('unavailable');broken.set.side_effect=RedisError('unavailable')
        self.assertEqual(self.counted_rows(await self.page(cache=broken)),self.oracle('opponents'))
        self.assertEqual((await self.distribution(cache=broken))['games_count'],5)
        await self.page();await self.distribution()
        self.cache.data={k:'not json' for k in self.cache.data}
        self.assertEqual(self.counted_rows(await self.page()),self.oracle('opponents'))
        self.assertFalse((await self.distribution())['cache_hit'])

    async def test_cache_hits_and_saved_openings_do_not_require_game_source(self):
        first=await self.page('openings');distribution=await self.distribution()
        await summaries.refresh_player('alice',session_factory=self.factory)
        await self.db.execute('ALTER TABLE game RENAME TO hidden_game')
        self.assertEqual(await self.page('openings'),first)
        cached=await self.distribution();self.assertEqual(cached['y'],distribution['y']);self.assertTrue(cached['cache_hit'])
        with patch.dict(os.environ,{'CHESSISM_PLAYER_SUMMARIES':'1'}):
            saved=await self.page('openings');self.assertEqual(saved['rows'],first['rows'])
        with self.assertRaises(DBAPIError):await self.page(cache=Cache())
        with self.assertRaises(DBAPIError):await self.distribution(cache=Cache())
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_catalog_counts_and_opponent_ratings_share_one_read_snapshot(self):
        original=reads.catalog_page
        async def change_after_counts(*args,**kwargs):
            result=await original(*args,**kwargs)
            await self.db.execute('UPDATE game SET black_elo=2700 WHERE link=100')
            return result
        with patch.object(reads,'catalog_page',change_after_counts):result=await self.page()
        self.assertEqual(next(r for r in result['rows'] if r['key']=='bob')['latest_ratings']['blitz'],2600)
        later=await self.page(cache=Cache());self.assertEqual(next(r for r in later['rows'] if r['key']=='bob')['latest_ratings']['blitz'],2700)

    async def test_readonly_errors_propagate_without_caching(self):
        async def write(session,*args,**kwargs):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            await session.execute(text('DELETE FROM game'))
        for module,method,work in ((catalog,'catalog_page',self.page),(ppd,'rating_counts',self.distribution)):
            with patch.object(reads,method,write),self.assertRaises(DBAPIError):await work()
            self.assertEqual(module.READ_SLOTS._value,2);self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(self.cache.data,{})

    async def test_database_timeouts_map_to_busy_and_release_slots(self):
        async def slow(session,*args,**kwargs):
            await session.execute(text("SET LOCAL statement_timeout='20ms'"))
            await session.execute(text('SELECT pg_sleep(0.2)'))
        for module,method,work,error in ((catalog,'catalog_page',self.page,catalog.GameCatalogBusy),(ppd,'rating_counts',self.distribution,ppd.DistributionBusy)):
            with patch.object(reads,method,slow),self.assertRaises(error):await work()
            self.assertEqual(module.READ_SLOTS._value,2);self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(self.cache.data,{})

    async def test_repeated_cancellation_finishes_cleanup_and_does_not_cache(self):
        for module,method in ((catalog,'catalog_page'),(ppd,'rating_counts')):
            entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
            async def pause(*args,**kwargs):entered.set();await asyncio.Future()
            def factory():
                session=self.factory();rollback,close=session.rollback,session.close
                async def delayed():rolling.set();await release.wait();await rollback()
                async def closing():
                    try:await close()
                    finally:closed.set()
                session.rollback=delayed;session.close=closing;return session
            with patch.object(reads,method,pause):
                operation=(catalog.get_player_game_catalog('alice',self.cache,kind='opponents',session_factory=factory) if module is catalog
                           else ppd.get_player_rating_distribution('alice','all',self.cache,session_factory=factory))
                task=self.start(operation)
                try:
                    await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                    for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
                finally:
                    release.set()
                    if not task.done():task.cancel()
                    await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
            self.assertEqual(module.READ_SLOTS._value,2);self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(self.cache.data,{})

    async def test_invalid_selections_are_rejected_before_connecting(self):
        factory=AsyncMock()
        for options in ({'kind':'invalid'},{'kind':'openings','color':'orange'},{'kind':'openings','sort_by':'drop table game'},
                        {'kind':'openings','ratio_bin':1},{'kind':'openings','mode':'daily'},{'kind':'openings','page':0}):
            with self.assertRaises(ValueError):await catalog.get_player_game_catalog('alice',self.cache,session_factory=factory,**options)
        with self.assertRaises(ValueError):await ppd.get_player_rating_distribution('','all',self.cache,session_factory=factory)
        factory.assert_not_called()

    async def test_real_http_routes_keep_response_shape_and_cache_headers(self):
        app=FastAPI();app.include_router(players.router,prefix='/players')
        async def redis():return self.cache
        app.dependency_overrides[players.get_redis_pool]=redis
        with patch.object(catalog,'AsyncDBSession',self.factory),patch.object(ppd,'AsyncDBSession',self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                result=await client.get('/players/ALICE/ppd?mode=blitz')
                self.assertEqual(result.status_code,200);self.assertEqual(result.json()['y'],[50,25,0,25])
                self.assertEqual(result.headers['Cache-Control'],'private, no-store')
                result=await client.get('/players/ALICE/analysis/behavioural/catalog?kind=opponents')
                self.assertEqual(result.status_code,200);self.assertEqual(self.counted_rows(result.json()),self.oracle('opponents'))
                self.assertEqual(result.headers['Cache-Control'],'private, no-store')
