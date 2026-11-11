"""Player-perspective views, compared with an independent synthetic record oracle."""
import asyncio
import json
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_game_reads as reads
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import player_game_views as views
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL, FENS


# link, white, black, UTC stamp, mode, white/black rating, white/black score,
# length, ECO, white/black termination. Self-play has two independent perspectives.
GAMES = (
    (101,'alice','bob','2025-12-31T23:59:59+00:00','bullet',2000,1800,1,0,20,'B-opening','win','resigned'),
    (105,'alice','alice','2026-01-01T00:01:00+00:00','blitz',1700,1600,1,0,10,'A-opening','win','timeout'),
    (106,'alice','bob','2026-01-02T00:00:00+00:00','rapid',1900,1950,.5,.5,30,'A-opening','agreed','agreed'),
    (107,'bob','alice','2026-01-03T00:00:00+00:00',None,2100,2050,0,1,40,'no_eco','checkmated','win'),
    (108,'alice','alice','2026-01-04T00:00:00+00:00','bullet',2200,2300,0,1,50,'C-opening','abandoned','win'),
)


def perspectives(player):
    rows = []
    for link,white,black,at,mode,wr,br,ws,bs,length,eco,we,be in GAMES:
        for name,color,rating,opponent,score,ending in (
            (white,'white',wr,br,ws,we),(black,'black',br,wr,bs,be)):
            if name == player:
                rows.append(dict(link=link,color=color,rating=rating,opponent_rating=opponent,
                    result=score,end_by=ending,n_moves=length,eco=eco,mode=mode,
                    played_at=datetime.fromisoformat(at)))
    return rows


def ordered(rows):
    # Match public chronology, then numeric identity, then alphabetical color.
    return sorted(rows, key=lambda r: (-r['played_at'].timestamp(), -r['link'], r['color']))


def game_payload(rows):
    return [{'link':r['link'],'played_at':r['played_at'].strftime('%Y-%m-%d %H:%M:%S'),
             'color':r['color'],'result':{1:'win',.5:'draw',0:'loss'}[r['result']]} for r in rows]


class CanonicalPlayerViewTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        ids = await seed(self.db)
        self.ids = {'alice':ids['white'], 'bob':ids['black']}
        await self.db.execute("UPDATE player SET player_name='alice',joined=1,title='GM',avatar='a' WHERE id=$1",ids['white'])
        await self.db.execute("UPDATE player SET player_name='bob',joined=1,title='  im  ',avatar='b' WHERE id=$1",ids['black'])
        await self.db.execute("""INSERT INTO player(player_name,joined,title,deleted_at) VALUES
            ('carol',1,NULL,NULL),('dave',1,'Coach',NULL),('eve',1,'WGM',now()),('ghost',0,'GM',NULL)""")
        self.modes = {}
        for mode in ('bullet','blitz','rapid'):
            self.modes[mode] = await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id', mode)
        for link,white,black,at,mode,wr,br,ws,bs,length,eco,we,be in GAMES:
            if link not in (101,105):
                await self.db.execute(GAME_SQL,link,self.ids[white],self.ids[black],datetime.fromisoformat(at),length,'chess',None)
            await self.db.execute('''UPDATE game SET played_at=$2,mode_id=$3,white_elo=$4,black_elo=$5,
                white_result=$6,black_result=$7,n_moves=$8,eco=$9,white_str_result=$10,black_str_result=$11
                WHERE link=$1''',link,datetime.fromisoformat(at),self.modes.get(mode),wr,br,ws,bs,length,eco,we,be)
        # Unified storage retains these records; none belonged to active-game statistics.
        await self.db.execute("UPDATE game SET played_at='2099-01-01',white_elo=9999,black_elo=9999 WHERE link IN (102,103,104)")
        await self.db.execute("UPDATE fen SET score=24,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1",FENS[0])
        self.engine = create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000','timezone':'America/Mexico_City'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine,expire_on_commit=False)
        self.tasks = []
        self.addAsyncCleanup(self.stop_tasks)
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done(): task.cancel()
        if self.tasks: await asyncio.gather(*self.tasks,return_exceptions=True)

    async def call(self, name, *args, **kwargs):
        return await getattr(views,name)(*args,factory=self.factory,**kwargs)

    async def test_performance_averages_and_endings_match_independent_side_oracle(self):
        for player in ('alice','bob','carol','missing'):
            with self.subTest(player=player):
                rows = perspectives(player)
                expected = {'total_games':len(rows)}
                for color in ('white','black'):
                    expected[color+'_games'] = sum(r['color']==color for r in rows) if rows else None
                    for label,score in [('wins',1),('losses',0),('draws',.5)]:
                        expected[color+'_'+label] = sum(r['color']==color and r['result']==score for r in rows) if rows else None
                self.assertEqual(await self.call('get_player_performance_summary',player),expected)
                averages = await self.call('get_player_game_averages',player)
                for field,source in [('avg_game_length','n_moves'),('avg_opponent_rating','opponent_rating')]:
                    if rows:
                        self.assertIsInstance(averages[field],Decimal)
                        self.assertAlmostEqual(float(averages[field]),sum(r[source] for r in rows)/len(rows))
                    else:self.assertIsNone(averages[field])
                endings = {key:sum(r['end_by']==key for r in rows) if rows else None
                           for key in ('checkmated','resigned','timeout','abandoned')}
                self.assertEqual(await self.call('get_player_termination_stats',player),endings)

    async def test_summary_and_recent_games_match_independent_side_oracle(self):
        for player in ('alice','bob','carol','missing'):
            rows = perspectives(player)
            expected = ordered(rows)
            page = await self.call('get_player_games_page',player)
            self.assertEqual(page,{'player_name':player,'page':1,'page_size':10,'total':len(rows),
                'total_pages':1 if rows else 0,'games':game_payload(expected)})
            summary = await self.call('get_player_game_summary',player)
            modes = Counter(r['mode'] for r in rows)
            ordered_modes = sorted(modes,key=lambda mode:(-modes[mode],mode is None,mode or ''))
            self.assertEqual(summary,{'player_name':player,
                'wins':sum(r['result']==1 for r in rows) if rows else None,
                'losses':sum(r['result']==0 for r in rows) if rows else None,
                'draws':sum(r['result']==.5 for r in rows) if rows else None,
                'total_games':len(rows),'date_from':expected[-1]['played_at'].isoformat() if rows else None,
                'date_to':expected[0]['played_at'].isoformat() if rows else None,
                'time_controls':[{'time_control':m,'total':modes[m]} for m in ordered_modes]})

    async def test_mode_games_and_statistics_keep_ratings_results_and_unknown_mode(self):
        for player in ('alice','bob','carol','missing'):
            rows = perspectives(player)
            actual = await self.call('get_player_modes_stats',player)
            self.assertEqual(set(actual),{r['mode'] or 'unknown' for r in rows})
            for mode in {r['mode'] for r in rows}:
                selected = [r for r in rows if r['mode']==mode]
                newest = ordered(selected)[0]
                oldest = min(selected,key=lambda r:(r['played_at'],r['link'],r['color']))
                wins,draws,losses=(sum(r['result']==v for r in selected) for v in (1,.5,0))
                self.assertEqual(actual[mode or 'unknown'],{'n_games':len(selected),
                    'as_white':sum(r['color']=='white' for r in selected),'as_black':sum(r['color']=='black' for r in selected),
                    'wins':wins,'draws':draws,'losses':losses,'oldest_rating':oldest['rating'],
                    'newest_rating':newest['rating'],'score_rate':round((wins+.5*draws)/len(selected),4)})
            for mode in ('bullet','blitz','rapid'):
                result = await self.call('get_player_mode_games',player,' '+mode.upper()+' ')
                selected = [r for r in rows if r['mode']==mode]
                self.assertEqual(result['games'],game_payload(ordered(selected)))
                self.assertEqual(result['total_games'],len(selected))
                self.assertEqual(await self.call('get_player_time_control_results',player,mode),result)

    async def test_openings_exclude_unknown_and_tie_break_by_eco(self):
        for player in ('alice','bob','missing'):
            rows = [r for r in perspectives(player) if r['eco']!='no_eco']
            groups = {eco:[r for r in rows if r['eco']==eco] for eco in {r['eco'] for r in rows}}
            expected = [{'eco':eco,'total_games':len(groups[eco]),
                'wins':sum(r['result']==1 for r in groups[eco]),'losses':sum(r['result']==0 for r in groups[eco]),
                'draws':sum(r['result']==.5 for r in groups[eco])}
                for eco in sorted(groups,key=lambda e:(-len(groups[e]),e))]
            self.assertEqual(await self.call('get_player_top_openings',player),expected)
            self.assertEqual(await self.call('get_player_top_openings',player,1),expected[:1])
        await self.db.execute("UPDATE game SET eco='D-opening' WHERE link=105")
        # The two self-play games have equal frequency; deterministic lexical tie.
        value = await self.call('get_player_top_openings','alice')
        self.assertEqual([r['eco'] for r in value[:2]],['C-opening','D-opening'])

    async def test_coverage_counts_both_self_play_sides_and_preserves_occurrence_aliases(self):
        value = await self.call('get_player_fen_score_counts','alice')
        self.assertEqual(value,{'player_name':'alice','total_games':7,'analyzed_games':0,
            'total_positions':9,'analyzed_positions':3,'unscored_positions':6,'total_fens':9,'analyzed_fens':3,
            'latest_rating':2300,'latest_rating_mode':'bullet','latest_rating_at':'2026-01-04T00:00:00+00:00'})
        await self.db.execute("UPDATE fen SET score=24,analysis_source='stockfish',analyzed_at=now() WHERE score IS NULL")
        async with write_transaction(self.factory) as session:await refresh_coverage(session)
        value = await self.call('get_player_fen_score_counts','alice')
        self.assertEqual((value['analyzed_games'],value['analyzed_positions'],value['unscored_positions']),(3,9,0))
        for player in ('carol','missing'):
            empty = await self.call('get_player_fen_score_counts',player)
            self.assertTrue(all(empty[k]==0 for k in ('total_games','analyzed_games','total_fens','analyzed_fens')))
            self.assertIsNone(empty['latest_rating']);self.assertIsNone(empty['latest_rating_at'])

    async def test_rating_uses_real_date_first_then_game_and_side_ties(self):
        await self.db.execute(GAME_SQL,999,self.ids['alice'],self.ids['bob'],
            datetime(2010,1,1,tzinfo=timezone.utc),1,'chess',None)
        await self.db.execute('UPDATE game SET white_elo=9999,mode_id=$1 WHERE link=999',self.modes['bullet'])
        value = await self.call('get_player_fen_score_counts','alice')
        self.assertEqual(value['latest_rating'],2300)  # Larger link with old date cannot win.
        modes = await self.call('get_player_modes_stats','alice')
        self.assertEqual((modes['bullet']['oldest_rating'],modes['bullet']['newest_rating']),(9999,2300))
        await self.db.execute('UPDATE game SET played_at=$1,white_elo=2400 WHERE link=999',datetime(2026,1,4,tzinfo=timezone.utc))
        self.assertEqual((await self.call('get_player_fen_score_counts','alice'))['latest_rating'],2400)
        await self.db.execute('UPDATE game SET black_player_id=$1,black_elo=2500 WHERE link=999',self.ids['alice'])
        self.assertEqual((await self.call('get_player_fen_score_counts','alice'))['latest_rating'],2500)
        recent = await self.call('get_player_games_page','alice',page_size=2)
        self.assertEqual([(g['link'],g['color']) for g in recent['games']],[(999,'black'),(999,'white')])

    async def test_pagination_has_stable_side_boundaries_and_preserves_out_of_range_page(self):
        first = await self.call('get_player_games_page','alice',page=1,page_size=1)
        second = await self.call('get_player_games_page','alice',page=2,page_size=1)
        self.assertEqual((first['games'][0]['link'],first['games'][0]['color']),(108,'black'))
        self.assertEqual((second['games'][0]['link'],second['games'][0]['color']),(108,'white'))
        last = await self.call('get_player_games_page','alice',page=20,page_size=1)
        self.assertEqual((last['page'],last['total_pages'],last['games']),(20,7,[]))
        clamped = await self.call('get_player_games_page','alice',page=-3,page_size=999)
        self.assertEqual((clamped['page'],clamped['page_size']),(1,100))

    async def test_invalid_modes_never_open_session_and_keep_each_response_shape(self):
        factory = Mock(side_effect=AssertionError('Invalid mode opened a session'))
        self.assertEqual(await views.get_player_mode_games('alice',' DAILY ',factory=factory),
            {'player_name':'alice','mode':'daily','total_games':0,'wins':0,'losses':0,'draws':0,'score_rate':0.0,
             'date_from':None,'date_to':None,'games':[]})
        self.assertEqual(await views.get_player_time_control_results('alice',' DAILY ',factory=factory),
            {'mode':'daily','total_games':0,'wins':0,'losses':0,'draws':0,'score_rate':0.0})
        factory.assert_not_called()

    async def test_navigation_normalizes_title_groups_and_excludes_deleted_untracked(self):
        names = lambda rows:[r['player_name'] for r in rows]
        self.assertEqual(names(await self.call('get_tracked_player_navigation')),['alice','bob','carol','dave'])
        self.assertEqual(names(await self.call('get_tracked_player_navigation',' ALICE ')),['alice','bob'])
        self.assertEqual(names(await self.call('get_tracked_player_navigation','dave')),['carol','dave'])
        self.assertEqual(names(await self.call('get_tracked_player_navigation','ghost')),['alice','bob'])
        for selected in ('missing','eve'):
            self.assertEqual(await self.call('get_tracked_player_navigation',selected),[])
        self.assertEqual((await self.call('get_tracked_player_navigation'))[0]['avatar'],'a')

    async def test_neighbors_wrap_and_handle_missing_single_and_empty_lists(self):
        for player,before,after in [('alice','dave','bob'),('dave','carol','alice'),('bobby','bob','carol')]:
            self.assertEqual(await self.call('get_player_neighbors',player),
                {'player_name':player,'previous_player':before,'next_player':after})
        await self.db.execute("UPDATE player SET joined=0 WHERE player_name<>'alice'")
        self.assertEqual(await self.call('get_player_neighbors','alice'),
            {'player_name':'alice','previous_player':'alice','next_player':'alice'})
        await self.db.execute('UPDATE player SET joined=0')
        self.assertEqual(await self.call('get_player_neighbors','alice'),
            {'player_name':'alice','previous_player':None,'next_player':None})

    async def test_statistical_reads_keep_recorded_owner_history_when_profile_is_deleted_or_untracked(self):
        before = await self.call('get_player_performance_summary','alice')
        await self.db.execute("UPDATE player SET deleted_at=now(),joined=0 WHERE player_name='alice'")
        self.assertEqual(await self.call('get_player_performance_summary','alice'),before)
        self.assertNotIn('alice',[r['player_name'] for r in await self.call('get_tracked_player_navigation')])

    async def test_page_count_and_rows_share_snapshot_during_owner_change(self):
        original = reads.game_count
        async def after_count(*args):
            value = await original(*args)
            await self.db.execute('UPDATE game SET white_player_id=$1,black_player_id=$1 WHERE link=108',self.ids['bob'])
            return value
        with patch.object(reads,'game_count',after_count):
            page = await self.call('get_player_games_page','alice')
        self.assertEqual((page['total'],len(page['games'])),(7,7))
        self.assertEqual((await self.call('get_player_games_page','alice'))['total'],5)

    async def test_summary_and_modes_share_snapshot_across_game_deletion(self):
        original = reads.summary
        async def after_summary(*args):
            value = await original(*args)
            await self.db.execute('DELETE FROM game WHERE link=108')
            return value
        with patch.object(reads,'summary',after_summary):
            value = await self.call('get_player_game_summary','alice')
        self.assertEqual(value['total_games'],7)
        self.assertEqual(sum(row['total'] for row in value['time_controls']),7)
        self.assertEqual((await self.call('get_player_game_summary','alice'))['total_games'],5)

    async def test_query_failure_is_not_an_empty_player_and_all_connections_close(self):
        await self.db.execute('ALTER TABLE game RENAME TO game_missing')
        for name in ('get_player_fen_score_counts','get_player_performance_summary','get_player_game_averages',
                     'get_player_termination_stats','get_player_top_openings','get_player_games_page',
                     'get_player_game_summary','get_player_modes_stats'):
            with self.subTest(name=name),self.assertRaises(DBAPIError):
                await self.call(name,'alice')
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_read_only_snapshot_and_caller_owned_adapter_boundary(self):
        original = reads.performance
        async def bad_write(session, player):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            self.assertEqual(await session.scalar(text('SHOW transaction_isolation')),'repeatable read')
            self.assertEqual(await session.scalar(text('SHOW timezone')),'UTC')
            await original(session,player)
            await session.execute(text("UPDATE player SET avatar='wrong' WHERE player_name='alice'"))
        with patch.object(reads,'performance',bad_write),self.assertRaises(DBAPIError):
            await self.call('get_player_performance_summary','alice')
        self.assertEqual(await self.db.fetchval("SELECT avatar FROM player WHERE player_name='alice'"),'a')
        async with self.factory() as session:
            for function,args in [(reads.coverage,('alice',)),(reads.neighbors,('alice',)),(reads.navigation,(None,)),
                (reads.performance,('alice',)),(reads.averages,('alice',)),(reads.terminations,('alice',)),
                (reads.openings,('alice',5)),(reads.game_count,('alice',)),(reads.games,('alice',)),
                (reads.summary,('alice',)),(reads.time_controls,('alice',)),(reads.mode_stats,('alice',))]:
                with self.assertRaisesRegex(ValueError,'caller-owned'):await function(session,*args)
            self.assertFalse(session.in_transaction())

    async def test_repeated_cancellation_finishes_rollback_and_close(self):
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        original = reads.games
        async def pause(*args,**kwargs):
            await original(*args,**kwargs)
            entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed_rollback():
                rolling.set();await release.wait();await rollback()
            async def finished_close():
                try:await close()
                finally:closed.set()
            session.rollback=delayed_rollback;session.close=finished_close
            return session
        with patch.object(reads,'games',pause):
            task=asyncio.create_task(views.get_player_games_page('alice',factory=factory));self.tasks.append(task)
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2);task.cancel();task.cancel()
                self.assertFalse(task.done())
            finally:release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual((await self.call('get_player_games_page','alice'))['total'],7)

    async def test_real_routes_serialize_canonical_player_views(self):
        from chessism_api.routers import players,games
        with patch.object(views,'AsyncDBSession',self.factory):
            counts=json.loads((await players.api_get_player_fen_counts('ALICE')).body)
            recent=json.loads((await games.api_get_recent_player_games('ALICE',page=1,page_size=2)).body)
            total=json.loads((await games.api_get_player_game_count('ALICE')).body)
            summary=json.loads((await games.api_get_player_game_summary('ALICE')).body)
            mode=json.loads((await games.api_get_player_mode_games('ALICE',mode='bullet')).body)
            navigation=json.loads((await players.api_get_player_navigation(same_group_as='alice')).body)
        self.assertEqual(counts['total_games'],7)
        self.assertEqual(total,{'player_name':'alice','total_games':7})
        self.assertEqual(recent['games'],game_payload(ordered(perspectives('alice'))[:2]))
        self.assertEqual(summary['date_from'],'2025-12-31T23:59:59+00:00')
        self.assertEqual(mode['total_games'],3)
        self.assertEqual(navigation,{'players':['alice','bob'],'avatars':{'alice':'a','bob':'b'}})
