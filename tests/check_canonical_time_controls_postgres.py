"""Time-control endpoints on canonical PostgreSQL, with independent record oracles."""
import asyncio
from bisect import bisect_right
from collections import Counter
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import json
from unittest.mock import Mock, patch

from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, time_control_reads as reads
from chessism_api.operations import time_control_views as views
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL


# link, owners, mode, UTC time, ratings, scores, full-move count, elapsed seconds.
GAMES = (
    (101,'alice','bob','bullet','2025-12-31T23:59:00+00:00',1000,2000,1,0,10,59.9),
    (105,'alice','alice','bullet','2026-01-01T00:01:00+00:00',1400,1600,.5,.5,20,60),
    (106,'bob','alice','bullet','2026-01-01T00:01:00+00:00',1800,2200,0,1,30,120),
    (107,'bob','carol','rapid','2026-02-01T02:05:00+00:00',2300,2400,0,1,90,2400),
    (108,'alice','bob','blitz','2026-03-01T05:00:00+00:00',1600,1500,1,0,10,0),
)
OPENINGS = {101:'e4 e5 Nf3 Nc6 Bb5 a6',105:'d4 d5 c4 e6 Nc3 Nf6',
            106:'d4 d5 c4 e6 Nc3 Nf6',107:'e4 e5 Nf3 Nc6 Bb5 a6',108:'e4 e5 Nf3 Nc6 Bb5 a6'}


def records(mode, player=None):
    rows=[]
    for link,w,b,m,at,wr,br,ws,bs,moves,seconds in GAMES:
        if m!=mode:continue
        common=dict(link=link,at=datetime.fromisoformat(at),moves=moves,seconds=seconds,
                    avg=(wr+br)/2,white_score=ws,black_score=bs,white_rating=wr,black_rating=br)
        if player is None:rows.append(common)
        else:
            for owner,color,rating,score in ((w,'white',wr,ws),(b,'black',br,bs)):
                if owner==player:rows.append(dict(common,color=color,rating=rating,score=score))
    return rows


def percentile(values, fraction):
    ordered=sorted(values); index=(len(ordered)-1)*fraction; lower=int(index)
    return ordered[lower]+(ordered[min(lower+1,len(ordered)-1)]-ordered[lower])*(index-lower)


def rounded_mean(values):
    mean=sum(Decimal(str(value)) for value in values)/len(values)
    return float(mean.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP))


class CanonicalTimeControlTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        ids=await seed(self.db)
        self.ids={'alice':ids['white'],'bob':ids['black']}
        for name,key in self.ids.items():
            await self.db.execute('UPDATE player SET player_name=$2 WHERE id=$1',key,name)
        self.ids['carol']=await self.db.fetchval("INSERT INTO player(player_name) VALUES('carol') RETURNING id")
        modes={mode:await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',mode)
               for mode in ('bullet','blitz','rapid')}
        for link,w,b,mode,at,wr,br,ws,bs,moves,seconds in GAMES:
            if link not in (101,105):
                await self.db.execute(GAME_SQL,link,self.ids[w],self.ids[b],datetime.fromisoformat(at),moves,'chess',None)
            await self.db.execute('''UPDATE game SET played_at=$2,mode_id=$3,white_elo=$4,black_elo=$5,
                white_result=$6,black_result=$7,n_moves=$8,time_elapsed=$9 WHERE link=$1''',
                link,datetime.fromisoformat(at),modes[mode],wr,br,ws,bs,moves,seconds)
        # These still exist in the new schema but never counted in active-game analytics.
        await self.db.execute('''UPDATE game SET mode_id=$1,white_elo=9999,black_elo=9999,
            played_at='2099-01-01',time_elapsed=99999 WHERE link IN (102,103,104)''',modes['bullet'])
        self.moves={}
        for link in (*OPENINGS,102,103,104):
            opening=OPENINGS.get(link,'a4 a5 h4 h5 b4 b5')
            identifier=await self.db.fetchval('''INSERT INTO opening_sequence(opening) VALUES($1)
                ON CONFLICT(opening) DO UPDATE SET opening=EXCLUDED.opening RETURNING id''',opening)
            await self.db.execute('INSERT INTO game_opening VALUES($1,3,$2)',link,identifier)
            words=opening.split()
            for index in range(3):
                self.moves[(link,index+1)]=(words[index*2],words[index*2+1])
                await self.db.execute('INSERT INTO moves VALUES($1,$2,$3,$4,0,0,0,0)',link,index+1,*self.moves[(link,index+1)])
        # Text collation belongs to PostgreSQL, not Python's Unicode ordering.
        # Obtain only its ordering of fixture tokens; independently count owners,
        # games and move frequencies below, without invoking application queries.
        tokens=sorted({move for pair in self.moves.values() for move in pair})
        ordered=await self.db.fetch('SELECT token FROM unnest($1::text[]) AS token ORDER BY token',tokens)
        self.move_order={row['token']:rank for rank,row in enumerate(ordered)}
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000','timezone':'America/Mexico_City'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)

    async def call(self, name, *args, **kwargs):
        return await getattr(views,name)(*args,factory=self.factory,**kwargs)

    async def test_ratings_count_both_sides_but_exclude_variant_custom_and_zero_games(self):
        for mode in ('bullet','blitz','rapid'):
            histogram=Counter(r[k] for r in records(mode) for k in ('white_rating','black_rating'))
            actual=await self.call('get_rating_time_control_chart',' '+mode.upper()+' ')
            self.assertEqual(actual['x'],sorted(histogram))
            self.assertEqual(actual['y'],[histogram[k] for k in sorted(histogram)])
            self.assertEqual(actual['time_control'],mode)
        self.assertEqual((await self.call('get_rating_time_control_chart','bullet'))['bins'],{
            'jenks_1':{'min_rating':1000,'max_rating':1000},
            'jenks_2':{'min_rating':1400,'max_rating':1600},
            'jenks_3':{'min_rating':1800,'max_rating':2200}})

    async def test_global_moves_use_selected_side_rating_and_player_moves_use_owned_color(self):
        for color in ('white','black'):
            for player in (None,'alice','bob','missing'):
                rows=records('bullet',player)
                if player is not None:rows=[r for r in rows if r['color']==color]
                expected=[]
                for number in range(1,5):
                    counts=Counter(self.moves.get((r['link'],number),('',''))[color=='black'] for r in rows)
                    counts.pop('',None)
                    move=min(counts,key=lambda v:(-counts[v],self.move_order[v])) if counts else '-'
                    expected.append({'move_number':number,'move':move,'times_played':counts.get(move,0)})
                args=('bullet',) if player is None else (player,'bullet')
                fn='get_time_control_top_moves' if player is None else 'get_player_time_control_top_moves'
                value=await self.call(fn,*args,move_color=color,max_move=4)
                self.assertEqual(value['rows'],expected)
        filtered=await self.call('get_time_control_top_moves','bullet',min_rating=1900,max_rating=1901,max_move=1)
        self.assertEqual(filtered['rows'],[{'move_number':1,'move':'-','times_played':0}])
        filtered=await self.call('get_time_control_top_moves','bullet',move_color='black',min_rating=1900,max_rating=2000,max_move=1)
        self.assertEqual(filtered['rows'][0],{'move_number':1,'move':'e5','times_played':1})

    async def test_move_ties_missing_moves_and_page_bounds(self):
        await self.db.execute("UPDATE moves SET white_move='c4' WHERE link=106 AND n_move=1")
        await self.db.execute("UPDATE moves SET white_move='--',black_move='' WHERE link=105 AND n_move=2")
        top=await self.call('get_time_control_top_moves','bullet',move_color='invalid',max_move=3)
        self.assertEqual(top['move_color'],'white')
        self.assertEqual(top['rows'][0],{'move_number':1,'move':'c4','times_played':1})
        page=await self.call('get_player_time_control_top_moves','alice','bullet',page=999,page_size=2,max_move=3)
        self.assertEqual((page['page'],page['total_pages'],[r['move_number'] for r in page['rows']]),(2,2,[3]))
        limits=await self.call('get_time_control_top_moves','bullet',page=-1,page_size=99,max_move=99)
        self.assertEqual((limits['page'],limits['page_size'],limits['total']),(1,10,30))

    async def test_openings_normalized_identity_counts_ratings_and_pagination(self):
        value=await self.call('get_time_control_top_openings','bullet',page_size=1)
        self.assertEqual((value['total'],value['total_pages'],value['rows'][0]['times_played']),(2,2,2))
        self.assertEqual(value['rows'][0]['mean_rating_for_this_opening'],1750)
        self.assertEqual(value['rows'][0]['half_moves'],OPENINGS[105].split())
        self.assertEqual(value['rows'][0]['moves'],{'move_one':'d4,d5','move_two':'c4,e6','move_three':'Nc3,Nf6'})
        last=await self.call('get_time_control_top_openings','bullet',page_size=1,page=100)
        self.assertEqual((last['page'],last['rows'][0]['half_moves']),(2,OPENINGS[101].split()))
        # Count and page must both exclude empty opening sequences.
        blank=await self.db.fetchval("INSERT INTO opening_sequence(opening) VALUES('') RETURNING id")
        await self.db.execute('UPDATE game_opening SET opening_id=$1 WHERE link=101',blank)
        self.assertEqual((await self.call('get_time_control_top_openings','bullet'))['total'],1)

    async def test_player_openings_keep_result_filters_and_self_play_perspectives(self):
        for player in ('alice','bob','missing'):
            for label,score in (('win',1),('loss',0),('draw',.5)):
                counts=Counter(OPENINGS[r['link']] for r in records('bullet',player) if r['score']==score)
                value=await self.call('get_player_time_control_top_openings',player,'bullet',label)
                self.assertEqual(value['total'],len(counts))
                self.assertEqual([(r['opening'],r['times_played']) for r in value['rows']],
                                 sorted(counts.items(),key=lambda pair:(-pair[1],pair[0])))
        self.assertEqual((await self.call('get_player_time_control_top_openings','alice','bullet','invalid'))['result_filter'],'win')
        self.assertEqual((await self.call('get_player_time_control_top_openings','alice','bullet','draw'))['rows'][0]['times_played'],2)

    async def test_global_average_rating_filters_swap_bounds_and_keep_partial_range_semantics(self):
        for fn in ('get_time_control_top_openings','get_time_control_result_color_matrix',
                   'get_time_control_game_length_analytics','get_time_control_activity_trend'):
            broad=await self.call(fn,'bullet')
            partial=await self.call(fn,'bullet',min_rating=9999)
            self.assertEqual({k:v for k,v in broad.items() if k not in ('min_rating','max_rating')},
                             {k:v for k,v in partial.items() if k not in ('min_rating','max_rating')})
            forward=await self.call(fn,'bullet',min_rating=1499,max_rating=1501)
            reverse=await self.call(fn,'bullet',min_rating=1501,max_rating=1499)
            self.assertEqual(forward,reverse)
        value=await self.call('get_time_control_result_color_matrix','bullet',min_rating=1499,max_rating=1501)
        self.assertEqual((value['total_games'],value['white']['wins'],value['white']['draws']),(2,1,1))
        self.assertEqual((await self.call('get_time_control_top_openings','bullet',min_rating=1499,max_rating=1501))['rows'][0]['times_played'],1)

    async def test_result_matrix_counts_each_game_once_with_correct_color_scores(self):
        for mode in ('bullet','blitz','rapid'):
            rows=records(mode); value=await self.call('get_time_control_result_color_matrix',mode)
            self.assertEqual(value['total_games'],len(rows))
            for color in ('white','black'):
                counts={label:sum(r[color+'_score']==score for r in rows) for label,score in [('wins',1),('draws',.5),('losses',0)]}
                self.assertEqual(value[color],dict(counts,total=len(rows),score_rate=round((counts['wins']+.5*counts['draws'])/len(rows),4)))

    async def test_average_rating_does_not_overflow_valid_integer_ratings(self):
        await self.db.execute('UPDATE game SET white_elo=2147483647,black_elo=2147483647 WHERE link=107')
        value=await self.call('get_time_control_top_openings','rapid',min_rating=2147483647,max_rating=2147483647)
        self.assertEqual(value['rows'][0]['mean_rating_for_this_opening'],2147483647)

    async def test_lengths_match_independent_percentiles_and_distinct_histogram_contracts(self):
        for player in (None,'alice','bob'):
            rows=records('bullet',player)
            fn='get_time_control_game_length_analytics' if player is None else 'get_player_time_control_lengths'
            value=await self.call(fn,*(('bullet',) if player is None else (player,'bullet')))
            self.assertEqual(value['total_games'],len(rows))
            self.assertEqual(value['summary']['avg_n_moves'],rounded_mean([r['moves'] for r in rows]))
            self.assertEqual(value['summary']['median_n_moves'],percentile([r['moves'] for r in rows],.5))
            self.assertAlmostEqual(value['summary']['p90_n_moves'],percentile([r['moves'] for r in rows],.9))
            self.assertEqual(value['summary']['avg_time_elapsed_sec'],rounded_mean([r['seconds'] for r in rows]))
            move_counts=Counter(r['moves']//10*10 for r in rows)
            self.assertEqual(value['n_moves_hist'],{
                'x':[f'{start}-{start+9}' for start in sorted(move_counts)],
                'y':[move_counts[start] for start in sorted(move_counts)]})
            if player is None:
                elapsed_counts=Counter(bisect_right([60,120,180,300,600,1200,2400],r['seconds']) for r in rows)
                labels=['0-1','1-2','2-3','3-5','5-10','10-20','20-40','40+']
                expected_labels=[labels[index] for index in sorted(elapsed_counts)]
            else:
                elapsed_counts=Counter(int(r['seconds']//60) for r in rows)
                expected_labels=[f'{start}-{start+1}' for start in sorted(elapsed_counts)]
            self.assertEqual(value['time_elapsed_hist'],{'x':expected_labels,
                'y':[elapsed_counts[index] for index in sorted(elapsed_counts)],'unit':'minutes'})
        value=await self.call('get_time_control_game_length_analytics','rapid')
        self.assertEqual(value['time_elapsed_hist'],{'x':['40+'],'y':[1],'unit':'minutes'})
        value=await self.call('get_player_time_control_lengths','bob','rapid')
        self.assertEqual(value['time_elapsed_hist'],{'x':['40-41'],'y':[1],'unit':'minutes'})

    async def test_duration_averages_keep_decimal_half_up_rounding(self):
        for seconds,expected in ((1.005,1.01),(1.015,1.02),(2.675,2.68)):
            await self.db.execute('UPDATE game SET time_elapsed=$1 WHERE link=107',seconds)
            global_value=await self.call('get_time_control_game_length_analytics','rapid')
            player_value=await self.call('get_player_time_control_lengths','bob','rapid')
            self.assertEqual(global_value['summary']['avg_time_elapsed_sec'],expected)
            self.assertEqual(player_value['summary']['avg_time_elapsed_sec'],expected)

    async def test_global_elapsed_histogram_retains_every_boundary(self):
        boundaries=(0,59.9,60,119.9,120,179.9,180,299.9,300,599.9,600,1199.9,1200,2399.9,2400)
        expected=('0-1','0-1','1-2','1-2','2-3','2-3','3-5','3-5','5-10','5-10','10-20','10-20','20-40','20-40','40+')
        for seconds,label in zip(boundaries,expected):
            await self.db.execute('UPDATE game SET time_elapsed=$1 WHERE link=107',seconds)
            self.assertEqual((await self.call('get_time_control_game_length_analytics','rapid'))['time_elapsed_hist']['x'],[label])

    async def test_activity_uses_utc_and_keeps_global_dense_player_sparse_heatmaps(self):
        for player in (None,'alice','bob'):
            rows=records('bullet',player)
            fn='get_time_control_activity_trend' if player is None else 'get_player_time_control_activity_trend'
            value=await self.call(fn,*(('bullet',) if player is None else (player,'bullet')))
            for key,labels,indices in (
                ('month_heat',['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],[r['at'].month-1 for r in rows]),
                ('weekday_heat',['Sun','Mon','Tue','Wed','Thu','Fri','Sat'],[(r['at'].weekday()+1)%7 for r in rows]),
                ('hour_heat',[f'{i:02d}' for i in range(24)],[r['at'].hour for r in rows])):
                counts=Counter(indices); positions=range(len(labels)) if player is None else sorted(counts)
                self.assertEqual(value[key],{'labels':[labels[i] for i in positions],'values':[counts[i] for i in positions]})

    async def test_rating_chart_preserves_formula_ranges_and_deterministic_self_play_order(self):
        value=await self.call('get_player_mode_chart','alice',' BULLET ')
        expected=[1000,1600,1400,2200]  # UTC time, link, black-before-white side tie.
        self.assertEqual([r['y'] for r in value['points']],expected)
        self.assertEqual([r['x'] for r in value['points']],['2025-12-31','2026-01-01','2026-01-01','2026-01-01'])
        mean=sum(expected)/len(expected); std=(sum((v-mean)**2 for v in expected)/len(expected))**.5
        self.assertEqual(value['stats'],dict(mean=round(mean,2),std=round(std,2),lower=round(mean-std,2),upper=round(mean+std,2),count=4))
        self.assertEqual([r['bucket'] for r in value['points']],['std_behind','mean_band','mean_band','std_ahead'])
        class Clock(datetime):
            @classmethod
            def utcnow(cls):return cls(2026,12,31)
        with patch.object(views,'datetime',Clock):
            recent=await self.call('get_player_mode_chart','alice','bullet','six_months')
        self.assertEqual(recent['points'],[])
        self.assertEqual(recent['range']['type'],'six_months')
        self.assertIsNotNone(recent['range']['history'])

    async def test_empty_and_invalid_responses_do_not_hide_real_errors(self):
        fail=Mock(side_effect=AssertionError('Invalid mode opened a session'))
        for fn,args in (
            ('get_rating_time_control_chart',('daily',)),('get_time_control_top_moves',('daily',)),
            ('get_time_control_top_openings',('daily',)),('get_time_control_result_color_matrix',('daily',)),
            ('get_time_control_game_length_analytics',('daily',)),('get_time_control_activity_trend',('daily',)),
            ('get_player_time_control_top_moves',('alice','daily')),
            ('get_player_time_control_top_openings',('alice','daily','win')),
            ('get_player_time_control_lengths',('alice','daily')),
            ('get_player_time_control_activity_trend',('alice','daily'))):
            await getattr(views,fn)(*args,factory=fail)
        fail.assert_not_called()
        self.assertEqual((await self.call('get_player_mode_chart','missing','bullet'))['points'],[])
        self.assertEqual((await self.call('get_player_time_control_lengths','missing','bullet'))['total_games'],0)
        await self.db.execute('DELETE FROM game')
        value=await self.call('get_time_control_game_length_analytics','bullet')
        self.assertEqual(value['total_games'],0);self.assertTrue(all(v is None for v in value['summary'].values()))
        self.assertEqual((await self.call('get_time_control_activity_trend','bullet'))['hour_heat']['values'],[0]*24)

    async def test_opening_count_and_page_share_snapshot_across_concurrent_delete(self):
        original=reads.opening_count
        async def after_count(*args,**kwargs):
            value=await original(*args,**kwargs)
            await self.db.execute('DELETE FROM game WHERE link=101')
            return value
        with patch.object(reads,'opening_count',after_count):
            value=await self.call('get_time_control_top_openings','bullet')
        self.assertEqual((value['total'],len(value['rows'])),(2,2))
        self.assertEqual((await self.call('get_time_control_top_openings','bullet'))['total'],1)

    async def test_read_only_and_required_transaction_with_failure_propagation(self):
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError,'caller-owned'):
                await reads.rating_histogram(session,'bullet')
        original=reads.rating_histogram
        async def attempt_write(session,mode):
            await original(session,mode)
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            self.assertEqual(await session.scalar(text('SHOW transaction_isolation')),'repeatable read')
            self.assertEqual(await session.scalar(text('SHOW timezone')),'UTC')
            await session.execute(text('DELETE FROM game'))
        with patch.object(reads,'rating_histogram',attempt_write),self.assertRaises(DBAPIError):
            await self.call('get_rating_time_control_chart','bullet')
        self.assertEqual(self.engine.pool.checkedout(),0)
        await self.db.execute('ALTER TABLE game RENAME TO missing_game')
        for fn,args in [('get_rating_time_control_chart',('bullet',)),('get_time_control_top_openings',('bullet',)),
                        ('get_player_mode_chart',('alice','bullet'))]:
            with self.assertRaises(DBAPIError):await self.call(fn,*args)
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_repeated_cancellation_waits_for_rollback_and_session_closure(self):
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        original=reads.rating_histogram
        async def pause(*args):
            await original(*args);entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():
                rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=closing
            return session
        with patch.object(reads,'rating_histogram',pause):
            task=asyncio.create_task(views.get_rating_time_control_chart('bullet',factory=factory))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2);task.cancel();task.cancel()
                self.assertFalse(task.done())
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled())
        self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_routes_serialize_new_operations_without_public_contract_changes(self):
        from chessism_api.routers import games,players
        with patch.object(views,'AsyncDBSession',self.factory):
            ratings=json.loads((await games.api_get_rating_time_control_chart('bullet')).body)
            moves=json.loads((await games.api_get_time_control_top_moves('bullet','white',None,None,1,5,3)).body)
            openings=json.loads((await games.api_get_time_control_top_openings('bullet',None,None,1,5,3)).body)
            results=json.loads((await games.api_get_time_control_result_color_matrix('bullet',None,None)).body)
            lengths=json.loads((await games.api_get_time_control_game_length_analytics('bullet',None,None)).body)
            activity=json.loads((await games.api_get_time_control_activity_trend('bullet',None,None)).body)
            chart=json.loads((await players.api_get_mode_chart('ALICE','bullet','all',None)).body)
        self.assertEqual(sum(ratings['y']),6);self.assertEqual(moves['rows'][0]['move'],'d4')
        self.assertEqual(openings['total'],2);self.assertEqual(results['total_games'],3)
        self.assertEqual(lengths['total_games'],3);self.assertEqual(sum(activity['hour_heat']['values']),3)
        self.assertEqual(chart['stats']['count'],4)
