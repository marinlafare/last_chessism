"""Canonical FEN reads against real PostgreSQL and independent fixture calculations."""
import asyncio
import json
from unittest.mock import patch

import chess
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError, NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, fen_reads as reads
from chessism_api.database.postgresql.evaluation_results import stage_fen_results
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations import fen_explorer as explorer, fen_views as views
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS, GAME_SQL, PLAYED


def positions():
    found = list(FENS)
    board = chess.Board()
    for san in 'e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3'.split():
        board.push_san(san)
        fen = ' '.join(board.fen().split()[:4])
        if fen not in found: found.append(fen)
    return found


class CanonicalFenViewTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        scores = (None,0,49,-50,149,-150,299,-300,8999,-9000,9001)
        self.rows = []
        for i,fen in enumerate(positions()):
            score = scores[i] if i<len(scores) else None
            wdl = (200,500,300) if score is not None and i%2 else (None,None,None)
            row = dict(fen=fen,n_games=(1,2,10)[i] if i<3 else (i%4)*7,score=score,moves_counter=json.dumps({'e2e4':i}),
                       next_moves='e2e4 e7e5' if score is not None and i%2 else None,
                       wdl_win=wdl[0],wdl_draw=wdl[1],wdl_loss=wdl[2])
            self.rows.append(row)
            await self.db.execute('INSERT INTO fen(fen) VALUES($1) ON CONFLICT(fen) DO NOTHING',fen)
            await self.db.execute('''UPDATE fen SET n_games=$2,score=$3,moves_counter=$4,next_moves=$5,
                wdl_win=$6,wdl_draw=$7,wdl_loss=$8,
                analysis_source=CASE WHEN $3::integer IS NULL THEN NULL ELSE 'stockfish' END,
                analyzed_at=CASE WHEN $3::integer IS NULL THEN NULL ELSE now() END WHERE fen=$1''',
                *[row[k] for k in ('fen','n_games','score','moves_counter','next_moves','wdl_win','wdl_draw','wdl_loss')])
        # The read contract includes tablebase evaluations without source-dependent filtering.
        await self.db.execute("UPDATE fen SET analysis_source='tablebase',tablebase_wdl=-2,tablebase_dtz=-8 WHERE fen=$1",FENS[3])
        self.engine = create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000','timezone':'America/Mexico_City'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine,expire_on_commit=False)
        self.tasks = []
        self.addAsyncCleanup(self.stop_tasks)

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done(): task.cancel()
        if self.tasks: await asyncio.gather(*self.tasks,return_exceptions=True)

    async def call(self,name,*args,**kwargs):
        return await getattr(views,name)(*args,factory=self.factory,**kwargs)

    def sorted_scored(self,sort,minimum=0):
        rows = [r for r in self.rows if r['score'] is not None and abs(r['score'])>=minimum]
        if sort=='frequency': key=lambda r:(-r['n_games'],-abs(r['score']),r['fen'])
        elif sort=='evaluation': key=lambda r:(-abs(r['score']),-r['n_games'],r['fen'])
        else:key=lambda r:(-r['n_games']*abs(r['score']),-r['n_games'],r['fen'])
        return sorted(rows,key=key)

    def page_row(self,row):
        return {**row,'next_moves':row['next_moves'] or '', 'score':float(row['score']),
                **{k:float(row[k]) if row[k] is not None else None for k in ('wdl_win','wdl_draw','wdl_loss')},
                'side_to_move':row['fen'].split()[1],'impact_score':float(row['n_games']*abs(row['score']))}

    async def test_scored_pages_match_independent_order_and_payload_oracle(self):
        for sort in ('impact','frequency','evaluation'):
            expected = self.sorted_scored(sort)
            pages = [await self.call('get_scored_positions_page',sort,page=page,page_size=3)
                     for page in range(1,(len(expected)+2)//3+1)]
            actual = [row for page in pages for row in page['rows']]
            self.assertEqual(actual,[self.page_row(r) for r in expected])
            self.assertEqual(len({r['fen'] for r in actual}),len(actual))
            for page in pages:
                self.assertEqual(page['total'],len(expected))
                for row in page['rows']:
                    self.assertIsInstance(row['score'],float);self.assertIsInstance(row['n_games'],int)
                    self.assertIsInstance(row['impact_score'],float)

    async def test_filters_limits_normalization_and_last_page(self):
        for minimum in (0,49,50,150,300,9000,10000):
            data = await self.call('get_scored_positions_page',' EVALUATION ',minimum,page_size=50)
            self.assertEqual(data['rows'],[self.page_row(r) for r in self.sorted_scored('evaluation',minimum)])
        data=await self.call('get_scored_positions_page','unknown',-1,-2,1000)
        self.assertEqual((data['sort'],data['min_abs_score'],data['page'],data['page_size']),('impact',0,1,50))
        data=await self.call('get_scored_positions_page',page=999,page_size=3)
        self.assertEqual((data['page'],data['total_pages']),(4,4))
        self.assertEqual(data['rows'],[self.page_row(self.sorted_scored('impact')[-1])])
        data=await self.call('get_scored_positions_page',min_abs_score=50000,page=8)
        self.assertEqual((data['min_abs_score'],data['total'],data['page'],data['rows']),(10000,0,8,[]))

    async def test_integer_extremes_and_large_impact_do_not_overflow(self):
        for fen,score,n_games in [(FENS[1],-2147483648,9223372036854775807),(FENS[2],2147483647,12)]:
            await self.db.execute('UPDATE fen SET score=$2,n_games=$3 WHERE fen=$1',fen,score,n_games)
        for sort in ('impact','frequency','evaluation'):
            data=await self.call('get_scored_positions_page',sort,10000)
            self.assertEqual([r['fen'] for r in data['rows']],[FENS[1],FENS[2]])
            self.assertEqual(data['rows'][0]['impact_score'],float(9223372036854775807*2147483648))
            self.assertEqual(data['rows'][0]['score'],-2147483648.0)

    async def test_top_pending_ties_offset_clamping_and_sum(self):
        ordered = sorted(self.rows,key=lambda r:(-r['n_games'],r['fen']))
        self.assertEqual(await self.call('get_top_fens',5),
                         [{k:r[k] for k in ('fen','n_games','moves_counter','score')} for r in ordered[:5]])
        pending = [r for r in ordered if r['score'] is None]
        actual = await self.call('get_top_fens_unscored',6,offset=5)
        self.assertEqual(actual,[{k:r[k] for k in ('fen','n_games','score')} for r in pending[5:11]])
        self.assertEqual(await self.call('get_top_fens_unscored',-1,offset=-1),
                         [{k:pending[0][k] for k in ('fen','n_games','score')}])
        self.assertEqual(len(await self.call('get_top_fens_unscored',1000)),len(pending))
        self.assertEqual(await self.call('get_sum_n_games',14),sum(r['n_games'] for r in self.rows if r['n_games']>14))
        self.assertIsNone(await self.call('get_sum_n_games',1000))

    async def test_empty_initialized_database_and_missing_summary_rows_are_distinct(self):
        await self.db.execute('DELETE FROM game;DELETE FROM fen')
        self.assertEqual(await self.call('get_top_fens'),[])
        self.assertEqual(await self.call('get_top_fens_unscored'),[])
        self.assertIsNone(await self.call('get_sum_n_games'))
        self.assertEqual(await self.call('get_fen_analysis_counts'),
                         {'total_fens':0,'analyzed_fens':0,'unscored_fens':0,'nonzero_scored_fens':0})
        overview=await self.call('get_scored_positions_overview')
        self.assertEqual(overview['analyzed_fens'],0)
        self.assertTrue(all(row['positions']==row['appearances']==row['avg_abs_score']==0 for row in overview['buckets']))
        self.assertEqual((await self.call('get_scored_positions_page'))['rows'],[])
        self.assertEqual(await explorer.position_games(FENS[0],factory=self.factory),{'rows':[],'next_after':None})
        await self.db.execute('DELETE FROM database_summary;DELETE FROM scored_position_summary')
        for name in ('get_fen_analysis_counts','get_scored_positions_overview'):
            with self.assertRaises(NoResultFound): await self.call(name)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM database_summary'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM scored_position_summary'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_scored_overview_preserves_bucket_and_wdl_arithmetic(self):
        from chessism_api.database.postgresql.ask_db import refresh_scored_position_summary
        scored=[r for r in self.rows if r['score'] is not None]
        wdl=[r for r in scored if r['wdl_win'] is not None]
        summary={'n_positions':len(self.rows),'analyzed_fens':len(scored),
                 'nonzero_scored_fens':sum(r['score']!=0 for r in scored),'unscored_fens':len(self.rows)-len(scored)}
        await refresh_scored_position_summary(session_factory=self.factory,database_summary=summary)
        data=await self.call('get_scored_positions_overview')
        self.assertEqual({k:data[k] for k in ('total_positions','analyzed_fens','scored_positions','nonzero_scored_fens','unscored_fens')},
            {'total_positions':len(self.rows),'analyzed_fens':len(scored),'scored_positions':len(scored),
             'nonzero_scored_fens':summary['nonzero_scored_fens'],'unscored_fens':summary['unscored_fens']})
        for bucket,(low,high) in zip(data['buckets'],[(0,50),(50,150),(150,300),(300,9000),(9000,float('inf'))]):
            subset=[r for r in scored if low<=abs(r['score'])<high]
            self.assertEqual((bucket['positions'],bucket['appearances'],bucket['avg_abs_score']),
                (len(subset),sum(r['n_games'] for r in subset),round(sum(abs(r['score']) for r in subset)/len(subset),2)))
        self.assertEqual(data['side_balance'],{'white_better':sum(r['score']>50 for r in scored),
            'black_better':sum(r['score']< -50 for r in scored),'balanced':sum(abs(r['score'])<=50 for r in scored),
            'avg_score':round(sum(r['score'] for r in scored)/len(scored),2),
            'avg_abs_score':round(sum(abs(r['score']) for r in scored)/len(scored),2)})
        self.assertEqual(data['wdl'],{'positions':len(wdl),'avg_white_win':200.0,'avg_draw':500.0,'avg_white_loss':300.0})

    async def test_live_score_counts_observe_only_committed_publication(self):
        count=len(self.rows)
        await self.db.execute('UPDATE database_summary SET n_positions=$1,unscored_fens=$1',count)
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session,[{'fen':FENS[0],'score':0}],[],count_new=True)
            before=await self.call('get_fen_analysis_counts')
            self.assertEqual(before,{'total_fens':count,'analyzed_fens':0,'unscored_fens':count,'nonzero_scored_fens':0})
        after=await self.call('get_fen_analysis_counts')
        self.assertEqual(after,{'total_fens':count,'analyzed_fens':1,'unscored_fens':count-1,'nonzero_scored_fens':0})

    async def test_count_and_scored_page_share_snapshot_during_concurrent_delete(self):
        original=reads.scored_count
        async def counted(session,minimum):
            count=await original(session,minimum)
            await self.db.execute('DELETE FROM game;DELETE FROM fen WHERE score IS NOT NULL')
            return count
        with patch.object(reads,'scored_count',counted):
            data=await self.call('get_scored_positions_page',page_size=50)
        self.assertEqual(data['rows'],[self.page_row(r) for r in self.sorted_scored('impact')])
        self.assertEqual(data['total'],len(data['rows']))
        self.assertEqual((await self.call('get_scored_positions_page'))['total'],0)

    async def test_explorer_deduplicates_games_preserves_sides_and_utc_dates(self):
        data=await explorer.position_games(FENS[0],factory=self.factory)
        self.assertEqual([r['link'] for r in data['rows']],[101,105])
        for row in data['rows']:
            self.assertEqual((row['year'],row['month'],row['day']),(2026,1,1))
            self.assertEqual((row['white'],row['black']),('synthetic_white','synthetic_white' if row['link']==105 else 'synthetic_black'))
            self.assertEqual(row['occurrences'],[{'n_move':0,'move_color':'white'}])
        self.assertFalse(data['occurrences_truncated']);self.assertIsNone(data['next_after'])
        await self.db.execute('UPDATE game SET n_moves=4 WHERE link=101')
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,4,false),(101,$1,4,true)',self.ids['fens'][0])
        data=await explorer.position_games(FENS[0],factory=self.factory)
        self.assertEqual(data['rows'][0]['occurrences'],[{'n_move':0,'move_color':'white'},
            {'n_move':4,'move_color':'black'},{'n_move':4,'move_color':'white'}])
        self.assertEqual(len(data['rows']),2)

    async def test_explorer_keyset_pages_and_occurrence_cap(self):
        await self.db.execute('UPDATE game SET n_moves=2010 WHERE link=101')
        for link in range(200,212):
            await self.db.execute(GAME_SQL,link,self.ids['white'],self.ids['black'],PLAYED,2002,'chess',None)
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,0,true)',link,self.ids['fens'][0])
        await self.db.execute('''INSERT INTO game_fen_association
            SELECT 101,$1,n,false FROM generate_series(10,2010) n''',self.ids['fens'][0])
        first=await explorer.position_games(FENS[0],factory=self.factory)
        self.assertEqual([r['link'] for r in first['rows']],[101,105,*range(200,208)])
        self.assertEqual(first['next_after'],207);self.assertTrue(first['occurrences_truncated'])
        self.assertEqual(sum(len(r['occurrences']) for r in first['rows']),2000)
        second=await explorer.position_games(FENS[0],first['next_after'],factory=self.factory)
        self.assertEqual([r['link'] for r in second['rows']],list(range(208,212)))
        self.assertIsNone(second['next_after']);self.assertFalse(second['occurrences_truncated'])

    async def test_explorer_game_and_occurrence_queries_keep_one_snapshot(self):
        original=reads.game_ids
        async def select_then_delete(*args):
            ids=await original(*args)
            await self.db.execute('DELETE FROM game')
            return ids
        with patch.object(reads,'game_ids',select_then_delete):
            data=await explorer.position_games(FENS[0],factory=self.factory)
        self.assertEqual([r['link'] for r in data['rows']],[101,105])
        self.assertTrue(all(r['occurrences'] for r in data['rows']))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),0)

    async def test_examples_use_canonical_sides_and_verify_replayed_position(self):
        await self.db.execute('DELETE FROM game_fen_association')
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,1,true)',self.ids['fens'][1])
        await self.db.execute("INSERT INTO moves VALUES(101,1,'e4','e5',1,1,59,59)")
        row={'fen':FENS[1],'appearances':7}
        example=(await explorer.describe_examples([row],factory=self.factory))[0]
        self.assertEqual(example,{**row,'label':'1. e4','example_line':'1. e4'})
        # Later arrivals cannot displace the deterministic earliest game/move.
        await self.db.execute('INSERT INTO game_fen_association VALUES(105,$1,1,false)',self.ids['fens'][1])
        self.assertEqual((await explorer.describe_examples([row],factory=self.factory))[0],example)
        await self.db.execute("UPDATE moves SET white_move='Qh9'")
        self.assertIsNone((await explorer.describe_examples([row],factory=self.factory))[0]['example_line'])
        orphan=(await explorer.describe_examples([{'fen':FENS[4],'appearances':1}],factory=self.factory))[0]
        self.assertEqual((orphan['label'],orphan['example_line']),('Position',None))

    async def test_sampling_sql_scopes_on_real_postgres(self):
        # 100% is injected only at the adapter to deterministically inspect every
        # fixture row. Production .002/.02/.2 bounds are asserted by the unit test.
        async with read_transaction(self.factory) as session:
            for scope in ('unique','repeated','scored','unscored','2','7','10'):
                data=await reads.sample(session,scope,100,1)
                predicates={'unique':lambda r:r['n_games']==1,'repeated':lambda r:r['n_games']>1,
                    'scored':lambda r:r['score'] is not None,'unscored':lambda r:r['score'] is None}
                predicate=predicates.get(scope,lambda r:r['n_games']==int(scope))
                self.assertEqual({r['fen'] for r in data},{r['fen'] for r in self.rows if predicate(r)})
        sampled=await explorer.sample_positions('unscored',factory=self.factory)
        self.assertLessEqual(len(sampled),100)
        self.assertTrue({r['fen'] for r in sampled}<={r['fen'] for r in self.rows if r['score'] is None})

    async def test_query_errors_propagate_and_connections_close(self):
        await self.db.execute('ALTER TABLE fen RENAME TO unavailable_fen')
        calls=[lambda:self.call('get_top_fens'),lambda:self.call('get_top_fens_unscored'),
               lambda:self.call('get_sum_n_games'),lambda:self.call('get_scored_positions_page'),
               lambda:explorer.sample_positions('scored',factory=self.factory),
               lambda:explorer.describe_examples([{'fen':FENS[0]}],factory=self.factory),
               lambda:explorer.position_games(FENS[0],factory=self.factory)]
        for call in calls:
            with self.assertRaises(DBAPIError):await call()
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_caller_ownership_read_only_utc_and_failed_reads_cannot_write(self):
        calls=[(reads.top,(1,)),(reads.repetition_sum,(10,)),(reads.analysis_counts,()),
               (reads.scored_summary,()),(reads.scored_count,(0,)),(reads.scored_page,('impact',0,20,0)),
               (reads.sample,('unique',.2,1)),(reads.arrival,(FENS[0],)),(reads.example_moves,(101,1)),
               (reads.game_ids,(FENS[0],0)),(reads.games,([101],)),(reads.occurrences,(FENS[0],[101]))]
        async with self.factory() as session:
            for fn,args in calls:
                with self.assertRaisesRegex(ValueError,'caller-owned'):await fn(session,*args)
            self.assertFalse(session.in_transaction())
        async def bad_write(session,threshold):
            self.assertEqual(await session.scalar(text('SHOW transaction_isolation')),'repeatable read')
            self.assertEqual(await session.scalar(text('SHOW timezone')),'UTC')
            await session.execute(text('DELETE FROM fen'))
        with patch.object(reads,'repetition_sum',bad_write),self.assertRaises(DBAPIError):
            await self.call('get_sum_n_games')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen'),len(self.rows))
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_repeated_cancellation_waits_for_rollback_and_closure(self):
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        original=reads.game_ids
        async def pause(*args):
            await original(*args);entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():rolling.set();await release.wait();await rollback()
            async def finish():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=finish
            return session
        with patch.object(reads,'game_ids',pause):
            task=asyncio.create_task(explorer.position_games(FENS[0],factory=factory));self.tasks.append(task)
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2);task.cancel();task.cancel()
                self.assertFalse(task.done())
            finally:release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(len((await explorer.position_games(FENS[0],factory=self.factory))['rows']),2)

    async def test_actual_fen_routes_serialize_pending_scored_and_unscored_top(self):
        from chessism_api.routers import fens
        from chessism_api.routers import fen_explorer as routes
        with patch.object(views,'AsyncDBSession',self.factory),patch.object(explorer,'AsyncDBSession',self.factory):
            page=json.loads((await fens.api_get_scored_positions_page('evaluation',0,1,5)).body)
            self.assertEqual(page['rows'],[self.page_row(r) for r in self.sorted_scored('evaluation')[:5]])
            pending=json.loads((await fens.api_get_most_repeated_pending_fens(1)).body)
            self.assertEqual(pending['definition'],'score_is_null');self.assertTrue(pending['has_next'])
            self.assertEqual(len(pending['rows']),5)
            top=json.loads((await fens.api_get_top_fens(100)).body)
            self.assertIn('Score:   None',top['results'])
            self.assertEqual((await routes.games(FENS[0],0))['rows'][0]['link'],101)
