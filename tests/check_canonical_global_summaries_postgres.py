"""Canonical summary refreshes and score/ingestion deltas share transaction locks."""
import asyncio
from unittest.mock import patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, game_coverage, summary_writes as storage
from chessism_api.database.postgresql.evaluation_results import stage_fen_results
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import global_summaries as operations
from chessism_api.operations.ingestion_pipeline.fen_bulk import persist_positions
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, FENS


class CanonicalGlobalSummaryTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine,expire_on_commit=False)
        self.children = []
        self.addAsyncCleanup(self.stop_children)
        await self.call('refresh_database_summary')

    async def stop_children(self):
        for task in self.children:
            if not task.done():task.cancel()
        await asyncio.gather(*self.children,return_exceptions=True)

    def start(self, operation):
        task=asyncio.create_task(operation);self.children.append(task);return task

    async def call(self,name,**kwargs):
        return await getattr(operations,name)(session_factory=self.factory,**kwargs)

    async def counts(self):
        return tuple([{k:v for k,v in dict(await self.db.fetchrow('SELECT * FROM '+table)).items()
                      if k not in ('id','refreshed_at')}
                     for table in ('database_summary','scored_position_summary')])

    async def snapshot(self):
        return [await self.db.fetch('SELECT * FROM '+table+' ORDER BY 1')
                for table in ('fen','database_summary','scored_position_summary')]

    async def publish(self, rows):
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session,rows,[],count_new=True)

    async def assert_unlocked(self):
        async with self.db.transaction():
            for table in ('database_summary','scored_position_summary'):
                await self.db.fetchval('SELECT id FROM '+table+' WHERE id=1 FOR UPDATE NOWAIT')
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def wait_for_lock(self,task):
        observer=await self.connect()
        async def wait():
            while not await observer.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                WHERE datname=current_database() AND wait_event_type='Lock'
                  AND query LIKE '%database_summary%')'''):
                if task.done():task.result();self.fail('Expected summary lock wait')
                await asyncio.sleep(.01)
        await asyncio.wait_for(wait(),2)

    async def test_game_counts_use_active_games_and_normalized_modes(self):
        for mode,link in (('bullet',101),('rapid',105),('blitz',102)):
            key=await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',mode)
            await self.db.execute('UPDATE game SET mode_id=$2 WHERE link=$1',link,key)
        await self.db.execute('UPDATE player SET joined=42 WHERE id=$1',self.ids['white'])
        before=(await self.counts())[0]
        value=await self.call('refresh_database_summary_game_counts')
        self.assertEqual({k:value[k] for k in ('n_games_in_db','main_characters','secondary_characters',
            'bullet_games','blitz_games','rapid_games')},dict(n_games_in_db=2,main_characters=1,
            secondary_characters=1,bullet_games=1,blitz_games=0,rapid_games=1))
        self.assertEqual(value['n_positions'],before['n_positions'])
        self.assertEqual(value,await self.call('refresh_database_summary'))

    async def test_empty_and_repeated_refresh_do_not_bootstrap_missing_rows(self):
        await self.db.execute('DELETE FROM game;DELETE FROM fen;DELETE FROM months;DELETE FROM player')
        for table in ('game','fen','months','player'):
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM '+table),0)
        self.assertTrue(all(v==0 for v in (await self.call('refresh_database_summary')).values()))
        first=await self.counts()
        await self.call('refresh_scored_position_summary')
        self.assertEqual(await self.counts(),first)
        await self.db.execute('DELETE FROM scored_position_summary')
        for operation in ('refresh_database_summary','refresh_scored_position_summary'):
            with self.assertRaisesRegex(ValueError,'totals are missing'):
                await self.call(operation)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM scored_position_summary'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_full_fen_aggregate_matches_independent_numeric_oracle(self):
        extra=('rnbqkbnr/pppppppp/8/8/7P/8/PPPPPPP1/RNBQKBNR b KQkq -',
               'rnbqkbnr/pppppppp/8/8/8/7N/PPPPPPPP/RNBQKB1R b KQkq -')
        for fen in extra:await self.db.execute('INSERT INTO fen(fen,n_games) VALUES($1,0)',fen)
        scores=(0,50,-150,300,-9000,-2147483648,None)
        rows=[]
        for index,(fen,score) in enumerate(zip((*FENS,*extra),scores)):
            appearances=index*2+1
            wdl=(200,500,300) if score is not None and index%2==0 else (None,None,None)
            await self.db.execute('''UPDATE fen SET score=$2,n_games=$3,wdl_win=$4,wdl_draw=$5,wdl_loss=$6,
                analysis_source=CASE WHEN $2::int IS NULL THEN NULL ELSE 'stockfish' END,
                analyzed_at=CASE WHEN $2::int IS NULL THEN NULL ELSE now() END WHERE fen=$1''',fen,score,appearances,*wdl)
            rows.append(dict(score=score,n_games=appearances,wdl=wdl))
        await self.call('refresh_scored_position_summary')
        database,scored=await self.counts()
        valid=[r for r in rows if r['score'] is not None]
        self.assertEqual((database['n_positions'],database['analyzed_fens'],database['unscored_fens'],
                          database['scored_fens'],database['nonzero_scored_fens']),(7,6,1,5,5))
        self.assertEqual({k:scored[k] for k in ('total_positions','analyzed_fens','scored_positions','nonzero_scored_fens','unscored_fens')},
            dict(total_positions=7,analyzed_fens=6,scored_positions=6,nonzero_scored_fens=5,unscored_fens=1))
        for name,low,high in (('equal',0,50),('small',50,150),('clear',150,300),('decisive',300,9000),('mate',9000,float('inf'))):
            subset=[r for r in valid if low<=abs(r['score'])<high]
            self.assertEqual((scored[name+'_positions'],scored[name+'_appearances'],scored[name+'_abs_score_sum']),
                             (len(subset),sum(r['n_games'] for r in subset),sum(abs(r['score']) for r in subset)))
        self.assertEqual((scored['white_better'],scored['black_better'],scored['balanced']),
            (sum(r['score']>50 for r in valid),sum(r['score']< -50 for r in valid),sum(abs(r['score'])<=50 for r in valid)))
        self.assertEqual(scored['score_sum'],sum(r['score'] for r in valid))
        self.assertEqual(scored['abs_score_sum'],sum(abs(r['score']) for r in valid))
        wdl=[r['wdl'] for r in valid if r['wdl'][0] is not None]
        self.assertEqual((scored['wdl_positions'],scored['wdl_win_sum'],scored['wdl_draw_sum'],scored['wdl_loss_sum']),
                         (len(wdl),sum(w[0] for w in wdl),sum(w[1] for w in wdl),sum(w[2] for w in wdl)))

    async def test_new_scores_and_both_summaries_commit_together_and_match_full_refresh(self):
        before=await self.snapshot()
        rows=[dict(fen=fen,score=score,next_moves=None,wdl_win=200,wdl_draw=500,wdl_loss=300)
              for fen,score in zip(FENS,(0,-50,150,9999,-2147483648))]
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session,rows,[],count_new=True)
            self.assertEqual(await self.snapshot(),before)
        database,scored=await self.counts()
        self.assertEqual((database['analyzed_fens'],database['scored_fens'],scored['scored_positions'],scored['unscored_fens']),(5,4,5,0))
        incremental=await self.counts()
        await self.call('refresh_scored_position_summary')
        self.assertEqual(await self.counts(),incremental)
        committed=await self.snapshot()
        with self.assertRaisesRegex(ValueError,'already scored'):
            await self.publish(rows)
        self.assertEqual(await self.snapshot(),committed)

    async def test_missing_scored_summary_rolls_back_evaluations_and_database_counts(self):
        await self.db.execute('DELETE FROM scored_position_summary')
        before=await self.snapshot()
        with self.assertRaisesRegex(ValueError,'scored_position_summary totals are missing'):
            await self.publish([dict(fen=FENS[0],score=1,next_moves=None)])
        self.assertEqual(await self.snapshot(),before)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_failure_after_delta_keeps_all_previously_committed_results(self):
        await self.publish([dict(fen=FENS[0],score=1,next_moves=None)])
        before=await self.snapshot()
        with self.assertRaisesRegex(RuntimeError,'before commit'):
            async with write_transaction(self.factory) as session:
                await stage_fen_results(session,[dict(fen=FENS[1],score=300,next_moves=None)],[],count_new=True)
                raise RuntimeError('before commit')
        self.assertEqual(await self.snapshot(),before)
        await self.assert_unlocked()

    async def test_refresh_waiting_for_score_commit_reads_new_source_once(self):
        async with write_transaction(self.factory) as session:
            await stage_fen_results(session,[dict(fen=FENS[0],score=0,next_moves=None)],[],count_new=True)
            task=self.start(self.call('refresh_scored_position_summary'))
            await self.wait_for_lock(task)
        value=await asyncio.wait_for(task,5)
        self.assertEqual((value['analyzed_fens'],value['scored_positions'],value['buckets'][0]['positions']),(1,1,1))
        self.assertEqual((await self.counts())[0]['analyzed_fens'],1)
        await self.assert_unlocked()

    async def test_score_waiting_for_refresh_does_not_double_count(self):
        entered,release=asyncio.Event(),asyncio.Event()
        original=operations.driver_connection
        class PausedSource:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,name):return getattr(self.connection,name)
            async def fetchrow(self,sql,*args):
                row=await self.connection.fetchrow(sql,*args)
                if sql==storage.FEN_SELECT:
                    entered.set();await release.wait()
                return row
        async def driver(session):return PausedSource(await original(session))
        with patch.object(operations,'driver_connection',driver):
            refresh=self.start(self.call('refresh_scored_position_summary'))
            try:
                await asyncio.wait_for(entered.wait(),2)
                score=self.start(self.publish([dict(fen=FENS[0],score=0,next_moves=None)]))
                await self.wait_for_lock(score)
            finally:release.set()
            value,_=await asyncio.wait_for(asyncio.gather(refresh,score),5)
        self.assertEqual(value['analyzed_fens'],0)
        database,scored=await self.counts()
        self.assertEqual((database['analyzed_fens'],scored['analyzed_fens'],scored['equal_positions']),(1,1,1))
        before=await self.counts()
        await self.call('refresh_scored_position_summary')
        self.assertEqual(await self.counts(),before)
        await self.assert_unlocked()

    async def test_ingestion_occurrence_deltas_preserve_scored_counts_and_retry_exactly(self):
        await self.publish([dict(fen=FENS[0],score=-2147483648,next_moves=None)])
        before=await self.counts()
        fens=[dict(fen=FENS[0],piece_count=32,moves_counter='')]
        links=[dict(game_link=101,n_move=77,move_color='white',fen_fen=FENS[0])]
        async with write_transaction(self.factory) as session:
            self.assertEqual(await persist_positions(session,fens,links),{'new_positions':0,'new_links':1})
            task=self.start(self.call('refresh_scored_position_summary'))
            await self.wait_for_lock(task)
        await asyncio.wait_for(task,5)
        once=await self.counts()
        self.assertEqual(once[0],before[0])
        self.assertEqual(once[1]['mate_positions'],1)
        self.assertEqual(once[1]['mate_appearances'],before[1]['mate_appearances']+1)
        async with write_transaction(self.factory) as session:
            self.assertEqual(await persist_positions(session,fens,links),{'new_positions':0,'new_links':0})
        self.assertEqual(await self.counts(),once)
        await self.assert_unlocked()

    async def test_full_refresh_cancellation_rolls_back_both_caches_and_finishes_cleanup(self):
        # Direct fixture write leaves caches intentionally stale to expose partial publication.
        await self.db.execute("UPDATE fen SET score=300,analysis_source='stockfish',analyzed_at=now()")
        cached_before=(await self.snapshot())[1:]
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        original=operations.driver_connection
        class PausedPublish:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,name):return getattr(self.connection,name)
            async def fetchrow(self,sql,*args):
                row=await self.connection.fetchrow(sql,*args)
                if sql.startswith('UPDATE public.database_summary SET'):
                    entered.set();await asyncio.Future()
                return row
        async def driver(session):return PausedPublish(await original(session))
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=closing;return session
        with patch.object(operations,'driver_connection',driver):
            task=self.start(operations.refresh_scored_position_summary(session_factory=factory))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2)
                task.cancel();await asyncio.sleep(0)
                self.assertFalse(task.done())
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
        self.assertEqual((await self.snapshot())[1:],cached_before)
        await self.assert_unlocked()

    async def test_failed_full_source_query_propagates_without_replacing_prior_summary(self):
        cached_before=(await self.snapshot())[1:]
        await self.db.execute('ALTER TABLE fen RENAME TO missing_fen')
        with self.assertRaises(asyncpg.UndefinedTableError):
            await self.call('refresh_scored_position_summary')
        cached_after=[await self.db.fetch('SELECT * FROM '+table+' ORDER BY 1')
                      for table in ('database_summary','scored_position_summary')]
        self.assertEqual(cached_after,cached_before)
        await self.assert_unlocked()

    async def test_pipeline_uses_canonical_eligibility_and_preserves_tablebase_clamping(self):
        async with write_transaction(self.factory) as session:
            await game_coverage.refresh_coverage(session)
        value=await self.call('refresh_fen_pipeline_summary')
        self.assertEqual({k:v for k,v in value.items() if k!='refreshed_at'},dict(parsed_games=2,
            analyzable_games=2,excluded_games=0,fen_extracted_games=2,tablebase_marked_games=0))
        self.assertIsInstance(value['refreshed_at'],str)
        self.assertEqual((await self.call('refresh_fen_pipeline_summary',mark_tablebase_current=True))['tablebase_marked_games'],2)
        await self.db.execute('DELETE FROM game WHERE link=105')
        value=await self.call('refresh_fen_pipeline_summary')
        self.assertEqual((value['parsed_games'],value['fen_extracted_games'],value['tablebase_marked_games']),(1,1,1))
