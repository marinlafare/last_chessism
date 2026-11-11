"""Canonical dashboard and ranking publication, with real import/refresh races."""
import asyncio
import copy
import json
from collections import Counter
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from unittest.mock import patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError, NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, dashboard_reads as reads, mode_summary as store
from chessism_api.database.postgresql.game_import_summaries import refresh_import_summaries
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction, driver_connection
from chessism_api.operations import dashboard_views as views, mode_summary as refresh
from chessism_api.operations.ingestion_pipeline import game_importer
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL
from tests.check_canonical_player_views_postgres import GAMES, perspectives
from tests.check_canonical_ingestion_postgres import archives


class CanonicalDashboardTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        ids=await seed(self.db);self.ids={'alice':ids['white'],'bob':ids['black']}
        await self.db.execute("UPDATE player SET player_name='alice',joined=1,title='GM',name='Alice',avatar='a',url='https://example.invalid/a' WHERE id=$1",ids['white'])
        await self.db.execute("UPDATE player SET player_name='bob',joined=1,title=' im ',name='Bob' WHERE id=$1",ids['black'])
        await self.db.execute("""INSERT INTO player(player_name,joined,title,deleted_at) VALUES
            ('carol',1,NULL,NULL),('dave',0,'GM',NULL),('eve',1,'WGM',now()),('ghost',-1,'GM',NULL)""")
        self.modes={}
        for mode in ('bullet','blitz','rapid'):
            self.modes[mode]=await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',mode)
        for link,white,black,at,mode,wr,br,ws,bs,length,eco,we,be in GAMES:
            if link not in (101,105):
                await self.db.execute(GAME_SQL,link,self.ids[white],self.ids[black],datetime.fromisoformat(at),length,'chess',None)
            await self.db.execute('''UPDATE game SET played_at=$2,mode_id=$3,white_elo=$4,black_elo=$5,
                white_result=$6,black_result=$7,n_moves=$8 WHERE link=$1''',
                link,datetime.fromisoformat(at),self.modes.get(mode),wr,br,ws,bs,length)
        await self.db.execute("UPDATE game SET mode_id=$1,white_elo=9999,black_elo=9999,played_at='2099-01-01' WHERE link IN (102,103,104)",self.modes['rapid'])
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000','timezone':'America/Mexico_City'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.tasks=[];self.addAsyncCleanup(self.stop_tasks)
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session)
            await refresh_import_summaries(await driver_connection(session),['alice','bob'],[])
        await self.db.execute('UPDATE database_summary SET n_positions=5,scored_fens=2')

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():task.cancel()
        if self.tasks:await asyncio.gather(*self.tasks,return_exceptions=True)

    async def call(self,name,*args,**kwargs):
        return await getattr(views,name)(*args,factory=self.factory,**kwargs)

    async def rebuild(self,names=None):
        if names is None:return await refresh.refresh_main_character_mode_summary(session_factory=self.factory)
        return await refresh.refresh_main_character_mode_summary_for_players(names,session_factory=self.factory)

    async def snapshot(self):
        return await self.db.fetch('SELECT * FROM main_character_mode_summary ORDER BY mode,player_id')

    def expected(self,player,mode):
        rows=[r for r in perspectives(player) if r['mode']==mode]
        newest=min(rows,key=lambda r:(-r['played_at'].timestamp(),-r['link'],r['color']!='white'))
        average=int((Decimal(sum(r['rating'] for r in rows))/len(rows)).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        wins,draws,losses=(sum(r['result']==v for r in rows) for v in (1,.5,0))
        return {'player_name':player,'full_name':player.title(),'title':'GM' if player=='alice' else 'IM',
            'avatar':'a' if player=='alice' else '', 'profile_url':'https://example.invalid/a' if player=='alice' else '',
            'last_game_date':newest['played_at'].strftime('%Y-%b-%d').upper(),
            'rating':newest['rating'],'last_rating':newest['rating'],'avg_game_rating':average,
            'n_games':len(rows),'wins':wins,'draws':draws,'losses':losses,
            'as_white':sum(r['color']=='white' for r in rows),'as_black':sum(r['color']=='black' for r in rows),
            'score_rate':round((wins+.5*draws)/len(rows),4)}

    async def test_import_and_explicit_full_refresh_match_independent_side_oracle(self):
        for mode in ('bullet','blitz','rapid'):
            expected=[self.expected(p,mode) for p in ('alice','bob') if any(r['mode']==mode for r in perspectives(p))]
            expected.sort(key=lambda r:(-r['rating'],-r['n_games'],r['player_name']))
            data=await self.call('get_top_main_characters_by_time_control',mode,50)
            self.assertEqual(data,{'time_control':mode,'limit':50,'title_status':'all','players':expected})
        self.assertEqual(await self.rebuild(),{'bullet':2,'blitz':1,'rapid':2,'total':5})
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','blitz'))['players'],[self.expected('alice','blitz')])

    async def test_dashboard_stored_totals_and_main_game_counts_preserve_distinct_games(self):
        self.assertEqual(await self.call('get_games_database_generalities'),
            {'n_games_in_db':5,'main_characters':5,'secondary_characters':1,'n_positions':5,'scored_fens':2})
        expected=dict(Counter(g[4] for g in GAMES if g[4]))
        self.assertEqual(await self.call('get_time_control_mode_counts'),expected)
        self.assertEqual(await self.call('get_main_character_time_control_counts'),expected)
        # Hiding profiles does not erase their recorded participation in main-game counts.
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name IN ('alice','bob')")
        self.assertEqual(await self.call('get_main_character_time_control_counts'),expected)
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet'))['players'],[])
        await self.db.execute("UPDATE player SET joined=0 WHERE player_name IN ('alice','bob')")
        self.assertEqual(await self.call('get_main_character_time_control_counts'),{'bullet':0,'blitz':0,'rapid':0})

    async def test_pipeline_and_remaining_extraction_use_canonical_eligibility(self):
        data=await self.call('get_fen_pipeline_overview')
        self.assertEqual({k:v for k,v in data.items() if k!='refreshed_at'},
            {'total_games':5,'parsed_games':5,'analyzable_games':5,'excluded_games':0,
             'fen_extracted_games':2,'tablebase_marked_games':0})
        self.assertTrue(data['refreshed_at'].endswith('+00:00'))
        self.assertEqual(await self.call('get_remaining_extraction_count'),5)
        await self.db.execute('UPDATE game SET fens_done=true WHERE link=101;UPDATE game SET fens_processing=true WHERE link=105')
        self.assertEqual(await self.call('get_remaining_extraction_count'),4)
        await self.db.execute('UPDATE game SET fens_done=true,fens_processing=false WHERE is_standard AND n_moves>0')
        self.assertEqual(await self.call('get_remaining_extraction_count'),0)

    async def test_title_filters_precede_limit_and_hide_deleted_untracked_profiles(self):
        # Include null ratings and arbitrary nonofficial titles as in the old ranking acceptance fixture.
        await self.db.execute("""INSERT INTO main_character_mode_summary(mode,player_id,n_games,rating)
            SELECT 'bullet',id,10,4000 FROM player WHERE player_name IN ('carol','dave','eve','ghost');
            UPDATE player SET title='Coach' WHERE player_name='carol'""")
        titled=await self.call('get_top_main_characters_by_time_control',' BULLET ',1,'titled')
        self.assertEqual([r['player_name'] for r in titled['players']],['alice'])
        untitled=await self.call('get_top_main_characters_by_time_control','bullet',1,'untitled')
        self.assertEqual([r['player_name'] for r in untitled['players']],['carol'])
        self.assertEqual([r['player_name'] for r in (await self.call('get_top_main_characters_by_time_control','bullet',50))['players']],['carol','alice','bob'])
        await self.db.execute("UPDATE main_character_mode_summary SET rating=NULL WHERE player_id=(SELECT id FROM player WHERE player_name='carol')")
        self.assertEqual([r['player_name'] for r in (await self.call('get_top_main_characters_by_time_control','bullet',50))['players']],['alice','bob','carol'])
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet',50))['players'][-1]['rating'],0)

    async def test_stable_rank_ties_latest_timestamp_and_side_ratings(self):
        await self.db.execute("UPDATE main_character_mode_summary SET rating=2000,n_games=5 WHERE mode='bullet'")
        self.assertEqual([r['player_name'] for r in (await self.call('get_top_main_characters_by_time_control','bullet'))['players']],['alice','bob'])
        # Same timestamp: the higher game ID wins, then the existing A2 white-side rule.
        await self.db.execute("UPDATE game SET played_at='2026-01-04' WHERE link=101")
        await self.rebuild()
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet'))['players'][0]['rating'],2200)
        await self.db.execute("UPDATE game SET played_at='2026-01-05' WHERE link=101")
        await self.rebuild({'alice'})
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet'))['players'][0]['rating'],2000)

    async def test_scoped_refresh_replaces_only_selected_players_and_normalizes_names(self):
        before=[r for r in await self.snapshot() if r['player_id']==self.ids['bob']]
        await self.db.execute('UPDATE game SET white_elo=2400 WHERE link=108')
        self.assertEqual(await self.rebuild({' ALICE ','alice',''}),{'bullet':1,'blitz':1,'rapid':1,'total':3})
        self.assertEqual([r for r in await self.snapshot() if r['player_id']==self.ids['bob']],before)
        await self.db.execute("UPDATE player SET joined=0 WHERE player_name='alice'")
        self.assertEqual(await self.rebuild({'alice'}),{'bullet':0,'blitz':0,'rapid':0,'total':0})
        self.assertTrue(all(r['player_id']==self.ids['bob'] for r in await self.snapshot()))
        self.assertEqual(await self.rebuild({'missing'}),{'bullet':0,'blitz':0,'rapid':0,'total':0})

    async def test_empty_scope_and_invalid_mode_do_not_open_database(self):
        def forbidden():raise AssertionError('Unexpected database access')
        self.assertEqual(await refresh.refresh_main_character_mode_summary_for_players([],session_factory=forbidden),
                         {'bullet':0,'blitz':0,'rapid':0,'total':0})
        self.assertEqual(await views.get_top_main_characters_by_time_control('invalid',factory=forbidden),
                         {'time_control':'invalid','players':[],'limit':0})
        with self.assertRaisesRegex(ValueError,'title filter'):
            await views.get_top_main_characters_by_time_control('bullet',title_status='unknown',factory=forbidden)
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet',0))['limit'],1)
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet',9000))['limit'],5000)

    async def test_empty_projection_is_not_rebuilt_by_reads_and_missing_singleton_fails(self):
        await self.db.execute('DELETE FROM main_character_mode_summary')
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','bullet'))['players'],[])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM main_character_mode_summary'),0)
        await self.db.execute('DELETE FROM database_summary;DELETE FROM fen_pipeline_summary')
        for name in ('get_games_database_generalities','get_time_control_mode_counts','get_fen_pipeline_overview'):
            with self.assertRaises(NoResultFound):await self.call(name)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM database_summary'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen_pipeline_summary'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_empty_database_can_refresh_and_read_zero_totals(self):
        await self.db.execute('DELETE FROM game;DELETE FROM fen;DELETE FROM months;DELETE FROM player')
        async with self.db.transaction():await refresh_import_summaries(self.db,[],[])
        await self.db.execute('UPDATE database_summary SET n_positions=0,scored_fens=0')
        self.assertEqual(await self.rebuild(),{'bullet':0,'blitz':0,'rapid':0,'total':0})
        self.assertEqual(await self.call('get_games_database_generalities'),
            {'n_games_in_db':0,'main_characters':0,'secondary_characters':0,'n_positions':0,'scored_fens':0})
        self.assertEqual(await self.call('get_main_character_time_control_counts'),{'bullet':0,'blitz':0,'rapid':0})
        self.assertEqual(await self.call('get_remaining_extraction_count'),0)
        self.assertEqual((await self.call('get_fen_pipeline_overview'))['total_games'],0)

    async def test_failure_after_delete_rolls_back_previous_projection(self):
        before=await self.snapshot();original=refresh.driver_connection
        class FailAfterDelete:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,name):return getattr(self.connection,name)
            async def execute(self,sql,*args):
                result=await self.connection.execute(sql,*args)
                if sql.startswith('DELETE'):raise RuntimeError('after projection delete')
                return result
        async def broken(session):return FailAfterDelete(await original(session))
        with patch.object(refresh,'driver_connection',broken),self.assertRaisesRegex(RuntimeError,'after projection delete'):
            await self.rebuild()
        self.assertEqual(await self.snapshot(),before);self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_adapter_transaction_ownership_and_caller_rollback(self):
        with self.assertRaisesRegex(ValueError,'caller-owned'):await store.refresh_modes(self.db)
        before=await self.snapshot()
        observer=await self.connect()
        transaction=self.db.transaction();await transaction.start()
        try:
            await self.db.execute("UPDATE player SET joined=0 WHERE player_name='alice'")
            counts=await store.refresh_modes(self.db,['alice'])
            self.assertEqual(counts['total'],0)
            self.assertEqual(await observer.fetch('SELECT * FROM main_character_mode_summary ORDER BY mode,player_id'),before)
        finally:await transaction.rollback()
        self.assertEqual(await self.snapshot(),before)
        async with self.factory() as session:
            for fn,args in [(reads.database_totals,()),(reads.pipeline_totals,()),(reads.remaining_extraction,()),
                            (reads.main_game_counts,()),(reads.ranked_players,('bullet',5,'all'))]:
                with self.assertRaisesRegex(ValueError,'caller-owned'):await fn(session,*args)

    async def test_actual_import_blocks_refresh_until_commit_then_new_rows_are_visible(self):
        raw=copy.deepcopy(archives()[2026][1][0]);raw['url']='https://www.chess.com/game/live/900'
        raw['white']['username']='alice';raw['black']['username']='bob'
        raw['white']['rating']=2500;raw['black']['rating']=2400
        formatted=await game_importer.format_games({2026:{1:[raw]}},'alice')
        imported,release,refresh_started=(asyncio.Event() for _ in range(3));waiting_pid=None
        original_import=game_importer.refresh_import_summaries;original_refresh=refresh.refresh_modes
        async def hold_import(*args):
            await original_import(*args);imported.set();await release.wait()
        async def identify_waiter(connection,names):
            nonlocal waiting_pid
            waiting_pid=await connection.fetchval('SELECT pg_backend_pid()')
            refresh_started.set();return await original_refresh(connection,names)
        async def wait_for_lock():
            while await self.db.fetchval("SELECT wait_event='advisory' FROM pg_stat_activity WHERE pid=$1",waiting_pid) is not True:
                await asyncio.sleep(.01)
        with patch.object(game_importer,'AsyncDBSession',self.factory),patch.object(game_importer,'refresh_import_summaries',hold_import),patch.object(refresh,'refresh_modes',identify_waiter):
            importer=asyncio.create_task(game_importer.insert_games_months_moves_and_players(formatted,'alice'));self.tasks.append(importer)
            try:
                await asyncio.wait_for(imported.wait(),2)
                rebuilding=asyncio.create_task(self.rebuild());self.tasks.append(rebuilding)
                await asyncio.wait_for(refresh_started.wait(),2);await asyncio.wait_for(wait_for_lock(),2)
                self.assertFalse(rebuilding.done())
                self.assertIsNone(await self.db.fetchval('SELECT link FROM game WHERE link=900'))
            finally:release.set()
            result,counts=await asyncio.gather(importer,rebuilding)
        self.assertIn('inserted 1 games',result);self.assertEqual(counts['total'],5)
        rows=(await self.call('get_top_main_characters_by_time_control','rapid'))['players']
        self.assertEqual([(r['player_name'],r['n_games'],r['last_rating']) for r in rows],[('alice',2,2500),('bob',2,2400)])

    async def test_overlapping_full_and_scoped_refreshes_do_not_duplicate_rows(self):
        results=await asyncio.gather(self.rebuild(),self.rebuild({'alice'}),self.rebuild({'alice','bob'}))
        self.assertEqual([r['total'] for r in results],[5,3,5])
        self.assertEqual(len(await self.snapshot()),5)
        self.assertEqual((await self.call('get_top_main_characters_by_time_control','blitz'))['players'],[self.expected('alice','blitz')])

    async def test_repeated_cancellation_finishes_rollback_and_close_after_publication(self):
        before=await self.snapshot();written,rolling,release,closed=(asyncio.Event() for _ in range(4))
        original=refresh.refresh_modes
        async def paused(*args):await original(*args);written.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():rolling.set();await release.wait();await rollback()
            async def finish():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=finish;return session
        with patch.object(refresh,'refresh_modes',paused):
            task=asyncio.create_task(refresh.refresh_main_character_mode_summary(session_factory=factory));self.tasks.append(task)
            try:
                await asyncio.wait_for(written.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2);task.cancel();task.cancel()
                self.assertFalse(task.done())
            finally:release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(closed.is_set());self.assertEqual(await self.snapshot(),before)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_sql_failures_are_not_empty_successes_and_read_only_is_enforced(self):
        await self.db.execute('ALTER TABLE main_character_mode_summary RENAME TO unavailable_modes')
        for fn,error in ((lambda:self.call('get_top_main_characters_by_time_control','bullet'),DBAPIError),
                         (self.rebuild,asyncpg.UndefinedTableError)):
            with self.assertRaises(error):await fn()
            self.assertEqual(self.engine.pool.checkedout(),0)
        async def bad_write(session):await session.execute(text('DELETE FROM game'))
        with patch.object(reads,'main_game_counts',bad_write),self.assertRaises(DBAPIError):
            await self.call('get_main_character_time_control_counts')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),8)

    async def test_standalone_refresh_uses_read_committed_even_with_repeatable_read_default(self):
        engine=create_async_engine(self.engine.url,connect_args={'server_settings':{'default_transaction_isolation':'repeatable read'}})
        try:
            factory=async_sessionmaker(engine,expire_on_commit=False);original=refresh.refresh_modes
            async def inspect(connection,names):
                self.assertEqual(await connection.fetchval('SHOW transaction_isolation'),'read committed')
                return await original(connection,names)
            with patch.object(refresh,'refresh_modes',inspect):
                self.assertEqual((await refresh.refresh_main_character_mode_summary(session_factory=factory))['total'],5)
        finally:await engine.dispose()

    async def test_actual_routes_serialize_rankings_counts_and_remaining_games(self):
        from chessism_api.routers import games,players,fens
        with patch.object(views,'AsyncDBSession',self.factory):
            self.assertEqual(json.loads((await games.api_get_games_database_generalities()).body)['n_games_in_db'],5)
            self.assertEqual(json.loads((await fens.api_get_remaining_games_needing_fens()).body),{'remaining_games':5})
            self.assertEqual(json.loads((await fens.api_get_fen_pipeline_overview()).body)['fen_extracted_games'],2)
            ranking=json.loads((await players.api_get_top_main_characters('blitz',5,'titled')).body)
            self.assertEqual(ranking['players'],[self.expected('alice','blitz')])
