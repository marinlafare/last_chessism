"""Canonical read/preparation contracts on disposable PostgreSQL; no live data."""
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, game_analysis_reads as reads
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import game_analysis_views as views
from chessism_api.operations.cloud_analysis import prepared_selection
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL, FENS, PLAYED


class CanonicalAnalysisViewTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        await self.db.execute("UPDATE fen SET score=24,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1", FENS[0])
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000', 'timezone': 'America/Mexico_City'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.tasks = []
        self.addAsyncCleanup(self.stop_tasks)
        await self.refresh()

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    def start(self, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        return task

    async def refresh(self):
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session)

    async def game(self, link, at=PLAYED, fens=None):
        await self.db.execute(GAME_SQL, link, self.ids['white'], self.ids['black'], at, 2, 'chess', None)
        for move, fen_id in enumerate(fens or self.ids['fens'][:3]):
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',link,fen_id,move,move%2==0)

    async def page(self, **kwargs):
        return await views.get_scored_game_analysis_page(factory=self.factory, **kwargs)

    async def preview(self, **kwargs):
        return await views.preview_player_games_for_analysis('synthetic_white',
            factory=self.factory, **{'order': 'latest', 'game_limit': None, **kwargs})

    async def scope(self, **kwargs):
        return await views.get_player_game_analysis_scope('synthetic_white', factory=self.factory, **kwargs)

    async def test_empty_database_and_absent_player_preserve_response_contracts(self):
        await self.db.execute('DELETE FROM game')
        self.assertEqual(await views.get_scored_game_analysis_overview(factory=self.factory),
            {'games_with_positions': 0, 'fully_analyzed_games': 0, 'incomplete_games': 0,
             'total_game_positions': 0, 'analyzed_game_positions': 0, 'unscored_game_positions': 0})
        self.assertEqual(await self.page(page=9),
            {'status': 'fully', 'page': 9, 'page_size': 20, 'total': 0, 'total_pages': 0, 'rows': []})
        self.assertEqual(await self.scope(), {'player_name':'synthetic_white', 'games_with_fens':0,
            'complete_games':0, 'incomplete_games':0, 'earliest_game':None, 'latest_game':None})
        self.assertEqual(await self.preview(prepared_plan_id='empty'), {'player_name':'synthetic_white',
            'game_links':[], 'selected_games':0, 'position_occurrences':0, 'analyzed_occurrences':0,
            'unscored_occurrences':0, 'unique_fens':0, 'analyzed_unique_fens':0, 'fens_to_analyze':0,
            'tablebase_fens':0, 'available_periods':0, 'sampled_periods':0})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_game_prepared_selection'), 0)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_overview_counts_games_once_and_counts_each_position_occurrence(self):
        self.assertEqual(await views.get_scored_game_analysis_overview(factory=self.factory),
            {'games_with_positions':2,'fully_analyzed_games':0,'incomplete_games':2,
             'total_game_positions':6,'analyzed_game_positions':2,'unscored_game_positions':4})
        scope = await self.scope()
        self.assertEqual((scope['games_with_fens'], scope['incomplete_games']), (2, 2))
        self.assertEqual(scope['earliest_game'], '2026-01-01T00:00:00+00:00')
        absent = await views.get_player_game_analysis_scope('absent', factory=self.factory)
        self.assertEqual(absent['games_with_fens'], 0)

    async def test_page_ties_pagination_bounds_and_status_normalization(self):
        await self.game(106)
        await self.refresh()
        first = await self.page(status='incomplete', page=1, page_size=2)
        second = await self.page(status='incomplete', page=99, page_size=2)
        self.assertEqual([r['link'] for r in first['rows']], [106, 105])
        self.assertEqual([r['link'] for r in second['rows']], [101])
        self.assertEqual((second['page'], second['total'], second['total_pages']), (2, 3, 2))
        result = await self.page(status=' invalid SQL ', page=-2, page_size=500)
        self.assertEqual((result['status'], result['page'], result['page_size'], result['total']), ('fully', 1, 50, 0))
        self.assertEqual((await self.page(status=' ALL ', page_size=0))['page_size'], 20)

    async def test_fully_order_uses_count_then_link_and_scores_are_visible_page_aggregates(self):
        await self.game(106, fens=[self.ids['fens'][0]])
        await self.db.execute('UPDATE game_fen_association SET fen_id=$1 WHERE game_link=101', self.ids['fens'][0])
        await self.refresh()
        result = await self.page(status='fully', page_size=1)
        self.assertEqual([r['link'] for r in result['rows']], [101])
        self.assertEqual(result['total'], 2)
        row = result['rows'][0]
        self.assertEqual((row['avg_score'], row['avg_abs_score'], row['max_abs_score']), (24., 24., 24.))
        self.assertEqual((row['white'], row['black'], row['mode']), ('synthetic_white', 'synthetic_black', ''))
        self.assertEqual(row['played_at'], '2026-01-01T00:00:00+00:00')
        mixed = await self.page(status='all')
        self.assertEqual([r['link'] for r in mixed['rows']], [101, 106, 105])

    async def test_unscored_page_averages_are_null_and_game_mode_comes_from_relationship(self):
        mode = await self.db.fetchval("INSERT INTO game_mode(name) VALUES('rapid') RETURNING id")
        await self.game(106, fens=[self.ids['fens'][1]])
        await self.db.execute('UPDATE game SET mode_id=$1 WHERE link=106', mode)
        await self.refresh()
        row = next(r for r in (await self.page(status='all'))['rows'] if r['link'] == 106)
        self.assertEqual((row['avg_score'], row['avg_abs_score'], row['max_abs_score']), (None, None, None))
        self.assertEqual(row['mode'], 'rapid')

    async def test_scopes_and_previews_share_eligible_games_and_self_play_counts_once(self):
        await self.db.execute('UPDATE game_fen_association SET fen_id=$1 WHERE game_link=101', self.ids['fens'][0])
        await self.refresh()
        scope = await self.scope()
        self.assertEqual((scope['games_with_fens'], scope['complete_games'], scope['incomplete_games']), (2, 1, 1))
        self.assertEqual((await self.preview())['game_links'], [105])
        self.assertEqual((await views.preview_player_games_for_analysis('synthetic_black', order='latest',
            game_limit=10, factory=self.factory))['selected_games'], 0)
        # Variants, custom setup and zero-move metadata exist but never enter analysis.
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'), 5)

    async def test_latest_oldest_ties_and_limit_are_deterministic(self):
        await self.game(106)
        await self.refresh()
        self.assertEqual((await self.preview(game_limit=2))['game_links'], [106, 105])
        self.assertEqual((await self.preview(order='oldest', game_limit=2))['game_links'], [101, 105])
        self.assertEqual((await self.preview(order='unknown', game_limit=0))['game_links'], [106])
        self.assertEqual((await self.preview(order='oldest'))['game_links'], [101, 105, 106])

    async def test_utc_inclusive_lower_exclusive_upper_and_month_fairness(self):
        await self.db.execute("UPDATE game SET played_at='2026-01-31 23:59:59+00' WHERE link=105")
        await self.game(106, datetime(2026, 2, 1, tzinfo=timezone.utc))
        await self.game(107, datetime(2026, 3, 1, tzinfo=timezone.utc))
        await self.refresh()
        bounds = {'date_from':datetime(2026, 2, 1, tzinfo=timezone.utc),
                  'date_to_exclusive':datetime(2026, 3, 1, tzinfo=timezone.utc)}
        self.assertEqual((await self.scope(**bounds))['games_with_fens'], 1)
        selected = await self.preview(**bounds)
        self.assertEqual(selected['game_links'], [106])
        self.assertEqual(selected['first_game_at'], '2026-02-01T00:00:00+00:00')
        sampled = await self.preview(order='fair_range', game_limit=3)
        self.assertEqual(sampled['game_links'], [101, 106, 107])
        self.assertEqual((sampled['available_periods'], sampled['sampled_periods']), (3, 3))
        self.assertEqual((await self.preview(order='fair_range', game_limit=2))['game_links'], [101, 107])

    async def test_preview_distinct_fens_and_occurrences_include_claims_but_report_tablebase_separately(self):
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,9,false)', self.ids['fens'][3])
        await self.db.execute('INSERT INTO game_fen_association VALUES(105,$1,9,false)', self.ids['fens'][3])
        await self.db.execute('INSERT INTO game_fen_association VALUES(105,$1,10,true)', self.ids['fens'][4])
        await self.db.execute("UPDATE fen SET analysis_source='tablebase_unavailable' WHERE fen=$1", FENS[4])
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", FENS[1])
        await self.refresh()
        value = await self.preview()
        self.assertEqual({k:value[k] for k in ('position_occurrences','analyzed_occurrences','unscored_occurrences',
            'unique_fens','analyzed_unique_fens','fens_to_analyze','tablebase_fens')},
            {'position_occurrences':9,'analyzed_occurrences':2,'unscored_occurrences':7,
             'unique_fens':5,'analyzed_unique_fens':1,'fens_to_analyze':4,'tablebase_fens':1})

    async def test_page_count_and_rows_share_snapshot_during_concurrent_game_deletion(self):
        observed = []
        def factory():
            session = self.factory(); scalar = session.scalar
            async def counted(statement, *args, **kwargs):
                result = await scalar(statement, *args, **kwargs)
                if 'SELECT count(*) FROM public.game_analysis_coverage' in str(statement):
                    observed.append((await scalar(text('SHOW transaction_isolation')),
                                     await scalar(text('SHOW transaction_read_only'))))
                    await self.db.execute('DELETE FROM game WHERE link=105')
                return result
            session.scalar = counted
            return session
        value = await views.get_scored_game_analysis_page(status='all', factory=factory)
        self.assertEqual(observed, [('repeatable read', 'on')])
        self.assertEqual(value['total'], 2)
        self.assertEqual([r['link'] for r in value['rows']], [105, 101])
        self.assertEqual((await self.page(status='all'))['total'], 1)

    async def test_preview_counts_share_snapshot_with_selected_games(self):
        original = reads.incomplete_games
        async def selected_then_delete(*args, **kwargs):
            result = await original(*args, **kwargs)
            await self.db.execute('DELETE FROM game WHERE link=105')
            return result
        with patch.object(reads, 'incomplete_games', selected_then_delete):
            result = await self.preview()
        self.assertEqual(result['game_links'], [105, 101])
        self.assertEqual(result['position_occurrences'], 6)
        self.assertEqual((await self.preview())['position_occurrences'], 3)

    async def test_prepared_preview_commits_chunks_digest_and_claims_for_next_request(self):
        await self.db.execute("INSERT INTO cloud_fen_claim(fen,run_id) VALUES($1,'run-a')", FENS[1])
        value = await self.preview(prepared_plan_id='saved')
        self.assertEqual(value, {'player_name':'synthetic_white','game_links':[105,101],'selected_games':2,
            'fens_to_analyze':2,'claimed_fens':1,'prepared_fens_version':1,'available_periods':0,'sampled_periods':0})
        row = await self.db.fetchrow("SELECT * FROM cloud_game_prepared_selection WHERE id='saved'")
        self.assertEqual(row['games_sha256'], prepared_selection.games_digest([105,101]))
        self.assertEqual((row['missing'], row['claimed']), (2, 1))
        self.assertGreater(row['expires_at'], datetime.now(timezone.utc))
        chunk = await self.db.fetchrow("SELECT * FROM cloud_game_prepared_chunk WHERE plan_id='saved'")
        self.assertEqual(chunk['fen_ids'], self.ids['fens'][1:3])
        self.assertEqual(chunk['claimed'], [True, False])
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_failed_preparation_rolls_back_chunks_and_expiry_cleanup_then_retries(self):
        await self.db.execute("""INSERT INTO cloud_game_prepared_selection(id,games_sha256,missing,claimed,expires_at)
            VALUES('expired','old',0,0,now()-interval '1 hour')""")
        build = prepared_selection.build
        async def failed(*args):
            await build(*args)
            raise RuntimeError('after chunks before commit')
        with patch.object(prepared_selection, 'build', failed), self.assertRaisesRegex(RuntimeError, 'before commit'):
            await self.preview(prepared_plan_id='retry')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_game_prepared_chunk'), 0)
        self.assertEqual(await self.db.fetchval('SELECT id FROM cloud_game_prepared_selection'), 'expired')
        self.assertEqual(self.engine.pool.checkedout(), 0)
        self.assertEqual((await self.preview(prepared_plan_id='retry'))['fens_to_analyze'], 2)
        self.assertEqual(await self.db.fetchval('SELECT id FROM cloud_game_prepared_selection'), 'retry')

    async def test_cancelled_preparation_never_commits_partial_chunks(self):
        staged = asyncio.Event(); build = prepared_selection.build
        async def pause(*args):
            await build(*args)
            staged.set()
            await asyncio.Future()
        with patch.object(prepared_selection, 'build', pause):
            task = self.start(self.preview(prepared_plan_id='cancelled'))
            await asyncio.wait_for(staged.wait(), 3)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_game_prepared_selection'), 0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_game_prepared_chunk'), 0)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        self.assertEqual((await self.preview(prepared_plan_id='cancelled'))['fens_to_analyze'], 2)

    async def test_query_failures_propagate_instead_of_becoming_empty_results(self):
        await self.db.execute('ALTER TABLE game_analysis_coverage RENAME TO coverage_missing')
        for operation in (lambda:views.get_scored_game_analysis_overview(factory=self.factory),
                          lambda:self.page(), lambda:self.scope(), lambda:self.preview()):
            with self.assertRaises(DBAPIError):
                await operation()
            self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_read_views_refuse_writes_and_low_level_reads_require_a_transaction(self):
        original = reads.overview
        async def tries_to_write(session):
            await original(session)
            await session.execute(text("INSERT INTO player(player_name) VALUES('forbidden')"))
        with patch.object(reads, 'overview', tries_to_write), self.assertRaises(DBAPIError):
            await views.get_scored_game_analysis_overview(factory=self.factory)
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM player WHERE player_name='forbidden'"), 0)
        async with self.factory() as session:
            for operation in (lambda:reads.overview(session),lambda:reads.coverage_page(session,'all',1,20),
                lambda:reads.player_scope(session,'synthetic_white'),
                lambda:reads.incomplete_games(session,'synthetic_white',descending=True),lambda:reads.position_counts(session,[])):
                with self.assertRaisesRegex(ValueError, 'caller-owned'):
                    await operation()
            self.assertFalse(session.in_transaction())

    async def test_existing_routes_serialize_real_canonical_views(self):
        from chessism_api.routers import analysis, fens
        with patch.object(views, 'AsyncDBSession', self.factory):
            overview = json.loads((await fens.api_get_scored_game_analysis_overview()).body)
            page = json.loads((await fens.api_get_scored_game_analysis_page(
                status='incomplete', page=1, page_size=1)).body)
            scope = await analysis.api_get_player_game_analysis_scope(
                analysis.PlayerGameScopeRequest(player_name='synthetic_white'))
        self.assertEqual(overview['games_with_positions'], 2)
        self.assertEqual(page['total'], 2)
        self.assertEqual(page['rows'][0]['link'], 105)
        self.assertEqual(page['rows'][0]['played_at'], '2026-01-01T00:00:00+00:00')
        self.assertEqual(scope['incomplete_games'], 2)

    async def test_synthetic_page_plan_aggregates_only_visible_games(self):
        # Synthetic fanout; this is not evidence about historical corpus scale.
        await self.db.executemany(GAME_SQL, [(link,self.ids['white'],self.ids['black'],PLAYED,2,'chess',None)
                                           for link in range(1000,2000)])
        await self.db.execute('''INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white)
            SELECT g.link,f.fen_id,f.fen_id::int,true FROM game g CROSS JOIN fen f WHERE g.link>=1000''')
        await self.refresh()
        await self.db.execute('ANALYZE game; ANALYZE game_fen_association; ANALYZE game_analysis_coverage; ANALYZE fen')
        _, query = reads.page_sql('all')
        query = query.replace(':offset','$1').replace(':limit','$2')
        plan = json.loads(await self.db.fetchval('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query, 0, 2))[0]
        def nodes(node):
            yield node
            for child in node.get('Plans', []):
                yield from nodes(child)
        aggregates = [n for n in nodes(plan['Plan']) if n['Node Type']=='Aggregate']
        self.assertEqual(plan['Plan']['Actual Rows'], 2)
        self.assertEqual(len(aggregates), 1)
        self.assertEqual(aggregates[0]['Actual Loops'], 2)
        print('CANONICAL_ANALYSIS_PAGE_PLAN '+json.dumps({'games':1002,'associations':5006,
            'page_size':2,'aggregate_loops':aggregates[0]['Actual Loops'],
            'execution_ms':plan['Execution Time']}), flush=True)
