"""Native engine insights and hero quality reads on disposable PostgreSQL."""
from datetime import date
from unittest.mock import AsyncMock, patch

from sqlalchemy.exc import DBAPIError

from chessism_api.operations import player_analytics as insights
from chessism_api.operations import player_hero_analytics as hero
from chessism_api.operations import player_hero_efficiency as efficiency
from chessism_api.operations.player_game_scores import refresh_game_player_engine_summaries
from chessism_api.operations.cuda_summaries import score_publication
from tests.check_canonical_cuda_postgres import CanonicalCudaTests, DEVICE


class CanonicalQualityTests(CanonicalCudaTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        run=await self.request();await self.stage(run)
        for _ in range(3):await score_publication.finalize(self.factory,run,DEVICE)
        for module in (insights,hero,efficiency):
            change=patch.object(module,'AsyncDBSession',self.factory)
            change.start();self.addCleanup(change.stop)
        await self.db.execute("UPDATE player SET timezone='UTC'")

    async def test_range_daily_calendar_and_position_contracts_keep_native_sides(self):
        with patch.object(hero,'refresh_game_player_engine_summaries',AsyncMock(side_effect=AssertionError('unexpected refresh'))), \
             patch.object(efficiency,'refresh_game_player_engine_summaries',AsyncMock(side_effect=AssertionError('unexpected refresh'))):
            scores=await hero.get_player_range_game_scores('alice',game_ids=list(self.links))
            self.assertEqual(scores['coverage']['eligible_games'],3)
            self.assertEqual({(r['game_id'],r['player_color']) for r in scores['games']},
                {(1,'white'),(self.links[1],'white'),(self.links[1],'black')})
            cp=await hero.get_player_daily_game_cp('alice',timezone_name='UTC')
            accuracy=await efficiency.get_player_daily_efficiency('alice',timezone_name='UTC')
            calendar=await hero.get_player_quality_calendar('alice',timezone_name='UTC')
            game=await hero.get_player_game_measures('alice',1,timezone_name='UTC')
            hour=await hero.get_player_hour_measures('alice',date(2026,1,1),12,timezone_name='UTC')
        self.assertEqual(len(cp['points']),1);self.assertEqual(len(accuracy['points']),1)
        self.assertEqual(cp['points'][0][0],'2026-01-01')
        self.assertEqual(len(game['game']['positions']),22)
        self.assertEqual({r['color'] for r in hour['games']},{'white','black'})
        self.assertEqual(len(hour['games']),3)
        self.assertEqual(calendar['coverage']['total_games'],3)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_insight_windows_do_not_mix_self_play_sides_and_patterns_keep_counts(self):
        filters=insights.normalize_player_analytics_filters('alice','all',date(2026,1,1),date(2026,1,1))
        payload=await insights.get_player_engine_insights(filters)
        patterns=await insights.get_player_playing_patterns(filters)
        self.assertEqual(payload['coverage']['sampled_games'],3)
        # The first White ply has no prior evaluation; Black has all 11 transitions.
        self.assertEqual(payload['coverage']['move_samples'],10+10+11)
        self.assertEqual(patterns['summary']['games'],3)
        self.assertEqual(patterns['summary']['wins'],2)
        self.assertEqual(patterns['summary']['losses'],1)
        self.assertEqual(payload['filters']['date_from'],'2026-01-01')
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_missing_engine_results_refresh_only_after_read_transaction_closes(self):
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        await self.db.execute('DELETE FROM game_player_engine_summary WHERE game_link=1')
        calls=[]
        async def refresh(links):
            self.assertEqual(self.engine.pool.checkedout(),0)
            calls.append(tuple(links))
            await refresh_game_player_engine_summaries(links,factory=self.factory)
        with patch.object(hero,'refresh_game_player_engine_summaries',side_effect=refresh):
            scores=await hero.get_player_range_game_scores('alice',game_ids=[1])
        self.assertEqual(calls,[(1,)])
        self.assertEqual(len(scores['games']),1)
        self.assertGreater(scores['games'][0]['analyzed_player_moves'],0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_unknown_player_empty_selection_and_database_errors_are_not_success(self):
        with self.assertRaises(LookupError):await hero.get_player_game_measures('absent',1)
        with self.assertRaises(LookupError):await hero.get_player_game_measures('alice',4)
        scores=await hero.get_player_range_game_scores('alice',game_ids=[999])
        self.assertEqual(scores['games'],[])
        await self.db.execute('ALTER TABLE game_analysis_coverage RENAME TO broken_test_coverage')
        with self.assertRaises(DBAPIError):await hero.get_player_quality_calendar('alice')
        self.assertEqual(self.engine.pool.checkedout(),0)
