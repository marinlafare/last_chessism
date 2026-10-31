import unittest
from types import SimpleNamespace
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations.player_game_explorer_query import normalized_candidates, engine_lookup


class ExplorerQueryTests(unittest.TestCase):
    def setUp(self):
        self.session = SimpleNamespace(info={SESSION_KEY: RelationalStorage('core', 'archive', 'reads')})

    def test_date_and_owner_are_filtered_before_summary_lookups(self):
        query = normalized_candidates(self.session,
            date_window='AND gp.played_at>=:day_start AND gp.played_at<:day_end',
            selection_sql='gp.played_at::date=:target_date', minimum_moves_filter='AND gp.n_moves>10')
        self.assertIn('white_player_id=(SELECT id FROM owner)', query)
        self.assertIn('black_player_id=(SELECT id FROM owner)', query)
        self.assertEqual(query.count('g.played_at>=:day_start'), 2)
        self.assertIn('UNION ALL', query)  # preserve both perspectives of self-play
        self.assertIn('member.player_id=owner.id', query)
        self.assertIn('selected_games AS MATERIALIZED', query)

    def test_saved_gpu_blunders_are_keyed_by_game_not_full_canonical_engine_scan(self):
        query = engine_lookup(self.session)
        self.assertIn('saved.game_player_id=gp.game_player_id', query)
        self.assertIn('assessment.game_link=gp.link', query)
        self.assertIn('assessment.model_id=(', query)
        self.assertIn('white_complete', query)
        self.assertNotIn('saved.blunder_count', query)
        self.assertNotIn('canonical_blunder_engine', query)
        self.assertIn('OFFSET 0', query)

    def test_legacy_fallback_remains_parameterized(self):
        session = SimpleNamespace(info={})
        self.assertIsNone(normalized_candidates(session, date_window='',selection_sql='true',minimum_moves_filter=''))
        self.assertIn('saved.game_link=gp.link',engine_lookup(session))
