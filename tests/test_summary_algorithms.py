import unittest
from unittest.mock import patch

import numpy as np

from chessism_api.operations.matrix_constructor.defaults import default_formula, default_formulas
from chessism_api.operations.matrix_constructor.definitions import definition_config, resolve_definition
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from chessism_api.operations.research_algorithms import summary_validation as validation
from scripts.validate_summary_cuda import synthetic_rows


class SummaryMatrixTests(unittest.TestCase):
    def test_defaults_are_reusable_not_hidden_data_selections(self):
        defaults = {row['builtin_key']: row for row in default_formulas()}
        self.assertEqual(set(defaults), {'game-move-scores', 'blunder-moves', 'blunder-fixed', 'score-distribution'})
        for row in defaults.values():
            self.assertTrue(row['read_only'])
            self.assertEqual(row['config']['definition_kind'], 'template')
            self.assertNotIn('filters', row['config'])

    def test_blunder_default_omits_metadata_and_outcomes_but_keeps_ratings(self):
        config = default_formula('blunder-moves')['config']
        self.assertEqual(config['label_columns'], [])
        self.assertIn('player_rating', config['feature_columns'])
        self.assertIn('opponent_rating', config['feature_columns'])
        for column in ('result', 'player_color', 'started_at', 'ply', 'blunders'):
            self.assertNotIn(column, config['feature_columns'])

    def test_score_default_has_five_columns_and_a_versioned_population(self):
        default = default_formula('score-distribution')
        self.assertEqual(default['builtin_version'], 2)
        self.assertEqual(default['config']['feature_columns'], ['player', 'mode', 'player_score', 'score_kind', 'mate_moves'])
        self.assertEqual(definition_config(default['config'])['summary_input_version'], 2)
        # A saved v1 recipe must not silently acquire the new population rule.
        old = {**default['config'], 'feature_columns': ['player', 'player_score']}
        old.pop('summary_input_version')
        restored = resolve_definition(old, {'filters': {'max_rows': 10}})
        self.assertEqual(restored['summary_input_version'], 1)
        self.assertNotIn('scored.score IS NOT NULL', matrix_sql(restored)[1])

    def recipe(self, key):
        return resolve_definition(default_formula(key)['config'], {'filters': {
            'players': ["test'player"], 'modes': ['blitz'], 'date_from': '2025-01-01', 'max_rows': 10}})

    def test_score_rows_do_not_filter_to_the_players_own_moves(self):
        count, query, params = matrix_sql(self.recipe('score-distribution'))
        self.assertNotIn('gfa.move_color=gp.color', query)
        self.assertNotIn('DISTINCT', query)
        self.assertNotIn('is_fully_analyzed', query)
        self.assertLess(query.index('LIMIT :max_rows'), query.index('LEFT JOIN fen'))
        self.assertNotIn("test'player", query)
        self.assertEqual(params['players'], ["test'player"])
        self.assertIn('LIMIT :count_limit', count)
        self.assertIn('scored.score IS NOT NULL', count)
        self.assertLess(query.index('scored.score IS NOT NULL'), query.index('LIMIT :max_rows'))

    def test_blunders_use_prior_ply_not_lag_over_a_sample(self):
        _, query, _ = matrix_sql(self.recipe('blunder-moves'))
        self.assertIn('gfa.move_color=gp.color', query)
        self.assertIn('coverage.is_fully_analyzed', query)
        self.assertIn("THEN s.n_move-1 ELSE s.n_move", query)
        self.assertIn('15.0', query)
        self.assertNotIn('LAG(', query)
        self.assertNotIn('game_player_engine_summary', query)
        self.assertNotIn('gp.result', query)

    def test_empty_label_preview_is_valid_sql(self):
        config = self.recipe('score-distribution')
        config['feature_columns'] = []
        _, query, _ = matrix_sql(config)
        self.assertIn('SELECT input.row_key FROM input', query)


class ScoreValidationTests(unittest.TestCase):
    def test_numpy_vector_contract_matches_independent_legacy_oracle(self):
        rows = validation.validate_rows(synthetic_rows())
        scores = np.array([row['player_score'] for row in rows])
        kinds = np.array([validation.KINDS[row['score_kind']] for row in rows])
        moves = np.array([-1 if row['mate_moves'] is None else row['mate_moves'] for row in rows])
        np.testing.assert_array_equal(validation.reference(rows), validation.score_kernel(np, scores, kinds, moves))

    def test_all_bin_boundaries_and_special_kinds(self):
        score = np.array([-551, -550, -450, -50, 0, 50, 150, 550, 551, 9998, -9996, 10000, 9998, 9998.5, 9001])
        kinds = np.array([0] * 9 + [1, 1, 1, 2, 1, 1])
        moves = np.array([-1] * 9 + [2, 4, 0, -1, -1, -1])
        result = validation.score_kernel(np, score, kinds, moves)
        np.testing.assert_array_equal(result[:, 0], [0, 1, 2, 6, 6, 6, 7, 11, 12, 13, 13, 13, 14, 13, 13])
        np.testing.assert_array_equal(result[9:, 1], [2, 4, 0, -1, -1, -1])
        np.testing.assert_array_equal(result[9:, 2], [1, -1, 1, 0, 1, 1])

    def test_equal_values_remain_separate_occurrences(self):
        row = synthetic_rows()[1]
        rows = validation.validate_rows([row, row])
        decisions = validation.reference(rows)
        counts = validation.group_counts(np, decisions, np.zeros(2, dtype=np.int64), 1)
        self.assertEqual(int(counts.sum()), 2)
        self.assertEqual(counts.shape, (1, 15))
        validation.validate_rows([{**row, 'row_key': 'first'}, {**row, 'row_key': 'second'}])
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            validation.validate_rows([{**row, 'row_key': 'same'}] * 2)

    def test_mate_group_counters_keep_unknown_zero_and_direction(self):
        decisions = np.array([[13,0,-1],[13,2,1],[13,2,1],[13,-1,1],[6,-1,0]])
        groups = np.array([0,0,0,1,0])
        counts = dict(zip(*validation.group_mates(np, decisions, groups)))
        self.assertEqual(validation.mate_records(counts,0), [
            {'moves':0,'side':'against','positions':1}, {'moves':2,'side':'for','positions':2}])
        self.assertEqual(validation.mate_records(counts,1), [{'moves':None,'side':'for','positions':1}])

    def test_labels_do_not_enter_inference_input(self):
        rows = validation.validate_rows([{**synthetic_rows()[0], 'result': 1, 'player_rating': 1000}])
        self.assertNotIn('result', rows[0])

    def test_bad_inputs_fail_closed(self):
        row = synthetic_rows()[0]
        for invalid in ({**row, 'player_score': float('inf')}, {**row, 'player_score': True},
                        {**row, 'mode': 'classical'}, {**row, 'player_score': None},
                        {**row, 'score_kind': 'unscored'}, {**row, 'score_kind': 'mate'},
                        {**row, 'mate_moves': 3}):
            with self.assertRaises(ValueError):
                validation.validate_rows([invalid])

    def test_optional_row_keys_must_be_complete(self):
        row = synthetic_rows()[0]
        with self.assertRaisesRegex(ValueError, 'every row'):
            validation.validate_rows([{**row, 'row_key': 'one'}, row])

    def test_no_cpu_fallback_when_cuda_is_unavailable(self):
        with patch.object(validation, 'cuda_backend', side_effect=RuntimeError('CUDA unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'CUDA unavailable'):
                validation.compare_score_distribution(synthetic_rows())

    def test_batch_and_source_limits_are_enforced(self):
        with self.assertRaises(ValueError):
            validation.compare_score_distribution(synthetic_rows(), batch_rows=8193)
        with self.assertRaises(ValueError):
            validation.validate_rows([])


if __name__ == '__main__':
    unittest.main()
