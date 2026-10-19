"""Regression checks for bounded collection move-score previews."""

import unittest

from chessism_api.operations.matrix_constructor.defaults import default_formula
from chessism_api.operations.matrix_constructor.definitions import resolve_definition
from chessism_api.operations.matrix_constructor.move_scores import (
    expand_sample_moves, score_matrix_sql,
)


def recipe(*, analyzed=True, all_moves=True):
    return resolve_definition(default_formula('game-move-scores')['config'], {
        'filters': {'players': ['hikaru'], 'modes': ['bullet'],
                    'max_rows': None, 'analyzed_only': analyzed},
        'move_scores': {'all_moves': all_moves, 'first_move': 1, 'last_move': 40},
    })


class MoveScorePreviewTests(unittest.TestCase):
    def test_ordered_candidates_are_not_limited_before_score_eligibility(self):
        _, query, params = score_matrix_sql(recipe(), sample_all_moves=True)
        before_check, after_check = query.split(') gp WHERE (SELECT BOOL_AND', 1)
        self.assertIn('gp.player_name = ANY(CAST(:players AS text[]))', before_check)
        self.assertIn('coverage.is_fully_analyzed', before_check)
        self.assertIn('ORDER BY gp.played_at NULLS LAST, gp.link, gp.color OFFSET 0', before_check)
        self.assertNotIn('LIMIT', before_check)
        self.assertIn('LIMIT :max_rows', after_check)
        self.assertEqual(query.count('LIMIT :max_rows'), 1)
        self.assertEqual(query.count('MATERIALIZED'), 1)
        self.assertNotIn('hikaru', query)
        self.assertEqual(params['players'], ['hikaru'])
        # All moves: completeness covers both players, not just the selected side.
        self.assertNotIn('side.color=gp.color', after_check.split('LIMIT :max_rows')[0])
        self.assertNotIn('expected_moves', params)

    def test_fixed_range_still_requires_all_requested_moves_before_limit(self):
        _, query, params = score_matrix_sql(recipe(all_moves=False))
        before_limit = query.split('LIMIT :max_rows')[0]
        self.assertIn('BOOL_AND(f.score IS NOT NULL)', before_limit)
        self.assertIn('side.color=gp.color', before_limit)
        self.assertIn('m.n_move BETWEEN :first_move AND :last_move', before_limit)
        self.assertEqual(params['expected_moves'], 40)
        self.assertIn('AS "move_score_40"', query)

    def test_missing_scores_allowed_and_count_path_unchanged(self):
        _, query, _ = score_matrix_sql(recipe(analyzed=False), sample_all_moves=True)
        self.assertNotIn('BOOL_AND', query)
        self.assertNotIn('OFFSET 0', query)
        self.assertIn('LEFT JOIN fen', query)
        count, query, params = score_matrix_sql(recipe(), count_only=True)
        self.assertEqual(query, '')
        self.assertIn('BOOL_AND(f.score IS NOT NULL)', count)
        self.assertIn('LIMIT :count_limit', count)
        self.assertEqual(params['count_limit'], 10001)

    def test_all_moves_preserves_zeros_nulls_and_rejects_duplicates(self):
        rows, keys = expand_sample_moves([
            {'row_key': '1:white', '_move_numbers': [1, 2], '_move_values': [0, -30]},
            {'row_key': '2:black', '_move_numbers': [1], '_move_values': [None]},
        ])
        self.assertEqual(keys, ['move_score_1', 'move_score_2'])
        self.assertEqual(rows[0]['move_score_1'], 0)
        self.assertIsNone(rows[1]['move_score_1'])
        self.assertIsNone(rows[1]['move_score_2'])
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            expand_sample_moves([{'_move_numbers': [1, 1], '_move_values': [0, 2]}])


if __name__ == '__main__':
    unittest.main()
