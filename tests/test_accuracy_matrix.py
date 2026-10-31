"""Accuracy input contract; no live database or calculation jobs."""
from datetime import date
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.defaults import default_formula
from chessism_api.operations.matrix_constructor.definitions import resolve_definition
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from chessism_api.operations.matrix_constructor import live_preview


def recipe(**filters):
    return resolve_definition(default_formula('accuracy')['config'],
                              {'filters': {'max_rows': None, **filters}})


class AccuracyMatrixTests(unittest.TestCase):
    def test_default_is_a_versioned_four_column_template_not_a_dataset(self):
        formula = default_formula('accuracy')
        self.assertTrue(formula['read_only'])
        self.assertEqual(formula['builtin_version'], 1)
        self.assertEqual(formula['config']['definition_kind'], 'template')
        self.assertNotIn('filters', formula['config'])
        self.assertEqual(formula['config']['feature_columns'], ['game', 'ply', 'score', 'score_kind'])
        self.assertEqual(formula['config']['label_columns'], [])
        columns = ROW_TYPES['accuracy_positions'].columns_by_key
        self.assertEqual([columns[key].numpy_dtype for key in columns],
                         ['int32', 'int32', 'float64', 'int32'])
        self.assertFalse(columns['game'].label_allowed)
        self.assertFalse(columns['ply'].label_allowed)

    def test_full_scoring_required_and_version_is_not_silently_changed(self):
        self.assertTrue(recipe(analyzed_only=False)['filters']['analyzed_only'])
        for version in (0, 2, '1', True):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, 'accuracy input version'):
                resolve_definition({**default_formula('accuracy')['config'], 'summary_input_version': version},
                                   {'filters': {'max_rows': None}})

    def test_bound_filters_and_whole_game_budget(self):
        count, query, params = matrix_sql(recipe(players=["test'player"], modes=['blitz'],
                                               date_from='2025-01-01', max_rows=100))
        self.assertNotIn("test'player", query)
        self.assertEqual(params['players'], ["test'player"])
        self.assertEqual(params['date_from'], date(2025, 1, 1))
        self.assertEqual(params['max_rows'], 100)
        self.assertIsNone(params['preview_rows'])
        self.assertIn('end_row<=:max_rows', query)
        self.assertIn('EXISTS (SELECT 1 FROM game_player', query)
        self.assertIn('count(DISTINCT ply)=count(*)', query)
        self.assertIn('LIMIT :count_limit', count)

    def test_preview_limits_display_separately_from_whole_game_budget(self):
        _, _, params = matrix_sql(recipe(), preview_rows=3)
        self.assertIsNone(params['max_rows'])
        self.assertEqual(params['preview_rows'], 3)
        self.assertEqual(params['candidate_games'], 3)


class AccuracyPreviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_preview_is_read_only_bounded_and_keeps_source_keys(self):
        records = [{'row_key': f'1:{ply}', 'game': '1', 'ply': ply, 'score': score, 'score_kind': kind}
                   for ply, score, kind in [(1, 15, 'cp'), (2, -20, 'cp'), (3, 9998, 'mate')]]
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value = records
        session.execute.return_value = result
        factory = MagicMock()
        factory.return_value.__aenter__.return_value = session
        with patch.object(live_preview, 'AsyncDBSession', factory):
            preview = await live_preview.preview_definition(default_formula('accuracy')['config'], limit=2,
                                                           parameters={'filters': {'max_rows': None}})
        self.assertEqual(preview['rows'], [['1', 1, 15, 'cp'], ['1', 2, -20, 'cp']])
        self.assertTrue(preview['more_matching_rows'])
        self.assertEqual(preview['selection_unit'], 'whole_games')
        self.assertFalse(preview['materialized'])
        calls = session.execute.call_args_list
        self.assertEqual(str(calls[0].args[0]), 'SET TRANSACTION READ ONLY')
        self.assertEqual(len(calls), 5)  # Four safety settings, one bounded SELECT; no COUNT.
        self.assertIsNone(calls[-1].args[1]['max_rows'])
        self.assertEqual(calls[-1].args[1]['preview_rows'], 3)


if __name__ == '__main__':
    unittest.main()
