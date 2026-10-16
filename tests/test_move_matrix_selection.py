"""Current matrix contract plus immutable v1-v3 jobs; isolated DB when enabled."""
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from pydantic import ValidationError
from sqlalchemy import text

from chessism_api.operations.exports.analytical_preview import preview
from chessism_api.operations.exports.analytical_spec import (
    AnalyticalPreviewSpec, MATRIX_CONTRACT_KEY, MoveMatrixSpec, parse_request,
)
from chessism_api.operations.exports.move_matrix import plan
from chessism_api.operations.exports.service import request_document
from tests.test_analytical_exports_database import AnalyticalDatabaseFixture, SCHEMA


class MatrixContractTests(unittest.TestCase):
    def test_versions_keep_their_column_bounds_order_and_score_scope(self):
        public = MoveMatrixSpec(player='hikaru', last_move=124)
        self.assertIn('p.played_at AS played_at', plan(public)[0])
        with self.assertRaises(ValidationError):
            MoveMatrixSpec(player='hikaru', last_move=125)
        for version in (1, 2, 3, 4):
            document = request_document(public)
            document[MATRIX_CONTRACT_KEY] = version
            if version < 4:
                document['last_move'] = 125
            saved = parse_request(document)
            sql, _, _ = plan(saved, start_year=2022)
            self.assertEqual('p.played_at AS played_at' in sql, version == 4)
            self.assertEqual('CASE WHEN played_at IS NULL' in sql, version >= 3)
            self.assertEqual(saved.complete_score_scope, 'requested_moves' if version == 1 else 'whole_game')
            self.assertEqual(request_document(saved).get(MATRIX_CONTRACT_KEY, 1), version)

    def test_result_values_and_year_are_bound_not_interpolated(self):
        for outcome, points in (('win', 1.0), ('loss', 0.0), ('draw', 0.5)):
            spec = MoveMatrixSpec(player="name'with'quotes", player_win=outcome)
            sql, values, _ = plan(spec, start_year=2022)
            self.assertEqual(values, [spec.player, 'bullet', 1000, points, 2022])
            self.assertNotIn(spec.player, sql)
            self.assertIn('$4::double precision', sql)
            self.assertIn('$5::integer', sql)
            self.assertLess(sql.index('g.white_result + g.black_result = 1.0'), sql.index('LIMIT $3'))
        with self.assertRaises(ValidationError):
            MoveMatrixSpec(player='hikaru', player_win='winning-score')


class MatrixSelectionDatabaseTests(AnalyticalDatabaseFixture):
    async def test_already_submitted_versions_keep_their_original_schema(self):
        for version in (1, 2, 3):
            document = request_document(self.design(max_games=2))
            document[MATRIX_CONTRACT_KEY] = version
            saved = parse_request(document)
            status, directory = await self.run_spec(saved)
            self.assertEqual(status.status, 'complete', status.error)
            table = pq.read_table(directory/'data.parquet')
            self.assertEqual(table.column_names, ['game_id','player_name','color','move_1','move_2'])
            self.assertEqual(table.num_rows, 2)

    async def years_fixture(self):
        async with self.engine.begin() as conn:
            await conn.execute(text(f'DELETE FROM {SCHEMA}.game'))
            await conn.execute(text(f'''INSERT INTO {SCHEMA}.game
                (link,played_at,white_player_id,black_player_id,mode_id,white_result,black_result)
                VALUES (101,'2020-01-01Z',2,1,1,1,0), (105,'2021-01-01Z',1,2,1,0,1),
                    (102,'2022-01-01Z',2,1,1,0,1), (103,'2022-02-01Z',1,2,1,0.5,0.5),
                    (104,'2023-01-01Z',2,1,1,1,0), (100,'2024-01-01Z',2,1,1,NULL,NULL),
                    (106,NULL,2,1,1,1,0)'''))

    def design(self, **changes):
        return MoveMatrixSpec(**{'player':'hikaru', 'game_mode':'blitz', 'max_games':5,
                                'first_move':1, 'last_move':2, **changes})

    async def test_forward_then_backward_selection_is_finally_date_sorted(self):
        await self.years_fixture()
        with patch('chessism_api.operations.exports.analytical_query.random.Random.choice', return_value=2022):
            result = await preview(AnalyticalPreviewSpec(design=self.design()))
            status, directory = await self.run_spec(self.design())
        self.assertEqual(status.status, 'complete', status.error)
        self.assertEqual([row[0] for row in result['rows']], [105,102,103,104,100])
        self.assertEqual(result['selection']['year_search_order'], [2022,2023,2024,2021,2020])
        self.assertFalse(result['selection']['insufficient_games'])
        self.assertIsNone(result['selection']['available_games'])
        table = pq.read_table(directory/'data.parquet')
        self.assertEqual(table['game_id'].to_pylist(), [105,102,103,104,100])
        self.assertEqual(str(table.schema.field('played_at').type), 'timestamp[us, tz=UTC]')
        manifest = json.loads((directory/'manifest.json').read_text())
        self.assertEqual(manifest['selection'], result['selection'])
        self.assertEqual(manifest['columns'], result['columns'])

    async def test_history_exhaustion_reports_shortfall_and_missing_dates_last(self):
        await self.years_fixture()
        design = self.design().model_copy(update={'max_games': 10})
        result = await preview(AnalyticalPreviewSpec(design=design))
        self.assertEqual([row[0] for row in result['rows']], [101,105,102,103,104,100,106])
        self.assertIsNone(result['rows'][-1][3])
        self.assertTrue(result['selection']['insufficient_games'])
        self.assertEqual(result['selection']['missing_games'], 3)
        self.assertEqual(result['selection']['available_games'], 7)
        self.assertEqual(result['selection']['undated_games'], 1)
        sample = await preview(AnalyticalPreviewSpec(design=design, preview_mode='sample', preview_rows=1))
        self.assertEqual(sample['row_count'], 1)
        self.assertEqual(sample['selection']['sample_game_limit'], 1)
        for key in ('insufficient_games', 'missing_games', 'available_games'):
            self.assertIsNone(sample['selection'][key])

    async def test_recorded_outcome_before_limit_for_either_color_and_both_rows(self):
        await self.years_fixture()
        for outcome, expected in (('win',[105,104]), ('loss',[102]), ('draw',[103])):
            design = self.design().model_copy(update={'max_games': 2, 'player_win': outcome, 'players': 'both'})
            with patch('chessism_api.operations.exports.analytical_query.random.Random.choice', return_value=2022):
                result = await preview(AnalyticalPreviewSpec(design=design))
            self.assertEqual([row[0] for row in result['rows']], [game for game in expected for _ in range(2)])
            self.assertEqual([row[2] for row in result['rows']], ['white','black']*len(expected))
            self.assertEqual(result['selection']['player_win'], outcome)
            status, directory = await self.run_spec(design)
            self.assertEqual(status.status, 'complete', status.error)
            manifest = json.loads((directory/'manifest.json').read_text())
            self.assertEqual(manifest['source_definition']['player_win'], outcome)

    async def test_empty_history_keeps_typed_columns_and_zero_available(self):
        result = await preview(AnalyticalPreviewSpec(design=self.design().model_copy(update={'player': 'unknown'})))
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['selection']['available_games'], 0)
        self.assertIsNone(result['selection']['start_year'])
        self.assertEqual([col['name'] for col in result['columns']],
                         ['game_id','player_name','color','played_at','move_1','move_2'])
