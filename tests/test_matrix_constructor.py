import tempfile
import unittest
from pathlib import Path

import numpy as np

from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES, matrix_catalog
from chessism_api.operations.matrix_constructor.arrays import TypedArrayWriter, array_layout, validate_typed_arrays
from chessism_api.operations.matrix_constructor.queries import (
    estimated_artifact_bytes,
    matrix_sql,
    normalize_matrix_config,
)


class MatrixConstructorTests(unittest.TestCase):
    def test_catalog_has_distinct_human_row_units(self):
        catalog = matrix_catalog()
        self.assertEqual(catalog["version"], 2)
        keys = {item["key"] for item in catalog["row_types"]}
        self.assertEqual(
            keys,
            {"game", "game_player", "move", "position", "game_position", "player_period"},
        )
        self.assertTrue(all(item["columns"] for item in catalog["row_types"]))

    def test_defaults_are_normalized_and_bounded(self):
        config = normalize_matrix_config({
            "name": "  Test matrix  ",
            "row_type": "game_player",
            "feature_columns": ["rating", "accuracy", "rating"],
            "label_columns": ["result"],
            "filters": {
                "players": [" Hikaru ", "HIKARU"],
                "modes": ["rapid", "classical"],
                "max_rows": 99_000_000,
            },
        })
        self.assertEqual(config["name"], "Test matrix")
        self.assertEqual(config["feature_columns"], ["rating", "accuracy"])
        self.assertEqual(config["filters"]["players"], ["hikaru"])
        self.assertEqual(config["filters"]["modes"], ["rapid"])
        self.assertEqual(config["filters"]["max_rows"], 5_000_000)

    def test_unknown_columns_never_reach_sql(self):
        with self.assertRaisesRegex(ValueError, "Unsupported columns"):
            normalize_matrix_config({
                "row_type": "game",
                "feature_columns": ["rating); DROP TABLE game; --"],
            })

    def test_feature_and_label_cannot_overlap(self):
        with self.assertRaisesRegex(ValueError, "both features and labels"):
            normalize_matrix_config({
                "row_type": "game",
                "feature_columns": ["white_rating"],
                "label_columns": ["white_rating"],
            })

    def test_game_player_sql_uses_bound_filter_parameters(self):
        config = normalize_matrix_config({
            "row_type": "game_player",
            "feature_columns": ["rating", "accuracy"],
            "label_columns": ["result"],
            "filters": {
                "players": ["hikaru"],
                "modes": ["blitz"],
                "date_from": "2025-01-01",
                "analyzed_only": True,
                "max_rows": 1000,
            },
        })
        count_sql, select_sql, params = matrix_sql(config)
        self.assertIn("gp.player_name = ANY", select_sql)
        self.assertIn("gps.analyzed_player_moves > 0", select_sql)
        self.assertIn('AS "rating"', select_sql)
        self.assertIn("SELECT COUNT(*)", count_sql)
        self.assertEqual(params["players"], ["hikaru"])
        self.assertEqual(params["max_rows"], 1000)
        self.assertNotIn("hikaru", select_sql)

    def test_game_player_matrix_exposes_persistent_salience(self):
        config = normalize_matrix_config({
            "row_type": "game_player",
            "feature_columns": ["accuracy", "game_salience", "unique_positions"],
            "filters": {"players": ["hikaru"], "max_rows": 100},
        })
        _, select_sql, _ = matrix_sql(config)
        self.assertIn("LEFT JOIN game_player_salience gsl", select_sql)
        self.assertIn('gsl.salience AS "game_salience"', select_sql)
        self.assertIn('gsl.unique_position_count AS "unique_positions"', select_sql)

    def test_move_matrix_can_derive_occurrence_salience(self):
        config = normalize_matrix_config({
            "row_type": "move",
            "feature_columns": ["position_salience", "depth_weight", "weighted_move_salience"],
            "filters": {"players": ["lafareto"], "max_rows": 100},
        })
        _, select_sql, _ = matrix_sql(config)
        self.assertIn("LEFT JOIN player_position_frequency ppf", select_sql)
        self.assertIn("LEFT JOIN LATERAL", select_sql)
        self.assertIn("salience_occurrence.occurrence_index", select_sql)
        self.assertIn('AS "weighted_move_salience"', select_sql)

    def test_position_filters_use_an_exists_scope(self):
        config = normalize_matrix_config({
            "row_type": "position",
            "feature_columns": ["score", "piece_count"],
            "filters": {"players": ["lafareto"], "analyzed_only": True},
        })
        _, select_sql, params = matrix_sql(config)
        self.assertIn("EXISTS", select_sql)
        self.assertIn("scope_gfa.fen_fen = f.fen", select_sql)
        self.assertIn("f.score IS NOT NULL", select_sql)
        self.assertEqual(params["players"], ["lafareto"])

    def test_every_default_column_is_allowlisted(self):
        for row_type in ROW_TYPES.values():
            defaults = [column for column in row_type.columns if column.default]
            self.assertTrue(defaults, row_type.key)
            self.assertEqual(len(defaults), len({column.key for column in defaults}))

    def test_catalog_publishes_typed_storage(self):
        game_player = ROW_TYPES["game_player"].columns_by_key
        self.assertEqual(game_player["rating"].numpy_dtype, "int32")
        self.assertEqual(game_player["started_at"].numpy_dtype, "int64")
        self.assertEqual(game_player["accuracy"].numpy_dtype, "float32")
        self.assertEqual(game_player["player_color"].numpy_dtype, "int32")

    def test_typed_layout_and_profiles_are_reversible(self):
        row_type = ROW_TYPES["game_player"]
        keys = ["rating", "accuracy", "player_color"]
        groups, layout = array_layout(keys, role="features", row_type=row_type)
        self.assertEqual(groups, {
            "int32": ["rating", "player_color"],
            "float32": ["accuracy"],
        })
        self.assertEqual(layout[2]["encoding"], "dictionary")
        self.assertEqual(layout[2]["array_column"], 1)


        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            writer = TypedArrayWriter(root, row_type, {
                "feature_columns": keys + ["average_rating", "started_at"],
                "label_columns": ["result"],
            }, 3)
            try:
                writer.write([dict(rating=1700, accuracy=80.1, player_color="white",
                    average_rating=1700.5, started_at=1780000000, result=1)])
                writer.write([
                    dict(rating=None, accuracy=float("inf"), player_color="black",
                        average_rating=1800, started_at=1780000001, result=0),
                    dict(rating=1800, accuracy=1e50, player_color="white",
                        average_rating=None, started_at=1780000002, result=None),
                ])
                profiles = writer.finish()
                self.assertEqual(writer.dictionaries["player_color"], {"white": 0, "black": 1})
                validation = validate_typed_arrays(root, rows=3, layouts=writer.layouts)
                self.assertEqual(validation["status"], "passed")
                indexed = {item["key"]: item for item in profiles["features"]}
                self.assertEqual(indexed["rating"]["mean"], 1750)
                self.assertEqual(indexed["average_rating"]["minimum"], 1700.5)
                self.assertEqual(indexed["accuracy"]["missing_count"], 2)
                self.assertAlmostEqual(indexed["started_at"]["standard_deviation"], (2/3)**0.5)
                data = np.load(root / "features_float32.npy", allow_pickle=False)
                self.assertEqual(float(data[0, 1]), 1700.5)
                masks = np.load(root / "features_missing.npy", allow_pickle=False)
                np.testing.assert_array_equal(masks[:, 1], [0, 1, 1])
            finally:
                writer.close()

    def test_integer_truncation_and_short_stream_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            writer = TypedArrayWriter(Path(folder), ROW_TYPES["game_player"], {
                "feature_columns": ["rating"], "label_columns": [],
            }, 1)
            try:
                with self.assertRaisesRegex(ValueError, "exactly"):
                    writer.write([{"rating": 1700.5}])
                with self.assertRaisesRegex(ValueError, "exactly"):
                    writer.write([{"rating": 2**32}])
                with self.assertRaisesRegex(ValueError, "expected 1"):
                    writer.finish()
            finally:
                writer.close()

    def test_minimal_queries_omit_unused_projections(self):
        for key, fields in {"game": ["white_rating"], "game_player": ["rating"],
                            "move": ["fullmove"], "game_position": ["fullmove"],
                            "player_period": ["games"]}.items():
            config = normalize_matrix_config({"row_type": key, "feature_columns": fields})
            count, select, _ = matrix_sql(config)
            for sql in (count, select):
                self.assertNotIn("game_player_salience", sql)
                self.assertNotIn("player_position_frequency", sql)
                self.assertNotIn("game_player_engine_summary", sql)

    def test_move_limit_precedes_salience_enrichment_and_excludes_empty_moves(self):
        config = normalize_matrix_config({"row_type": "move",
            "feature_columns": ["position_salience", "game_salience"]})
        count, select, _ = matrix_sql(config)
        self.assertLess(select.index("LIMIT :max_rows"), select.index("LEFT JOIN player_position_frequency"))
        self.assertIn("NULLIF(BTRIM(side.san), '') IS NOT NULL", select)
        self.assertNotIn("player_position_frequency", count)
        self.assertIn("pss.status = 'ready'", select)


    def test_size_estimate_grows_with_rows_and_columns(self):
        small = estimated_artifact_bytes(100, 2)
        many_rows = estimated_artifact_bytes(200, 2)
        many_columns = estimated_artifact_bytes(100, 4)
        self.assertGreater(many_rows, small)
        self.assertGreater(many_columns, small)


if __name__ == "__main__":
    unittest.main()
