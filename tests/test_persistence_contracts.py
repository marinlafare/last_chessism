"""Current persistence contracts, independent of historical profile adapters."""
import unittest
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql


class PersistenceContractTests(unittest.TestCase):
    def test_saved_score_matrix_metric_is_computed_only_when_selected(self):
        config=normalize_matrix_config({'row_type':'game','feature_columns':['average_abs_score'],
            'filters':{'max_rows':50}})
        count,select,_=matrix_sql(config)
        self.assertNotIn('score_stats',count)
        self.assertIn('WITH matrix_scope AS MATERIALIZED',select)
        self.assertIn('AVG(ABS(position.score::bigint))',select)
        self.assertNotIn('gas.avg_abs_score',select)
        config=normalize_matrix_config({'row_type':'game','feature_columns':['white_rating']})
        self.assertNotIn('score_stats',matrix_sql(config)[1])

    def test_empty_games_keep_metadata_without_an_archive_profile(self):
        from tests.check_canonical_imports_postgres import bundle
        from chessism_api.operations.ingestion_pipeline.game_importer import _prepare_game_bundle
        from chessism_api.operations.models import GameCreateData
        game=GameCreateData(**dict(bundle()[0][1],n_moves=0,year=2026,month=1,day=31,hour=23,minute=59,second=0,
            initial_setup='preserve-the-original-starting-position')).model_dump()
        archived=_prepare_game_bundle([game])
        self.assertEqual(archived['games'],[game])
        self.assertEqual(archived['games'][0]['initial_setup'],'preserve-the-original-starting-position')
        standard=dict(game,rules='chess',initial_setup=None)
        self.assertEqual(_prepare_game_bundle([standard])['games'],[standard])

    def test_lossless_saved_integers_and_nulls(self):
        from chessism_api.evaluations import integer
        from chessism_api.operations.fen_backup.database import restore_values
        for value in (0, 1, 1.0, -19997, 20000, -2147483648, 2147483647):
            integer(value, 'score', -2147483648, 2147483647)
        for value in (None, True, False, '10', 1.5, -0.01, float('nan'), float('inf'), float('-inf'), 2147483648):
            with self.subTest(value=value), self.assertRaises(ValueError):
                integer(value, 'score', -2147483648, 2147483647)
        row = {'type':'fen_analysis','fen':'8/8/8/8/8/8/4k3/6K1 w - -',
               'score':-10000.0,'wdl_win':0.0,'wdl_draw':1000.0,'wdl_loss':0.0,
               'analysis_source':None,'analyzed_at':None,'next_moves':None}
        converted, _ = restore_values(row)
        self.assertIs(type(converted['score']), int)
        self.assertIs(type(converted['wdl_draw']), int)
        self.assertIs(type(row['score']), float)
        self.assertIsNone(converted['analyzed_at'])
        self.assertIsNone(converted['analysis_source'])
        without_wdl = {key:value for key,value in row.items() if not key.startswith('wdl_')}
        self.assertIsNone(restore_values(without_wdl)[0]['wdl_loss'])
        for value in (-1, 1001, 0.5, float('nan')):
            with self.assertRaises(ValueError): restore_values({**row, 'wdl_win':value})
