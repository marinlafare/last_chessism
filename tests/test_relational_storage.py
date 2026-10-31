import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from chessism_api.database.relational_storage import RelationalStorage,SESSION_KEY,storage_for,identifier
from chessism_api.operations.ingestion_pipeline.relational_repository import standard_game,short_eco,START
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql


class RelationalStorageTests(unittest.TestCase):
    def test_explicit_opt_in_and_quoted_identifiers(self):
        layout=RelationalStorage('core','archive','reads')
        self.assertIsNone(storage_for(SimpleNamespace(info={})))
        self.assertIsNone(storage_for(AsyncMock()))
        self.assertIs(storage_for(SimpleNamespace(info={SESSION_KEY:layout})),layout)
        self.assertEqual(layout.table('game'),'"core"."game"')
        self.assertEqual(layout.table('game',archived=True),'"archive"."game"')
        with self.assertRaises(TypeError):
            storage_for(SimpleNamespace(info={SESSION_KEY:'automatic'}))
        for name in ('public; DROP TABLE game','a.b','',None,'pg_temp,public'):
            with self.assertRaises(ValueError): identifier(name)
        with self.assertRaises(ValueError): RelationalStorage('public','public','reads')

    def test_no_lossy_archive_classification(self):
        self.assertTrue(standard_game({'rules':'chess','initial_setup':START+' 0 1'}))
        self.assertFalse(standard_game({'rules':'chess960','initial_setup':None}))
        self.assertFalse(standard_game({'rules':'chess','initial_setup':'custom'}))
        self.assertEqual(short_eco('https://www.chess.com/openings/A-B'),'A-B')
        with self.assertRaises(ValueError):short_eco('unknown')

    def test_saved_score_matrix_metric_is_computed_only_when_selected(self):
        config=normalize_matrix_config({'row_type':'game','feature_columns':['average_abs_score'],
            'filters':{'max_rows':50}})
        count,select,_=matrix_sql(config)
        self.assertNotIn('score_stats',count)
        self.assertIn('WITH matrix_scope AS MATERIALIZED',select)
        self.assertIn('AVG(ABS(position.score))',select)
        self.assertNotIn('gas.avg_abs_score',select)
        config=normalize_matrix_config({'row_type':'game','feature_columns':['white_rating']})
        self.assertNotIn('score_stats',matrix_sql(config)[1])

    def test_empty_nonstandard_game_keeps_archival_metadata(self):
        from scripts.db_refactor_write_checks import fixture_bundle
        from chessism_api.operations.ingestion_pipeline.game_importer import _prepare_game_bundle
        game=dict(fixture_bundle()['games'][-1],n_moves=0)
        archived=_prepare_game_bundle([game],archive_nonstandard=True)
        self.assertEqual(archived['games'],[game])
        self.assertEqual(archived['no_move_games'],[])
        self.assertEqual(archived['games'][0]['initial_setup'],'preserve-the-original-starting-position')
        legacy=_prepare_game_bundle([game])
        self.assertEqual(legacy['games'],[])
        self.assertEqual(legacy['no_move_games'][0]['game_id'],game['link'])
