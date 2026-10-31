"""Frozen threshold comparison, without a live database or cloud credentials."""
from copy import deepcopy
import math
import unittest

import numpy as np

from chessism_api.operations.research_algorithms import blunder_threshold_compare as audit
from chessism_api.operations.research_algorithms import blunder_shadow_compute as compute
from chessism_api.operations.research_algorithms import blunder_v2_model as model
from tests.test_blunder_shadow import FITS, PARENT_ID, numerical_fixture


class ThresholdTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.games = numerical_fixture()
        cls.previous = compute.calculate(np, cls.rows, cls.games, FITS)['records']
        cls.result = audit.calculate(np, cls.rows, cls.games, FITS, cls.previous)

    def test_exact_oracle_and_monotonicity(self):
        r = self.result
        self.assertEqual(r['status'], 'passed'); self.assertFalse(any(r['checks'].values()))
        self.assertTrue(all(x['candidate15'] >= x['candidate20'] for x in r['records']))
        self.assertEqual(model.THRESHOLD, .2)
        self.assertTrue(all('candidate15' not in row for row in self.previous))
        self.assertEqual(r, audit.calculate(np, self.rows, self.games, FITS, self.previous))

    def test_change_counts_reconcile_and_histograms(self):
        for r in self.result['records']:
            c = r['changes']
            self.assertEqual(r['candidate15']-r['candidate20'], c['restored_old']+c['new_from_lowering'])
            self.assertEqual(r['candidate15']-r['old15'], c['added_vs_old']-c['still_removed'])
        for group in self.result['groups']+self.result['rating_groups']:
            self.assertTrue(all(sum(h) == group['assessed_games'] for h in group['histograms'].values()))

    def test_boundaries_forced_mates_and_known_example(self):
        edge = -math.log(.65/.35)/model.K0
        self.assertEqual(audit.scalar15(0, edge-1e-7, 0, 0, 0, 0, 0, 0), 1)
        self.assertEqual(audit.scalar15(0, edge+1e-7, 0, 0, 0, 0, 0, 0), 0)
        for before, after, bk, ak, expected in ((105, -9998, 0, 1, 1), (-701, -9998, 0, 1, 0),
                (9998, 700, 1, 0, 1), (9998, 701, 1, 0, 0), (0, 0, 2, 0, -1), (float('nan'), 0, 0, 0, -1)):
            self.assertEqual(audit.scalar15(before, after, bk, ak, 0, 0, 0, 0), expected)
        args = (492, -11, 0, 0, ((720+663)/2-2300)/400, (720-663)/400, .2472667694091797, 2.3239002227783203)
        self.assertEqual(model.scalar(*args), 0); self.assertEqual(audit.scalar15(*args), 1)

    def test_rating_boundaries_no_move_histograms_and_example_bounds(self):
        self.assertEqual([audit.band(x) for x in (999, 1000, 1499, 1500, 1999, 2000, 2499, 2500, 2999, 3000)],
            ['<1000', '1000–1499', '1000–1499', '1500–1999', '1500–1999', '2000–2499', '2000–2499', '2500–2999', '2500–2999', '3000+'])
        empty = deepcopy(self.result['records'][0]); empty.update(moves=0, unassessed=0, **dict.fromkeys(audit.FIELDS, 0))
        self.assertEqual(audit.aggregate([empty])[0]['assessed_games'], 0)
        self.assertEqual(sum(audit.aggregate([empty])[0]['histograms']['candidate15']), 0)
        examples = self.result['examples']
        self.assertLessEqual(len(examples), 36); self.assertEqual(len(examples), len({x['game'] for x in examples}))

    def test_configuration_pins_complete_parent_and_rejects_tuning(self):
        row = {'status': 'complete', 'result': {'format': 'chessism_blunder_shadow_pilot_v1', 'summary_version': 2,
            'status': 'passed', 'canonical': False, 'production_changed': False, 'source': {'games': 6000},
            'checks': {'mismatches': 0}, 'fits': FITS, 'model_id': 'frozen'}}
        config = audit.configuration(PARENT_ID, row)
        self.assertEqual(config['thresholds'], [.15, .2]); self.assertFalse(config['refitted'])
        changed = deepcopy(row); changed['result']['checks']['mismatches'] = 1
        with self.assertRaises(ValueError): audit.configuration(PARENT_ID, changed)
        changed = deepcopy(row); changed['result']['summary_version'] = 1
        with self.assertRaises(ValueError): audit.configuration(PARENT_ID, changed)
        from chessism_api.routers.research_algorithms import ThresholdBlunderRequest
        from pydantic import ValidationError
        args = dict(parent_run_id=PARENT_ID, acknowledgement='compare_thresholds_only_no_live_changes')
        ThresholdBlunderRequest(**args)
        with self.assertRaises(ValidationError): ThresholdBlunderRequest(**args, threshold=.1)

    def test_cancellation_before_calculation(self):
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            audit.calculate(np, self.rows, self.games, FITS, self.previous,
                lambda: (_ for _ in ()).throw(RuntimeError('cancel')))
