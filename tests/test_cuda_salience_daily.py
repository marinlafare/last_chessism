import unittest
import numpy as np
from chessism_api.operations.cuda_summaries import salience_daily as daily


class DailySalience(unittest.TestCase):
    def test_exact_boundaries_missing_values_modes_and_ties(self):
        values=[0,.5,.50001,.6,.60001,.7,.70001,.8,.80001,.9,.90001,1,None,float('nan'),-.1,1.1]
        rows=[{'day':'2026-01-01','mode_bit':2,'salience':value} for value in values]
        rows.append({'day':'2026-01-01','mode_bit':1,'salience':.95})
        saved=daily.calculate(np,rows,require_cuda=False)
        blitz=next(r for r in saved if r['mode_mask']==2)
        self.assertEqual(blitz['counts'],[2,2,2,2,2,2])
        self.assertEqual(blitz['modal_bands'],list(range(6)))
        all_modes=next(r for r in saved if r['mode_mask']==7)
        self.assertEqual(all_modes['counts'],[2,2,2,2,2,3])
        self.assertEqual(all_modes['modal_bands'],[5])
        self.assertEqual(all_modes['games'],13)
        self.assertFalse(any(r['mode_mask']==4 for r in saved))
        self.assertEqual(daily.calculate(np,[],require_cuda=False),[])

    def test_cpu_fallback_is_not_allowed(self):
        with self.assertRaises(RuntimeError):daily.calculate(np,[])
