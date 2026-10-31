"""Exact final histogram counters; --cuda verifies actual atomic-add support."""
import sys
import unittest
import numpy as np
from chessism_api.operations.cuda_summaries import blunder_summary, no_move_upgrade
from chessism_api.operations.canonical_blunders.assessment import empty_mode


def fixture():
    rows = []
    expected = {mode:empty_mode() for mode in ('bullet','blitz','rapid')}
    for mode in expected:
        rows.extend([
            dict(mode=mode,status='assessed',n=0,assessed=20,unassessed=0,complete=True),
            dict(mode=mode,status='assessed',n=8,assessed=30,unassessed=0,complete=True),
            dict(mode=mode,status='assessed',n=1,assessed=5,unassessed=2,complete=False),
            dict(mode=mode,status='assessed',n=None,assessed=0,unassessed=12,complete=False),
            dict(mode=mode,status='unsupported_or_ineligible',n=None,assessed=0,unassessed=None,complete=False),
            # Synthetic stress value proves precision beyond int32 and float64.
            dict(mode=mode,status='assessed',n=2,assessed=2**53+1,unassessed=0,complete=True),
            dict(mode=mode,status='assessed',n=None,assessed=0,unassessed=0,complete=False),
        ])
        expected[mode].update(counts=[1,0,1,0,0,0,1],games_count=3,partial_games=1,
            unassessed_games=1,ineligible_games=1,assessed_moves=2**53+56,unassessed_moves=14)
    return rows,expected


class BlunderReductionTests(unittest.TestCase):
    def test_cached_correction_only_removes_zero_move_counts(self):
        rows,expected=fixture()
        snapshots={'alice':expected}
        result=no_move_upgrade.corrected(np,snapshots,[{'player_name':'alice','mode':'blitz'}],require_cuda=False)
        self.assertEqual(result['alice']['blitz']['unassessed_games'],0)
        self.assertEqual(expected['blitz']['unassessed_games'],1)
        for mode in expected:
            for key in expected[mode]:
                if mode=='blitz' and key=='unassessed_games': continue
                self.assertEqual(result['alice'][mode][key],expected[mode][key])
        with self.assertRaises(ValueError):
            no_move_upgrade.corrected(np,snapshots,[{'player_name':'alice','mode':'blitz'}]*2,require_cuda=False)
    def test_exact_counts_and_coverage(self):
        rows,expected = fixture()
        self.assertEqual(blunder_summary.reduce(np,rows,require_cuda=False),expected)
        with self.assertRaises(RuntimeError): blunder_summary.reduce(np,rows)

    def test_empty(self):
        self.assertEqual(blunder_summary.reduce(np,[],require_cuda=False),
                         {mode:empty_mode() for mode in ('bullet','blitz','rapid')})


def cuda_check():
    from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
    cp,hardware = cuda_backend(0)
    assert hardware['name']=='NVIDIA GeForce RTX 4060',hardware
    rows,expected = fixture()
    assert blunder_summary.reduce(cp,rows)==expected
    assert blunder_summary.reduce(cp,[])=={mode:empty_mode() for mode in expected}
    assert blunder_summary.reduce(cp,[rows[0]])['blitz']==empty_mode()
    assert blunder_summary.reduce(cp,[rows[4]])['bullet']['ineligible_games']==1
    assert no_move_upgrade.corrected(cp,{'alice':expected},[{'player_name':'alice','mode':'blitz'}])['alice']['blitz']['unassessed_games']==0
    cp.cuda.get_current_stream().synchronize()
    print('PASS RTX 4060 final blunder reduction: exact counts beyond 2^53, all modes, coverage, empty bins; no CPU fallback',flush=True)


if __name__=='__main__':
    if '--cuda' in sys.argv: cuda_check()
    else: unittest.main()
