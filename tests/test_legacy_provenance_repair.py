import copy
import unittest
from unittest.mock import patch
import chess

from chessism_api.operations.evaluation_repair import legacy_policy as policy, rating_delta


def row(fen=None, score=24, fen_id=1):
    return {'fen_id': fen_id, 'fen': fen or chess.Board().fen(), 'score': score,
            'n_games': 3, 'next_moves': 'e2e4', 'analysis_source': None, 'analyzed_at': None,
            'piece_count': None, 'wdl_win': None, 'wdl_draw': None, 'wdl_loss': None,
            'tablebase_wdl': None, 'tablebase_dtz': None}


class LegacyPolicyTests(unittest.TestCase):
    def test_assumption_only_changes_source_and_leaves_timestamp_unknown(self):
        old = row()
        candidates, skipped = policy.prepare([old], None)
        self.assertEqual(skipped, [])
        self.assertEqual(candidates[0]['replacement'], {**old, 'analysis_source': policy.SOURCE})
        policy.validate(candidates)
        for field, value in (('score', 99), ('analyzed_at', '2026-10-27T00:00:00+00:00'),
                             ('fen_id', 2), ('analysis_source', 'stockfish')):
            changed = copy.deepcopy(candidates)
            changed[0]['replacement'][field] = value
            with self.assertRaises(ValueError): policy.validate(changed)

    def test_known_unscored_and_large_mates_are_never_relabelled(self):
        for source in ('stockfish', 'tablebase', policy.SOURCE):
            with self.assertRaises(ValueError): policy.prepare([{**row(), 'analysis_source': source}], None)
        with self.assertRaises(ValueError): policy.prepare([row(score=None)], None)
        for score in (9000, 9997, 10007, -9997):
            candidates, skipped = policy.prepare([row(score=score)], None)
            self.assertEqual(candidates, [])
            self.assertEqual(skipped[0]['reason'], 'historical_mate_encoding')

    def test_small_positions_get_real_tablebase_result_or_stay_unchanged(self):
        old = row('8/8/8/2k5/4Q3/4K3/8/8 b - -', 10007)
        solved = {'fen': old['fen'], 'piece_count': 3, 'score': 1000.0, 'next_moves': 'c5b6',
                  'wdl_win': 1000.0, 'wdl_draw': 0.0, 'wdl_loss': 0.0, 'tablebase_wdl': -2, 'tablebase_dtz': -3}
        with patch('chessism_api.operations.tablebase._probe_batch_sync', return_value=([solved], [])):
            candidates, skipped = policy.prepare([old], object())
        self.assertEqual(skipped, [])
        self.assertEqual(candidates[0]['replacement']['analysis_source'], 'tablebase')
        self.assertIsNotNone(candidates[0]['replacement']['analyzed_at'])
        self.assertIs(type(candidates[0]['replacement']['score']), int)
        self.assertEqual(candidates[0]['before'], old)
        policy.validate(candidates)
        changed = copy.deepcopy(candidates); changed[0]['replacement']['score'] = -1000
        with self.assertRaisesRegex(ValueError, 'disagree'): policy.validate(changed)
        with patch('chessism_api.operations.tablebase._probe_batch_sync', return_value=([], [old])):
            candidates, skipped = policy.prepare([old], object())
        self.assertEqual(candidates, [])
        self.assertEqual(skipped[0]['reason'], 'tablebase_unavailable')
        self.assertIsNone(old['analysis_source'])

    def test_rating_patch_preserves_totals_and_other_groups(self):
        groups = [{'key': 'bad', 'positions': 5, 'buckets': [
            {'key': key, 'positions': 5 if key == 'mate' else 0} for key in rating_delta.BINS]}]
        result = rating_delta.patch_groups(groups, [{'group_key': 'bad', 'old_bucket': 'mate',
                    'new_bucket': 'decisive', 'occurrences': 3}])
        self.assertEqual([x['positions'] for x in result[0]['buckets']], [0, 0, 0, 3, 2])
        self.assertEqual(groups[0]['buckets'][-1]['positions'], 5)
        with self.assertRaises(ValueError):
            rating_delta.patch_groups(groups, [{'group_key': 'bad', 'old_bucket': 'mate',
                    'new_bucket': 'equal', 'occurrences': 6}])


if __name__ == '__main__': unittest.main()
