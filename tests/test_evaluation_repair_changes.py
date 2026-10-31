"""Reviewed replacements, stable-mate evidence and exact counter boundaries."""
import copy
import unittest
import chess
import chess.engine
from chessism_api.operations.evaluation_repair import aggregates, artifacts, publish
from chessism_api.operations.evaluation_repair.profile import profile
from tests.test_evaluation_repair import fixture
from tests.test_evaluation_repair_publication import matching_shadow, approval
from chessism_api.operations.evaluation_repair import shadow as shadow_runner


def cp_fixture(score=32):
    plan = fixture(); item = plan['items'][0]
    item['before'].update(score=score, n_games=3)
    item['snapshot_sha256'] = artifacts.snapshot_digest(item)
    plan = artifacts.seal(plan)
    return plan, matching_shadow(plan)


def reviewed_args(plan, shadow):
    return {**approval(plan, shadow), 'acknowledgement': publish.REPLACEMENT_ACK,
            'policy': publish.REPLACEMENT_POLICY}


def mate_fixture():
    plan = fixture(); item = plan['items'][0]
    item['before'].update(score=10007, n_games=3)
    item['snapshot_sha256'] = artifacts.snapshot_digest(item)
    plan = artifacts.seal(plan); shadow = matching_shadow(plan)
    row = shadow['candidates'][0]['replacement']
    row.update(score=9994, signed_mate_distance=6, mate_winner='white')
    shadow['candidates'][0]['alternative_lines'] = []
    shadow['profile'] = profile(10_000_000); shadow = artifacts.seal(shadow)
    confirmation = copy.deepcopy(shadow); confirmation['profile'] = profile(20_000_000)
    confirmation = artifacts.seal(confirmation)
    endgame = artifacts.seal({'version': artifacts.VERSION, 'kind': 'endgame_review',
        'plan_sha256': plan['sha256'], 'shadow_sha256': shadow['sha256'],
        'records': [{'fen_id': 1, 'status': 'probed_not_published', 'wdl_white': 2}]})
    return plan, shadow, confirmation, endgame


class ChangedRepairTests(unittest.TestCase):
    def test_bounded_profile_and_nonexact_engine_scores(self):
        for nodes in (0, True, 100001, 100_000_000):
            with self.assertRaises(ValueError): profile(nodes)
        for bound in ('lowerbound', 'upperbound'):
            with self.assertRaisesRegex(ValueError, 'Bound-only'):
                shadow_runner.normalize_line(chess.Board(), {bound: True,
                    'score': chess.engine.PovScore(chess.engine.Cp(24), chess.WHITE),
                    'pv': [chess.Move.from_uci('e2e4')]})

    def test_explicit_policy_and_same_bucket(self):
        plan, shadow = cp_fixture()
        shadow['candidates'][0]['replacement'].update(score=20, wdl_win=100, wdl_draw=800, wdl_loss=100)
        shadow = artifacts.seal(shadow)
        selected = publish.reviewed(plan, shadow, [1], **reviewed_args(plan, shadow))
        self.assertEqual(aggregates.delta(selected), {'equal_abs_score_sum': -12, 'score_sum': -12,
            'abs_score_sum': -12, 'wdl_win_sum': 100, 'wdl_draw_sum': 800, 'wdl_loss_sum': 100, 'wdl_positions': 1})
        with self.assertRaises(ValueError):
            publish.reviewed(plan, shadow, [1], **approval(plan, shadow))
        shadow['candidates'][0]['replacement']['score'] = 51; shadow = artifacts.seal(shadow)
        with self.assertRaisesRegex(ValueError, 'Cross-bucket'):
            publish.reviewed(plan, shadow, [1], **reviewed_args(plan, shadow))

    def test_counter_boundaries(self):
        for score, bucket, side in ((0, 'equal', 'balanced'), (49, 'equal', 'balanced'),
            (50, 'small', 'balanced'), (-50, 'small', 'balanced'), (51, 'small', 'white_better'),
            (-51, 'small', 'black_better'), (149, 'small', 'white_better'),
            (150, 'clear', 'white_better'), (299, 'clear', 'white_better'),
            (300, 'decisive', 'white_better'), (8999, 'decisive', 'white_better'),
            (9000, 'mate', 'white_better'), (-9990, 'mate', 'black_better')):
            row = aggregates.contribution({'score': score, 'n_games': 7})
            self.assertEqual(row[f'{bucket}_positions'], 1)
            self.assertEqual(row[f'{bucket}_appearances'], 7)
            self.assertEqual(row[side], 1)
            self.assertEqual(row['nonzero_scored_fens'], int(score != 0))
        self.assertEqual(aggregates.contribution({'score': 0, 'n_games': 2, 'wdl_win': 1000})['wdl_positions'], 0)

    def test_stable_mate_requires_stronger_search_and_independent_winner(self):
        plan, shadow, confirmation, endgame = mate_fixture()
        def check(c=confirmation, e=endgame):
            return publish.reviewed(plan, shadow, [1], **reviewed_args(plan, shadow), confirmation=c, endgame_review=e)
        self.assertEqual(check()[0]['replacement']['signed_mate_distance'], 6)
        with self.assertRaises(ValueError): check(c=None)
        with self.assertRaises(ValueError): check(e=None)
        changed = copy.deepcopy(confirmation)
        changed['candidates'][0]['replacement'].update(score=9993, signed_mate_distance=7)
        with self.assertRaisesRegex(ValueError, 'stabilize'): check(c=artifacts.seal(changed))
        changed = copy.deepcopy(confirmation); changed['profile'] = shadow['profile']
        with self.assertRaisesRegex(ValueError, 'higher-budget'): check(c=artifacts.seal(changed))
        changed = copy.deepcopy(endgame); changed['records'][0]['wdl_white'] = -2
        with self.assertRaisesRegex(ValueError, 'winning side'): check(e=artifacts.seal(changed))
        changed = copy.deepcopy(endgame); changed['shadow_sha256'] = 'wrong'
        with self.assertRaises(ValueError): check(e=artifacts.seal(changed))

    def test_no_reinterpretation_or_mate_downgrade(self):
        plan, shadow, _, _ = mate_fixture()
        row = shadow['candidates'][0]['replacement']
        row.update(score=542, score_kind='cp', signed_mate_distance=None, mate_winner=None)
        shadow = artifacts.seal(shadow)
        with self.assertRaises(ValueError):
            publish.reviewed(plan, shadow, [1], **reviewed_args(plan, shadow))


if __name__ == '__main__':
    unittest.main()
