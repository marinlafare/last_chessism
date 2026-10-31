"""Offline validation of reviewed, matching-only publication artifacts."""
import copy
from datetime import datetime, timezone
import unittest

import chess
from chessism_api.operations.evaluation_repair import artifacts, publish
from tests.test_evaluation_repair import fixture


def matching_shadow(plan):
    candidates = []
    for item in plan['items']:
        old = item['before']; board = chess.Board(old['fen'])
        score = old['score']; mate = abs(score) >= 9000
        distance = (10000-abs(score)) * (1 if score > 0 else -1) if mate else None
        moves = list(board.legal_moves)
        line = {'score': score, 'score_kind': 'mate' if mate else 'cp',
                'signed_mate_distance': distance,
                'mate_winner': ('white' if score > 0 else 'black') if mate else None,
                'next_moves': moves[0].uci() if moves else None,
                **{k: old.get(k) for k in publish.WDL}}
        replacement = {'fen': old['fen'], 'analysis_source': 'stockfish',
                       'piece_count': len(board.piece_map()),
                       'analyzed_at': datetime.now(timezone.utc).isoformat(), **line}
        candidates.append({'fen_id': old['fen_id'], 'before': old,
            'snapshot_sha256': item['snapshot_sha256'], 'replacement': replacement,
            'alternative_lines': [{**line, 'next_moves': moves[1].uci()}] if len(moves) > 1 else []})
    return artifacts.seal({'version': artifacts.VERSION, 'kind': 'shadow', 'plan_sha256': plan['sha256'],
        'profile': {'id': publish.PROFILE_ID, 'binary_sha256': publish.ENGINE_SHA256,
                    'nodes': publish.NODES, 'threads': publish.THREADS, 'hash_mib': publish.HASH_MIB,
                    'multipv': publish.MULTIPV, 'perspective': 'white',
                    'mate_encoding': 'python_chess_mate_score_10000'}, 'candidates': candidates})


def approval(plan, shadow):
    return dict(plan_sha256=plan['sha256'], shadow_sha256=shadow['sha256'], acknowledgement=publish.ACK)


class PublicationValidationTests(unittest.TestCase):
    def test_matching_explicit_candidate(self):
        plan = fixture(); shadow = matching_shadow(plan)
        selected = publish.reviewed(plan, shadow, [1], **approval(plan, shadow))
        self.assertEqual(selected[0]['replacement']['score'], 9997)
        for ids in ([], [1, 1], [True], [2]):
            with self.assertRaises(ValueError):
                publish.reviewed(plan, shadow, ids, **approval(plan, shadow))

    def test_changed_or_fabricated_metadata_fails_even_with_new_checksum(self):
        plan = fixture(); original = matching_shadow(plan)
        for field, value in (('score', 34), ('score_kind', 'cp'), ('signed_mate_distance', -3),
                             ('mate_winner', 'black'), ('next_moves', 'e2e5'),
                             ('piece_count', 31), ('analysis_source', 'legacy'),
                             ('analyzed_at', '2099-01-01T00:00:00+00:00'),
                             ('analyzed_at', '2026-01-01'), ('tablebase_wdl', 2),
                             ('wdl_win', 1000)):
            with self.subTest(field=field, value=value):
                shadow = copy.deepcopy(original)
                shadow['candidates'][0]['replacement'][field] = value
                shadow = artifacts.seal(shadow)
                with self.assertRaises(ValueError):
                    publish.reviewed(plan, shadow, [1], **approval(plan, shadow))

    def test_profile_checksums_approval_and_input_binding(self):
        plan = fixture(); original = matching_shadow(plan)
        for field in ('binary_sha256', 'nodes', 'threads', 'multipv', 'perspective'):
            shadow = copy.deepcopy(original); shadow['profile'][field] = 'wrong'
            shadow = artifacts.seal(shadow)
            with self.assertRaises(ValueError):
                publish.reviewed(plan, shadow, [1], **approval(plan, shadow))
        for field in ('plan_sha256', 'shadow_sha256', 'acknowledgement'):
            args = {**approval(plan, original), field: 'wrong'}
            with self.assertRaises(ValueError):
                publish.reviewed(plan, original, [1], **args)
        shadow = copy.deepcopy(original); shadow['candidates'][0]['before']['score'] = 99
        shadow = artifacts.seal(shadow)
        with self.assertRaises(ValueError):
            publish.reviewed(plan, shadow, [1], **approval(plan, shadow))

    def test_duplicate_alternative_and_terminal_mate(self):
        plan = fixture(); shadow = matching_shadow(plan)
        candidate = shadow['candidates'][0]
        candidate['alternative_lines'][0]['next_moves'] = candidate['replacement']['next_moves']
        shadow = artifacts.seal(shadow)
        with self.assertRaises(ValueError):
            publish.reviewed(plan, shadow, [1], **approval(plan, shadow))
        board = chess.Board('8/K1k5/8/r7/8/8/8/8 w - - 0 1')
        line = {'score': -10000, 'score_kind': 'mate', 'signed_mate_distance': 0,
                'mate_winner': 'black', 'next_moves': None}
        publish.validate_line(board, line)
        with self.assertRaises(ValueError):
            publish.validate_line(chess.Board(), line)
        with self.assertRaises(ValueError):
            publish.validate_line(board, {**line, 'score': 10000, 'mate_winner': 'white'})
        with self.assertRaises(ValueError):
            publish.validate_line(board, {**line, 'score': -9997, 'signed_mate_distance': -3})
        with self.assertRaises(ValueError):
            publish.validate_line(board, {'score': 0, 'score_kind': 'cp', 'signed_mate_distance': None,
                                         'mate_winner': None, 'next_moves': None})


if __name__ == '__main__':
    unittest.main()
