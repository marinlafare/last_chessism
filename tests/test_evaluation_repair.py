"""Offline repair-plan integrity and shadow-engine checks; never live writes."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import chess
import chess.engine
from chessism_api.operations.evaluation_repair import artifacts, shadow


def fixture():
    old = {'fen_id': 1, 'fen': chess.STARTING_FEN, 'score': 9997,
           'analysis_source': None, 'analyzed_at': None,
           'tablebase_wdl': None, 'tablebase_dtz': None}
    item = {'before': old, 'row_revision': '123', 'continuations': [], 'players': [],
            'references': [{'game_link': 1, 'n_move': 1, 'is_white': True}]}
    item['snapshot_sha256'] = artifacts.snapshot_digest(item)
    return artifacts.seal({'version': artifacts.VERSION, 'kind': 'plan', 'items': [item]})


class ArtifactTests(unittest.TestCase):
    def test_non_overwriting_report_and_tampering(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'plan.json'
            saved = artifacts.write(path, fixture())
            self.assertEqual(artifacts.read(path, 'plan'), saved)
            with self.assertRaises(FileExistsError):
                artifacts.write(path, fixture())
            changed = copy.deepcopy(saved)
            changed['items'][0]['before']['score'] = 24
            with self.assertRaises(ValueError):
                artifacts.verify(changed, 'plan')
            with self.assertRaises(ValueError):
                artifacts.verify(artifacts.seal(changed), 'plan')

    def test_bounds_duplicates_and_known_sources_rejected(self):
        for count in (0, 2, 17):
            plan = fixture(); plan['items'] *= count
            with self.assertRaises(ValueError):
                artifacts.verify(artifacts.seal(plan), 'plan')
        for field, value in (('analysis_source', 'stockfish'), ('analyzed_at', '2026-01-01'),
                             ('score', None), ('tablebase_wdl', 0)):
            plan = fixture(); item = plan['items'][0]
            item['before'][field] = value
            item['snapshot_sha256'] = artifacts.snapshot_digest(item)
            with self.assertRaises(ValueError):
                artifacts.verify(artifacts.seal(plan), 'plan')

    def test_illegal_pv_and_type_collision_rejected(self):
        board = chess.Board()
        for info in (
            {'score': chess.engine.PovScore(chess.engine.Cp(24), chess.WHITE), 'pv': []},
            {'score': chess.engine.PovScore(chess.engine.Cp(24), chess.WHITE),
             'pv': [chess.Move.from_uci('e2e5')]},
            {'score': chess.engine.PovScore(chess.engine.Cp(9997), chess.WHITE),
             'pv': [chess.Move.from_uci('e2e4')]},
        ):
            with self.assertRaises(ValueError):
                shadow.normalize_line(board, info)

    def test_native_mate_direction_survives_serialization(self):
        board = chess.Board()
        line = shadow.normalize_line(board, {'score': chess.engine.PovScore(chess.engine.Mate(3), chess.BLACK),
                                           'pv': [chess.Move.from_uci('e2e4')]})
        self.assertEqual((line['score'], line['signed_mate_distance'], line['mate_winner']), (-9997, -3, 'black'))

    def test_legacy_pv_is_not_mistaken_for_forced_mate_proof(self):
        from chessism_api.operations.evaluation_repair.tablebase_review import legacy_pv
        result = legacy_pv({'fen': chess.STARTING_FEN, 'next_moves': 'f2f3 e7e5 g2g4 d8h4'})
        self.assertTrue(result['legal'] and result['ends_checkmate'])
        self.assertEqual(result['winner'], 'black')
        self.assertFalse(result['proves_forced_mate'])
        self.assertFalse(legacy_pv({'fen': chess.STARTING_FEN, 'next_moves': 'e2e5'})['legal'])


class ShadowTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_verified_shadow_preserves_old_data(self):
        plan = fixture(); original = copy.deepcopy(plan)
        engine, transport = AsyncMock(), Mock()
        engine.id = {'name': 'Stockfish 16.1'}
        lines = [{'score': chess.engine.PovScore(chess.engine.Cp(24), chess.WHITE),
                  'pv': [chess.Move.from_uci('e2e4')]}]
        with patch.object(shadow, 'verify_binary') as binary, \
                patch.object(shadow.chess.engine, 'popen_uci', AsyncMock(return_value=(transport, engine))), \
                patch.object(shadow, 'configure', AsyncMock()) as configure, \
                patch.object(shadow, 'analyse', AsyncMock(return_value=lines)) as analyse:
            result = await shadow.run(plan)
        self.assertEqual(plan, original)
        self.assertEqual(result['scores_changed'], 0)
        self.assertEqual(result['status'], 'needs_review_not_published')
        self.assertEqual(result['candidates'][0]['before']['score'], 9997)
        self.assertEqual(result['candidates'][0]['replacement']['score'], 24)
        self.assertEqual(result['candidates'][0]['old_interpretation'], 'unknown')
        self.assertEqual(result['candidates'][0]['review'], 'hold_legacy_mate_not_reproduced')
        self.assertEqual(result['review_counts']['hold_legacy_mate_not_reproduced'], 1)
        binary.assert_called_once_with('/usr/local/bin/stockfish')
        configure.assert_awaited_once_with(engine, 1, 256)
        self.assertEqual(analyse.await_args.args[2:], (100000, 4))
        engine.quit.assert_awaited_once(); transport.close.assert_called_once()

    async def test_wrong_binary_never_starts_engine(self):
        with patch.object(shadow, 'verify_binary', side_effect=ValueError('wrong binary')), \
                patch.object(shadow.chess.engine, 'popen_uci', AsyncMock()) as launch:
            with self.assertRaises(ValueError):
                await shadow.run(fixture())
            launch.assert_not_awaited()

    async def test_engine_failure_closes_only_its_own_process(self):
        engine, transport = AsyncMock(), Mock()
        engine.id = {'name': 'Stockfish 16.1'}
        with patch.object(shadow, 'verify_binary'), \
                patch.object(shadow.chess.engine, 'popen_uci', AsyncMock(return_value=(transport, engine))), \
                patch.object(shadow, 'configure', AsyncMock()), \
                patch.object(shadow, 'analyse', AsyncMock(side_effect=TimeoutError('synthetic stall'))):
            with self.assertRaises(TimeoutError):
                await shadow.run(fixture())
        engine.quit.assert_awaited_once(); transport.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
