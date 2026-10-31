from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_full_source as source, blunder_full_compute as compute
from chessism_api.operations.research_algorithms import blunder_full_diagnostics as diagnostics, blunder_v2_model as model
from tests.test_blunder_data_audit import POLICY


def staged_fixture(folder):
    games = []
    for split in (0, 1):
        for mode in model.MODES:
            for i in range(100):
                games.append({'link': len(games)+1, 'split': split, 'mode': mode,
                    'played_at': datetime(2022 if split == 0 else 2024, 1, 1, tzinfo=timezone.utc),
                    'white_name': f'w{i%10}', 'black_name': f'b{i}', 'white_complete': True, 'black_complete': i%3 != 0,
                    'white_elo': 800 if i%2 else 3200, 'black_elo': 1000 if i%2 else 2800,
                    'sample_order': i, 'rating_band': 0 if i%2 else 8, 'total_positions': 4})
    streams = {(s, m): (folder/f'staged-{s}-{m}.bin').open('xb') for s in (0, 1) for m in model.MODES}
    moves = (folder/'staged-bullet-moves.bin').open('xb')
    grouped = {}; outcomes = {}; offsets = Counter(); digest = hashlib.sha256()
    for g in games:
        i = g['sample_order']; outcomes[g['link']] = (0., .5, 1.)[i%3]
        scores = [(400., 0), (0., 0), (-400., 0), (0., 0)] if i%2 else [(200., 0), (105., 0), (-9998., 1), (0., 2)]
        if i == 99: scores = [(9998., 1)]*4
        grouped[g['link']] = [(ply+1, cp, g['link']*10+ply, kind) for ply, (cp, kind) in enumerate(scores)]
    try: source.stage_block(games, outcomes, grouped, streams, moves, offsets, digest)
    finally:
        for stream in streams.values(): stream.close()
        moves.close()
    frozen = {'reserved_games': len(games), 'reserved_roles': {name: [g['link'] for g in games if g['split'] == s]
              for s, name in enumerate(('training', 'validation'))}}
    with patch.object(source.balance, 'held_player', return_value=False):
        groups, diagnostic_games = source.finalize_inputs(games, folder, frozen, POLICY)
    return groups, diagnostic_games, frozen


def diagnostic_fixture(xp, folder, games, budget):
    fits = []
    for mode in model.MODES:
        rows = sum(g['cp_count'] for g in games if g['mode'] == mode and g['split'] == 1)
        # Blitz inputs are not retained in the diagnostic metadata.
        if mode == 'blitz': continue
        data = compute.Input(xp, folder/f'1-{mode}.bin', rows, budget, lambda: None)
        try: metrics = data.metrics([[0., 0.], [.12, 2.3], [.21, 2.8]], calibration=True)
        finally: data.close()
        fits.append({'mode': mode, 'status': 'fitted', 'a': .21, 'b': 2.8, **metrics[2],
            'baseline': metrics[0], 'frozen_comparison': {'a': .12, 'b': 2.3, **metrics[1]}})
    return diagnostics.run(xp, folder, games, fits, budget, lambda: None, lambda *a: None)


class DiagnosticsTests(unittest.TestCase):
    def test_cap_applied_after_cp_filter_with_no_weight_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); groups, games, frozen = staged_fixture(folder)
            self.assertEqual(frozen['reserved_games'], 600)
            self.assertEqual(frozen['selected_games'], 540)
            self.assertEqual(frozen['cp_eligible_games'], 594)
            self.assertEqual(frozen['removed_by_player_cap'], 54)
            self.assertEqual(frozen['no_cp_games'], 6)
            self.assertTrue(all(g['player_balance_passed'] and g['largest_player_share'] == .1 for g in groups))
            self.assertEqual(sum(map(len, frozen['reserved_roles'].values())), 600)
            self.assertFalse(list(folder.glob('staged-*')))
            for g in groups:
                raw = np.fromfile(folder/f"{g['split']}-{g['mode']}.bin", dtype='<f8').reshape(-1, 5)
                self.assertEqual(len(raw), g['cp_rows']); self.assertAlmostEqual(raw[:, 4].sum(), 90.)
            bullet = [g for g in games if g['mode'] == 'bullet']
            raw = np.fromfile(folder/'bullet-moves.bin', dtype=model.DTYPE)
            self.assertEqual(len(raw), sum(g['total_positions'] for g in bullet))
            self.assertEqual(set(raw['game']), set(range(90)))
            self.assertEqual(raw['before'][0], 15.)

    def test_rapid_groups_and_bullet_mover_oracles(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); _, games, _ = staged_fixture(folder)
            report = diagnostic_fixture(np, folder, games, compute.GIB)
            self.assertFalse(report['test_evaluated'])
            self.assertLess(report['rapid']['validation_metric_max_error'], 1e-9)
            groups = report['rapid']['groups']
            self.assertTrue({'overall', 'average_rating', 'rating_gap', 'year', 'player_participation'} <= {g['kind'] for g in groups})
            for split in (0, 1):
                g = next(g for g in groups if g['kind'] == 'overall' and g['split'] == split)
                self.assertEqual(g['games'], 90)
                for m in g['models'].values(): self.assertAlmostEqual(sum(b['game_weight'] for b in m['bins']), 90.)
            bullet = report['bullet']
            self.assertEqual(bullet['status'], 'passed'); self.assertFalse(any(bullet['checks'].values()))
            self.assertEqual({g['rating_band'] for g in bullet['groups']}, {'all', '<1000', '1000–1499', '2500–2999', '3000+'})
            self.assertTrue(bullet['groups'][0]['unassessed'] > 0)
            self.assertTrue(all(e['models']['candidate']['loss'] is not None for e in bullet['examples']))
            json.dumps(report, allow_nan=False)

    def test_no_final_test_or_held_player_can_survive_balance(self):
        rows = [{'link': i+1, 'white_name': f'w{i%10}', 'black_name': f'b{i}', 'mode': 'rapid',
                 'played_at': datetime(2022, 1, 1, tzinfo=timezone.utc), 'sample_order': i} for i in range(100)]
        rows += [{**rows[0], 'link': 101, 'played_at': datetime(2026, 1, 1, tzinfo=timezone.utc)},
                 {**rows[0], 'link': 102, 'black_name': 'held'}]
        with patch.object(source.balance, 'held_player', side_effect=lambda n: n == 'held'):
            kept, _ = source.choose_maximum_pool(rows, POLICY)
        self.assertEqual({g['link'] for g in kept}, set(range(1, 101)))
        with patch.object(source.balance, 'held_player', return_value=False), self.assertRaisesRegex(RuntimeError, 'cancel'):
            source.choose_maximum_pool(rows, POLICY, lambda: (_ for _ in ()).throw(RuntimeError('cancel')))

    def test_cancel_and_corrupt_local_input_do_not_get_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); _, games, _ = staged_fixture(folder)
            part = [g for g in games if g['mode'] == 'rapid' and g['split'] == 0]
            with self.assertRaisesRegex(RuntimeError, 'cancel'):
                diagnostics.rapid_metrics(np, folder, part, 0, [[0., 0.]]*3, compute.GIB, lambda: (_ for _ in ()).throw(RuntimeError('cancel')))
            part[0]['cp_offset'] = 1
            with self.assertRaisesRegex(ValueError, 'offsets'):
                diagnostics.rapid_metrics(np, folder, part, 0, [[0., 0.]]*3, compute.GIB, lambda: None)

    def test_cp_to_forced_mate_and_tablebase_rules(self):
        args = [np.array(v)[:, None] for v in ([105., 800., 9998., 0.], [-9998., -9998., 0., 0.],
                [0, 0, 1, 0], [1, 1, 0, 2], [0.]*4, [0.]*4)]
        flags = diagnostics.reference_flags(*args, [[0., 0.], [.1, 2.], [.2, 3.]])
        np.testing.assert_array_equal(flags, np.array([[1]*3, [1]*3, [1]*3, [-1]*3]))
