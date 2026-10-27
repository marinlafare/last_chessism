"""GPU metrics/bootstrap, independent CPU decisions, explicit promotion gates."""
import time

import numpy as np

from .blunder_fixed import kernel as old_kernel
from .blunder_v2_calibration import METRIC_COLUMNS, compare, player_dependence
from .blunder_v2_model import MODES, chunks, decisions, expected, scalar

MIN_SUPPORT = (500, 100, 100)
BOOTSTRAPS = 1000


def interval(xp, candidate, baseline, check=lambda: None):
    """Paired whole-game bootstrap, not millions of independent move samples."""
    # Identical resampling identities on CPU/GPU; CUDA performs the reductions.
    # Host index generation also avoids adding a cuRAND runtime dependency.
    rng = np.random.default_rng(20261028)
    results = []
    for start in range(0, BOOTSTRAPS, 8):
        check()
        indices = xp.asarray(rng.integers(0, len(candidate), size=(min(8, BOOTSTRAPS-start), len(candidate))))
        base = xp.mean(baseline[indices], axis=1)
        delta = xp.mean(candidate[indices] - baseline[indices], axis=1)
        results.append(delta / xp.maximum(base, 1e-15))
    values = xp.percentile(xp.concatenate(results), xp.asarray([2.5, 97.5]))
    return [float(value) for value in values]


def metric(xp, matrix, selected, check=lambda: None):
    rows = matrix[xp.asarray(selected)]
    rows = rows[rows[:, 0] > 0]
    if not len(rows):
        return {'games': 0, 'status': 'insufficient_data'}
    w = rows[:, :1]
    normalized = rows[:, 1:] / w
    mse, baseline = normalized[:, 0], normalized[:, 1]
    calibration = compare(xp, normalized, BOOTSTRAPS, check)
    ece = calibration['ece']
    report = {'games': len(rows), 'mse': float(xp.mean(mse)), 'baseline_mse': float(xp.mean(baseline)),
              **calibration}
    if len(rows) < MIN_SUPPORT[2] or float(xp.mean(baseline)) <= 0:
        return {**report, 'status': 'insufficient_data'}
    ci = interval(xp, mse, baseline, check)
    return {**report, 'relative_mse_change_ci95': ci, 'status': 'passed' if ci[1] <= .02 and ece <= .05 else 'failed'}


def add_metrics(xp, matrix, ids, p, base, outcome):
    xp.add.at(matrix[:, 0], ids, 1)
    xp.add.at(matrix[:, 1], ids, (p-outcome)**2)
    xp.add.at(matrix[:, 2], ids, (base-outcome)**2)
    for offset, prediction in ((3, p), (33, base)):
        bins = xp.minimum(9, (prediction*10).astype(xp.int32))
        xp.add.at(matrix, (ids, offset+bins), 1)
        xp.add.at(matrix, (ids, offset+10+bins), prediction)
        xp.add.at(matrix, (ids, offset+20+bins), outcome)


class Validation:
    def __init__(self, xp, rows, games, metadata, fits, evaluation_splits=(1, 2, 3)):
        self.xp, self.rows, self.games, self.metadata, self.fits = xp, rows, games, metadata, fits
        self.evaluation_splits = evaluation_splits
        self.metrics = xp.zeros((5, len(metadata), METRIC_COLUMNS), dtype=xp.float64)
        # CuPy scatter-add supports int32, not signed int64. A whole source
        # has <=3m rows, safely below int32 even before per-game grouping.
        self.counts = xp.zeros((len(metadata)*2, 4), dtype=xp.int32)
        self.max_error = 0.
        self.mismatches = 0
        self.examples = []
        self.rows_checked = self.seen_fens = self.test_fens = 0
        self.assessed_moves = self.unassessed_moves = 0
        self.seconds = {'cpu_reference': 0., 'gpu_inference_and_metrics': 0.}
        # Identity-only overlap audit, not a prediction feature.
        keep = (games['split'][rows['game']] == 0) & (rows['ak'] == 0)
        self.training_fens = np.unique(rows['fen'][keep])

    def consume(self, block):
        xp, g = self.xp, self.games
        ids = block['game']
        for mode, fit in enumerate(self.fits):
            if fit['status'] != 'fitted':
                continue
            selected = (g['mode'][ids] == mode) & np.isin(g['split'][ids], self.evaluation_splits)
            sub = block[selected]
            if not len(sub):
                continue
            indices = sub['game']
            sign = np.where(sub['ply'] % 2, 1., -1.)
            before, after = sign*sub['before'], sign*sub['after']
            u, d = g['u'][indices], sign*g['d'][indices]
            a, b = fit['a'], fit['b']
            started = time.perf_counter()
            oracle = np.array([scalar(*values, a, b) for values in zip(before, after, sub['bk'], sub['ak'], u, d)])
            cpu_p = expected(np, np.where(sub['ak'] == 0, after, 0.), u, d, a, b)
            self.seconds['cpu_reference'] += time.perf_counter()-started
            started = time.perf_counter()
            arrays = [xp.asarray(x) for x in (before, after, sub['bk'], sub['ak'], u, d)]
            actual = decisions(xp, *arrays, a, b)
            p = expected(xp, xp.where(arrays[3] == 0, arrays[1], 0.), arrays[4], arrays[5], a, b)
            before_p = expected(xp, xp.where(arrays[2] == 0, arrays[0], 0.), arrays[4], arrays[5], a, b)
            before_cpu = expected(np, np.where(sub['bk'] == 0, before, 0.), u, d, a, b)
            self.max_error = max(self.max_error, float(xp.max(xp.abs(p-xp.asarray(cpu_p)))),
                                 float(xp.max(xp.abs(before_p-xp.asarray(before_cpu)))))
            different = actual != xp.asarray(oracle)
            self.mismatches += int(xp.count_nonzero(different))
            self.rows_checked += len(sub)
            gpu_indices = xp.asarray(indices)
            sides = xp.asarray(indices*2 + (sub['ply'] % 2 == 0))
            assessed = actual >= 0
            self.assessed_moves += int(xp.count_nonzero(assessed))
            self.unassessed_moves += int(xp.count_nonzero(~assessed))
            # Four per-side counts: old15, baseline20, candidate20, scalar candidate.
            old = old_kernel(xp, *arrays[:4])
            baseline20 = decisions(xp, *arrays, 0., 0.)
            for column, decision in enumerate((old, baseline20, actual, xp.asarray(oracle))):
                xp.add.at(self.counts[:, column], sides, ((decision == 1) & assessed).astype(xp.int32))
            cp = sub['ak'] == 0
            if np.any(cp):
                # Calibration uses one White-perspective observation per board;
                # complementing the mover prediction avoids duplicate games.
                white_p = xp.where(xp.asarray(sign) > 0, p, 1-p)
                base = expected(xp, xp.asarray(np.where(cp, sub['after'], 0.)), 0., 0., 0., 0.)
                outcomes = xp.asarray(g['outcome'][indices])
                seen = np.isin(sub['fen'], self.training_fens)
                test = g['split'][indices] >= 2
                self.test_fens += int(np.count_nonzero(cp & test))
                self.seen_fens += int(np.count_nonzero(cp & test & seen))
                masks = [cp, cp & (sub['ply'] <= 20), cp & (sub['ply'] > 20) & (sub['ply'] <= 60),
                         cp & (sub['ply'] > 60), cp & ~seen]
                for dimension, mask in enumerate(masks):
                    mask = xp.asarray(mask)
                    add_metrics(xp, self.metrics[dimension], gpu_indices[mask], white_p[mask], base[mask], outcomes[mask])
            # Compact disagreement examples separate threshold and model effects.
            changed = xp.asnumpy((old != actual) & assessed) if hasattr(xp, 'asnumpy') else (old != actual) & assessed
            for j in np.flatnonzero(changed)[:max(0, 20-len(self.examples))]:
                self.examples.append({'game': int(self.metadata[indices[j]]['link']), 'ply': int(sub['ply'][j]),
                    'before': float(before[j]), 'after': float(after[j]), 'old15': int(old[j]),
                    'baseline20': int(baseline20[j]), 'candidate20': int(actual[j])})
            if hasattr(xp, 'cuda'):
                xp.cuda.get_current_stream().synchronize()
            self.seconds['gpu_inference_and_metrics'] += time.perf_counter()-started

    def finish(self, check=lambda: None):
        xp, g, metadata = self.xp, self.games, self.metadata
        primary = (g['split'] == 2) & (2 in self.evaluation_splits)
        all_test = metric(xp, self.metrics[0], primary, check)
        held = metric(xp, self.metrics[0], (g['split'] == 3) & (3 in self.evaluation_splits), check)
        strata = []
        avg = np.array([(m['white_elo']+m['black_elo'])/2 for m in metadata])
        bands = np.clip(np.floor((avg-1000)/200), 0, 8).astype(int)
        gaps = np.abs(g['d']*400)
        timeout = np.array([any(e in ('timeout', 'timevsinsufficient') for e in (m['white_end'], m['black_end'])) for m in metadata])
        abandoned = np.array(['abandoned' in (m['white_end'], m['black_end']) for m in metadata])
        masks = [(mode, g['mode'] == i) for i, mode in enumerate(MODES)]
        masks += [(f'{mode}:rating-band-{band}', (g['mode'] == i) & (bands == band)) for i, mode in enumerate(MODES) for band in range(9)]
        masks += [('rating-gap-0-200', gaps <= 200), ('rating-gap-201-400', (gaps > 200) & (gaps <= 400)),
                  ('rating-gap-over-400', gaps > 400), ('timeout', timeout), ('abandoned', abandoned), ('other-endings', ~timeout & ~abandoned)]
        for name, mask in masks:
            check()
            counts = [int(np.count_nonzero(mask & (g['split'] == s) & (g['cp_count'] > 0))) for s in range(3)]
            item = metric(xp, self.metrics[0], mask & primary, check)
            supported = all(n >= need for n, need in zip(counts, MIN_SUPPORT))
            strata.append({'name': name, **item, 'split_games': counts, 'supported': supported,
                           'status': item['status'] if supported else 'insufficient_data'})
        audits = {name: metric(xp, self.metrics[i], primary, check) for i, name in enumerate(
            ('all', 'opening-plies-1-20', 'middle-plies-21-60', 'late-plies-over-60', 'unseen-training-fens')) if i}
        # Diagnostic cross-groups do not change the predeclared validation gates.
        cross_groups = []
        for i, mode in enumerate(MODES):
            for label, mask in (('gap-0-200', gaps <= 200), ('gap-over-400', gaps > 400),
                                ('timeout', timeout), ('other-endings', ~timeout & ~abandoned)):
                cross_groups.append({'name': f'{mode}:{label}',
                    **metric(xp, self.metrics[0], primary & (g['mode'] == i) & mask, check)})
        totals = []
        for i, mode in enumerate(MODES):
            if self.fits[i]['status'] != 'fitted':
                totals.append({'mode': mode, 'status': 'unassessed', 'old15': None, 'baseline20': None,
                               'candidate20': None, 'cpu_candidate20': None})
                continue
            mask = np.repeat(primary & (g['mode'] == i), 2)
            values = xp.sum(self.counts[xp.asarray(mask)], axis=0)
            totals.append({'mode': mode, **dict(zip(('old15', 'baseline20', 'candidate20', 'cpu_candidate20'), map(int, values)))})
        numerical = self.rows_checked > 0 and self.max_error <= 1e-10 and self.mismatches == 0 and bool(xp.all(self.counts[:, 2] == self.counts[:, 3]))
        mathematical = True
        cp_grid = xp.arange(-1000., 1001.)
        for fit in self.fits:
            if fit['status'] != 'fitted':
                mathematical = False
                continue
            for u, d in ((-4., -3.), (0., 0.), (2., 3.)):
                p = expected(xp, cp_grid, u, d, fit['a'], fit['b'])
                inverse = expected(xp, -cp_grid, u, -d, fit['a'], fit['b'])
                mathematical &= bool(xp.all(xp.isfinite(p) & (p >= 0) & (p <= 1)))
                mathematical &= bool(xp.all(xp.diff(p) >= 0)) and float(xp.max(xp.abs(p+inverse-1))) <= 1e-10
        gates = {'numerical': 'passed' if numerical else 'failed',
            'mathematical': 'passed' if mathematical else 'not_passed',
            'overall_improvement': 'passed' if all_test.get('relative_mse_change_ci95', [0, 1])[1] < 0 else 'not_passed',
            'overall_calibration': 'passed' if all_test.get('ece', 1) <= .03 else 'not_passed',
            'held_out_players': held['status'],
            'subgroups': 'passed' if all(s['status'] == 'passed' for s in strata if s['supported']) and any(s['supported'] for s in strata) else 'not_passed',
            'coverage': 'passed' if all(s['supported'] for s in strata[:30]) else 'insufficient_data',
            'search': 'passed' if all(f['status'] == 'fitted' and not f['search_boundary'] for f in self.fits) else 'not_passed',
            'unseen_positions': audits['unseen-training-fens']['status'],
            'game_phases': 'passed' if all(audits[k]['status'] == 'passed' for k in audits if k != 'unseen-training-fens') else 'not_passed',
            'production_recovery': 'not_run', 'production_performance': 'not_run'}
        return {'gates': gates, 'cpu_cuda_passed': numerical, 'max_prediction_error': self.max_error,
            'decision_mismatches': self.mismatches, 'per_game_count_mismatches': int(xp.count_nonzero(self.counts[:, 2] != self.counts[:, 3])),
            'checked_moves': self.rows_checked, 'test': all_test, 'held_out_players': held, 'strata': strata,
            'assessed_moves': self.assessed_moves, 'unassessed_moves': self.unassessed_moves,
            'audits': audits, 'repeated_position_audit': {'test_cp_rows': self.test_fens, 'seen_training_fens': self.seen_fens},
            'blunder_comparison': totals, 'disagreement_examples': self.examples, 'timings': self.seconds,
            'calibration_diagnostics_version': 1, 'calibration_cross_groups': cross_groups,
            'player_dependence': {name: player_dependence(metadata, (g['split'] == split) & (g['cp_count'] > 0))
                                  for name, split in (('test', 2), ('held_out_players', 3))},
            'promotion_ready': False}
