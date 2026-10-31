"""Frozen-model GPU pilot; legacy and independent CPU/SQL references stay separate."""
from collections import defaultdict
import time

import numpy as np

from . import blunder_fixed as old, blunder_v2_model as model


def host(xp, value):
    return xp.asnumpy(value) if hasattr(xp, 'asnumpy') else np.asarray(value)


def sync(xp):
    if hasattr(xp, 'cuda'):
        xp.cuda.get_current_stream().synchronize()


def context(games, fits):
    modes = np.array([model.MODES.index(g['mode']) for g in games])
    own = np.array([g['player_rating'] for g in games], dtype=np.float64)
    other = np.array([g['opponent_rating'] for g in games], dtype=np.float64)
    return {'u': ((own+other)/2-2300)/400, 'd': (own-other)/400,
        'a': np.array([f['a'] for f in fits])[modes], 'b': np.array([f['b'] for f in fits])[modes]}


def arrays(xp, batch, values):
    ids = batch['game']
    return [xp.asarray(batch[k]) for k in ('before', 'after', 'before_kind', 'after_kind')] + [
        xp.asarray(values[k][ids]) for k in ('u', 'd', 'a', 'b')]


def counts(xp, ids, flags, size):
    selected = ids[flags]
    return xp.bincount(selected, minlength=size) if selected.size else xp.zeros(size, dtype=xp.int64)


def calculate(xp, rows, games, fits, check=lambda: None):
    values = context(games, fits)
    size = len(games)
    totals = {k: np.zeros(size, dtype=np.int64) for k in ('old15', 'baseline20', 'candidate20', 'moves', 'unassessed')}
    checks = dict.fromkeys(('old_cpu_move_mismatches', 'old_sql_move_mismatches', 'new_cpu_move_mismatches',
        'baseline20_move_mismatches', 'saved_game_mismatches', 'saved_move_mismatches', 'new_per_game_mismatches'), 0)
    oracle_counts = np.zeros(size, dtype=np.int64)
    timings = dict.fromkeys(('old_scalar_reference_seconds', 'new_scalar_reference_seconds',
        'gpu_three_rules_and_comparison_seconds'), 0.)
    examples, example_counts, changes = [], defaultdict(int), defaultdict(int)
    max_error = 0.
    for batch in model.chunks(rows):
        check()
        ids = batch['game']
        if batch.dtype != old.DTYPE or not len(batch) or ids.min() < 0 or ids.max() >= size:
            raise ValueError('Invalid shadow transition batch.')
        started = time.perf_counter(); old_cpu = old.reference(batch)
        timings['old_scalar_reference_seconds'] += time.perf_counter()-started
        args = [batch[k] for k in ('before', 'after', 'before_kind', 'after_kind')] + [values[k][ids] for k in ('u', 'd', 'a', 'b')]
        started = time.perf_counter()
        new_cpu = np.array([model.scalar(*items) for items in zip(*args)], dtype=np.int8)
        baseline_cpu = model.decisions(np, *args[:4], 0., 0., 0., 0.)
        oracle_counts += np.bincount(ids[new_cpu == 1], minlength=size)
        timings['new_scalar_reference_seconds'] += time.perf_counter()-started
        started = time.perf_counter()
        gpu = arrays(xp, batch, values); gpu_ids = xp.asarray(ids)
        flags = [old.kernel(xp, *gpu[:4]), model.decisions(xp, *gpu[:4], 0., 0., 0., 0.), model.decisions(xp, *gpu)]
        # Prediction agreement is distinct from exact decision/count agreement.
        for index in (0, 1):
            observed = model.expected(xp, xp.where(gpu[index+2] == 0, gpu[index], 0.), *gpu[4:])
            expected = model.expected(np, np.where(args[index+2] == 0, args[index], 0.), *args[4:])
            max_error = max(max_error, float(xp.max(xp.abs(observed-xp.asarray(expected)))))
        for name, flag in zip(('old15', 'baseline20', 'candidate20'), flags):
            totals[name] += host(xp, counts(xp, gpu_ids, flag == 1, size))
        totals['moves'] += np.bincount(ids, minlength=size)
        totals['unassessed'] += host(xp, counts(xp, gpu_ids, flags[2] == -1, size))
        legacy, baseline, candidate = [host(xp, x) for x in flags]
        sync(xp)
        timings['gpu_three_rules_and_comparison_seconds'] += time.perf_counter()-started
        checks['old_cpu_move_mismatches'] += int(np.count_nonzero(legacy != old_cpu))
        checks['old_sql_move_mismatches'] += int(np.count_nonzero((legacy == 1) != (batch['sql'] == 1)))
        checks['new_cpu_move_mismatches'] += int(np.count_nonzero(candidate != new_cpu))
        checks['baseline20_move_mismatches'] += int(np.count_nonzero(baseline != baseline_cpu))
        cp = (batch['before_kind'] == 0) & (batch['after_kind'] == 0)
        loss = model.expected(np, np.where(cp, args[0], 0.), *args[4:])-model.expected(np, np.where(cp, args[1], 0.), *args[4:])
        categories = {'threshold_removed': (legacy == 1) & (baseline == 0),
            'threshold_added': (legacy == 0) & (baseline == 1),
            'rating_added': (baseline == 0) & (candidate == 1),
            'rating_removed': (baseline == 1) & (candidate == 0),
            'old_to_new_added': (legacy == 0) & (candidate == 1),
            'old_to_new_removed': (legacy == 1) & (candidate == 0),
            'forced_mate': ~cp & (candidate == 1),
            'near_threshold': cp & (np.abs(loss-.2) <= .002)}
        for category, mask in categories.items():
            for mode in model.MODES:
                chosen = np.flatnonzero(mask & np.array([games[i]['mode'] == mode for i in ids]))
                changes[category, mode] += len(chosen)
                for i in chosen[:max(0, 3-example_counts[category, mode])]:
                    g = games[ids[i]]; example_counts[category, mode] += 1
                    examples.append({'category': category, 'mode': mode, 'player': g['player'],
                        'game': g['link'], 'ply': int(batch['ply'][i]), 'player_rating': g['player_rating'],
                        'opponent_rating': g['opponent_rating'], 'before': float(args[0][i]), 'after': float(args[1][i]),
                        'before_kind': int(args[2][i]), 'after_kind': int(args[3][i]),
                        'candidate_loss': float(loss[i]) if cp[i] else None,
                        'old15': int(legacy[i]), 'baseline20': int(baseline[i]), 'candidate20': int(candidate[i])})
    checks['saved_game_mismatches'] = int(np.count_nonzero(totals['old15'] != [g['saved_blunders'] for g in games]))
    checks['saved_move_mismatches'] = int(np.count_nonzero(totals['moves'] != [g['saved_moves'] for g in games]))
    checks['new_per_game_mismatches'] = int(np.count_nonzero(totals['candidate20'] != oracle_counts))
    records = [{'game': g['link'], 'player': g['player'], 'is_white': g['is_white'], 'mode': g['mode'],
        'played_at': g['played_at'], 'player_rating': g['player_rating'], 'opponent_rating': g['opponent_rating'],
        'input_sha256': g['input_sha256'], **{k: int(v[i]) for k, v in totals.items()}} for i, g in enumerate(games)]
    return {'status': 'passed' if not any(checks.values()) and max_error <= 1e-10 else 'failed',
        'checks': checks, 'checked_moves': len(rows), 'max_prediction_error': max_error,
        'records': records, 'examples': examples, 'changes': [dict(category=c, mode=m, moves=n) for (c, m), n in sorted(changes.items())],
        'timings': timings}


def vector_summary(xp, rows, games, fits, check=lambda: None):
    """Candidate-only calculation, including transfers and per-game reduction."""
    values = context(games, fits)
    output = xp.zeros(len(games), dtype=xp.int64)
    for batch in model.chunks(rows):
        check()
        flags = model.decisions(xp, *arrays(xp, batch, values))
        output += counts(xp, xp.asarray(batch['game']), flags == 1, len(games))
    result = host(xp, output); sync(xp)
    return result


def benchmark(xp, rows, games, fits, check=lambda: None):
    expected = vector_summary(np, rows, games, fits, check)
    np.testing.assert_array_equal(vector_summary(xp, rows, games, fits, check), expected)
    timings = {'cpu_vectorized_seconds': [], 'gpu_with_transfers_seconds': []}
    for repeat in range(3):
        for name, backend in (('cpu_vectorized_seconds', np), ('gpu_with_transfers_seconds', xp))[::(-1 if repeat % 2 else 1)]:
            check(); sync(backend); start = time.perf_counter()
            observed = vector_summary(backend, rows, games, fits, check)
            timings[name].append(time.perf_counter()-start)
            np.testing.assert_array_equal(observed, expected)
    return {'status': 'passed', 'repeats': 3, 'warmup_excluded': True, 'refitted': False,
        'scope': 'same_frozen_rating_aware_formula_and_per_game_counts_including_transfers',
        **timings, 'median_cpu_seconds': float(np.median(timings['cpu_vectorized_seconds'])),
        'median_gpu_seconds': float(np.median(timings['gpu_with_transfers_seconds']))}
