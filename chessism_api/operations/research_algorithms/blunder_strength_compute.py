"""Broadcast all fixed strength candidates together; unchanged CP/mate rules."""
from collections import Counter, defaultdict
import hashlib
import time

import numpy as np

from . import blunder_fixed as old, blunder_v2_model as model
from .blunder_shadow_compute import context, host, sync
from .blunder_threshold_compare import band, scalar15

THRESHOLD = .15
CANDIDATES = (
    ('current', 'Current rating adjustment', 1., 1.),
    ('joint_50', 'Both adjustments 50%', .5, .5),
    ('joint_25', 'Both adjustments 25%', .25, .25),
    ('slope_50', 'Average-rating effect 50%', .5, 1.),
    ('slope_25', 'Average-rating effect 25%', .25, 1.),
    ('gap_50', 'Rating-gap effect 50%', 1., .5),
    ('gap_25', 'Rating-gap effect 25%', 1., .25),
    ('coefficient_only', 'Coefficient-only control', 0., 0.),
)
BATCH_ROWS = 32768


def candidates():
    return [{'id': k, 'label': label, 'average_rating_strength': a, 'rating_gap_strength': b}
            for k, label, a, b in CANDIDATES]


def expected_scalar(cp, u, d, a, b):
    import math
    return .5 + .5*math.tanh((model.K0*max(-1000., min(1000., cp))*math.exp(a*u)+b*d)/2)


def describe(metrics, valid):
    values = metrics[valid].sum(axis=0)
    n = int(np.count_nonzero(valid))
    if not n: return {'calibration_games': 0, 'mse': None, 'ece_pp': None, 'ece_5_bins_pp': None}
    weights, predictions, outcomes = values[1:11], values[11:21], values[21:31]
    return {'calibration_games': n, 'mse': float(values[0]/n),
        'ece_pp': float(np.abs(predictions-outcomes).sum()*100/n),
        'ece_5_bins_pp': float(np.abs((predictions-outcomes).reshape(5, 2).sum(axis=1)).sum()*100/n),
        'bins': [{'bin': i, 'game_weight': float(w), 'predicted': float(predictions[i]/w),
                  'observed': float(outcomes[i]/w)} for i, w in enumerate(weights) if w > 0]}


def calculate(xp, rows, games, fits, check=lambda: None):
    if rows.dtype != old.DTYPE or not len(rows) or not games or len({g['link'] for g in games}) != len(games):
        raise ValueError('Expected a nonempty unique-game sample with exact own-move transitions.')
    if rows['game'].min() < 0 or rows['game'].max() >= len(games):
        raise ValueError('Move references an unexpected game.')
    values = context(games, fits); n, c = len(games), len(CANDIDATES)
    strengths_a = np.array([v[2] for v in CANDIDATES]); strengths_b = np.array([v[3] for v in CANDIDATES])
    cp_counts = np.bincount(rows['game'][rows['after_kind'] == 0], minlength=n)
    own_counts = np.bincount(rows['game'], minlength=n)
    outcomes = np.array([g['outcome'] for g in games])
    if not np.isin(outcomes, [0., .5, 1.]).all(): raise ValueError('Invalid calibration outcome.')
    gpu_counts = xp.zeros((c, n), dtype=xp.int64)
    gpu_metrics = xp.zeros((c, n, 31), dtype=xp.float64)
    cpu_counts = np.zeros((c, n), dtype=np.int64)
    cpu_metrics = np.zeros((c, n, 31), dtype=np.float64)
    legacy_counts, unassessed = np.zeros(n, dtype=np.int64), np.zeros(n, dtype=np.int64)
    checks = dict.fromkeys(('decision_mismatches', 'per_game_mismatches', 'legacy_move_mismatches',
        'saved_counts_mismatches', 'saved_moves_mismatches', 'frozen_candidate_mismatches',
        'mate_rule_mismatches', 'assessment_mismatches'), 0)
    added, removed = np.zeros((c, n), dtype=np.int64), np.zeros((c, n), dtype=np.int64)
    examples = defaultdict(list); batches = []
    max_prediction_error = 0.; started = time.perf_counter(); gpu_seconds = cpu_seconds = 0.
    for offset in range(0, len(rows), BATCH_ROWS):
        check(); block = rows[offset:offset+BATCH_ROWS]; ids = block['game']
        cp = (block['before_kind'] == 0) & (block['after_kind'] == 0)
        valid_cp = block['after_kind'] == 0
        u, d = values['u'][ids, None], values['d'][ids, None]
        a, b = values['a'][ids, None]*strengths_a, values['b'][ids, None]*strengths_b
        before, after = block['before'][:, None], block['after'][:, None]
        bk, ak = block['before_kind'][:, None], block['after_kind'][:, None]
        sync(xp); t = time.perf_counter()
        gpu = [xp.asarray(x) for x in (before, after, bk, ak, u, d, a, b)]
        flags = model.decisions(xp, *gpu, threshold=THRESHOLD)
        predictions = model.expected(xp, xp.where(gpu[3] == 0, gpu[1], 0.), *gpu[4:])
        gi = xp.asarray(ids); keep = xp.asarray(valid_cp); selected_ids = gi[keep]
        weights = xp.asarray(1./np.maximum(cp_counts[ids[valid_cp]], 1))
        y = xp.asarray(outcomes[ids[valid_cp]])
        for k in range(c):
            gpu_counts[k] += xp.bincount(gi, weights=(flags[:, k] == 1).astype(xp.int64), minlength=n).astype(xp.int64)
            p = predictions[keep, k]
            bins = xp.minimum(9, (p*10).astype(xp.int64))
            gpu_metrics[k, :, 0] += xp.bincount(selected_ids, weights=weights*(p-y)**2, minlength=n)
            for start, v in ((1, weights), (11, weights*p), (21, weights*y)):
                gpu_metrics[k, :, start:start+10] += xp.bincount(selected_ids*10+bins, weights=v, minlength=n*10).reshape(n, 10)
        observed = host(xp, flags); predicted = host(xp, predictions); sync(xp)
        gpu_seconds += time.perf_counter()-t
        t = time.perf_counter()
        legacy = old.reference(block)
        legacy_counts += np.bincount(ids[legacy == 1], minlength=n)
        unassessed += np.bincount(ids[legacy == -1], minlength=n)
        checks['legacy_move_mismatches'] += int(np.count_nonzero((legacy == 1) != (block['sql'] == 1)))
        for k in range(c):
            oracle = np.array([scalar15(float(v[0]), float(v[1]), int(v[2]), int(v[3]), float(v[4]), float(v[5]), float(v[6]), float(v[7]))
                for v in zip(block['before'], block['after'], block['before_kind'], block['after_kind'], u[:, 0], d[:, 0], a[:, k], b[:, k])], dtype=np.int8)
            cpu_counts[k] += np.bincount(ids[oracle == 1], minlength=n)
            checks['decision_mismatches'] += int(np.count_nonzero(observed[:, k] != oracle))
            checks['mate_rule_mismatches'] += int(np.count_nonzero(~cp & (observed[:, k] != legacy)))
            checks['assessment_mismatches'] += int(np.count_nonzero((observed[:, k] == -1) != (legacy == -1)))
            prediction = np.array([expected_scalar(float(cp_), float(u_), float(d_), float(a_), float(b_))
                for cp_, u_, d_, a_, b_ in zip(block['after'][valid_cp], u[valid_cp, 0], d[valid_cp, 0], a[valid_cp, k], b[valid_cp, k])])
            if len(prediction): max_prediction_error = max(max_prediction_error, float(np.abs(prediction-predicted[valid_cp, k]).max()))
            ii = ids[valid_cp]; w = 1./np.maximum(cp_counts[ii], 1); y_cpu = outcomes[ii]
            bins_cpu = np.minimum(9, (prediction*10).astype(np.int64))
            cpu_metrics[k, :, 0] += np.bincount(ii, weights=w*(prediction-y_cpu)**2, minlength=n)
            for start, v in ((1, w), (11, w*prediction), (21, w*y_cpu)):
                cpu_metrics[k, :, start:start+10] += np.bincount(ii*10+bins_cpu, weights=v, minlength=n*10).reshape(n, 10)
            plus = (observed[:, k] == 1) & (legacy == 0)
            minus = (observed[:, k] == 0) & (legacy == 1)
            added[k] += np.bincount(ids[plus], minlength=n); removed[k] += np.bincount(ids[minus], minlength=n)
        for i in np.flatnonzero(np.any(observed != observed[:, :1], axis=1)):
            g = games[ids[i]]; key = g['mode'], band(g['player_rating'])
            rank = hashlib.sha256(f"strength-example-v1:{g['link']}:{int(block['ply'][i])}".encode()).hexdigest()
            bucket = examples[key]
            if len(bucket) >= 2 and rank >= bucket[-1][0]: continue
            loss = model.expected(np, before[i, 0], u[i, 0], d[i, 0], a[i], b[i])-model.expected(np, after[i, 0], u[i, 0], d[i, 0], a[i], b[i])
            item = {'game': g['link'], 'ply': int(block['ply'][i]), 'player': g['player'], 'mode': g['mode'],
                'rating_band': key[1], 'player_rating': g['player_rating'], 'opponent_rating': g['opponent_rating'],
                'before': float(before[i, 0]), 'after': float(after[i, 0]), 'before_kind': int(bk[i, 0]), 'after_kind': int(ak[i, 0]),
                'old15': int(legacy[i]), 'candidates': {v[0]: {'blunder': int(observed[i, k]), 'loss': float(loss[k])} for k, v in enumerate(CANDIDATES)}}
            bucket.append((rank, item)); bucket.sort(key=lambda v: v[0]); del bucket[2:]
        batches.append({'moves': len(block), 'players': sorted({games[i]['player'] for i in np.unique(ids)})})
        cpu_seconds += time.perf_counter()-t
    counts = host(xp, gpu_counts); metrics = host(xp, gpu_metrics)
    checks['per_game_mismatches'] = int(np.count_nonzero(counts != cpu_counts))
    checks['saved_counts_mismatches'] = int(np.count_nonzero(legacy_counts != [g['saved_blunders'] for g in games]))
    checks['saved_moves_mismatches'] = int(np.count_nonzero(own_counts != [g['saved_moves'] for g in games]))
    checks['frozen_candidate_mismatches'] = int(np.count_nonzero(counts[0] != [g['cohort_candidate15'] for g in games]))
    metrics_error = float(np.abs(metrics-cpu_metrics).max())
    groups = []
    selectors = [('all', '*', np.ones(n, dtype=bool))]
    selectors += [('mode', m, np.array([g['mode'] == m for g in games])) for m in model.MODES]
    selectors += [('rating_mode', f'{b}/{m}', np.array([band(g['player_rating']) == b and g['mode'] == m for g in games]))
        for b in sorted({band(g['player_rating']) for g in games}) for m in model.MODES]
    selectors += [('gap_mode', f'{gap}/{m}', np.array([('0–200' if abs(g['player_rating']-g['opponent_rating']) <= 200 else '201–400' if abs(g['player_rating']-g['opponent_rating']) <= 400 else '>400') == gap and g['mode'] == m for g in games]))
        for gap in ('0–200', '201–400', '>400') for m in model.MODES]
    for kind, label, mask in selectors:
        if not mask.any(): continue
        owners = Counter(g['player'] for g, keep in zip(games, mask) if keep)
        for k, candidate in enumerate(candidates()):
            assessed = mask & (own_counts > unassessed)
            groups.append({'kind': kind, 'group': label, 'candidate': candidate['id'], 'games': int(mask.sum()),
                'assessed_games': int(assessed.sum()), 'moves': int(own_counts[mask].sum()),
                'unassessed': int(unassessed[mask].sum()), 'players': len(owners), 'largest_owner_share': max(owners.values())/int(mask.sum()),
                'old15': int(legacy_counts[mask].sum()), 'blunders': int(counts[k, mask].sum()),
                'added_vs_old': int(added[k, mask].sum()), 'removed_vs_old': int(removed[k, mask].sum()),
                'histogram': np.bincount(np.minimum(6, counts[k, assessed]), minlength=7).tolist(),
                **describe(metrics[k], mask & (cp_counts > 0))})
    records = [{'game': g['link'], 'player': g['player'], 'mode': g['mode'], 'player_rating': g['player_rating'],
        'opponent_rating': g['opponent_rating'], 'moves': int(own_counts[i]), 'unassessed': int(unassessed[i]),
        'input_sha256': g['input_sha256'], 'old15': int(legacy_counts[i]),
        'counts': {v[0]: int(counts[k, i]) for k, v in enumerate(CANDIDATES)}} for i, g in enumerate(games)]
    return {'status': 'passed' if not any(checks.values()) and max_prediction_error < 1e-10 and metrics_error < 1e-10 else 'failed',
        'checks': checks, 'max_prediction_error': max_prediction_error, 'max_metric_error': metrics_error,
        'candidates': candidates(), 'groups': groups, 'records': records,
        'examples': [item for key in sorted(examples) for _, item in examples[key]], 'gpu_batches': batches,
        'timings': {'gpu_all_candidates_with_transfers_and_metrics_seconds': gpu_seconds,
            'cpu_references_and_example_audit_seconds': cpu_seconds, 'comparison_wall_seconds': time.perf_counter()-started}}
