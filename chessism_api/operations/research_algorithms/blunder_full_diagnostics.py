"""Development-only diagnostics on staged inputs; no SQL or live-summary writes."""
from collections import Counter, defaultdict
import hashlib
import time

import numpy as np

from . import blunder_full_compute as compute, blunder_v2_model as model, blunder_fixed as fixed
from .blunder_shadow_compute import host
from .blunder_strength_compute import describe
from .blunder_threshold_compare import BANDS, scalar15

MODELS = ('coefficient_only', 'frozen_reference', 'candidate')


def parameters(fit):
    return [[0., 0.], [fit['frozen_comparison']['a'], fit['frozen_comparison']['b']], [fit['a'], fit['b']]]


def rapid_metrics(xp, folder, games, split, params, budget, check):
    """CUDA equal-game sufficient statistics; retain per-game rows for grouping."""
    n = len(games); rows = sum(g['cp_count'] for g in games)
    if not n: return np.zeros((3, 0, 31))
    ends = np.cumsum([g['cp_count'] for g in games])
    if [g['cp_offset'] for g in games] != [0, *ends[:-1]]:
        raise ValueError('Rapid diagnostic game offsets do not match compact input.')
    data = compute.Input(xp, folder/f'{split}-rapid.bin', rows, budget, check)
    result = xp.zeros((3, n, 31), dtype=xp.float64); params = xp.asarray(params)
    offset = 0
    try:
        for block in data.blocks():
            ids = xp.asarray(np.searchsorted(ends, np.arange(offset, offset+len(block)), side='right'))
            p = model.expected(xp, block[:, :1], block[:, 1:2], block[:, 2:3], params[:, 0], params[:, 1])
            w, y = block[:, 4], block[:, 3]
            for k in range(3):
                result[k, :, 0] += xp.bincount(ids, weights=w*(p[:, k]-y)**2, minlength=n)
                bins = xp.minimum(9, (p[:, k]*10).astype(xp.int64))
                for start, values in ((1, w), (11, w*p[:, k]), (21, w*y)):
                    result[k, :, start:start+10] += xp.bincount(ids*10+bins, weights=values, minlength=n*10).reshape(n, 10)
            offset += len(block)
        return host(xp, result)
    finally:
        data.close()


def rapid_groups(games_by_split, metrics):
    participants = Counter(name for games in games_by_split for g in games for name in {g['white_name'], g['black_name']})
    top = sorted(participants, key=lambda n: (-participants[n], n))[:12]
    years = sorted({str(g['played_at'].year) for games in games_by_split for g in games})
    selectors = [('overall', 'all', lambda g: True)]
    selectors += [('average_rating', str(b), lambda g, b=b: g['rating_band'] == b) for b in range(9)]
    selectors += [('rating_gap', label, lambda g, lo=lo, hi=hi: lo <= abs(g['white_elo']-g['black_elo']) <= hi)
                  for label, lo, hi in (('0–200', 0, 200), ('201–400', 201, 400), ('>400', 401, 4000))]
    selectors += [('year', year, lambda g, year=year: str(g['played_at'].year) == year) for year in years]
    selectors += [('player_participation', name, lambda g, name=name: name in (g['white_name'], g['black_name'])) for name in top]
    result = []
    for split, games in enumerate(games_by_split):
        for kind, label, predicate in selectors:
            mask = np.array([predicate(g) for g in games], dtype=bool)
            if not mask.any(): continue
            selected = [g for g, keep in zip(games, mask) if keep]
            counts = Counter(n for g in selected for n in {g['white_name'], g['black_name']})
            result.append({'split': split, 'kind': kind, 'group': label, 'games': len(selected),
                'cp_rows': sum(g['cp_count'] for g in selected),
                'largest_player_share': max(counts.values())/len(selected),
                'white_results': {str(y): sum(g['white_result'] == y for g in selected) for y in (0., .5, 1.)},
                'from': min(g['played_at'] for g in selected).isoformat(), 'to': max(g['played_at'] for g in selected).isoformat(),
                'models': {name: describe(metrics[split][k], mask) for k, name in enumerate(MODELS)}})
    return result


def rapid_audit(xp, folder, games, fit, budget, check):
    parts = [[g for g in games if g['mode'] == 'rapid' and g['split'] == s] for s in (0, 1)]
    metrics = [rapid_metrics(xp, folder, part, s, parameters(fit), budget, check) for s, part in enumerate(parts)]
    groups = rapid_groups(parts, metrics)
    valid = next(g for g in groups if g['kind'] == 'overall' and g['split'] == 1)
    # A second path must reproduce the fit's complete validation metrics.
    error = max(abs(valid['models'][name][field]-reference[key]*scale)
        for name, reference in zip(MODELS, (fit['baseline'], fit['frozen_comparison'], fit))
        for field, key, scale in (('mse', 'mse', 1), ('ece_pp', 'ece', 100)))
    if error > 1e-9: raise ValueError('Rapid diagnostic metrics disagree with fitting validation.')
    return {'status': 'development_diagnostic', 'validation_metric_max_error': error, 'groups': groups,
        'notes': ['All CP plies, White perspective, equal total game weight. Training metrics are in-sample.',
            'Average-rating bands: <1200, 1200–1399, …, 2400–2599, 2600+. Player rows are overlapping participation groups, not additive.',
            'Top 12 participants selected by counts only. Small groups and descriptive drift are not independent causal or quality evidence.']}


def reference_flags(before, after, bk, ak, u, d, params):
    """Independent vector CPU equation; checked separately against scalar oracles."""
    a, b = np.asarray(params).T
    def expected(cp):
        z = .00368208*np.clip(cp, -1000, 1000)*np.exp(a*u)+b*d
        return 1./(1.+np.exp(-z))
    cp = (bk == 0) & (ak == 0)
    loss = expected(np.where(cp, before, 0.))-expected(np.where(cp, after, 0.))
    mate = ((bk == 0) & (ak == 1) & (after < 0) & (before >= -700)) | (
        (bk == 1) & (before > 0) & (((ak == 1) & (after < 0)) | ((ak == 0) & (after <= 700))))
    valid = np.isfinite(before) & np.isfinite(after) & np.isfinite(u) & np.isfinite(d)
    valid &= (bk >= 0) & (bk < 2) & (ak >= 0) & (ak < 2)
    flags = np.where(valid, (cp & (loss >= .15)) | mate, -1).astype(np.int8)
    # Exact production arithmetic for the fixed-coefficient comparison.
    flags[:, 0] = fixed.kernel(np, before[:, 0], after[:, 0], bk[:, 0], ak[:, 0])
    return flags


def bullet_audit(xp, folder, games, fit, check, progress):
    games = [g for g in games if g['mode'] == 'bullet' and g['split'] == 1]
    path = folder/'bullet-moves.bin'; total = sum(g['total_positions'] for g in games)
    if not total or path.stat().st_size != total*model.DTYPE.itemsize:
        raise ValueError('Missing or truncated bullet diagnostic moves.')
    rows = np.memmap(path, mode='r', dtype=model.DTYPE)
    n = len(games); params = parameters(fit); gpu_params = xp.asarray(params)
    ratings = np.array([[g['white_elo'], g['black_elo']] for g in games])
    complete = np.array([[g['white_complete'], g['black_complete']] for g in games], dtype=bool)
    own = np.zeros((n, 2), dtype=np.int64); unknown = own.copy()
    counts = xp.zeros((n*2, 3), dtype=xp.int64); cpu_counts = np.zeros((n*2, 3), dtype=np.int64)
    added = np.zeros((n*2, 2), dtype=np.int64); removed = added.copy()
    examples = defaultdict(list)
    checks = dict.fromkeys(('cpu_vector_decision_mismatches', 'cpu_scalar_decision_mismatches',
        'per_game_side_mismatches', 'mate_rule_mismatches', 'assessment_mismatches'), 0)
    scalar_checked = 0
    try:
        for offset in range(0, len(rows), 262144):
            check(); block = rows[offset:offset+262144]; ids = block['game']; sides = (block['ply']+1)%2
            if ids.min() < 0 or ids.max() >= n: raise ValueError('Unexpected bullet game index.')
            keep = complete[ids, sides]; block = block[keep]; ids = ids[keep]; sides = sides[keep]
            if not len(block): continue
            side_ids = ids*2+sides; sign = np.where(sides == 0, 1., -1.)
            before = block['before']*sign; after = block['after']*sign
            own_rating = ratings[ids, sides]; other_rating = ratings[ids, 1-sides]
            u = ((own_rating+other_rating)/2-2300)/400; d = (own_rating-other_rating)/400
            args = [a[:, None] for a in (before, after, block['bk'], block['ak'], u, d)]
            gpu = [xp.asarray(a) for a in args]
            flags = model.decisions(xp, *gpu, gpu_params[:, 0], gpu_params[:, 1], threshold=.15)
            flags[:, 0] = fixed.kernel(xp, *[a[:, 0] for a in gpu[:4]])
            gi = xp.asarray(side_ids)
            for k in range(3):
                counts[:, k] += xp.bincount(gi, weights=(flags[:, k] == 1).astype(xp.int64), minlength=n*2).astype(xp.int64)
            observed = host(xp, flags); reference = reference_flags(*args, params)
            checks['cpu_vector_decision_mismatches'] += int(np.count_nonzero(observed != reference))
            # Independent scalar check across every batch, both colors and all models.
            for i in np.linspace(0, len(block)-1, min(256, len(block)), dtype=int):
                for k, (a, b) in enumerate(params):
                    expected = scalar15(before[i], after[i], int(block['bk'][i]), int(block['ak'][i]), u[i], d[i], a, b)
                    if k == 0:
                        legacy = np.zeros(1, dtype=fixed.DTYPE)
                        for field, value in zip(('before', 'after', 'before_kind', 'after_kind'), (before[i], after[i], block['bk'][i], block['ak'][i])): legacy[field] = value
                        expected = fixed.reference(legacy)[0]
                    checks['cpu_scalar_decision_mismatches'] += int(observed[i, k] != expected)
                scalar_checked += 1
            cp = (block['bk'] == 0) & (block['ak'] == 0)
            checks['mate_rule_mismatches'] += int(np.count_nonzero(observed[~cp] != observed[~cp, :1]))
            checks['assessment_mismatches'] += int(np.count_nonzero((observed < 0) != (observed[:, :1] < 0)))
            own += np.bincount(side_ids, minlength=n*2).reshape(n, 2)
            unknown += np.bincount(side_ids[observed[:, 0] < 0], minlength=n*2).reshape(n, 2)
            for k in range(3): cpu_counts[:, k] += np.bincount(side_ids[reference[:, k] == 1], minlength=n*2)
            for k in (0, 1):
                plus = (observed[:, 2] == 1) & (observed[:, k] == 0)
                minus = (observed[:, 2] == 0) & (observed[:, k] == 1)
                added[:, k] += np.bincount(side_ids[plus], minlength=n*2)
                removed[:, k] += np.bincount(side_ids[minus], minlength=n*2)
            bands = np.searchsorted([1000, 1500, 2000, 2500, 3000], own_rating, side='right')
            for i in np.flatnonzero((observed[:, 2] != observed[:, 0]) | (observed[:, 2] != observed[:, 1])):
                g = games[ids[i]]; direction = 'added' if observed[i, 2] == 1 else 'removed'
                key = int(bands[i]), direction
                rank = hashlib.sha256(f"full-dev-v2:{g['link']}:{int(block['ply'][i])}".encode()).hexdigest()
                bucket = examples[key]
                if len(bucket) >= 2 and rank >= bucket[-1][0]: continue
                losses = model.expected(np, before[i], u[i], d[i], np.array(params)[:, 0], np.array(params)[:, 1])-model.expected(np, after[i], u[i], d[i], np.array(params)[:, 0], np.array(params)[:, 1]) if cp[i] else [None]*3
                item = {'game': g['link'], 'ply': int(block['ply'][i]), 'player': g['white_name'] if sides[i] == 0 else g['black_name'],
                    'rating': int(own_rating[i]), 'opponent_rating': int(other_rating[i]), 'rating_band': BANDS[bands[i]], 'change': direction,
                    'before': float(before[i]), 'after': float(after[i]), 'before_kind': int(block['bk'][i]), 'after_kind': int(block['ak'][i]),
                    'models': {name: {'blunder': int(observed[i, k]), 'loss': None if losses[k] is None else float(losses[k])} for k, name in enumerate(MODELS)}}
                bucket.append((rank, item)); bucket.sort(key=lambda x: x[0]); del bucket[2:]
            progress(min(offset+262144, total), total, 'Bullet validation: mixed-player CUDA move comparisons; final tests untouched.')
        counts = host(xp, counts)
        checks['per_game_side_mismatches'] = int(np.count_nonzero(counts != cpu_counts))
        bands = np.searchsorted([1000, 1500, 2000, 2500, 3000], ratings, side='right').ravel()
        groups = []
        for band in range(-1, len(BANDS)):
            mask = complete.ravel() & (own.ravel() > 0) & (np.ones(n*2, dtype=bool) if band == -1 else bands == band)
            if not mask.any(): continue
            groups.append({'rating_band': 'all' if band == -1 else BANDS[band], 'game_sides': int(mask.sum()),
                'moves': int(own.ravel()[mask].sum()), 'unassessed': int(unknown.ravel()[mask].sum()),
                'blunders': {name: int(counts[mask, k].sum()) for k, name in enumerate(MODELS)},
                'changes': {name: {'added': int(added[mask, k].sum()), 'removed': int(removed[mask, k].sum())} for k, name in enumerate(MODELS[:2])}})
        return {'status': 'passed' if not any(checks.values()) else 'failed', 'checks': checks, 'groups': groups,
            'scalar_checked_moves': scalar_checked, 'examples': [v for key in sorted(examples) for _, v in examples[key]],
            'notes': ['Development validation only, completed profiles only, mover perspective. Games with two complete players contribute two game-sides.',
                'Changed counts are diagnostic, not quality approval. Coefficient-only uses current production arithmetic, not a claim of exact contemporary Lichess behavior.',
                'Same staged positions for all models; no live SQL/saved-summary comparison in this diagnostic. Examples are fixed-hash selections, two per rating band and change direction.']}
    finally:
        rows._mmap.close()


def run(xp, folder, games, fits, budget, check, progress):
    started = time.monotonic(); result = {'version': 1, 'test_evaluated': False}
    by_mode = {f['mode']: f for f in fits}
    for mode, function in (('rapid', rapid_audit), ('bullet', bullet_audit)):
        check(); fit = by_mode[mode]
        if fit.get('status') != 'fitted':
            result[mode] = {'status': 'insufficient_data'}; continue
        progress(0, 1, f'{mode}: development-only diagnostics on staged inputs.')
        result[mode] = function(xp, folder, games, fit, budget, check) if mode == 'rapid' else function(xp, folder, games, fit, check, progress)
    result['seconds'] = time.monotonic()-started
    return result
