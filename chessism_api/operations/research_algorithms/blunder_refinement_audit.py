"""Development diagnostics and conservative repeated-player uncertainty.

Games connected through EITHER player are resampled together. If the player
network forms too few independent components, report unavailable uncertainty
instead of presenting a whole-game interval as player-independent evidence.
"""
import numpy as np

from .blunder_v2_calibration import describe, statistics
from .blunder_v2_model import MODES


def describe_group(xp, matrix, mask):
    rows = matrix[xp.asarray(mask)]
    rows = rows[rows[:, 0] > 0]
    if not len(rows):
        return {'games': 0, 'status': 'insufficient_data'}
    normalized = rows[:, 1:]/rows[:, :1]
    ece, bins = describe(xp, statistics(xp, normalized, 2))
    baseline, base_bins = describe(xp, statistics(xp, normalized, 32))
    return {'games': len(rows), 'mse': float(xp.mean(normalized[:, 0])),
        'baseline_mse': float(xp.mean(normalized[:, 1])), 'ece': ece,
        'baseline_ece': baseline, 'calibration': bins, 'baseline_calibration': base_bins,
        'status': 'development_diagnostic'}


def development(acc):
    xp, g, meta = acc.xp, acc.games, acc.metadata
    val = g['split'] == 1
    ratings = np.array([(m['white_elo']+m['black_elo'])/2 for m in meta])
    bands = np.clip(np.floor((ratings-1000)/200), 0, 8).astype(int)
    gap = np.abs(g['d']*400)
    masks = []
    for i, mode in enumerate(MODES):
        masks.append((mode, g['mode'] == i))
        masks.extend((f'{mode}:rating-band-{band}', (g['mode'] == i) & (bands == band)) for band in range(9))
        masks.extend((f'{mode}:{name}', (g['mode'] == i) & mask) for name, mask in (
            ('gap-0-200', gap <= 200), ('gap-201-400', (gap > 200) & (gap <= 400)), ('gap-over-400', gap > 400)))
    groups = [{'name': name, **describe_group(xp, acc.metrics[0], val & mask)} for name, mask in masks]
    phases = {name: describe_group(xp, acc.metrics[i], val) for i, name in enumerate(
        ('all', 'opening-plies-1-20', 'middle-plies-21-60', 'late-plies-over-60')) if i}
    return {'partition': 'validation_only', 'overall': describe_group(xp, acc.metrics[0], val),
        'groups': groups, 'phases': phases, 'acceptance_evidence': False}


def components(metadata):
    parents = {}
    def root(name):
        parents.setdefault(name, name)
        while parents[name] != name:
            parents[name] = parents[parents[name]]
            name = parents[name]
        return name
    pairs = []
    for game in metadata:
        names = [str(game.get(key) or '').strip().lower() for key in ('white_name', 'black_name')]
        if not all(names):
            return None
        a, b = map(root, names)
        parents[max(a, b)] = min(a, b)
        pairs.append(names)
    keys = [root(a) for a, _ in pairs]
    _, indices = np.unique(keys, return_inverse=True)
    return indices


def player_intervals(xp, matrix, metadata, mask, check=lambda: None, repeats=1000):
    indices = np.flatnonzero(mask)
    nonempty = matrix[xp.asarray(indices), 0] > 0
    indices = indices[xp.asnumpy(nonempty) if hasattr(xp, 'asnumpy') else nonempty]
    group = components([metadata[i] for i in indices])
    report = {'method': 'paired_player_connected_component_bootstrap', 'repeats': repeats,
        'fixed_fitted_parameters': True, 'games': len(indices),
        'limitations': 'Shared players in either color stay together. Unrelated temporal/event dependence can remain; intervals are not population guarantees.'}
    if group is None or not len(group):
        return {**report, 'status': 'unavailable', 'reason': 'Missing identities or assessed games.'}
    sizes = np.bincount(group)
    report.update(components=len(sizes), largest_component_share=float(sizes.max()/len(group)))
    if len(sizes) < 20 or sizes.max()/len(group) > .2:
        return {**report, 'status': 'insufficient_independent_components',
            'reason': 'Need at least 20 player-disjoint components, with none exceeding 20% of games.'}
    rows = matrix[xp.asarray(indices)]
    normal = rows[:, 1:]/rows[:, :1]
    candidate, baseline = (statistics(xp, normal, offset) for offset in (2, 32))
    values = xp.column_stack((candidate[:, 1]-candidate[:, 2], baseline[:, 1]-baseline[:, 2], normal[:, :2]))
    totals = xp.zeros((len(sizes), 22), dtype=xp.float64)
    xp.add.at(totals, xp.asarray(group), values)
    weights = xp.asarray(sizes)
    rng = np.random.default_rng(20261030)
    samples = []
    for start in range(0, repeats, 8):
        check()
        ids = xp.asarray(rng.integers(0, len(sizes), size=(min(8, repeats-start), len(sizes))))
        means = xp.sum(totals[ids], axis=1)/xp.sum(weights[ids], axis=1)[:, None]
        ece = xp.sum(xp.abs(means[:, :10]), axis=1)
        base = xp.sum(xp.abs(means[:, 10:20]), axis=1)
        relative = (means[:, 20]-means[:, 21])/xp.maximum(means[:, 21], 1e-15)
        samples.append(xp.column_stack((ece, base, ece-base, relative)))
    bounds = xp.percentile(xp.concatenate(samples), xp.asarray([2.5, 97.5]), axis=0)
    return {**report, 'status': 'estimated', **{key: [float(bounds[0, i]), float(bounds[1, i])]
        for i, key in enumerate(('ece_ci95', 'baseline_ece_ci95', 'ece_change_ci95', 'relative_mse_change_ci95'))}}
