"""Rating-aware expected-score model v1; candidate only, never canonical.

This is our fitted model, not Chess.com's proprietary model. It learns a
positive CP slope for each (average game rating band, mode). Labels are final
points (0, .5, 1), not inference features. A draw is not half a probability of
winning: the prediction is expected points. Forced-mate rules remain separate.
"""
import hashlib
import math
import time

import numpy as np

from ..coefficient_research import RATING_BINS

VERSION = 1
MODES = ('bullet', 'blitz', 'rapid')
SPLITS = ('training', 'validation', 'test')
GROUPS = [(rating, mode) for rating in RATING_BINS for mode in MODES]
BASELINE = .00368208
CP = np.arange(-1000, 1001, dtype=np.float64)
GRID = np.geomspace(.0001, .01, 129)
MIN_GAMES = (500, 50, 50)
TRANSITION = np.dtype([('group', 'u1'), ('split', 'u1'), ('game', '<i8'),
    ('before', '<f8'), ('after', '<f8'), ('before_kind', 'i1'), ('after_kind', 'i1')])


def split_game(game, seed=20261026):
    # Stable across processes/platforms; both colors and every ply stay together.
    number = int.from_bytes(hashlib.sha256(f'{seed}:{game}'.encode()).digest()[:8], 'big') % 100
    return 0 if number < 70 else 1 if number < 85 else 2


def group_id(mode, white_rating, black_rating):
    if mode not in MODES or not all(math.isfinite(float(x)) and x > 0 for x in (white_rating, black_rating)):
        raise ValueError('Valid mode and both game-time ratings are required.')
    if abs(white_rating - black_rating) > 200:
        raise ValueError('This candidate is restricted to rating gaps of at most 200.')
    average = (white_rating + black_rating) / 2
    index = min(8, max(0, int((average - 1000) // 200)))
    return index * 3 + MODES.index(mode)


def expected_points(xp, score, coefficient):
    return 1 / (1 + xp.exp(-coefficient * xp.clip(score, -1000, 1000)))


class Dataset:
    """Sufficient statistics: exact weighted squared loss after integer-CP rounding.

    Each qualifying game contributes total weight one. Whole-game identities
    prevent duplicate training from profiles that share a game. No row-level
    outcome labels, identities or timestamps are supplied to the predictor.
    """
    def __init__(self):
        self.hist = np.zeros((27, 3, 2001, 3), dtype=np.float64)
        self.games = np.zeros((27, 3), dtype=np.int64)
        self.positions = np.zeros((27, 3), dtype=np.int64)
        self.seen = set()
        self.empty_games = 0

    def add_game(self, game, group, outcome, positions):
        if game in self.seen:
            raise ValueError('A game may enter the fitting dataset only once.')
        if outcome not in (0., .5, 1.) or not 0 <= group < 27:
            raise ValueError('Invalid training label or group.')
        self.seen.add(game)
        split = split_game(game)
        # positions: (ply, White-perspective score, kind). kind 0 is confirmed
        # Stockfish CP, 1 mate, 2 tablebase, -1 missing/unsupported source.
        scores = [score for ply, score, kind in positions if ply > 20 and kind == 0]
        if not scores:
            self.empty_games += 1
            return split
        values = np.asarray(scores, dtype=np.float64)
        if not np.isfinite(values).all() or np.any(np.abs(values) >= 9000):
            raise ValueError('Invalid CP training observation.')
        # Explicit nearest integer, ties away from zero, matching PostgreSQL ROUND.
        indices = (np.sign(values) * np.floor(np.abs(values) + .5)).clip(-1000, 1000).astype(np.int64) + 1000
        weights = np.bincount(indices, minlength=2001) / len(values)
        self.hist[group, split, :, 0] += weights
        self.hist[group, split, :, 1] += weights * outcome
        self.hist[group, split, :, 2] += weights * outcome ** 2
        self.games[group, split] += 1
        self.positions[group, split] += len(values)
        return split


def loss(xp, histogram, coefficients):
    coefficients = xp.atleast_1d(coefficients)
    prediction = expected_points(xp, xp.asarray(CP)[None, :], coefficients[:, None])
    w, y, y2 = histogram.T
    return xp.sum(w * prediction ** 2 - 2 * y * prediction + y2, axis=1) / xp.sum(w)


def fit(xp, histogram):
    # Grid first: do not assume the entire bounded objective is unimodal.
    grid = xp.asarray(GRID)
    losses = loss(xp, histogram, grid)
    index = int(xp.argmin(losses))
    left, right = GRID[max(0, index - 1)], GRID[min(len(GRID) - 1, index + 1)]
    ratio = (math.sqrt(5) - 1) / 2
    for _ in range(48):
        a, b = right - ratio * (right - left), left + ratio * (right - left)
        errors = loss(xp, histogram, xp.asarray([a, b]))
        if float(errors[0]) < float(errors[1]):
            right = b
        else:
            left = a
    # Include endpoint/grid optimum explicitly, including bounded fits.
    candidates = xp.asarray([GRID[index], (left + right) / 2])
    return float(candidates[int(xp.argmin(loss(xp, histogram, candidates)))])


def fit_group(dataset, group, *, device=0):
    from .cuda_runtime import cuda_backend
    rating, mode = GROUPS[group]
    result = {'group': group, 'rating_band': rating['label'], 'mode': mode,
        'samples': {name: {'games': int(dataset.games[group, i]), 'positions': int(dataset.positions[group, i])}
                    for i, name in enumerate(SPLITS)}}
    if any(dataset.games[group, i] < minimum for i, minimum in enumerate(MIN_GAMES)):
        return {**result, 'status': 'insufficient_games'}
    histogram = dataset.hist[group]
    if np.sum(histogram[0, CP != 0, 0]) == 0:
        return {**result, 'status': 'uninformative_scores'}
    started = time.perf_counter()
    reference = fit(np, histogram[0])
    cpu_seconds = time.perf_counter() - started
    cp, hardware = cuda_backend(device)
    started = time.perf_counter()
    gpu_histogram = cp.asarray(histogram)
    coefficient = fit(cp, gpu_histogram[0])
    cp.cuda.get_current_stream().synchronize()
    gpu_seconds = time.perf_counter() - started
    error = float(np.max(np.abs(expected_points(np, CP, coefficient) - expected_points(np, CP, reference))))
    metrics = {}
    for i, name in enumerate(SPLITS):
        candidate, baseline = loss(np, histogram[i], np.array([coefficient, BASELINE])).tolist()
        metrics[name] = {'expected_points_mse': candidate, 'baseline_mse': baseline,
                        'improvement_percent': 100 * (baseline - candidate) / baseline if baseline else None}
        if name != 'training':
            prediction = expected_points(np, CP, coefficient)
            buckets = np.minimum(9, (prediction * 10).astype(int))
            calibration = []
            for bucket in range(10):
                selected = buckets == bucket
                weight = float(histogram[i, selected, 0].sum())
                if weight:
                    calibration.append({'bin': bucket, 'game_weight': weight,
                        'predicted_points': float((prediction[selected] * histogram[i, selected, 0]).sum() / weight),
                        'observed_points': float(histogram[i, selected, 1].sum() / weight)})
            metrics[name]['calibration'] = calibration
    return {**result, 'status': 'fitted' if error <= 1e-7 else 'numerical_mismatch',
        'coefficient': coefficient, 'cpu_coefficient': reference, 'max_prediction_error': error,
        'at_search_boundary': bool(coefficient <= GRID[0] * 1.001 or coefficient >= GRID[-1] * .999),
        'validation_improved': metrics['validation']['expected_points_mse'] < metrics['validation']['baseline_mse'],
        'metrics': metrics, 'hardware': hardware,
        'timings': {'cpu_fit_seconds': cpu_seconds, 'cuda_fit_with_transfer_seconds': gpu_seconds}}


def decisions(xp, before, after, before_kind, after_kind, coefficient):
    """-1 = unsupported, 0 = not a blunder, 1 = blunder. Ratings select coefficient.

    No labels, game outcomes or future positions enter this inference function.
    CP transition threshold stays 15 points; forced-mate rules stay unchanged.
    """
    valid = xp.isfinite(before) & xp.isfinite(after) & (before_kind >= 0) & (after_kind >= 0)
    cp_pair = (before_kind == 0) & (after_kind == 0)
    # Keep the float64 threshold literal; report, never hide, boundary mismatches.
    cp_blunder = expected_points(xp, before, coefficient) - expected_points(xp, after, coefficient) >= .15
    mate_blunder = ((before_kind == 0) & (after_kind == 1) & (after < 0) & (before >= -700)) | (
        (before_kind == 1) & (before > 0) & (((after_kind == 1) & (after < 0)) | ((after_kind == 0) & (after <= 700))))
    valid &= (before_kind != 2) & (after_kind != 2)
    return xp.where(valid, xp.where(cp_pair, cp_blunder, mate_blunder), -1).astype(xp.int8)


def compare_transitions(batch, fits, *, device=0):
    """Held-out test games only: exact backend agreement and old/new count changes.

    Count changes are descriptive, not proof that blunder labels are correct.
    """
    from .cuda_runtime import cuda_backend
    from .legacy_blunder_reference import is_lichess_blunder
    cp, _ = cuda_backend(device)
    result = []
    for model in fits:
        group = model['group']
        selected = batch[(batch['group'] == group) & (batch['split'] == 2)]
        if not len(selected):
            continue
        record = {'group': group, 'test_moves': len(selected)}
        if model['status'] != 'fitted':
            result.append({**record, 'unsupported_moves': len(selected)})
            continue
        arrays = [selected[key] for key in ('before', 'after', 'before_kind', 'after_kind')]
        expected = decisions(np, *arrays, model['coefficient'])
        actual = cp.asnumpy(decisions(cp, *(cp.asarray(a) for a in arrays), model['coefficient']))
        eligible = expected >= 0
        kinds = {0: 'cp', 1: 'mate', 2: 'tablebase'}
        old = np.array([is_lichess_blunder(float(b), float(a), kinds.get(int(bk), 'unscored'), kinds.get(int(ak), 'unscored'))
                       for b, a, bk, ak in zip(*arrays)], dtype=bool)
        record.update({'assessed_moves': int(eligible.sum()), 'unsupported_moves': int((~eligible).sum()),
            'cpu_cuda_mismatches': int((expected != actual).sum()),
            'old_blunders': int((old & eligible).sum()), 'new_blunders': int((actual == 1).sum()),
            'new_only': int(((actual == 1) & ~old).sum()), 'old_only': int(((actual == 0) & old).sum())})
        result.append(record)
    return result
