"""Smooth rating-aware candidate; GPU fitting, never a production writer.

E = sigmoid(k0 * clip(cp) * exp(a_mode * u) + b_mode * d).
The baseline is fixed, the effective sensitivity is not. No clock/complexity model.
"""
import math

import numpy as np

K0 = .00368208
THRESHOLD = .20
MODES = ('bullet', 'blitz', 'rapid')
BATCH = 32768
DTYPE = np.dtype([('game', '<i4'), ('ply', '<i4'), ('fen', '<i8'),
                  ('before', '<f8'), ('after', '<f8'), ('bk', 'i1'), ('ak', 'i1')])
LAMBDAS = (0., .0001, .001, .01)
BOUNDS = ((-1., 1.), (0., 4.))
MIN_GAMES = (500, 100, 100)


def expected(xp, score, u, d, a, b):
    z = K0 * xp.clip(score, -1000., 1000.) * xp.exp(a * u) + b * d
    # Stable sigmoid, including very large log odds. No finite CP is a mate.
    return .5 + .5 * xp.tanh(z / 2)


def decisions(xp, before, after, bk, ak, u, d, a, b, *, threshold=THRESHOLD):
    valid = xp.isfinite(before) & xp.isfinite(after) & xp.isfinite(u) & xp.isfinite(d)
    valid &= (bk >= 0) & (bk < 2) & (ak >= 0) & (ak < 2)
    cp_pair = (bk == 0) & (ak == 0)
    # Mate/tablebase encodings never become numerical CP observations.
    loss = expected(xp, xp.where(cp_pair, before, 0.), u, d, a, b) - expected(xp, xp.where(cp_pair, after, 0.), u, d, a, b)
    cp = cp_pair & (loss >= threshold)
    mate = ((bk == 0) & (ak == 1) & (after < 0) & (before >= -700)) | (
        (bk == 1) & (before > 0) & (((ak == 1) & (after < 0)) | ((ak == 0) & (after <= 700))))
    return xp.where(valid, cp | mate, -1).astype(xp.int8)


def scalar(before, after, bk, ak, u, d, a, b):
    """Independent scalar oracle; game outcomes never enter inference."""
    if not all(math.isfinite(x) for x in (before, after, u, d)) or bk not in (0, 1) or ak not in (0, 1):
        return -1
    if bk == ak == 0:
        def point(c):
            z = K0 * max(-1000., min(1000., c)) * math.exp(a * u) + b * d
            return .5 + .5 * math.tanh(z / 2)
        return int(point(before) - point(after) >= THRESHOLD)
    return int((bk == 0 and ak == 1 and after < 0 and before >= -700) or
               (bk == 1 and before > 0 and ((ak == 1 and after < 0) or (ak == 0 and after <= 700))))


def chunks(rows):
    for start in range(0, len(rows), BATCH):
        yield rows[start:start+BATCH]


class Objective:
    """Bounded GPU losses; CPU controls the two-dimensional search only."""
    def __init__(self, xp, rows, games, mode, split, check=lambda: None):
        self.xp, self.rows, self.games, self.mode, self.split = xp, rows, games, mode, split
        self.check = check

    def __call__(self, parameters):
        xp, g = self.xp, self.games
        params = xp.asarray(parameters, dtype=xp.float64)
        total = xp.zeros(len(params), dtype=xp.float64)
        weight = 0.
        for block in chunks(self.rows):
            self.check()
            ids = block['game']
            keep = (g['mode'][ids] == self.mode) & (g['split'][ids] == self.split) & (block['ak'] == 0)
            ids, scores = ids[keep], block['after'][keep]
            if not len(ids):
                continue
            weights = xp.asarray(1. / g['cp_count'][ids])
            p = expected(xp, xp.asarray(scores)[:, None], xp.asarray(g['u'][ids])[:, None],
                         xp.asarray(g['d'][ids])[:, None], params[:, 0], params[:, 1])
            total += xp.sum(weights[:, None] * (p - xp.asarray(g['outcome'][ids])[:, None])**2, axis=0)
            weight += float(xp.sum(weights))
        if not weight:
            raise ValueError('No scored CP observations in the requested fitting partition.')
        return np.array([float(x) / weight for x in total])


def fit(objective, regularization):
    """Deterministic multistart bounded pattern search. Boundary fits are flagged."""
    initial = np.array([(a, b) for a in (-1., -.5, 0., .5, 1.) for b in (0., 1., 2., 4.)])
    def losses(points):
        return objective(points) + regularization * np.square(points).sum(axis=1)
    bests = initial[np.argsort(losses(initial))[:3]]
    results = []
    directions = np.array([(a, b) for a in (-1, 0, 1) for b in (-1, 0, 1)])
    for best in bests:
        step = np.array([.25, .5])
        converged = False
        for iteration in range(100):
            points = np.unique(np.clip(best + directions * step, [-1., 0.], [1., 4.]), axis=0)
            errors = losses(points)
            winner = points[int(np.argmin(errors))]
            if np.array_equal(winner, best):
                step *= .5
            best = winner
            if step.max() < 1e-6:
                converged = True
                break
        results.append((float(losses(best[None])[0]), best.tolist(), converged))
    _, parameters, converged = min(results, key=lambda row: row[0])
    a, b = parameters
    return {'a': a, 'b': b, 'converged': converged,
            'search_boundary': abs(a) > .99999 or b > 3.99999, 'regularization': regularization}


def fit_mode(xp, rows, games, mode, check=lambda: None):
    counts = [int(np.count_nonzero((games['mode'] == mode) & (games['split'] == split) &
                                   (games['cp_count'] > 0))) for split in range(4)]
    result = {'mode': MODES[mode], 'games_by_split': counts}
    if any(counts[i] < minimum for i, minimum in enumerate(MIN_GAMES)):
        return {**result, 'status': 'insufficient_data'}
    train, validation = (Objective(xp, rows, games, mode, split, check) for split in (0, 1))
    fits = [fit(train, value) for value in LAMBDAS]
    errors = validation([[f['a'], f['b']] for f in fits])
    # Only validation selects regularization. Never refit on test/held-out data.
    selected = min(range(len(fits)), key=lambda i: (errors[i], -fits[i]['regularization']))
    chosen = fits[selected]
    return {**result, **chosen, 'status': 'fitted' if chosen['converged'] else 'not_converged',
            'validation_mse': float(errors[selected]),
            'regularization_trials': [{**f, 'validation_mse': float(e)} for f, e in zip(fits, errors)]}
