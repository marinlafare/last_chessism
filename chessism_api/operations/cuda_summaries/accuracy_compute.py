"""Versioned CUDA port of player_game_scores, not a new accuracy formula.

The legacy +/-1000 CP clamp, +15 CP opening prior, mate/tablebase treatment,
window endpoints and zero-accuracy harmonic rule are deliberately preserved.
"""
import numpy as np
from .. import player_game_scores as constants
from .score_compute import check_device, host

VERSION = 'game_accuracy_v1'
BANDS = (50., 60., 70., 80., 90.)
LABELS = ('0–50', '51–60', '61–70', '71–80', '81–90', '91–100')
FIELDS = ('analyzed_player_moves', 'own_move_cp_gain', 'own_move_cp_loss', 'game_efficiency',
          'mean_win_percent_loss', 'median_win_percent_loss', 'mate_for_positions',
          'mate_against_positions', 'final_player_cp', 'result', 'end_by')


def _ending(game, white):
    result = game['white_result' if white else 'black_result']
    side = ('black' if white else 'white') if result == 1 else ('white' if white else 'black')
    reason = str(game[side + '_str_result'] or '').lower()
    mapped = {'checkmated': 'checkmate', 'resigned': 'resignation', 'abandoned': 'abandonment',
        'agreed': 'agreement', 'insufficient': 'insufficient_material',
        'timevsinsufficient': 'timeout_vs_insufficient_material', '50move': 'fifty_move_rule'}
    return ('win' if result == 1 else 'draw' if result == .5 else 'loss'), mapped.get(reason, reason or 'unknown')


def inputs(raw, games):
    """Validate whole sequences on the host; never infer accuracy here."""
    chosen = [i for i, g in enumerate(games) if g['analyzed'] and g['total_positions'] > 0
              and (g['white_owner'] or g['black_owner']) and g['mode'] in ('bullet', 'blitz', 'rapid')]
    selected_games = [games[i] for i in chosen]
    remap = np.full(len(games), -1, dtype=np.int32); remap[chosen] = np.arange(len(chosen))
    rows = raw[remap[raw['game']] >= 0].copy(); rows['game'] = remap[rows['game']]
    counts = np.asarray([g['total_positions'] for g in selected_games], dtype=np.int64)
    if len(rows) != counts.sum() or (len(rows) and not np.array_equal(np.bincount(rows['game'], minlength=len(counts)), counts)):
        raise ValueError('Accuracy coverage disagrees with shared input.')
    starts = np.r_[0, np.cumsum(counts)[:-1]] if len(counts) else np.empty(0, dtype=np.int64)
    if len(rows) and (not np.array_equal(rows['ply'], np.arange(len(rows))-np.repeat(starts, counts)+1)
                     or np.any(~np.isfinite(rows['score'])) or np.any(rows['source'] == 3)):
        raise ValueError('Accuracy requires complete, finite, contiguous game evaluations.')
    return rows, selected_games, counts, starts


def calculate(xp, raw, games, *, require_cuda=True, device=None):
    check_device(xp, require_cuda)
    raw, games, counts, starts = inputs(raw, games)
    if not games:
        return []
    arrays = device.select(games) if device is not None else {name:xp.asarray(raw[name]) for name in raw.dtype.names}
    ids, ply, score, src = (arrays[name] for name in ('game','ply','score','source'))
    start = xp.asarray(starts); size = xp.asarray(counts)
    previous = xp.roll(score, 1); previous[start] = 15.
    kinds = xp.where(src == 2, 2, xp.where(xp.abs(score) >= 9000, 1, 0))
    before_kind = xp.roll(kinds, 1); before_kind[start] = 0
    def win_percent(values):
        bounded = xp.clip(values, -constants.LICHESS_CP_CEILING, constants.LICHESS_CP_CEILING)
        return 50. + 50. * (2. / (1. + xp.exp(constants.LICHESS_WINNING_CHANCE_MULTIPLIER * bounded)) - 1.)
    # Black's symmetric win percentage is 100 minus White's; volatility is identical.
    after = win_percent(score); before = win_percent(previous)
    white = ply % 2 == 1; sign = xp.where(white, 1., -1.)
    loss = xp.maximum(0., (before-after)*sign)
    accuracy = xp.where(loss == 0, 100., xp.clip(constants.LICHESS_ACCURACY_SCALE
        * xp.exp(-constants.LICHESS_ACCURACY_DECAY*loss) + constants.LICHESS_ACCURACY_OFFSET
        + constants.LICHESS_ACCURACY_UNCERTAINTY_BONUS, 0., 100.))
    window = xp.clip(size // 10, 2, 8)[ids]
    left = xp.maximum(0, ply-window+1)
    initial = constants.lichess_win_percent(15.)
    mean = xp.zeros(len(raw), dtype=xp.float64)
    for offset in range(8):
        local = left+offset
        values = xp.where(local == 0, initial, after[xp.minimum(start[ids]+xp.maximum(local-1, 0), len(raw)-1)])
        mean += xp.where(offset < window, values, 0.)
    mean /= window
    variance = xp.zeros(len(raw), dtype=xp.float64)
    for offset in range(8):
        local = left+offset
        values = xp.where(local == 0, initial, after[xp.minimum(start[ids]+xp.maximum(local-1, 0), len(raw)-1)])
        variance += xp.where(offset < window, (values-mean)**2, 0.)
    weight = xp.clip(xp.sqrt(variance/window), .5, 12.)
    groups = ids*2 + (~white).astype(xp.int64); group_count = len(games)*2
    def summed(values):
        return xp.bincount(groups, weights=values.astype(xp.float64), minlength=group_count)
    n = xp.bincount(groups, minlength=group_count); denominator = xp.maximum(n, 1)
    weighted = summed(accuracy*weight) / xp.maximum(summed(weight), xp.finfo(xp.float64).tiny)
    reciprocal = summed(1. / xp.where(accuracy > 0, accuracy, 1.))
    harmonic = xp.where(summed(accuracy == 0) > 0, 0., n/xp.maximum(reciprocal, xp.finfo(xp.float64).tiny))
    game_accuracy = xp.where(n > 0, (weighted+harmonic)/2., xp.nan)
    change = (score-previous)*sign
    valid_cp = (kinds == 0) & (before_kind == 0)
    gains = summed(xp.where(valid_cp, xp.maximum(change, 0.), 0.))
    losses = summed(xp.where(valid_cp, xp.maximum(-change, 0.), 0.))
    mean_loss = xp.where(n > 0, summed(loss)/denominator, xp.nan)
    ordered = xp.lexsort(xp.stack((loss, groups))); boundaries = xp.r_[xp.array([0]), xp.cumsum(n)[:-1]]
    low = xp.minimum(boundaries+xp.maximum(n-1, 0)//2, len(raw)-1)
    high = xp.minimum(boundaries+n//2, len(raw)-1)
    median = xp.where(n > 0, (loss[ordered[low]]+loss[ordered[high]])/2., xp.nan)
    for_white = xp.bincount(ids, weights=((kinds == 1) & (score > 0)).astype(xp.float64), minlength=len(games))
    against_white = xp.bincount(ids, weights=((kinds == 1) & (score <= 0)).astype(xp.float64), minlength=len(games))
    mate_for = xp.stack((for_white, against_white), axis=1).reshape(-1)
    mate_against = xp.stack((against_white, for_white), axis=1).reshape(-1)
    last = start+size-1
    final = xp.where(kinds[last] == 0, score[last], xp.nan)
    final = xp.stack((final, -final), axis=1).reshape(-1)
    values = host(xp, xp.stack((n, gains, losses, game_accuracy, mean_loss, median, mate_for, mate_against, final), axis=1))
    records = []
    for index, g in enumerate(games):
        for side, white in enumerate((True, False)):
            owner = g['white_owner' if white else 'black_owner']
            if not owner:
                continue
            row = values[index*2+side]
            payload = {key: (None if not np.isfinite(value) else int(value) if key in
                ('analyzed_player_moves','mate_for_positions','mate_against_positions') else float(value))
                for key, value in zip(FIELDS, row)}
            payload['result'], payload['end_by'] = _ending(g, white)
            records.append({'game_link': g['game_link'], 'player_name': owner, 'is_white': white,
                            'played_at': g['played_at'], 'mode': g['mode'], 'payload': payload})
    return records


def daily(xp, rows, *, require_cuda=True):
    """Seven mode selections, six exact bands, all ties, saved day salience sums."""
    check_device(xp, require_cuda)
    if not rows:
        return []
    dates = sorted({r['day'] for r in rows}); date_ids = {day: i for i, day in enumerate(dates)}
    days = xp.asarray([date_ids[r['day']] for r in rows]); modes = xp.asarray([r['mode_bit'] for r in rows])
    accuracy = xp.asarray([r['accuracy'] for r in rows], dtype=xp.float64)
    salience = xp.asarray([r['salience'] if r['salience'] is not None else xp.nan for r in rows], dtype=xp.float64)
    bands = xp.searchsorted(xp.asarray(BANDS), accuracy, side='left')
    output = []
    for mask in range(1, 8):
        chosen = (modes & mask) != 0
        if not int(xp.count_nonzero(chosen)):
            continue
        counts = xp.bincount(days[chosen]*6+bands[chosen], minlength=len(dates)*6).reshape(-1, 6)
        totals = counts.sum(axis=1); winners = (counts == counts.max(axis=1)[:, None]) & (totals[:, None] > 0)
        sums = xp.bincount(days[chosen], weights=xp.nan_to_num(salience[chosen]), minlength=len(dates))
        known = xp.bincount(days[chosen], weights=xp.isfinite(salience[chosen]).astype(xp.float64), minlength=len(dates))
        counts, totals, winners, sums, known = [host(xp, value) for value in (counts, totals, winners, sums, known)]
        for i, day in enumerate(dates):
            if totals[i]:
                output.append({'day': day, 'mode_mask': mask, 'counts': counts[i].tolist(),
                    'modal_bands': np.flatnonzero(winners[i]).tolist(), 'games': int(totals[i]),
                    'daily_salience': float(sums[i]) if known[i] == totals[i] else None})
    return output
