"""First 15 full moves, equal weights. Device-only grouping and reductions.

Input columns: owner (2*player_id+is_white), game_player_id, fen_id, ply.
Whole (owner, FEN) groups stay together, even across extraction checkpoints.
This makes large mixed-player batches exact without a player-by-player loop.
"""
import numpy as np
from .score_compute import check_device

VERSION = 'salience_first15_equal_cuda_v1'
MAX_PLY = 30
SHARDS = 32


def host(xp, value):
    return xp.asnumpy(value) if hasattr(xp, 'asnumpy') else np.asarray(value)


def groups(xp, *keys):
    order = xp.lexsort(xp.stack(tuple(reversed(keys))))
    boundary = xp.ones(len(order), dtype=xp.bool_)
    if len(order) > 1:
        same = xp.ones(len(order)-1, dtype=xp.bool_)
        for key in keys:
            same &= key[order[1:]] == key[order[:-1]]
        boundary[1:] = ~same
    starts = xp.flatnonzero(boundary)
    group = xp.cumsum(boundary, dtype=xp.int64)-1
    return order, starts, group


def calculate(xp, raw, *, require_cuda=True):
    """One or more complete hash shards; return per-game parts and frequencies."""
    check_device(xp,require_cuda)
    if not len(raw):
        return np.empty((0,4)), np.empty((0,4), dtype=np.int64)
    data = xp.asarray(raw, dtype=xp.int64)
    order = xp.lexsort(xp.stack((data[:,3],data[:,1],data[:,2],data[:,0])))
    owner, game, fen, ply = (data[order,i] for i in range(4))
    if bool(xp.any((ply < 1) | (ply > MAX_PLY))):
        raise ValueError('Salience input must contain only plies 1–30.')
    new_fen = xp.ones(len(data), dtype=xp.bool_)
    new_fen[1:] = (owner[1:] != owner[:-1]) | (fen[1:] != fen[:-1])
    new_game = new_fen.copy()
    new_game[1:] |= game[1:] != game[:-1]
    starts = xp.flatnonzero(new_fen)
    ids = xp.cumsum(new_fen, dtype=xp.int64)-1
    frequency = xp.bincount(ids, weights=new_game.astype(xp.float64)).astype(xp.int64)
    occurrences = xp.bincount(ids)
    repeat_starts = xp.flatnonzero(new_game)[xp.cumsum(new_game,dtype=xp.int64)-1]
    repeat = xp.arange(len(data))-repeat_starts+1
    contribution = 1.0 / (frequency[ids] * repeat)
    gorder, gstarts, gids = groups(xp, game)
    parts = xp.column_stack((game[gorder[gstarts]],
        xp.bincount(gids, weights=contribution[gorder]), xp.bincount(gids),
        xp.bincount(gids, weights=new_game[gorder].astype(xp.float64))))
    repeated = frequency > 1
    frequencies = xp.column_stack((owner[starts][repeated],fen[starts][repeated],
        frequency[repeated],occurrences[repeated]))
    return host(xp,parts), host(xp,frequencies)


def reduce_games(xp, raw, *, require_cuda=True):
    """Merge partial sums on CUDA; division happens only after all shards finish."""
    check_device(xp,require_cuda)
    if not len(raw):
        return np.empty((0,5))
    data = xp.asarray(raw, dtype=xp.float64)
    order, starts, ids = groups(xp, data[:,0])
    numerator = xp.bincount(ids, weights=data[order,1])
    count = xp.bincount(ids, weights=data[order,2])
    unique = xp.bincount(ids, weights=data[order,3])
    return host(xp,xp.column_stack((data[order[starts],0],numerator,count,unique,numerator/count)))


def totals(xp, raw, *, require_cuda=True):
    check_device(xp,require_cuda)
    data = xp.asarray(raw, dtype=xp.float64)
    return {'source_game_count':len(data),
        'source_position_count':int(xp.sum(data[:,1])) if len(data) else 0,
        'effective_game_count':float(xp.sum(data[:,0])) if len(data) else 0.0}
