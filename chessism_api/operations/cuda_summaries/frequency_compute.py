"""Exact full-game frequencies. All copies of a player/FEN share one shard."""
import numpy as np
from .score_compute import check_device, host

VERSION = 'player_full_position_frequency_v1'
SHARDS = 128


def calculate(xp, raw, *, require_cuda=True):
    """Input: player, is_white, fen_id, mode_id. Output adds appearances.

    Drop singletons across the PLAYER, not per color/mode. Counts of one in
    individual subgroups are necessary when a FEN repeats across filters.
    """
    check_device(xp, require_cuda)
    if not len(raw):
        return np.empty((0, 5), dtype=np.int64)
    data = xp.asarray(raw, dtype=xp.int64)
    data = data[xp.lexsort(xp.stack((data[:, 3], data[:, 1], data[:, 2], data[:, 0])))]
    different = xp.ones(len(data), dtype=xp.bool_)
    different[1:] = xp.any(data[1:] != data[:-1], axis=1)
    starts = xp.flatnonzero(different)
    counts = xp.diff(xp.concatenate((starts, xp.asarray([len(data)], dtype=xp.int64))))
    fen_start = xp.ones(len(data), dtype=xp.bool_)
    fen_start[1:] = (data[1:, 0] != data[:-1, 0]) | (data[1:, 2] != data[:-1, 2])
    fens = xp.flatnonzero(fen_start)
    fen_counts = xp.diff(xp.concatenate((fens, xp.asarray([len(data)], dtype=xp.int64))))
    fen_groups = xp.cumsum(fen_start, dtype=xp.int64) - 1
    repeated = fen_counts[fen_groups[starts]] > 1
    return host(xp, xp.column_stack((data[starts][repeated], counts[repeated])))
