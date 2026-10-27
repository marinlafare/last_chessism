"""Synthetic numeric inputs only; no API/auth/database imports."""
import numpy as np
from chessism_api.operations.research_algorithms import blunder_fixed as model


def synthetic():
    rng = np.random.default_rng(20261026)
    rows = np.zeros(100_016, dtype=model.DTYPE)
    rows['game'] = np.arange(len(rows)) % 3
    rows['link'] = rows['game']+1
    rows['ply'] = np.arange(len(rows))+1
    rows['before'] = rng.integers(-2000, 2001, len(rows))
    rows['after'] = rng.integers(-2000, 2001, len(rows))
    boundary = -np.log(.65/.35)/model.COEFFICIENT
    rows['before'][-16:] = [0,0,-700,-700.001,9998,9998,9998,-9998,-9998,0,0,0,0,np.nan,100,100]
    rows['after'][-16:] = [boundary-1e-7,boundary+1e-7,-9998,-9998,700,700.001,-9998,9998,-9999,0,-9998,9998,0,0,-100,-100]
    rows['before_kind'][-16:] = [0,0,0,0,1,1,1,1,1,2,2,0,-1,0,0,0]
    rows['after_kind'][-16:] = [0,0,1,1,0,0,1,1,1,0,1,2,0,0,0,0]
    expected = model.reference(rows)
    rows['sql'] = (expected == 1).astype(np.int8)
    games = [{'link': i+1, 'mode': mode, 'is_white': True,
              'total_positions': int(np.count_nonzero(rows['game'] == i)),
              'saved_moves': int(np.count_nonzero(rows['game'] == i)),
              'saved_blunders': int(np.count_nonzero((rows['game'] == i) & (expected == 1)))}
             for i, mode in enumerate(model.MODES)]
    return rows, games
