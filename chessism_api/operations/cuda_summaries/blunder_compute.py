"""Large-batch execution of the frozen canonical blunder equation.

The original infer function stays frozen as the model's reference implementation
and is used in parity tests. Only transport/batching changes here: one device
invocation per memory-budgeted shared input, without the old 2M-row sub-loop.
"""
import hashlib
import inspect
import json
import numpy as np
from ..canonical_blunders import compute as reference
from ..research_algorithms import blunder_v2_model as model
from .score_compute import check_device, host

VERSION = 'shared_blunder_execution_v2'


def infer(xp, rows, games, config, *, require_cuda=True, device=None):
    check_device(xp, require_cuda)
    if config['threshold'] != .15 or config['k0'] != model.K0:
        raise ValueError('Unsupported canonical equation.')
    if config['kernel_sha256'] != reference.implementation_hash():
        raise ValueError('Canonical CUDA implementation changed; publish an explicit new version.')
    if len(rows) and (rows['game'].min() < 0 or rows['game'].max() >= len(games)):
        raise ValueError('Unknown game index.')
    if len(rows) > 1 and np.any(np.diff(rows['game']) < 0):
        raise ValueError('Unordered canonical game rows.')
    fits = {f['mode']: f for f in config['fits']}
    flags = np.empty(len(rows), dtype=np.int8)
    counts = assessed = np.zeros((len(games), 2), dtype=np.int64)
    if len(rows):
        ratings = xp.asarray([[g['white_elo'], g['black_elo']] for g in games], dtype=xp.float64)
        ab = xp.asarray([[fits[g['mode']]['a'], fits[g['mode']]['b']] for g in games])
        if device is None:
            ids = xp.asarray(rows['game']); ply = xp.asarray(rows['ply'])
            before, after, bk, ak = (xp.asarray(rows[name]) for name in ('before','after','bk','ak'))
        else:
            arrays = device.select(games)
            ids, ply, after, src = (arrays[name] for name in ('game','ply','score','source'))
            starts = xp.asarray(np.r_[0,np.cumsum([g['total_positions'] for g in games])[:-1]])
            ak = xp.where(src == 2,2,xp.where(src == 1,xp.where(xp.abs(after)>=9000,1,0),-1)).astype(xp.int8)
            before = xp.roll(after,1); before[starts] = 15.
            bk = xp.roll(ak,1); bk[starts] = 0
        sign = xp.where(ply % 2, 1., -1.)
        actual = model.decisions(xp, before*sign, after*sign, bk, ak,
            ((ratings[ids, 0]+ratings[ids, 1])/2-2300)/400,
            (ratings[ids, 0]-ratings[ids, 1])*sign/400, ab[ids, 0], ab[ids, 1], threshold=.15)
        groups = ids*2+(ply+1)%2
        counts = host(xp, xp.bincount(groups, weights=(actual == 1).astype(xp.int64), minlength=len(games)*2)).astype(np.int64).reshape(-1, 2)
        assessed = host(xp, xp.bincount(groups, weights=(actual >= 0).astype(xp.int64), minlength=len(games)*2)).astype(np.int64).reshape(-1, 2)
        flags = host(xp, actual)
    records = []; boundaries = np.searchsorted(rows['game'], np.arange(len(games)+1))
    for i, g in enumerate(games):
        sl = slice(boundaries[i], boundaries[i+1]); block = rows[sl]; decision = flags[sl]
        records.append({**g, 'status': 'assessed', 'white_count': int(counts[i, 0]) if assessed[i, 0] else None,
            'black_count': int(counts[i, 1]) if assessed[i, 1] else None,
            'white_assessed': int(assessed[i, 0]), 'black_assessed': int(assessed[i, 1]),
            'flags': {'blunders': block['ply'][decision == 1].tolist(), 'unassessed': block['ply'][decision < 0].tolist()},
            'source_sha256': hashlib.sha256(block.tobytes()+json.dumps(g, sort_keys=True, default=str).encode()).hexdigest()})
    return records, {'moves': len(rows), 'games': len(games), 'cuda_batch_rows': len(rows),
                     'execution_version': VERSION, 'execution_sha256': hashlib.sha256(inspect.getsource(infer).encode()).hexdigest()}
