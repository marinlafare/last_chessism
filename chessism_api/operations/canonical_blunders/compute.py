"""Fixed production inference. GPU only; no fitting, labels, or CPU fallback."""
import hashlib
import json
import inspect
import numpy as np

from ..research_algorithms import blunder_v2_model as model

DTYPE = np.dtype([('game','<i4'),('ply','<i4'),('before','<f8'),('after','<f8'),('bk','i1'),('ak','i1')])


def pack(game, records, index):
    if len(records) != game['total_positions']: raise ValueError('Coverage disagrees with canonical source positions.')
    result = np.zeros(len(records), dtype=DTYPE); previous, kind = 15., 0
    for i, (ply, score, current_kind) in enumerate(records):
        if ply != i+1: raise ValueError('Missing, duplicate or unordered predecessor; no ply may be skipped.')
        if score is None: score, current_kind = np.nan, -1
        if current_kind >= 0 and not np.isfinite(score): raise ValueError('Nonfinite scored position.')
        result[i] = (index, ply, previous, score, kind, current_kind)
        previous, kind = score, current_kind
    return result


def infer(xp, rows, games, config, *, require_cuda=True):
    if require_cuda and not hasattr(xp, 'cuda'): raise RuntimeError('Canonical blunders require CUDA; no CPU fallback.')
    if config['threshold'] != .15 or config['k0'] != model.K0: raise ValueError('Unsupported canonical equation.')
    actual_hash=implementation_hash()
    if actual_hash!=config['kernel_sha256']: raise ValueError('Canonical CUDA implementation changed; publish an explicit new version.')
    fits = {f['mode']: f for f in config['fits']}
    ratings = np.array([[g['white_elo'], g['black_elo']] for g in games])
    ab = np.array([[fits[g['mode']]['a'],fits[g['mode']]['b']] for g in games])
    flags = np.empty(len(rows), dtype=np.int8)
    if len(rows) and (rows['game'].min()<0 or rows['game'].max()>=len(games)): raise ValueError('Unknown game index.')
    if len(rows)>1 and np.any(np.diff(rows['game'])<0): raise ValueError('Unordered canonical game rows.')
    free, _ = xp.cuda.runtime.memGetInfo() if hasattr(xp,'cuda') else (2**30, 2**30)
    size = max(32768, min(2097152, int((free-512*1024**2)//192)//32768*32768))
    offset = 0
    while offset < len(rows):
        block = rows[offset:offset+size]; ids = block['game']; sign = np.where(block['ply']%2,1.,-1.)
        values = (block['before']*sign,block['after']*sign,block['bk'],block['ak'],
            ((ratings[ids,0]+ratings[ids,1])/2-2300)/400,
            (ratings[ids,0]-ratings[ids,1])*sign/400,ab[ids,0],ab[ids,1])
        try:
            actual = model.decisions(xp,*[xp.asarray(v) for v in values],threshold=.15)
            flags[offset:offset+len(block)] = xp.asnumpy(actual) if hasattr(xp,'asnumpy') else actual
        except Exception as error:
            if hasattr(xp,'cuda') and isinstance(error,xp.cuda.memory.OutOfMemoryError) and size>32768:
                size = max(32768,size//2//32768*32768); xp.get_default_memory_pool().free_all_blocks(); continue
            raise
        offset += len(block)
    records=[]
    # Per-game reduction on CUDA, not another full CPU blunder calculation.
    if len(rows):
        ids=xp.asarray(rows['game']*2+(rows['ply']+1)%2); actual=xp.asarray(flags)
        counts=xp.bincount(ids,weights=(actual==1).astype(xp.int64),minlength=len(games)*2)
        assessed=xp.bincount(ids,weights=(actual>=0).astype(xp.int64),minlength=len(games)*2)
        if hasattr(xp,'asnumpy'): counts,assessed=xp.asnumpy(counts),xp.asnumpy(assessed)
        counts,assessed=counts.astype(np.int64).reshape(-1,2),assessed.astype(np.int64).reshape(-1,2)
    else: counts=assessed=np.zeros((len(games),2),dtype=np.int64)
    boundaries=np.searchsorted(rows['game'],np.arange(len(games)+1))
    for i,g in enumerate(games):
        sl=slice(boundaries[i],boundaries[i+1]); block=rows[sl]; decision=flags[sl]
        records.append({**g,'status':'assessed','white_count':int(counts[i,0]) if assessed[i,0] else None,
            'black_count':int(counts[i,1]) if assessed[i,1] else None,
            'white_assessed':int(assessed[i,0]),'black_assessed':int(assessed[i,1]),
            'flags':{'blunders':block['ply'][decision==1].tolist(),'unassessed':block['ply'][decision<0].tolist()},
            'source_sha256':hashlib.sha256(block.tobytes()+json.dumps(g,sort_keys=True,default=str).encode()).hexdigest()})
    return records, {'moves':len(rows),'games':len(games),'cuda_batch_rows':size}


def implementation_hash():
    """Freeze extraction/perspective/reduction as well as the decision equation."""
    return hashlib.sha256(''.join(inspect.getsource(f) for f in
        (model.expected,model.decisions,pack,infer)).encode()).hexdigest()
