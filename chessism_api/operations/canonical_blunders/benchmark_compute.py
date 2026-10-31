"""Instrumented sequential baseline, never used for canonical publication.

Same operations/order as frozen compute.infer, including its download/reupload.
Every batch is compared with that original function and the published move flags.
CUDA event pairs are resolved at the end, not synchronized around each kernel.
"""
from contextlib import contextmanager
import hashlib
import json
import time
import numpy as np
from . import compute
from ..research_algorithms import blunder_v2_model as model

FIELDS=('status','mode','white_owner','black_owner','white_count','black_count',
        'white_assessed','black_assessed','flags')


class Timer:
    def __init__(self,xp):
        self.xp=xp; self.values={}; self.events=[]

    @contextmanager
    def measure(self,key,*,cuda=False):
        if cuda:
            start=self.xp.cuda.Event(); end=self.xp.cuda.Event(); start.record()
        before=time.perf_counter()
        try:yield
        finally:
            if cuda:
                end.record(); self.events.append((key,start,end))
            name=key+'_wall_seconds'
            self.values[name]=self.values.get(name,0.)+time.perf_counter()-before

    def finish(self):
        for key,start,end in self.events:
            end.synchronize()
            name=key+'_cuda_seconds'
            self.values[name]=self.values.get(name,0.)+self.xp.cuda.get_elapsed_time(start,end)/1000
        self.events=[]
        return self.values


def infer(xp,rows,games,config,*,timer=None):
    if not hasattr(xp,'cuda'):raise RuntimeError('Benchmark requires CUDA; no CPU fallback.')
    if config['kernel_sha256']!=compute.implementation_hash() or config['threshold']!=.15 or config['k0']!=model.K0:
        raise ValueError('Frozen canonical inference contract changed.')
    if len(rows) and (rows['game'].min()<0 or rows['game'].max()>=len(games) or np.any(np.diff(rows['game'])<0)):
        raise ValueError('Invalid game order/index.')
    timer=timer or Timer(xp)
    with timer.measure('host_prepare'):
        fits={f['mode']:f for f in config['fits']}
        ratings=np.array([[g['white_elo'],g['black_elo']] for g in games])
        ab=np.array([[fits[g['mode']]['a'],fits[g['mode']]['b']] for g in games])
        flags=np.empty(len(rows),dtype=np.int8)
        free,_=xp.cuda.runtime.memGetInfo()
        size=max(32768,min(2097152,int((free-512*1024**2)//192)//32768*32768))
    for offset in range(0,len(rows),size):
        with timer.measure('host_prepare'):
            block=rows[offset:offset+size]; ids=block['game']; sign=np.where(block['ply']%2,1.,-1.)
            values=(block['before']*sign,block['after']*sign,block['bk'],block['ak'],
                ((ratings[ids,0]+ratings[ids,1])/2-2300)/400,
                (ratings[ids,0]-ratings[ids,1])*sign/400,ab[ids,0],ab[ids,1])
        with timer.measure('upload',cuda=True):device=[xp.asarray(v) for v in values]
        with timer.measure('decisions',cuda=True):actual=model.decisions(xp,*device,threshold=.15)
        del device
        with timer.measure('download',cuda=True):flags[offset:offset+len(block)]=xp.asnumpy(actual)
    if len(rows):
        with timer.measure('host_prepare'):host_ids=rows['game']*2+(rows['ply']+1)%2
        with timer.measure('upload',cuda=True):ids=xp.asarray(host_ids); actual=xp.asarray(flags)
        with timer.measure('reductions',cuda=True):
            counts=xp.bincount(ids,weights=(actual==1).astype(xp.int64),minlength=len(games)*2)
            assessed=xp.bincount(ids,weights=(actual>=0).astype(xp.int64),minlength=len(games)*2)
        with timer.measure('download',cuda=True):counts,assessed=xp.asnumpy(counts),xp.asnumpy(assessed)
        with timer.measure('host_prepare'):
            counts,assessed=counts.astype(np.int64).reshape(-1,2),assessed.astype(np.int64).reshape(-1,2)
    else:counts=assessed=np.zeros((len(games),2),dtype=np.int64)
    with timer.measure('result_pack'):
        boundaries=np.searchsorted(rows['game'],np.arange(len(games)+1)); records=[]
        for i,g in enumerate(games):
            sl=slice(boundaries[i],boundaries[i+1]); block=rows[sl]; decision=flags[sl]
            records.append({**g,'status':'assessed','white_count':int(counts[i,0]) if assessed[i,0] else None,
                'black_count':int(counts[i,1]) if assessed[i,1] else None,
                'white_assessed':int(assessed[i,0]),'black_assessed':int(assessed[i,1]),
                'flags':{'blunders':block['ply'][decision==1].tolist(),'unassessed':block['ply'][decision<0].tolist()},
                'source_sha256':hashlib.sha256(block.tobytes()+json.dumps(g,sort_keys=True,default=str).encode()).hexdigest()})
    return records,timer.finish()


def compare(actual,expected,*,digest=False):
    expected={r['game_link']:r for r in expected}
    if len(actual)!=len(expected) or len({r['game_link'] for r in actual})!=len(actual):
        raise ValueError('Missing or duplicate benchmark output.')
    mismatches=[]
    for row in actual:
        old=expected.get(row['game_link'])
        fields=FIELDS+(('source_sha256',) if digest else ())
        if old is None or any(row.get(k)!=old.get(k) for k in fields):mismatches.append(row['game_link'])
    return mismatches
