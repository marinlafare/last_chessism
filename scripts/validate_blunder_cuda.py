"""Synthetic-only actual-GPU acceptance check. No database connection or writes."""
import json

import numpy as np

from chessism_api.operations.research_algorithms.blunder_model import (
    CP, Dataset, TRANSITION, compare_transitions, expected_points, fit_group,
)


def main():
    dataset=Dataset()
    fits=[]
    for group, coefficient in [(0,.0027),(26,.007)]:
        probability=expected_points(np,CP,coefficient)
        dataset.hist[group]=np.column_stack([np.ones(2001),probability,probability])
        dataset.games[group]=[500,50,50]  # Synthetic sufficient-statistic fixture.
        fitted=fit_group(dataset,group)
        assert fitted['status']=='fitted' and abs(fitted['coefficient']-coefficient)<1e-7,fitted
        fits.append(fitted)
    rng=np.random.default_rng(42)
    batch=np.zeros(100_008,dtype=TRANSITION)
    batch['group']=np.where(np.arange(len(batch))%2,0,26)
    batch['split']=2; batch['game']=np.arange(len(batch))//100
    batch['before']=rng.integers(-2000,2001,len(batch))
    batch['after']=rng.integers(-2000,2001,len(batch))
    batch['before'][-8:]=[100,9998,9998,-9998,100,100,np.nan,-700]
    batch['after'][-8:]=[-9999,700,-9999,100,0,0,0,-9999]
    batch['before_kind'][-8:]=[0,1,1,1,2,-1,0,0]
    batch['after_kind'][-8:]=[1,0,1,0,0,0,0,1]
    results=compare_transitions(batch,fits)
    json.dumps(fits,allow_nan=False)  # The full candidate must persist as ordinary JSON.
    assert sum(row.get('cpu_cuda_mismatches',0) for row in results)==0,results
    assert sum(row.get('unsupported_moves',0) for row in results)==3,results
    print(json.dumps({'source':'synthetic_only','status':'passed','hardware':fits[0]['hardware'],
        'fits':[{'group':fit['group'],'coefficient':fit['coefficient'],'max_prediction_error':fit['max_prediction_error']} for fit in fits],
        'rows':len(batch),'comparison':results},indent=2))


if __name__=='__main__': main()
