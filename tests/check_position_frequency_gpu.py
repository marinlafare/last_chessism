"""Synthetic GPU smoke check, no database/network or published player data."""
from collections import Counter
import numpy as np
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
from chessism_api.operations.cuda_summaries.frequency_compute import calculate

xp,hardware=cuda_backend(0)
assert hardware['name']=='NVIDIA GeForce RTX 4060',hardware
rng=np.random.default_rng(20261030)
raw=np.column_stack((rng.integers(1,20,100000),rng.integers(0,2,100000),
                     rng.integers(1,2500,100000)+2**55,rng.integers(1,4,100000)))
counts=Counter(map(tuple,raw));totals=Counter((r[0],r[2]) for r in raw)
expected={(*key,count) for key,count in counts.items() if totals[key[0],key[2]]>1}
actual=calculate(xp,raw)
xp.cuda.Stream.null.synchronize()
assert {tuple(row) for row in actual}==expected
print(f'PASS: {hardware["name"]}; 100,000 synthetic occurrences, exact GPU/reference agreement, {len(actual):,} retained subgroup rows')
