"""Isolated GPU capacity/parity test; synthetic inputs, no database or network.

Run only while the production GPU worker is idle, in a 16 GiB disposable
container with only the RTX 4060 exposed. Does not write any model or summary.
"""
import gc
import json
import math
import time
from datetime import datetime, timezone
import numpy as np
from chessism_api.operations.cuda_summaries import batch_budget, shared_source, blunder_compute, score_compute, accuracy_compute, device_inputs
from chessism_api.operations.canonical_blunders import compute, registry
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
from tests.test_canonical_blunders import report


def main():
    cp, hardware = cuda_backend(0)
    assert hardware['name'] == 'NVIDIA GeForce RTX 4060', hardware
    budget = batch_budget.plan(*cp.cuda.runtime.memGetInfo())
    cp.get_default_memory_pool().set_limit(size=budget.device_bytes)
    plies = 128
    count = min(budget.device_bytes//(plies*batch_budget.DEVICE_ROW+batch_budget.DEVICE_GAME),
                budget.host_bytes//(plies*batch_budget.HOST_ROW+batch_budget.HOST_GAME))
    assert count*plies > 2_097_152, count
    print(json.dumps({'hardware':hardware, **budget.describe(), 'games': count, 'positions': count*plies}), flush=True)
    raw = np.empty(count*plies, shared_source.RAW)
    raw['game'] = np.repeat(np.arange(count), plies); raw['ply'] = np.tile(np.arange(1, plies+1), count)
    sequence = np.sin(np.arange(plies))*500
    sequence[12:16] = [105, -9998, 9996, 0]
    raw['score'] = np.tile(sequence, count); raw['source'] = 1
    games = [{'game_link':i+1, 'revision':1, 'total_positions':plies, 'mode':('bullet','blitz','rapid')[i%3],
        'analyzed':True, 'white_owner':'alice', 'black_owner':'bob', 'white_elo':800+(i%3)*1000,
        'black_elo':1000+(i%3)*1000, 'played_at':datetime(2025,1,1,tzinfo=timezone.utc),
        'white_result':1, 'black_result':0, 'white_str_result':'win', 'black_str_result':'resigned', 'last_white':False}
        for i in range(count)]
    items = [{'game_link':g['game_link'], 'revision':1, 'blunders':True} for g in games]
    eligible, unsupported, transitions = shared_source.blunder_inputs(raw, games, items)
    assert not unsupported
    _, config, _ = registry.freeze(report(), registry.ACK)
    started = time.monotonic()
    actual, info = blunder_compute.infer(cp, transitions, eligible, config)
    seconds = time.monotonic()-started
    peak = cp.get_default_memory_pool().total_bytes()
    expected, _ = compute.infer(cp, transitions, eligible, config)
    assert actual == expected
    assert info['cuda_batch_rows'] == len(raw)
    print(json.dumps({'stage':'blunders', 'seconds':seconds, 'positions_per_invocation':info['cuda_batch_rows'],
                      'pool_reserved_bytes':peak, 'parity':'exact flags/counts/source hashes against frozen CUDA'}), flush=True)
    del actual, expected, transitions, eligible, items
    gc.collect(); cp.get_default_memory_pool().free_all_blocks()
    owners = [{'game_link':g['game_link'], 'player_name':p, 'day':'2025-01-01', 'is_white':w}
              for g in games for p,w in (('alice',True), ('bob',False))]
    started = time.monotonic()
    actual = score_compute.calculate(cp, raw, games, owners)
    seconds = time.monotonic()-started; peak = cp.get_default_memory_pool().total_bytes()
    expected = score_compute.calculate(np, raw, games, owners, require_cuda=False)
    assert actual == expected
    print(json.dumps({'stage':'score_distribution', 'seconds':seconds, 'pool_reserved_bytes':peak, 'parity':'exact'}), flush=True)
    del owners, expected, actual
    gc.collect(); cp.get_default_memory_pool().free_all_blocks()
    expected = accuracy_compute.calculate(np, raw[:plies*3], games[:3], require_cuda=False)
    started = time.monotonic()
    actual = accuracy_compute.calculate(cp, raw, games)
    seconds = time.monotonic()-started; peak = cp.get_default_memory_pool().total_bytes()
    assert len(actual) == count*2
    error = 0.
    for i, record in enumerate(actual):
        for key, value in expected[i%6]['payload'].items():
            other = record['payload'][key]
            if isinstance(value, float):
                error = max(error, abs(value-other))
                assert math.isclose(value, other, rel_tol=1e-11, abs_tol=1e-9), (key, value, other)
            else:
                assert value == other, (key, value, other)
    print(json.dumps({'stage':'accuracy', 'seconds':seconds, 'pool_reserved_bytes':peak, 'max_numeric_error':error}), flush=True)
    del actual, expected
    gc.collect(); cp.get_default_memory_pool().free_all_blocks()
    # Exercise a single upload shared by all algorithms, no inter-stage reset.
    items = [{'game_link':g['game_link'], 'revision':1, 'blunders':True} for g in games]
    eligible, _, transitions = shared_source.blunder_inputs(raw, games, items)
    owners = [{'game_link':g['game_link'], 'player_name':p, 'day':'2025-01-01', 'is_white':w}
              for g in games for p,w in (('alice',True), ('bob',False))]
    started = time.monotonic()
    shared = device_inputs.upload(cp,raw,games)
    result, _ = blunder_compute.infer(cp, transitions, eligible, config,device=shared)
    histogram = score_compute.calculate(cp, raw, games, owners,device=shared)
    accuracy = accuracy_compute.calculate(cp, raw, games,device=shared)
    assert len(result) == count and len(accuracy) == count*2 and histogram
    seconds = time.monotonic()-started
    # Subset path parity is covered offline; check full-size reuse against reference.
    assert result == compute.infer(cp,transitions,eligible,config)[0]
    print(json.dumps({'stage':'shared_sequence_single_upload', 'seconds':seconds,
                      'position_upload_bytes':shared.bytes,
                      'pool_reserved_bytes':cp.get_default_memory_pool().total_bytes()}), flush=True)
    from tests.test_cuda_blunder_summary import cuda_check
    cuda_check()
    print('PASS memory-budgeted mixed-player CUDA, no 2M sub-loop, all three equations unchanged', flush=True)


if __name__ == '__main__': main()
