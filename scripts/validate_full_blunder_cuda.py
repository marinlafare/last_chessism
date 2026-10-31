"""Synthetic RTX 4060 check; no database, credentials, or cloud access."""
import json
from pathlib import Path
import tempfile

import numpy as np

from chessism_api.operations.research_algorithms import blunder_full_compute as compute, blunder_v2_model as model
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
from tests.test_blunder_full_development import input_fixture
from tests.test_blunder_full_diagnostics import staged_fixture, diagnostic_fixture


def main():
    xp, hardware = cuda_backend(0)
    assert hardware['name'] == 'NVIDIA GeForce RTX 4060', hardware
    plan = compute.memory_plan(*xp.cuda.runtime.memGetInfo())
    xp.get_default_memory_pool().set_limit(size=plan['pool_limit_bytes'])
    values = input_fixture(5000)
    values[:, 3] = model.expected(np, values[:, 0], values[:, 1], values[:, 2], .21, .37)
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder)/'input.bin'; path.write_bytes(values.astype('<f8').tobytes())
        gpu = compute.Input(xp, path, len(values), plan['pool_limit_bytes'], lambda: None)
        cpu = compute.Input(np, path, len(values), plan['pool_limit_bytes'], lambda: None)
        try:
            params = [[0., 0.], [.21, .37], [.1, 2.3]]
            a, b = gpu.metrics(params, calibration=True), cpu.metrics(params, calibration=True)
            np.testing.assert_allclose([[r['mse'], r['ece']] for r in a], [[r['mse'], r['ece']] for r in b], atol=1e-12, rtol=0)
            assert gpu.device is not None
            gpu.device = None; xp.get_default_memory_pool().free_all_blocks(); gpu.batch = 777
            streamed = gpu.metrics(params, calibration=True)
            np.testing.assert_allclose([[r['mse'], r['ece']] for r in streamed], [[r['mse'], r['ece']] for r in b], atol=1e-12, rtol=0)
            gpu.batch = 32768
            fitted = model.fit(lambda p: gpu.metrics(p), 0.)
            assert fitted['converged'] and abs(fitted['a']-.21) < 1e-5 and abs(fitted['b']-.37) < 1e-5, fitted
            oracle = compute.oracle_check(xp, gpu, params)
            print(json.dumps({'status': 'passed', 'source': 'synthetic_only', 'hardware': hardware,
                'memory_plan': plan, 'resident_and_streaming_match_cpu': True,
                'independent_scalar': oracle, 'known_parameter_recovery': fitted}, indent=2))
        finally:
            cpu.close(); gpu.close()
    with tempfile.TemporaryDirectory() as folder:
        _, games, frozen = staged_fixture(Path(folder))
        gpu = diagnostic_fixture(xp, Path(folder), games, plan['pool_limit_bytes'])
        cpu = diagnostic_fixture(np, Path(folder), games, plan['pool_limit_bytes'])
        assert frozen['selected_games'] == 540
        assert gpu['bullet']['status'] == 'passed' and gpu['bullet']['groups'] == cpu['bullet']['groups']
        for a, b in zip(gpu['rapid']['groups'], cpu['rapid']['groups']):
            for name in a['models']:
                for key in ('mse', 'ece_pp', 'ece_5_bins_pp'):
                    np.testing.assert_allclose(a['models'][name][key], b['models'][name][key], atol=1e-10, rtol=0)
        print(json.dumps({'status': 'passed', 'cp_first_balance': True,
            'rapid_group_metrics_match_cpu': True, 'bullet_checks': gpu['bullet']['checks'],
            'bullet_scalar_checked_moves': gpu['bullet']['scalar_checked_moves']}, indent=2))


if __name__ == '__main__': main()
