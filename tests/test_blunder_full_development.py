from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_full_source as source, blunder_full_compute as compute
from chessism_api.operations.research_algorithms import blunder_full_job as job, blunder_v2_model as model
from chessism_api.operations.research_algorithms import blunder_development_reuse as history
from tests.test_blunder_data_audit import POLICY


def input_fixture(n=1200):
    rng = np.random.default_rng(123)
    values = np.column_stack((rng.uniform(-1100, 1100, n), rng.uniform(-3, 1, n),
                             rng.uniform(-2, 2, n), rng.choice([0., .5, 1.], n), rng.uniform(.01, .1, n)))
    return values


class FullTests(unittest.TestCase):
    def test_loss_cache_preserves_exact_values_and_order(self):
        data = MagicMock(); data.metrics.side_effect = lambda rows: np.array([a*a+b for a,b in rows])
        objective = compute.CachedObjective(data)
        np.testing.assert_array_equal(objective([[.5, 1.], [.5, 1.], [0., 0.]]), [1.25, 1.25, 0.])
        np.testing.assert_array_equal(objective([[0., 0.], [.5, 1.]]), [0., 1.25])
        self.assertEqual(data.metrics.call_count, 1); self.assertEqual(objective.calculated, 2)

    def test_dedicated_card_memory_budget(self):
        plan = compute.memory_plan(7500*1024**2, 8*compute.GIB)
        self.assertEqual(plan['runtime_reserve_bytes'], 512*1024**2)
        self.assertEqual(plan['pool_limit_bytes'], (7500-512)*1024**2)
        self.assertNotIn('desktop_reserve_bytes', plan)
        with self.assertRaises(ValueError): compute.memory_plan(700*1024**2, 8*compute.GIB)

    def test_compact_input_preserves_cp_game_weight_and_mate_exclusion(self):
        game = {'total_positions': 4, 'white_elo': 1200, 'black_elo': 1600, 'white_result': .5}
        values, packed = source.compact(game, [(1, 105., 1, 0), (2, -9998., 2, 1), (3, -200., 3, 0), (4, 0., 4, 2)])
        self.assertEqual(len(values), 2); self.assertEqual(len(packed), 4)
        self.assertEqual(values[:, 0].tolist(), [105., -200.]); self.assertEqual(values[:, 4].sum(), 1.)
        self.assertTrue((values[:, 2] == -1.).all()); self.assertTrue((values[:, 3] == .5).all())
        with self.assertRaisesRegex(ValueError, 'Coverage'): source.compact(game, [])

    def test_exact_objective_metrics_and_chunk_invariance(self):
        values = input_fixture()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'data.bin'; path.write_bytes(values.astype('<f8').tobytes())
            data = compute.Input(np, path, len(values), compute.GIB, lambda: None)
            try:
                params = [[0., 0.], [.21, .37]]
                first = data.metrics(params, calibration=True)
                data.batch = 75
                second = data.metrics(params, calibration=True)
                np.testing.assert_allclose([[d['mse'], d['ece']] for d in first], [[d['mse'], d['ece']] for d in second], atol=1e-12)
                for parameters, metric in zip(params, second):
                    p = model.expected(np, values[:, 0], values[:, 1], values[:, 2], *parameters)
                    error = p-values[:, 3]; w = values[:, 4]
                    self.assertAlmostEqual(metric['mse'], np.sum(w*error**2)/w.sum())
                    bins = np.minimum(9, (p*10).astype(int))
                    self.assertAlmostEqual(metric['ece'], np.abs(np.bincount(bins, weights=w*error, minlength=10)).sum()/w.sum())
                self.assertLess(compute.oracle_check(np, data, params)['max_prediction_error'], 1e-10)
            finally: data.close()
            with self.assertRaisesRegex(ValueError, 'truncated'): compute.Input(np, path, len(values)+1, compute.GIB, lambda: None)

    def test_no_fixed_sample_cap_and_both_player_balance(self):
        rows = [{'link': i+1, 'white_name': f'w{i%20}', 'black_name': f'b{i}', 'mode': 'bullet',
                 'played_at': datetime(2022, 1, 1, tzinfo=timezone.utc), 'sample_order': 3000-i} for i in range(3000)]
        with patch.object(source.balance, 'held_player', return_value=False):
            selected, audit = source.choose_maximum_pool(deepcopy(rows), POLICY)
            reverse, _ = source.choose_maximum_pool(list(reversed(deepcopy(rows))), POLICY)
        self.assertEqual(len(selected), 3000); self.assertEqual(selected, reverse)
        self.assertLessEqual(audit[0]['largest_player_share'], .1)
        rows += [{**rows[0], 'link': 10000+i, 'white_name': 'dominant'} for i in range(5000)]
        with patch.object(source.balance, 'held_player', return_value=False):
            selected, audit = source.choose_maximum_pool(rows, POLICY)
        self.assertLessEqual(audit[0]['largest_player_share'], .1)
        self.assertGreater(len(selected), 3000)

    def test_missing_mode_support_does_not_train_or_evaluate_tests(self):
        groups = [{'mode': 'rapid', 'split': s, 'games': 0, 'cp_rows': 0} for s in (0, 1)]
        result = compute.fit_mode(np, Path('/unused'), groups, 'rapid', {'a': 0., 'b': 0.}, compute.GIB, lambda: None, lambda *a: None)
        self.assertEqual(result['status'], 'insufficient_data')

    def test_request_and_config_are_frozen_and_development_only(self):
        row = {'status': 'complete', 'result': {'format': 'chessism_blunder_strength_comparison_v1',
            'status': 'passed', 'production_changed': False, 'checks': {'decisions': 0},
            'frozen_fits': [{'mode': m, 'a': .2, 'b': 1.} for m in model.MODES]}}
        config = job.configuration('00000000-0000-0000-0000-000000000001', row, POLICY, {'training': [], 'validation': [], 'protected': []})
        self.assertEqual(config['frozen_reference_fits'][0]['a'], .1)
        self.assertEqual(config['frozen_reference_fits'][1]['a'], .2)
        self.assertEqual(row['result']['frozen_fits'][0]['a'], .2)
        self.assertEqual(config['threshold'], .15); self.assertEqual(config['final_test_policy'], 'not_loaded_or_evaluated')
        from chessism_api.routers.research_algorithms import FullDevelopmentRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            FullDevelopmentRequest(parent_run_id=config['parent_run_id'], acknowledgement='large_cuda_development_only_keep_tests_untouched', threshold=.2)


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_reservation_retains_training_and_validation_roles(self):
        roles = {'training': [10, 11], 'validation': [12]}
        report = {'format': source.RESERVATION, 'source': {'reserved_roles': roles, 'reserved_roles_sha256': history.digest(roles)}}
        response = MagicMock(); response.scalars.return_value.all.return_value = [report]
        session = AsyncMock(); session.execute.return_value = response
        self.assertEqual(await source.additional_roles(session), ({10, 11}, {12}))
        roles['training'].append(12)
        with self.assertRaisesRegex(ValueError, 'checksum'): await source.additional_roles(session)

    async def test_full_training_enters_all_future_exclusions(self):
        from tests.test_blunder_development_reuse import session_for, result_row, REF
        with patch.object(source, 'additional_roles', AsyncMock(return_value=({10, 11}, {12}))):
            roles, known, _ = await history.history(session_for([result_row()]), history.plan([REF]), AsyncMock())
        self.assertTrue({10, 11} <= set(roles['training'])); self.assertIn(12, roles['validation'])
        self.assertTrue({10, 11, 12} <= known)
