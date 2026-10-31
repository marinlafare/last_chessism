"""Offline refinement selection, leakage, uncertainty and wiring checks."""
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_refinement as r
from chessism_api.operations.research_algorithms import blunder_refinement_audit as audit
from chessism_api.operations.research_algorithms import blunder_v2_job as job
from chessism_api.operations.research_algorithms import blunder_v2_model as model
from chessism_api.operations.research_algorithms import blunder_v2_validation as validation
from chessism_api.operations.research_algorithms.blunder_balanced_sampling import plan
from tests.test_blunder_balanced import REF
from tests.test_blunder_v2 import population


def trial(mse=.12, ece=.02, **kw):
    return {'a': .2, 'b': .3, 'converged': True, 'search_boundary': False,
            'regularization': .01, 'rating_strength': .5, 'mse': mse, 'ece': ece, **kw}


class SelectionTests(TestCase):
    def test_lowest_mse_eligible_not_lowest_mse_overall(self):
        candidates = [trial(.10, .08), trial(.12, .025), trial(.14, .01), trial(.199, .001)]
        chosen, summary = r.select(candidates, {'mse': .2, 'ece': .04})
        self.assertEqual(chosen, 1)
        self.assertEqual(summary['status'], 'passed')
        self.assertAlmostEqual(summary['required_mse_gain'], .05)
        self.assertFalse(candidates[3]['predictive_eligible'])

    def test_no_qualifying_candidate_does_not_relax_rules(self):
        candidates = [trial(.1, .04), trial(.199, .01)]
        chosen, summary = r.select(candidates, {'mse': .2, 'ece': .02})
        self.assertEqual(chosen, 0)
        self.assertEqual(summary['status'], 'not_passed')
        self.assertEqual(summary['eligible_candidates'], 0)
        chosen, summary = r.select([trial(.21, .001)], {'mse': .2, 'ece': .02})
        self.assertEqual(summary['status'], 'not_passed')

    def test_invalid_search_and_nonfinite_cannot_be_selected(self):
        index, _ = r.select([trial(.01, .001, search_boundary=True), trial(.02, .001, converged=False),
                            trial(float('nan')), trial()], {'mse': .2, 'ece': .02})
        self.assertEqual(index, 3)
        self.assertIsNone(r.select([trial(converged=False)], {'mse': .2, 'ece': .02})[0])

    def test_validation_statistics_equal_game_weight_and_separate_bins(self):
        rows, games = population(8)
        games['split'][:] = 1
        params = [[0., 0.], [.2, .3], [.21, .37]]
        first = r.validation_scores(np, rows, games, 0, params)
        # Independent per-game metric accumulator checks both binning and loss.
        for values, (a, b) in zip(first, params):
            matrix = np.zeros((8, validation.METRIC_COLUMNS))
            p = model.expected(np, rows['after'], games['u'], games['d'], a, b)
            validation.add_metrics(np, matrix, rows['game'], p, p, games['outcome'])
            expected = audit.describe_group(np, matrix, np.ones(8, dtype=bool))
            self.assertAlmostEqual(values['ece'], expected['ece'])
            self.assertAlmostEqual(values['mse'], expected['mse'])
        expanded = np.concatenate([rows[:1].repeat(20), rows[1:]])
        games['cp_count'][0] = 20
        second = r.validation_scores(np, expanded, games, 0, params)
        np.testing.assert_allclose([[t['mse'], t['ece']] for t in first], [[t['mse'], t['ece']] for t in second], atol=1e-15)

    def test_selection_never_reads_test_or_held_out_outcomes(self):
        rows, games = population(1200)
        games['split'][700:900] = 1
        games['split'][900:1100] = 2
        games['split'][1100:] = 3
        def fitted(objective, value):
            return {'a': .21, 'b': .37, 'regularization': value, 'converged': True, 'search_boundary': False}
        with patch.object(model, 'fit', side_effect=fitted):
            first = r.fit_mode(np, rows, games, 0)
            games['outcome'][900:] = np.nan
            rows['after'][900:] = np.nan
            second = r.fit_mode(np, rows, games, 0)
        self.assertEqual(first, second)
        self.assertEqual(len(first['refinement_trials']), 28)
        self.assertEqual(first['selection']['status'], 'passed')
        self.assertEqual(first['rating_strength'], 1.)
        json.dumps(first, allow_nan=False)

    def test_candidate_budget_cancellation_and_sparse_data(self):
        rows, games = population(10)
        self.assertEqual(r.fit_mode(np, rows, games, 0)['status'], 'insufficient_data')
        with self.assertRaisesRegex(ValueError, 'budget'):
            r.validation_scores(np, rows, games, 0, [[0., 0.]]*30)
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            r.validation_scores(np, rows, games, 0, [[0., 0.]], lambda: (_ for _ in ()).throw(RuntimeError('cancel')))

    def test_config_versioned_and_fixed_rules(self):
        legacy = job.configuration()
        self.assertEqual(legacy['implementation_version'], 2)
        self.assertNotIn('refinement', legacy)
        config = job.configuration(1000, plan([REF]), r.policy())
        self.assertEqual(config['implementation_version'], 3)
        self.assertEqual(config['model']['k0'], .00368208)
        self.assertEqual(config['model']['threshold'], .2)
        self.assertEqual(config['validation']['overall_ece'], .03)
        self.assertFalse(config['canonical'])
        with self.assertRaisesRegex(ValueError, 'requires'):
            job.configuration(1000, None, r.policy())
        altered = r.policy(); altered['validation_ece_limit'] = .1
        with self.assertRaisesRegex(ValueError, 'requires'):
            job.configuration(1000, plan([REF]), altered)


class PlayerUncertaintyTests(TestCase):
    def metrics(self, count):
        matrix = np.zeros((count, validation.METRIC_COLUMNS))
        p = np.linspace(.05, .95, count)
        validation.add_metrics(np, matrix, np.arange(count), p, p, (p > .5).astype(float))
        return matrix

    def test_components_join_players_across_colors_and_transitively(self):
        meta = [{'white_name': ' A ', 'black_name': 'b'}, {'white_name': 'c', 'black_name': 'a'},
                {'white_name': 'x', 'black_name': 'y'}, {'white_name': 'B', 'black_name': 'z'}]
        groups = audit.components(meta)
        self.assertEqual(groups[0], groups[1]); self.assertEqual(groups[0], groups[3])
        self.assertNotEqual(groups[0], groups[2])

    def test_giant_component_does_not_claim_independent_precision(self):
        meta = [{'white_name': 'owner', 'black_name': f'p{i}'} for i in range(50)]
        result = audit.player_intervals(np, self.metrics(50), meta, np.ones(50, dtype=bool), repeats=32)
        self.assertEqual(result['status'], 'insufficient_independent_components')
        self.assertEqual(result['components'], 1)
        self.assertNotIn('ece_ci95', result)

    def test_paired_cluster_intervals_identical_models_and_determinism(self):
        meta = [{'white_name': f'w{i//2}', 'black_name': f'b{i//2}'} for i in range(60)]
        matrix = self.metrics(60)
        result = audit.player_intervals(np, matrix, meta, np.ones(60, dtype=bool), repeats=32)
        self.assertEqual(result['status'], 'estimated')
        self.assertEqual(result['components'], 30)
        self.assertEqual(result['ece_change_ci95'], [0., 0.])
        self.assertEqual(result['relative_mse_change_ci95'], [0., 0.])
        self.assertEqual(result, audit.player_intervals(np, matrix, meta, np.ones(60, dtype=bool), repeats=32))
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            audit.player_intervals(np, matrix, meta, np.ones(60, dtype=bool), lambda: (_ for _ in ()).throw(RuntimeError('cancel')))


class WithheldTestChecks(IsolatedAsyncioTestCase):
    async def execute_fixture(self, passed):
        rows, games = population(120)
        games['split'][:30] = 0; games['split'][30:60] = 1; games['split'][60:] = 2
        # Poison only the final test. Reading these labels would create NaNs.
        if not passed:
            games['outcome'][60:] = np.nan
        meta = [{'link': i+1, 'white_elo': 2300, 'black_elo': 2300, 'white_end': 'win', 'black_end': 'resigned',
                 'white_name': f'w{i}', 'black_name': f'b{i}'} for i in range(120)]
        async def freeze(config, reporter, stream):
            stream.write(rows.tobytes())
            return meta, games, {'sha256': 'a'*64, 'positions': len(rows), 'selected_games': len(meta), 'extraction_seconds': 0.,
                'sampling_audit': {'cp_concentration': [{'status': 'passed'}], 'prior_game_overlap': 0, 'limitations': []}}
        @contextmanager
        def workspace(_):
            with tempfile.TemporaryDirectory() as folder:
                yield Path(folder)
        def fit(xp, rows, games, index, check):
            return {'mode': model.MODES[index], 'status': 'fitted', 'a': .2, 'b': .3,
                'search_boundary': False, 'selection': {'status': 'passed' if passed else 'not_passed'}, 'games_by_split': [30, 30, 60, 0]}
        reporter = AsyncMock(); reporter.run_id = '00000000-0000-0000-0000-000000000001'
        with patch.object(job, 'cuda_backend', return_value=(np, {'name': 'offline oracle'})), \
             patch.object(job, 'ensure_capacity'), patch.object(job, 'workspace', workspace), \
             patch.object(job.source, 'freeze', freeze), patch.object(r, 'fit_mode', fit), \
             patch.object(validation, 'BOOTSTRAPS', 16):
            return await job.execute(job.configuration(1000, plan([REF]), r.policy()), reporter)

    async def test_full_job_keeps_test_outcomes_out_after_failed_selection(self):
        result = await self.execute_fixture(False)
        self.assertFalse(result['refinement']['test_evaluated'])
        self.assertEqual(result['checked_moves'], 30)
        self.assertEqual(result['test']['status'], 'not_run')
        self.assertEqual(result['status'], 'development_only')
        self.assertEqual(result['gates']['overall_calibration'], 'not_run')
        self.assertEqual(result['gates']['calibration_aware_selection'], 'not_passed')
        self.assertEqual(result['refinement']['development']['overall']['games'], 30)
        self.assertFalse(result['promotion_ready'])
        self.assertFalse(result['production_changed'])
        json.dumps(result, allow_nan=False)

    async def test_qualifying_selection_evaluates_test_but_never_promotes(self):
        result = await self.execute_fixture(True)
        self.assertTrue(result['refinement']['test_evaluated'])
        self.assertEqual(result['checked_moves'], 90)
        self.assertEqual(result['test']['games'], 60)
        self.assertEqual(result['gates']['calibration_aware_selection'], 'passed')
        self.assertEqual(result['player_cluster_uncertainty']['test']['status'], 'estimated')
        self.assertEqual(result['gates']['representative_population_audit'], 'not_run')
        self.assertFalse(result['promotion_ready'])
        self.assertFalse(result['canonical'])
        self.assertFalse(result['production_changed'])
        json.dumps(result, allow_nan=False)

    async def test_modified_refinement_is_rejected_before_gpu_or_source(self):
        config = job.configuration(1000, plan([REF]), r.policy())
        config['refinement']['minimum_relative_mse_improvement'] = 0
        with patch.object(job, 'cuda_backend') as backend:
            with self.assertRaises(ValueError):
                await job.execute(config, AsyncMock())
            backend.assert_not_called()
