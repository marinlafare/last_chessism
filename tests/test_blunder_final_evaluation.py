"""Frozen final evaluation: explicit rapid exception, no tuning, no gate laundering."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_final_evaluation as final
from chessism_api.operations.research_algorithms import blunder_development_reuse as reuse
from chessism_api.operations.research_algorithms import blunder_refinement, blunder_v2_job
from tests.test_blunder_balanced import REF
from tests.test_blunder_development_reuse import frozen
from tests.test_blunder_v2 import population

PARENT_ID = '00000000-0000-4000-8000-000000000077'


def parent_row(source=None, config=None):
    config = config or blunder_v2_job.configuration(1000, reuse.plan([REF]), blunder_refinement.policy())
    source = source or frozen({'training': [1, 2], 'validation': [3, 4], 'protected': [5, 6, 7]})
    report = {'format': reuse.REPORT_FORMAT, 'status': 'development_only', 'config': config,
        'source': source, 'canonical': False, 'production_changed': False, 'model_id': 'm'*64,
        'implementation_sha256': 'i'*64,
        'refinement': {'policy': blunder_refinement.policy(), 'selection_status': 'not_passed', 'test_evaluated': False},
        'gates': {**dict.fromkeys(('numerical', 'mathematical', 'search', 'historical_partition_safety'), 'passed'),
                  'calibration_aware_selection': 'not_passed', 'player_balance': 'not_passed'},
        'fits': [{'mode': mode, 'a': .1, 'b': .3, 'status': 'fitted', 'search_boundary': False,
                  'selection': {'status': 'not_passed' if mode == 'rapid' else 'passed'}} for mode in final.model.MODES]}
    return {'status': 'complete', 'config': config, 'result': report}


class ContractTests(TestCase):
    def test_frozen_identity_and_explicit_policy(self):
        row = parent_row(); config = final.configuration(PARENT_ID, row)
        self.assertEqual(config['parent_report_sha256'], reuse.digest(row['result']))
        self.assertEqual(config['fits_sha256'], reuse.digest(row['result']['fits']))
        self.assertEqual(config['policy']['exception_modes'], ['rapid'])
        self.assertEqual(row['config']['model']['k0'], .00368208)
        self.assertEqual(row['config']['model']['threshold'], .2)
        self.assertFalse(config['canonical'])

    def test_reject_evaluated_or_unqualified_parents(self):
        for mutate in (
            lambda r: r.update(status='failed'),
            lambda r: r['result']['refinement'].update(test_evaluated=True),
            lambda r: r['result']['gates'].update(numerical='failed'),
            lambda r: r['result']['fits'][0]['selection'].update(status='not_passed'),
            lambda r: r['result']['fits'][1].update(a=float('nan')),
            lambda r: r['result']['fits'][2].update(b=5.),
            lambda r: r['result']['fits'][2].update(status='insufficient_data'),
            lambda r: r['result'].update(canonical=True),
        ):
            row = parent_row(); mutate(row)
            with self.assertRaises(ValueError): final.configuration(PARENT_ID, row)

    def test_source_identity_and_partition_drift_fail_closed(self):
        parent = parent_row()['result']; source = deepcopy(parent['source'])
        metadata = [{'link': i, 'split': s} for s, ids in enumerate(([1, 2], [3, 4], [5], [6])) for i in ids]
        final.verify_replay(parent, metadata, source)
        for key in ('sha256', 'positions', 'selected_games', 'split', 'seen_game_ids_sha256', 'development_roles_sha256'):
            changed = {**source, key: 'changed'}
            with self.assertRaisesRegex(ValueError, 'Frozen input changed'):
                final.verify_replay(parent, metadata, changed)
        metadata[-1]['split'] = 2
        with self.assertRaisesRegex(ValueError, 'identities changed'): final.verify_replay(parent, metadata, source)

    def test_report_preserves_failed_gates_and_original_parameters(self):
        row = parent_row(); before = deepcopy(row)
        config = final.configuration(PARENT_ID, row)
        report = {'gates': {'overall_calibration': 'not_passed', 'coverage': 'insufficient_data'}, 'timings': {}}
        final.complete_report(report, row['result'], config, row['result']['source'], {'name': 'test'}, 'new')
        self.assertEqual(row, before)
        self.assertEqual(report['gates']['calibration_aware_selection'], 'not_passed')
        self.assertEqual(report['gates']['coverage'], 'insufficient_data')
        self.assertEqual(report['fits'], before['result']['fits'])
        self.assertEqual(report['model_id'], before['result']['model_id'])
        self.assertTrue(report['refinement']['test_evaluated'])
        self.assertEqual(report['refinement']['selection_status'], 'not_passed')
        self.assertFalse(report['promotion_ready'])
        self.assertFalse(report['production_changed'])
        json.dumps(report, allow_nan=False)

    def test_request_requires_acknowledgement_and_forbids_tuning(self):
        from chessism_api.routers.research_algorithms import FinalBlunderRequest
        from pydantic import ValidationError
        good = {'parent_run_id': PARENT_ID, 'acknowledgement': 'rapid_provisional_evaluation_v1'}
        FinalBlunderRequest(**good)
        for values in ({'parent_run_id': PARENT_ID}, {**good, 'threshold': .15}, {**good, 'acknowledgement': 'all_modes'}):
            with self.assertRaises(ValidationError): FinalBlunderRequest(**values)


class EvaluationTests(IsolatedAsyncioTestCase):
    async def test_changed_parent_rejected(self):
        row = parent_row(); config = final.configuration(PARENT_ID, row)
        row['result']['fits'][2]['b'] += .01
        session = AsyncMock(); response = MagicMock()
        response.mappings.return_value.one_or_none.return_value = row
        session.execute.return_value = response
        with self.assertRaisesRegex(ValueError, 'changed'): await final.load_parent(session, config)

    async def test_cancel_before_claim_does_not_open_database(self):
        reporter = AsyncMock(); reporter.check.side_effect = RuntimeError('cancel')
        factory = MagicMock()
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            await final.claim({}, {}, reporter, session_factory=factory)
        factory.assert_not_called()

    async def test_frozen_runner_never_calls_fit_and_uses_only_tests(self):
        row = parent_row(); config = final.configuration(PARENT_ID, row)
        rows, games = population(120)
        games['split'][:30] = 0; games['split'][30:60] = 1
        games['split'][60:100] = 2; games['split'][100:] = 3
        games['outcome'][:60] = np.nan  # Fitting/development reads would poison metrics.
        metadata = [{'link': i+1, 'white_elo': 2300, 'black_elo': 2300, 'white_end': 'win', 'black_end': 'resigned',
                     'white_name': f'w{i}', 'black_name': f'b{i}'} for i in range(120)]
        frozen_source = {**row['result']['source'], 'extraction_seconds': .1}
        async def freeze(config, reporter, stream):
            stream.write(rows.tobytes())
            return row['result'], metadata, games, frozen_source
        reporter = AsyncMock(); reporter.run_id = PARENT_ID
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(final, 'workspace') as work, \
             patch.object(final, 'cuda_backend', return_value=(np, {'name': 'offline oracle'})), \
             patch.object(final, 'ensure_capacity'), patch.object(final, 'freeze', side_effect=freeze), \
             patch.object(final, 'claim', AsyncMock()) as claim, \
             patch.object(final, 'cancellation_check', return_value=lambda: None), \
             patch.object(final.validation, 'BOOTSTRAPS', 16), \
             patch.object(final.model, 'fit_mode', side_effect=AssertionError('refit')), \
             patch.object(blunder_refinement, 'fit_mode', side_effect=AssertionError('refit')):
            work.return_value.__enter__.return_value = Path(folder)
            report = await final.execute(config, reporter)
        claim.assert_awaited_once()
        self.assertEqual(report['checked_moves'], 60)
        self.assertEqual(report['test']['games'], 40)
        self.assertEqual(report['held_out_players']['games'], 20)
        self.assertEqual(report['timings']['fitting_seconds'], 0)
        self.assertEqual(report['fits'], row['result']['fits'])
        self.assertEqual(report['decision_mismatches'], 0)
        self.assertFalse(report['final_evaluation']['refitted'])

    async def test_changed_source_cannot_reach_test_claim(self):
        reporter = AsyncMock(); reporter.run_id = PARENT_ID
        with tempfile.TemporaryDirectory() as folder, patch.object(final, 'workspace') as work, \
             patch.object(final, 'cuda_backend', return_value=(np, {'name': 'offline'})), \
             patch.object(final, 'ensure_capacity'), patch.object(final, 'cancellation_check'), \
             patch.object(final, 'freeze', AsyncMock(side_effect=ValueError('Frozen input changed'))), \
             patch.object(final, 'claim', AsyncMock()) as claim:
            work.return_value.__enter__.return_value = Path(folder)
            with self.assertRaisesRegex(ValueError, 'Frozen input changed'):
                await final.execute(final.configuration(PARENT_ID, parent_row()), reporter)
        claim.assert_not_awaited()
