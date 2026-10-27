"""GPU job wiring and comparison guards; no GPU or database required."""
from copy import deepcopy
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import unittest
import httpx
from fastapi import Depends, FastAPI

from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole

from chessism_api.operations.research_algorithms import score_validation_job as job, jobs
from chessism_api.operations.research_algorithms.repository import RunCancelled
from chessism_api.routers import research_algorithms as routes


def groups():
    return {'blitz': {'counts': [0]*15, 'mates': [{'moves': 2, 'side': 'for', 'positions': 3}]}}


class ContractTests(unittest.TestCase):
    def test_one_player_frozen_allowlisted_configuration(self):
        config = job.configuration(' Chefshouse ')
        self.assertEqual(config['player'], 'chefshouse')
        self.assertEqual(config['backend'], 'cuda_cupy')
        self.assertEqual(config['matrix']['filters']['players'], ['chefshouse'])
        self.assertEqual(config['matrix']['summary_input_version'], 2)
        self.assertEqual(config['columns'], ['player','mode','player_score','score_kind','mate_moves'])
        with self.assertRaises(ValueError): job.configuration(' ')

    def test_comparison_includes_bins_missing_modes_mate_distances_and_side(self):
        original = groups()
        self.assertEqual(job.compare_groups(original, deepcopy(original))['mismatches'], 0)
        for mutate in (lambda x: x['blitz']['counts'].__setitem__(0, 1),
                       lambda x: x['blitz']['mates'][0].update(moves=None),
                       lambda x: x['blitz']['mates'][0].update(side='against')):
            changed = deepcopy(original); mutate(changed)
            self.assertEqual(job.compare_groups(original, changed)['status'], 'failed')
        self.assertEqual(job.compare_groups(original, {})['status'], 'failed')

    def test_benchmark_is_opt_in_and_bound_to_known_sizes(self):
        self.assertNotIn('benchmark',job.configuration('alice'))
        self.assertEqual(job.configuration('alice',benchmark=True)['benchmark'],
            {'version':1,'batch_sizes':[8192,65536,262144],'repeats':3})


class WiringTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        override = patch.object(job, 'WORK_ROOT', Path(directory.name))
        override.start()
        self.addCleanup(override.stop)

    async def test_http_authentication_and_bounded_request(self):
        app = FastAPI()
        app.include_router(routes.router, prefix='/research/algorithms', dependencies=[Depends(require_superuser)])
        app.dependency_overrides[routes.get_redis_pool] = lambda: object()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            path='/research/algorithms/cuda-validation'
            self.assertEqual((await client.post(path,json={'player':'alice'})).status_code,401)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.user,id=None)
            self.assertEqual((await client.post(path,json={'player':'alice'})).status_code,403)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.admin,id=None)
            self.assertEqual((await client.post(path,json={'player':'a'*129})).status_code,422)
            with patch.object(routes,'enqueue_run',AsyncMock(return_value={'status':'queued'})) as enqueue:
                self.assertEqual((await client.post(path,json={'player':'alice'})).status_code,202)
                enqueue.assert_awaited_once()

    async def test_no_gpu_fails_before_source_reads(self):
        with patch.object(job, 'cuda_backend', side_effect=RuntimeError('CUDA unavailable')), patch.object(job, 'freeze_source', AsyncMock()) as freeze:
            with self.assertRaisesRegex(RuntimeError, 'CUDA unavailable'):
                await job.execute(job.configuration('alice'), AsyncMock())
            freeze.assert_not_awaited()

    async def test_modified_config_rejected_before_db(self):
        config = job.configuration('alice'); config['matrix']['filters']['players'] = ['bob']
        factory = MagicMock()
        with self.assertRaisesRegex(ValueError, 'modified'):
            await job.freeze_source(config, AsyncMock(), io.BytesIO(), session_factory=factory)
        factory.assert_not_called()

    async def test_summary_disagreement_is_not_a_pass(self):
        report = {'status':'passed', 'groups': [{'mode':'blitz','cuda':[0]*15,'mates_cuda':[]}], 'timings':{}, 'notes':['offline']}
        comparison = SimpleNamespace(rows=1,report=lambda:report,consume=lambda batch:None)
        with patch.object(job, 'cuda_backend', return_value=(None,{})), patch.object(job, 'ensure_capacity'), patch.object(job, 'freeze_source', AsyncMock(return_value=(groups(),groups(),{'rows':1},{}))), patch.object(job, 'Comparison', return_value=comparison):
            result = await job.execute(job.configuration('alice'), AsyncMock())
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['reference_checks']['saved_cpu_summary']['status'], 'failed')

    async def test_existing_queue_dispatches_cuda_without_cpu_normalization(self):
        row = SimpleNamespace(status='queued', config=job.configuration('alice'), started_at=None)
        session = AsyncMock(); session.begin = MagicMock(); session.get.return_value = row
        factory = MagicMock(); factory.return_value.__aenter__.return_value = session
        result = {'status':'passed','timings':{},'included_rows':10}
        with patch.object(jobs, 'AsyncDBSession', factory), patch.object(jobs, 'Reporter', return_value=AsyncMock()), patch.object(job, 'execute', AsyncMock(return_value=result)) as execute, patch.object(jobs, 'publish', AsyncMock()) as publish, patch.object(jobs, 'normalize_algorithm') as cpu:
            reply = await jobs.run_algorithm_job({}, run_id='test')
        self.assertEqual(reply['validation'], 'passed')
        execute.assert_awaited_once(); publish.assert_awaited_once(); cpu.assert_not_called()

    async def test_cancellation_prevents_gpu_calculation(self):
        reporter = AsyncMock(); reporter.check.side_effect = RunCancelled('cancelled')
        with patch.object(job, 'cuda_backend', return_value=(None,{})), patch.object(job, 'ensure_capacity'), patch.object(job, 'freeze_source', AsyncMock(return_value=({},{},{'rows':1},{}))), patch.object(job, 'Comparison') as compare:
            with self.assertRaises(RunCancelled): await job.execute(job.configuration('alice'), reporter)
        compare.assert_not_called()

    async def test_cancellation_between_gpu_batches_closes_temporary_stream(self):
        reporter = AsyncMock()
        reporter.check.side_effect = [None, RunCancelled('cancelled')]
        stream = io.BytesIO(b'0'*job.DTYPE.itemsize)
        with patch.object(job, 'cuda_backend', return_value=(None,{})), patch.object(job, 'ensure_capacity'), patch.object(job.tempfile, 'TemporaryFile', return_value=stream), patch.object(job, 'freeze_source', AsyncMock(return_value=({},{},{'rows':1},{}))), patch.object(job, 'Comparison') as compare:
            with self.assertRaises(RunCancelled): await job.execute(job.configuration('alice'), reporter)
        compare.return_value.consume.assert_not_called()
        self.assertTrue(stream.closed)

    async def test_endpoint_enqueues_only_a_bounded_validation(self):
        with patch.object(routes, 'enqueue_run', AsyncMock(return_value={'status':'queued'})) as enqueue:
            reply = await routes.start_cuda_validation(routes.CudaValidationRequest(player='Alice'), SimpleNamespace(id='a'), object())
        self.assertEqual(reply['status'], 'queued')
        config = enqueue.call_args.kwargs['draft'][1]
        self.assertEqual(config, job.configuration('alice'))

    async def test_benchmark_endpoint_uses_only_fixed_instructions(self):
        with patch.object(routes,'enqueue_run',AsyncMock(return_value={'status':'queued'})) as enqueue:
            await routes.start_cuda_validation(routes.CudaValidationRequest(player='Alice',benchmark=True),SimpleNamespace(id='a'),object())
        self.assertEqual(enqueue.call_args.kwargs['draft'][1],job.configuration('alice',benchmark=True))
        config=job.configuration('alice',benchmark=True)
        config['benchmark']['batch_sizes']=[2000000]
        factory=MagicMock()
        with self.assertRaisesRegex(ValueError,'modified'):
            await job.freeze_source(config,AsyncMock(),io.BytesIO(),session_factory=factory)
        factory.assert_not_called()


if __name__ == '__main__': unittest.main()
