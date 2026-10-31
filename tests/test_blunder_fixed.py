"""Fixed-rule validation checks: no live database, GPU or cloud access."""
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import numpy as np
from fastapi import Depends, FastAPI

from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole
from chessism_api.operations.research_algorithms import blunder_fixed as model, blunder_validation_job as job, jobs
from chessism_api.routers import research_algorithms as routes
from tests.fixed_blunder_fixtures import synthetic


class FixedTests(unittest.TestCase):
    def test_default_and_configuration_are_fixed_not_rating_trained(self):
        config = job.configuration(' Alice ')
        self.assertEqual(config['player'], 'alice')
        self.assertEqual(config['rules']['coefficient'], .00368208)
        self.assertEqual(config['rules']['probability_drop'], .15)
        self.assertFalse(config['rules']['rating_aware'])
        self.assertFalse(config['canonical'])
        self.assertEqual(config['matrix']['filters']['players'], ['alice'])
        self.assertNotIn('player_rating', config['columns'])
        self.assertNotIn('result', config['columns'])
        with self.assertRaises(ValueError): job.configuration(' ')

    def test_scalar_parity_boundaries_mates_missing_and_tablebases(self):
        rows, _ = synthetic()
        actual = model.kernel(np, *(rows[key] for key in ('before','after','before_kind','after_kind')))
        np.testing.assert_array_equal(actual, model.reference(rows))
        self.assertEqual(actual[-16:].tolist(), [1,0,1,0,1,0,1,0,0,-1,-1,-1,-1,-1,1,1])

    def test_perspective_first_move_and_exact_predecessor(self):
        meta = {'link': 1, 'is_white': True, 'total_positions': 4}
        positions = [(1,20,0),(2,-200,0),(3,0,0),(4,9998,1)]
        white = job.pack_game(0, meta, positions, {(1,1):0,(1,3):0})
        self.assertEqual(white['before'].tolist(), [15,-200])
        black = job.pack_game(0, {**meta,'is_white':False}, positions, {(1,2):0,(1,4):1})
        self.assertEqual(black['before'].tolist(), [-20,0])
        self.assertEqual(black['after'].tolist(), [200,-9998])
        for bad in (positions[:-1], [positions[0],positions[0],*positions[2:]],
                    [positions[0],(3,-200,0),*positions[2:]],
                    [positions[0],(2,None,-1),*positions[2:]],
                    [positions[0],(2,np.inf,0),*positions[2:]]):
            with self.assertRaises(ValueError): job.pack_game(0,meta,bad,{(1,1):0,(1,3):0})
        with self.assertRaisesRegex(ValueError, 'omitted'):
            job.pack_game(0,meta,positions,{})

    def test_corrupt_stream_fails(self):
        with self.assertRaisesRegex(ValueError, 'Truncated'): model.read_batch(io.BytesIO(b'x'))

    def test_sql_matches_production_threshold_not_new_point20(self):
        sql = job.baseline_sql(SimpleNamespace(info={}))
        self.assertIn('bp-ap>=15.0', sql)
        self.assertIn('-0.00368208', sql)
        self.assertIn('LAG(player_score)', sql)
        self.assertIn('LIMIT :row_limit', sql)


class WiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_config_cannot_override_rules_or_source(self):
        config = job.configuration('alice'); config['rules']['probability_drop'] = .20
        factory = MagicMock()
        with self.assertRaisesRegex(ValueError, 'modified'):
            await job.freeze_source(config,AsyncMock(),io.BytesIO(),session_factory=factory)
        factory.assert_not_called()

    async def test_gpu_failure_precedes_source_read_no_fallback(self):
        with patch.object(job, 'cuda_backend', side_effect=RuntimeError('no GPU')), patch.object(job, 'freeze_source', AsyncMock()) as freeze:
            with self.assertRaisesRegex(RuntimeError, 'no GPU'): await job.execute(job.configuration('alice'),AsyncMock())
        freeze.assert_not_awaited()

    async def test_cancel_before_gpu_and_between_batches(self):
        reporter = AsyncMock(); reporter.check.side_effect = jobs.RunCancelled('cancel')
        with patch.object(job, 'cuda_backend') as gpu:
            with self.assertRaises(jobs.RunCancelled): await job.execute(job.configuration('alice'),reporter)
        gpu.assert_not_called()
        reporter = AsyncMock(); reporter.check.side_effect = [None,None,jobs.RunCancelled('cancel')]
        stream = io.BytesIO()
        with patch.object(job,'cuda_backend',return_value=(None,{})),patch.object(job,'ensure_capacity'),patch.object(job.tempfile,'TemporaryFile',return_value=stream),patch.object(job,'Comparison') as compare,patch.object(job,'freeze_source',AsyncMock(return_value=([],{'rows':1},{}))):
            with self.assertRaises(jobs.RunCancelled): await job.execute(job.configuration('alice'),reporter)
        compare.return_value.consume.assert_not_called()
        self.assertTrue(stream.closed)

    async def test_endpoint_access_and_no_rule_overrides(self):
        app = FastAPI(); app.include_router(routes.router,dependencies=[Depends(require_superuser)])
        app.dependency_overrides[routes.get_redis_pool] = lambda: object()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            self.assertEqual((await client.post('/blunder-validation',json={'player':'alice'})).status_code,401)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.user,id=None)
            self.assertEqual((await client.post('/blunder-validation',json={'player':'alice'})).status_code,403)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.admin,id=None)
            for body in ({'player':''},{'player':'alice','coefficient':.01},{'player':'alice','threshold':.20}):
                self.assertEqual((await client.post('/blunder-validation',json=body)).status_code,422)
            with patch.object(routes,'enqueue_run',AsyncMock(return_value={'id':'test','status':'queued'})) as enqueue:
                self.assertEqual((await client.post('/blunder-validation',json={'player':'Alice'})).status_code,202)
            self.assertEqual(enqueue.call_args.kwargs['draft'][1],job.configuration('alice'))

    async def test_queue_dispatch_skips_training_and_cpu_builder(self):
        row = SimpleNamespace(status='queued',config=job.configuration('alice'),started_at=None)
        session = AsyncMock(); session.begin = MagicMock(); session.get.return_value=row
        factory=MagicMock();factory.return_value.__aenter__.return_value=session
        result={'status':'passed','included_rows':1,'timings':{}}
        with patch.object(jobs,'AsyncDBSession',factory),patch.object(jobs,'Reporter',return_value=AsyncMock()),patch.object(job,'execute',AsyncMock(return_value=result)) as execute,patch.object(jobs,'publish',AsyncMock()) as publish,patch.object(jobs,'normalize_algorithm') as cpu:
            reply = await jobs.run_algorithm_job({},run_id='test')
        self.assertEqual(reply['validation'],'passed');execute.assert_awaited_once();publish.assert_awaited_once();cpu.assert_not_called()


if __name__ == '__main__': unittest.main()
