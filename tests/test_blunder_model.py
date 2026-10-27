"""Rating candidate correctness; no database, CUDA, network or live summaries."""
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import Depends, FastAPI
import numpy as np

from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole
from chessism_api.operations.research_algorithms import blunder_model as model, blunder_training as training, jobs
from chessism_api.routers import research_algorithms as routes


def synthetic_histogram(coefficient=.005):
    # Fractional sufficient statistics represent a population of independent
    # Bernoulli observations with known conditional expectation.
    probability = model.expected_points(np, model.CP, coefficient)
    return np.column_stack([np.ones(2001), probability, probability])


class ModelTests(unittest.TestCase):
    def test_group_boundaries_and_unsupported_ratings(self):
        for rating, expected in [(500,0),(1199,0),(1200,1),(1399,1),(1400,2),(2599,7),(2600,8),(3500,8)]:
            self.assertEqual(model.group_id('blitz', rating, rating), expected*3+1)
        for args in [('blitz',0,1500),('blitz',1200,1500),('daily',1500,1500)]:
            with self.assertRaises(ValueError): model.group_id(*args)

    def test_game_splits_are_deterministic_and_cover_all_partitions(self):
        self.assertEqual([model.split_game(i) for i in range(500)], [model.split_game(i) for i in range(500)])
        self.assertEqual(set(model.split_game(i) for i in range(500)), {0,1,2})

    def test_equal_game_weight_exclusions_and_no_duplicate_game(self):
        dataset = model.Dataset()
        positions = [(20,500,0),(21,100,0),(22,-100,0),(23,9998,1),(24,500,2),(25,None,-1)]
        split = dataset.add_game(1, 0, 1., positions)
        self.assertAlmostEqual(dataset.hist[0,split,:,0].sum(), 1)
        self.assertEqual(dataset.positions[0,split],2)
        self.assertEqual(dataset.hist[0,split,1100,0],.5)
        with self.assertRaises(ValueError): dataset.add_game(1,0,1.,positions)
        dataset.add_game(2,0,.5,[(21,0,0)] * 100)
        self.assertAlmostEqual(dataset.hist[:,:,:,0].sum(),2)
        dataset.add_game(3,0,0.,[(1,10,0),(21,9998,1)])
        self.assertEqual(dataset.empty_games,1)

    def test_fit_recovers_known_curve_and_independent_scalar_loss(self):
        histogram = synthetic_histogram()
        coefficient = model.fit(np, histogram)
        self.assertAlmostEqual(coefficient,.005,places=7)
        predicted = 1/(1+np.exp(-.005 * model.CP))
        direct = np.mean(predicted*(1-predicted)**2+(1-predicted)*predicted**2)
        self.assertAlmostEqual(float(model.loss(np,histogram,[.005])[0]),direct,places=12)

    def test_symmetry_monotonicity_and_clip(self):
        p = model.expected_points(np,model.CP,.005)
        np.testing.assert_allclose(p,1-p[::-1],atol=1e-15)
        self.assertTrue(np.all(np.diff(p)>0))
        self.assertEqual(float(model.expected_points(np,0,.005)),.5)
        self.assertEqual(model.expected_points(np,2000,.005),model.expected_points(np,1000,.005))

    def test_cp_mate_tablebase_missing_and_rating_aware_decisions(self):
        before=np.array([100,100,0,9998,9998,100,100,np.nan])
        after=np.array([-100,-100,-9999,700,-9999,-9999,0,0])
        bk=np.array([0,0,0,1,1,2,-1,0]); ak=np.array([0,0,1,0,1,1,0,0])
        np.testing.assert_array_equal(model.decisions(np,before,after,bk,ak,.005),[1,1,1,1,1,-1,-1,-1])
        self.assertEqual(model.decisions(np,before[:1],after[:1],bk[:1],ak[:1],.001)[0],0)

    def test_transition_perspective_and_missing_predecessor(self):
        stream=io.BytesIO(); dataset=model.Dataset()
        metadata={'link':1,'mode':'blitz','white_elo':1500,'black_elo':1500,'white_result':1}
        training.add_game(dataset,stream,metadata,[(1,20,0),(2,-100,0),(4,9998,1)])
        rows=np.frombuffer(stream.getvalue(),dtype=model.TRANSITION)
        self.assertEqual(rows['before'][0],15)
        self.assertEqual((rows['before'][1],rows['after'][1]),(-20,100))
        self.assertTrue(np.isnan(rows['before'][2])); self.assertEqual(rows['before_kind'][2],-1)
        self.assertEqual(rows['after'][2],-9998)

    def test_sparse_groups_do_not_borrow_a_coefficient_or_require_gpu(self):
        result=model.fit_group(model.Dataset(),0)
        self.assertEqual(result['status'],'insufficient_games')
        self.assertNotIn('coefficient',result)

    def test_queries_are_bounded_by_positions_not_only_game_count(self):
        games=[{'link':i,'total_positions':10000} for i in range(10)]
        blocks=list(training.game_blocks(games))
        self.assertEqual([len(block) for block in blocks],[3,3,3,1])
        with self.assertRaisesRegex(ValueError,'bounded'):
            list(training.game_blocks([{'link':1,'total_positions':100000}]))

    def test_model_fit_never_reads_validation_or_test_labels(self):
        # Verify the coefficient is fitted using training only, while metrics
        # can change independently when held-out outcomes change.
        dataset=model.Dataset(); dataset.games[0]=[500,50,50]
        dataset.hist[0]=[synthetic_histogram(.004),synthetic_histogram(.006),synthetic_histogram(.008)]
        fake_cuda=SimpleNamespace(**{key:getattr(np,key) for key in ('asarray','atleast_1d','exp','clip','sum','argmin')},
            cuda=SimpleNamespace(get_current_stream=lambda:SimpleNamespace(synchronize=lambda:None)))
        with patch('chessism_api.operations.research_algorithms.cuda_runtime.cuda_backend',return_value=(fake_cuda,{})):
            first=model.fit_group(dataset,0)
            dataset.hist[0,1:]=synthetic_histogram(.001)
            second=model.fit_group(dataset,0)
        self.assertEqual(first['coefficient'],second['coefficient'])
        self.assertNotEqual(first['metrics']['test'],second['metrics']['test'])
        # AlgorithmRun.result uses strict JSON, not a NumPy-aware serializer.
        self.assertEqual(json.loads(json.dumps(first,allow_nan=False)),first)


class WiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_superuser_access_and_http_bounds(self):
        app=FastAPI()
        app.include_router(routes.router,prefix='/research/algorithms',dependencies=[Depends(require_superuser)])
        app.dependency_overrides[routes.get_redis_pool]=lambda:object()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            path='/research/algorithms/blunder-training'
            self.assertEqual((await client.post(path,json={})).status_code,401)
            app.dependency_overrides[get_current_account]=lambda:SimpleNamespace(role=AccountRole.user,id=None)
            self.assertEqual((await client.post(path,json={})).status_code,403)
            app.dependency_overrides[get_current_account]=lambda:SimpleNamespace(role=AccountRole.admin,id=None)
            for value in [True,99,3001,1000.5]:
                self.assertEqual((await client.post(path,json={'games_per_group':value})).status_code,422)
            with patch.object(routes,'enqueue_run',AsyncMock(return_value={'status':'queued'})):
                self.assertEqual((await client.post(path,json={'games_per_group':1000})).status_code,202)

    async def test_allowlisted_config_and_request_bounds(self):
        for value in [True,0,99,3001,1000.5]:
            with self.assertRaises(ValueError): training.configuration(value)
        config=training.configuration(); config['threshold_expected_points']=.01
        factory=MagicMock()
        with self.assertRaisesRegex(ValueError,'modified'):
            await training.freeze_source(config,AsyncMock(),io.BytesIO(),session_factory=factory)
        factory.assert_not_called()

    async def test_endpoint_enqueues_candidate_not_production(self):
        with patch.object(routes,'enqueue_run',AsyncMock(return_value={'status':'queued'})) as enqueue:
            await routes.start_blunder_training(routes.BlunderTrainingRequest(games_per_group=1200),SimpleNamespace(id='a'),object())
        config=enqueue.call_args.kwargs['draft'][1]
        self.assertEqual(config,training.configuration(1200))
        self.assertFalse(config['canonical'])

    async def test_queue_dispatches_training_before_cpu_builder(self):
        row=SimpleNamespace(status='queued',config=training.configuration(),started_at=None)
        session=AsyncMock(); session.begin=MagicMock(); session.get.return_value=row
        factory=MagicMock(); factory.return_value.__aenter__.return_value=session
        result={'status':'candidate','included_rows':1,'timings':{}}
        with patch.object(jobs,'AsyncDBSession',factory),patch.object(jobs,'Reporter',return_value=AsyncMock()),patch.object(training,'execute',AsyncMock(return_value=result)),patch.object(jobs,'publish',AsyncMock()) as publish,patch.object(jobs,'normalize_algorithm') as cpu:
            reply=await jobs.run_algorithm_job({},run_id='test')
        self.assertEqual(reply['model_status'],'candidate'); publish.assert_awaited_once(); cpu.assert_not_called()

    async def test_gpu_unavailable_fails_before_database_reads(self):
        with patch.object(training,'cuda_backend',side_effect=RuntimeError('no CUDA')),patch.object(training,'freeze_source',AsyncMock()) as freeze:
            with self.assertRaisesRegex(RuntimeError,'no CUDA'):
                await training.execute(training.configuration(),AsyncMock())
        freeze.assert_not_awaited()


if __name__=='__main__': unittest.main()
