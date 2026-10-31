"""Offline model, split, gate and route checks. No live data or cloud access."""
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import numpy as np
from fastapi import Depends, FastAPI

from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole
from chessism_api.operations.research_algorithms import blunder_v2_model as m, blunder_v2_source as s
from chessism_api.operations.research_algorithms import blunder_v2_validation as v, blunder_v2_job as job
from chessism_api.operations.research_algorithms import blunder_v2_calibration as calibration
from chessism_api.routers import research_algorithms as routes


def population(n=1000):
    rng = np.random.default_rng(20261028)
    rows = np.zeros(n, dtype=m.DTYPE)
    rows['game'] = np.arange(n); rows['ply'] = 1; rows['fen'] = np.arange(n)
    rows['before'] = 15; rows['after'] = rng.uniform(-1000, 1000, n)
    games = {'mode': np.zeros(n, dtype=int), 'split': np.zeros(n, dtype=int), 'cp_count': np.ones(n),
             'u': rng.uniform(-3, 1, n), 'd': rng.uniform(-2, 2, n)}
    games['outcome'] = m.expected(np, rows['after'], games['u'], games['d'], .21, .37)
    return rows, games


class ModelTests(unittest.TestCase):
    def test_105_cp_to_forced_mate_is_always_a_blunder(self):
        for rating in (600, 1500, 2300, 3000):
            u = (rating-2300)/400
            self.assertEqual(m.scalar(105, -9998, 0, 1, u, 0, .4, .8), 1)
            self.assertEqual(int(m.decisions(np, np.array(105.), np.array(-9998.),
                np.array(0), np.array(1), np.array(u), np.array(0.), .4, .8)), 1)

    def test_mates_missing_and_tablebases(self):
        cases = [(105,-9998,0,1,1),(-700,-9998,0,1,1),(-701,-9998,0,1,0),
                 (9998,700,1,0,1),(9998,701,1,0,0),(9998,-9998,1,1,1),
                 (-9998,-9999,1,1,0),(100,0,2,0,-1),(100,0,0,-1,-1),(np.nan,0,0,0,-1)]
        for before, after, bk, ak, expected in cases:
            self.assertEqual(m.scalar(before,after,bk,ak,0.,0.,.2,.5), expected)

    def test_baseline_anchor_symmetry_monotonicity_and_rating_effect(self):
        c = np.arange(-1000., 1001.)
        np.testing.assert_allclose(m.expected(np,c,0,0,.6,2), 1/(1+np.exp(-m.K0*c)), atol=1e-15)
        np.testing.assert_allclose(m.expected(np,c,-2,.5,.3,.4), 1-m.expected(np,-c,-2,-.5,.3,.4), atol=1e-15)
        self.assertTrue(np.all(np.diff(m.expected(np,c,-2,.5,.3,.4)) > 0))
        self.assertNotEqual(m.expected(np,100,-2,0,.3,.4),m.expected(np,100,0,0,.3,.4))
        self.assertNotEqual(m.expected(np,100,0,0,.3,.4),m.expected(np,100,0,1,.3,.4))
        self.assertEqual(m.expected(np,5000,0,0,.3,.4),m.expected(np,1000,0,0,.3,.4))
        self.assertTrue(np.isfinite(m.expected(np,np.array([-1000,1000]),10,10,1,4)).all())

    def test_exact_threshold_and_near_boundary(self):
        for loss, expected in ((.199999,0),(.200001,1)):
            before = np.log((.5+loss)/(.5-loss))/m.K0
            self.assertEqual(m.scalar(before,0.,0,0,0.,0.,0.,0.),expected)
        self.assertEqual(m.THRESHOLD,.20)

    def test_fit_recovers_synthetic_parameters(self):
        rows, games = population()
        fit = m.fit(m.Objective(np,rows,games,0,0),0.)
        self.assertTrue(fit['converged'])
        self.assertAlmostEqual(fit['a'],.21,places=5)
        self.assertAlmostEqual(fit['b'],.37,places=5)

    def test_equal_game_weight_and_training_partition_only(self):
        rows, games = population(5)
        games['split'][4] = 2
        objective = m.Objective(np,rows,games,0,0)
        first = objective([[.3,.4]])[0]
        games['outcome'][4] = 1234
        self.assertEqual(objective([[.3,.4]])[0],first)
        expanded = np.concatenate([rows[:1].repeat(10), rows[1:]])
        games['cp_count'][0] = 10
        self.assertAlmostEqual(m.Objective(np,expanded,games,0,0)([[.3,.4]])[0],first)

    def test_regularization_selection_does_not_read_test_labels(self):
        rows, games = population(1000)
        games['split'][700:850]=1; games['split'][850:]=2
        # Constant trial fits isolate selection behavior without refitting four searches.
        fake={'a':.21,'b':.37,'converged':True,'search_boundary':False,'regularization':.001}
        with patch.object(m,'fit',return_value=fake):
            first=m.fit_mode(np,rows,games,0)
            games['outcome'][850:]=999
            second=m.fit_mode(np,rows,games,0)
        self.assertEqual(first,second)

    def test_sparse_mode_has_no_fabricated_parameters(self):
        rows,games=population(5)
        result=m.fit_mode(np,rows,games,0)
        self.assertEqual(result['status'],'insufficient_data')
        self.assertNotIn('a',result)

    def test_cancellation_between_objective_batches(self):
        rows,games=population(5)
        with self.assertRaisesRegex(RuntimeError,'cancel'):
            m.Objective(np,rows,games,0,0,lambda:(_ for _ in ()).throw(RuntimeError('cancel')))([[0,0]])


class SourceTests(unittest.TestCase):
    def test_chronology_timestamp_ties_and_player_holdout(self):
        regular=next(f'p{i}' for i in range(100) if not s.held_player(f'p{i}'))
        held=next(f'p{i}' for i in range(100) if s.held_player(f'p{i}'))
        games=[{'link':i,'played_at':datetime(2020,1,1,tzinfo=timezone.utc)+timedelta(days=i//2),
                'white_name':held if i%5==0 else regular,'black_name':regular} for i in range(100)]
        s.partitions(games)
        for game in games:
            if game['white_name']==held: self.assertIn(game['split'],(-1,3))
        train={g['played_at'] for g in games if g['split']==0}
        val={g['played_at'] for g in games if g['split']==1}
        test={g['played_at'] for g in games if g['split'] in (2,3)}
        self.assertLess(max(train),min(val)); self.assertLess(max(val),min(test))
        self.assertEqual(s.held_player(held),s.held_player(held.upper()))
        with self.assertRaisesRegex(ValueError,'Duplicate'): s.partitions(games+[games[0]])

    def test_whole_game_predecessors_and_invalid_coverage(self):
        meta={'total_positions':3}
        rows=s.pack(0,meta,[(1,105,1,0),(2,-9998,2,1),(3,-9999,3,1)])
        self.assertEqual(rows['before'].tolist(),[15,105,-9998])
        with self.assertRaisesRegex(ValueError,'Coverage'): s.pack(0,meta,[(1,105,1,0)])
        with self.assertRaisesRegex(ValueError,'Missing'): s.pack(0,meta,[(1,105,1,0),(3,0,2,0),(4,0,3,0)])


class GateTests(unittest.TestCase):
    def test_calibration_and_sparse_metrics(self):
        matrix=np.zeros((100,v.METRIC_COLUMNS))
        v.add_metrics(np,matrix,np.arange(100),np.full(100,.8),np.full(100,.5),np.zeros(100))
        with patch.object(v,'BOOTSTRAPS',32): result=v.metric(np,matrix,np.ones(100,dtype=bool))
        self.assertEqual(result['status'],'failed'); self.assertAlmostEqual(result['ece'],.8)
        self.assertAlmostEqual(result['baseline_ece'],.5)
        self.assertAlmostEqual(result['ece_change'],.3)
        np.testing.assert_allclose(result['calibration_uncertainty']['ece_change_ci95'], [.3,.3])
        self.assertEqual(v.metric(np,matrix,np.zeros(100,dtype=bool))['status'],'insufficient_data')
        json.dumps(result,allow_nan=False)

    def test_validation_uses_same_model_cpu_gpu_and_never_promotes(self):
        rows,games=population(120)
        games['split'][:20]=0; games['split'][20:]=2
        meta=[{'link':i,'white_elo':2300,'black_elo':2300,'white_end':'win','black_end':'resigned'} for i in range(120)]
        fits=[{'status':'fitted','a':.21,'b':.37,'search_boundary':False}]+[{'status':'insufficient_data'}]*2
        acc=v.Validation(np,rows,games,meta,fits)
        acc.consume(rows)
        with patch.object(v,'BOOTSTRAPS',16): report=acc.finish()
        self.assertTrue(report['cpu_cuda_passed']); self.assertEqual(report['decision_mismatches'],0)
        self.assertFalse(report['promotion_ready']); self.assertEqual(report['gates']['production_recovery'],'not_run')
        json.dumps(report,allow_nan=False)


class CalibrationTests(unittest.TestCase):
    def metrics(self, p, base, outcomes, ids=None):
        ids = np.arange(len(p)) if ids is None else np.asarray(ids)
        matrix = np.zeros((int(ids.max())+1, v.METRIC_COLUMNS))
        v.add_metrics(np, matrix, ids, np.asarray(p), np.asarray(base), np.asarray(outcomes))
        return matrix[:, 1:]/matrix[:, :1]

    def test_hand_computed_separate_bins_and_contributions(self):
        values = self.metrics([.2,.2,.8,.8], [.5]*4, [0,0,1,1])
        result = calibration.compare(np, values, 32)
        self.assertAlmostEqual(result['ece'], .2)
        self.assertAlmostEqual(result['baseline_ece'], 0.)
        self.assertEqual([b['bin'] for b in result['calibration']], [2,8])
        self.assertEqual([b['bin'] for b in result['baseline_calibration']], [5])
        self.assertAlmostEqual(sum(b['ece_contribution'] for b in result['calibration']), result['ece'])
        self.assertEqual(result['calibration_uncertainty']['status'], 'insufficient_data')
        self.assertEqual(result['calibration'][0]['games_contributing'], 2)

    def test_equal_game_weight_not_move_weight(self):
        ids = [0]*20+[1]
        values = self.metrics([.8]*21, [.5]*21, [0]*20+[1], ids)
        result = calibration.compare(np, values, 32)
        self.assertAlmostEqual(result['ece'], .3)
        self.assertAlmostEqual(result['baseline_ece'], 0.)
        self.assertAlmostEqual(result['calibration'][0]['game_weight'], 2.)

    def test_paired_intervals_identical_model_zero_difference(self):
        rng = np.random.default_rng(42)
        p = rng.uniform(0,1,100)
        values = self.metrics(p,p,rng.choice([0,.5,1],100))
        result = calibration.compare(np, values, 64)
        np.testing.assert_array_equal(result['calibration_uncertainty']['ece_change_ci95'], [0.,0.])
        self.assertEqual(result['calibration_uncertainty']['ece_ci95'], result['calibration_uncertainty']['baseline_ece_ci95'])
        self.assertEqual(result['ece'], result['baseline_ece'])

    def test_perfect_extremes_coarsening_and_cancellation(self):
        p = np.tile([0.,1.],50)
        result = calibration.compare(np,self.metrics(p,p,p),32)
        self.assertEqual(result['ece'],0.)
        self.assertEqual([b['bin'] for b in result['calibration']],[0,9])
        self.assertEqual(result['bin_sensitivity']['bins_5']['ece'],0.)
        values = self.metrics(np.tile([.41,.59],50),np.full(100,.5),np.tile([0.,1.],50))
        result = calibration.compare(np,values,32)
        self.assertGreater(result['ece'],result['bin_sensitivity']['bins_5']['ece'])
        with self.assertRaisesRegex(RuntimeError,'cancel'):
            calibration.compare(np,values,32,lambda:(_ for _ in ()).throw(RuntimeError('cancel')))

    def test_player_concentration_is_not_independent_game_count(self):
        meta=[{'white_name':'Owner','black_name':f'p{i}'} for i in range(30)]
        result=calibration.player_dependence(meta,[True]*30)
        self.assertEqual(result['unique_players'],31)
        self.assertEqual(result['largest_player_game_share'],1.)
        self.assertFalse(result['independent_player_interval'])


class WiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_allowlisted_config_and_no_gpu_fallback(self):
        config=job.configuration(); config['model']['threshold']=.15
        with patch.object(job,'cuda_backend') as cuda:
            with self.assertRaisesRegex(ValueError,'modified'): await job.execute(config,AsyncMock())
            cuda.assert_not_called()
        with patch.object(job,'cuda_backend',side_effect=RuntimeError('CUDA unavailable')), patch.object(job.source,'freeze') as freeze:
            with self.assertRaisesRegex(RuntimeError,'CUDA unavailable'): await job.execute(job.configuration(),AsyncMock())
            freeze.assert_not_called()

    async def test_authenticated_route_and_fixed_model_parameters(self):
        app=FastAPI(); app.include_router(routes.router,prefix='/research/algorithms',dependencies=[Depends(require_superuser)])
        app.dependency_overrides[routes.get_redis_pool]=lambda:object()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            path='/research/algorithms/blunder-training-v2'
            self.assertEqual((await client.post(path,json={})).status_code,401)
            app.dependency_overrides[get_current_account]=lambda:SimpleNamespace(role=AccountRole.user,id=None)
            self.assertEqual((await client.post(path,json={})).status_code,403)
            app.dependency_overrides[get_current_account]=lambda:SimpleNamespace(role=AccountRole.admin,id=None)
            for body in ({'threshold':.1},{'games_per_group':True},{'games_per_group':99},{'games_per_group':3001}):
                self.assertEqual((await client.post(path,json=body)).status_code,422)
            with patch.object(routes,'enqueue_run',AsyncMock(return_value={'status':'queued'})) as enqueue:
                self.assertEqual((await client.post(path,json={'games_per_group':1000})).status_code,202)
                config=enqueue.call_args.kwargs['draft'][1]
                self.assertEqual(config['model']['threshold'],.2)
                self.assertFalse(config['canonical'])
                from tests.test_blunder_balanced import REF
                from chessism_api.operations.research_algorithms.blunder_balanced_sampling import plan
                with patch('chessism_api.operations.research_algorithms.blunder_balanced_source.reference_plan',AsyncMock(return_value=plan([REF]))),patch.object(routes,'AsyncDBSession') as session:
                    session.return_value.__aenter__=AsyncMock()
                    session.return_value.__aexit__=AsyncMock()
                    self.assertEqual((await client.post(path,json={'sampling':'balanced_v1'})).status_code,202)
                    self.assertEqual(enqueue.call_args.kwargs['draft'][1]['sampling']['player_share_limit'],.1)
                    self.assertEqual((await client.post(path,json={'sampling':'balanced_v1','refinement':'calibration_v1'})).status_code,202)
                    self.assertEqual(enqueue.call_args.kwargs['draft'][1]['refinement']['validation_ece_limit'],.03)
                    self.assertEqual(enqueue.call_args.kwargs['draft'][1]['implementation_version'],3)
                self.assertEqual((await client.post(path,json={'refinement':'calibration_v1'})).status_code,422)
                self.assertEqual((await client.post(path,json={'sampling':'development_reuse_v1'})).status_code,422)
                from chessism_api.operations.research_algorithms import blunder_development_reuse as reuse
                with patch.object(reuse,'reference_plan',AsyncMock(return_value=reuse.plan([REF]))),patch.object(routes,'AsyncDBSession') as session:
                    session.return_value.__aenter__=AsyncMock()
                    session.return_value.__aexit__=AsyncMock()
                    self.assertEqual((await client.post(path,json={'sampling':'development_reuse_v1','refinement':'calibration_v1'})).status_code,202)
                    self.assertEqual(enqueue.call_args.kwargs['draft'][1]['implementation_version'],4)
                    self.assertEqual(enqueue.call_args.kwargs['draft'][1]['sampling']['version'],'development_reuse_v1')
                self.assertEqual((await client.post(path,json={'refinement':'unknown'})).status_code,422)
                self.assertEqual((await client.post(path,json={'sampling':'balanced_v1','refinement':{'validation_ece_limit':.1}})).status_code,422)
                self.assertEqual((await client.post(path,json={'sampling':'unknown'})).status_code,422)


if __name__=='__main__': unittest.main()
