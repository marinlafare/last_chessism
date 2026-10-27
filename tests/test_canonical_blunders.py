from copy import deepcopy
import unittest
from unittest.mock import AsyncMock,patch
import numpy as np
from chessism_api.operations.canonical_blunders import registry,compute
from chessism_api.operations.research_algorithms.blunder_threshold_compare import scalar15


def report():
    return {'format':'chessism_blunder_full_development_v1','production_changed':False,'model_id':'fixture',
        'config':{'k0':.00368208,'threshold':.15},
        'gates':{**dict.fromkeys(('development_prediction_numerical','development_bullet_decision_numerical',
            'player_balance','historical_partition_safety','current_source_revision'),'passed'),
            'final_independent':'not_run','rapid_selection':'not_passed'},
        'fits':[{'mode':m,'a':a,'b':b,'ece':e} for m,a,b,e in (
            ('bullet',.2087383270263672,2.880035400390625,.023868),
            ('blitz',.2149028778076172,2.3756484985351562,.022259),
            ('rapid',.006809234619140625,.030462265014648438,.046335))]}


def fixture():
    rng=np.random.default_rng(431); games=[]; parts=[]
    for i in range(90):
        g={'game_link':i+1,'mode':('bullet','blitz','rapid')[i%3],'white_elo':int(rng.integers(1,4001)),
            'black_elo':int(rng.integers(1,4001)),'total_positions':200,'revision':1}
        cp=rng.uniform(-1500,1500,200); kinds=np.zeros(200,dtype=int)
        cp[:12]=[105,-9998,9999,700,-700,-9990,0,0,10,0,0,300]
        kinds[:12]=[0,1,1,0,0,1,2,-1,0,0,0,0]
        records=[(j+1,float(v),int(k)) for j,(v,k) in enumerate(zip(cp,kinds))]
        parts.append(compute.pack(g,records,i)); games.append(g)
    return games,np.concatenate(parts)


def check_inference(xp):
    _,config,_=registry.freeze(report(),registry.ACK)
    games,rows=fixture(); actual,info=compute.infer(xp,rows,games,config,require_cuda=xp is not np)
    fits={f['mode']:f for f in config['fits']}
    for g,out in zip(games,actual):
        f=fits[g['mode']]; block=rows[rows['game']==g['game_link']-1]; flags=[]
        for r in block:
            sign=1 if r['ply']%2 else -1
            flags.append(scalar15(r['before']*sign,r['after']*sign,int(r['bk']),int(r['ak']),
                ((g['white_elo']+g['black_elo'])/2-2300)/400,(g['white_elo']-g['black_elo'])*sign/400,f['a'],f['b']))
        flags=np.array(flags)
        assert out['flags']['blunders']==block['ply'][flags==1].tolist()
        assert out['flags']['unassessed']==block['ply'][flags<0].tolist()
        for side,parity in [('white',1),('black',0)]:
            selected=flags[block['ply']%2==parity]
            assert out[side+'_count']==int((selected==1).sum())
            assert out[side+'_assessed']==int((selected>=0).sum())
    return info


class CanonicalTests(unittest.TestCase):
    def test_frozen_provisional_evidence(self):
        source=report(); model,config,evidence=registry.freeze(source,registry.ACK)
        self.assertEqual(model,registry.digest(config)); self.assertEqual(config['threshold'],.15)
        self.assertFalse(evidence['quality_approved']); self.assertEqual(evidence['gates']['rapid_selection'],'not_passed')
        original=deepcopy(config); source['fits'][0]['a']=99
        self.assertEqual(config,original)
        with self.assertRaises(ValueError):registry.freeze(report(),'')
        source=report(); source['gates']['development_bullet_decision_numerical']='failed'
        with self.assertRaises(ValueError):registry.freeze(source,registry.ACK)

    def test_all_modes_both_sides_mates_and_missing(self):
        self.assertEqual(check_inference(np)['moves'],18000)

    def test_no_fallback_or_changed_kernel(self):
        _,config,_=registry.freeze(report(),registry.ACK); games,rows=fixture()
        with self.assertRaisesRegex(RuntimeError,'no CPU fallback'):compute.infer(np,rows,games,config)
        config['kernel_sha256']='changed'
        with self.assertRaisesRegex(ValueError,'implementation changed'):compute.infer(np,rows,games,config,require_cuda=False)

    def test_source_coverage_and_order(self):
        with self.assertRaisesRegex(ValueError,'Coverage'):compute.pack({'total_positions':2},[(1,0,0)],0)
        with self.assertRaisesRegex(ValueError,'predecessor'):compute.pack({'total_positions':1},[(2,0,0)],0)
        row=compute.pack({'total_positions':1},[(1,None,-1)],0)[0]
        self.assertEqual(row['before'],15); self.assertTrue(np.isnan(row['after']))

    def test_worker_is_serial(self):
        from chessism_api.operations.research_algorithms.worker import WorkerSettings
        self.assertEqual(WorkerSettings.max_jobs,1)
        self.assertEqual(WorkerSettings.cron_jobs[0].job_id,'canonical-blunder-refresh')
        from chessism_api.operations.canonical_blunders.worker import INPUT_READER
        self.assertEqual(INPUT_READER,'binary_v1')

    def test_game_review_uses_published_flags_not_legacy_labels(self):
        from chessism_api.operations.player_game_explorer import _classification
        self.assertEqual(_classification(14,-345,'cp','cp')[0],'blunder')
        self.assertEqual(_classification(14,-345,'cp','cp',0)[0],'mistake')
        self.assertEqual(_classification(14,-20,'cp','cp',1)[0],'blunder')
        self.assertIsNone(_classification(14,-345,'cp','cp',-1)[0])


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_chains_only_after_commit_and_reuses_stable_id(self):
        from chessism_api.operations.canonical_blunders import scheduler
        ctx={'redis':AsyncMock(),'job_id':scheduler.BATCH_ID}
        with patch.object(scheduler,'canonical_tick',AsyncMock(return_value={'status':'building','remaining':12})):
            await scheduler.run_batch(ctx)
        ctx['redis'].enqueue_job.assert_not_awaited()
        await scheduler.after_job_end(ctx)
        self.assertEqual(ctx['redis'].enqueue_job.await_args.kwargs['_job_id'],scheduler.BATCH_ID)
        for status in ('ready','retrying','waiting_cloud'):
            ctx['redis'].enqueue_job.reset_mock()
            with patch.object(scheduler,'canonical_tick',AsyncMock(return_value={'status':status})):
                await scheduler.run_batch(ctx)
            await scheduler.after_job_end(ctx)
            ctx['redis'].enqueue_job.assert_not_awaited()


if __name__=='__main__':unittest.main()
