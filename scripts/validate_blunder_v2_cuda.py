"""Synthetic-only real CUDA check. No database/network or production writes."""
import json
import numpy as np

from chessism_api.operations.research_algorithms import blunder_v2_model as m, blunder_v2_validation as v
from chessism_api.operations.research_algorithms import blunder_v2_calibration as calibration
from chessism_api.operations.research_algorithms import blunder_refinement as refinement, blunder_refinement_audit
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend


def main():
    cp, hardware = cuda_backend()
    rng = np.random.default_rng(20261028)
    n = 100_016
    before, after = rng.uniform(-1500,1500,(2,n))
    bk, ak = np.zeros(n,dtype=np.int8), np.zeros(n,dtype=np.int8)
    u, d = rng.uniform(-4,2,n), rng.uniform(-3,3,n)
    before[-8:] = [105,-700,-701,9998,9998,9998,0,np.nan]
    after[-8:] = [-9998,-9998,-9998,-9998,700,701,0,0]
    bk[-8:] = [0,0,0,1,1,1,2,0]; ak[-8:] = [1,1,1,1,0,0,0,0]
    oracle = np.array([m.scalar(*x,.21,.37) for x in zip(before,after,bk,ak,u,d)])
    actual = cp.asnumpy(m.decisions(cp,*(cp.asarray(x) for x in (before,after,bk,ak,u,d)),.21,.37))
    assert np.array_equal(oracle,actual)
    assert actual[-8:].tolist()==[1,1,0,1,1,0,-1,-1]
    for loss in (.2-1e-10,.2,.2+1e-10):
        c=np.log((.5+loss)/(.5-loss))/m.K0
        gpu=int(m.decisions(cp,cp.asarray(c),cp.asarray(0.),cp.asarray(0),cp.asarray(0),cp.asarray(0.),cp.asarray(0.),0.,0.))
        assert gpu==m.scalar(c,0.,0,0,0.,0.,0.,0.)
    rows=np.zeros(1200,dtype=m.DTYPE)
    rows['game']=np.arange(1200); rows['ply']=1; rows['fen']=np.arange(1200)
    rows['before']=15; rows['after']=rng.uniform(-1000,1000,1200)
    games={'mode':np.zeros(1200,dtype=int),'split':np.zeros(1200,dtype=int),
           'cp_count':np.ones(1200),'u':rng.uniform(-3,1,1200),'d':rng.uniform(-2,2,1200)}
    games['outcome']=m.expected(np,rows['after'],games['u'],games['d'],.21,.37)
    fit=m.fit(m.Objective(cp,rows,games,0,0),0.)
    assert abs(fit['a']-.21)<1e-5 and abs(fit['b']-.37)<1e-5,fit
    games['split'][700:900]=1; games['split'][900:]=2
    refined=refinement.fit_mode(cp,rows,games,0)
    assert refined['selection']['status']=='passed',refined
    assert len(refined['refinement_trials'])==28
    parameters=[[0.,0.]]+[[t['a'],t['b']] for t in refined['refinement_trials']]
    cpu_metrics=refinement.validation_scores(np,rows,games,0,parameters)
    gpu_metrics=refinement.validation_scores(cp,rows,games,0,parameters)
    np.testing.assert_allclose([[t['mse'],t['ece']] for t in cpu_metrics],[[t['mse'],t['ece']] for t in gpu_metrics],atol=1e-12,rtol=0)
    reference_trials=[{**t,**metric} for t,metric in zip(refined['refinement_trials'],cpu_metrics[1:])]
    selected,selection=refinement.select(reference_trials,cpu_metrics[0])
    assert selected==refined['selection']['selected_trial'] and selection['status']=='passed'
    refined_oracle=np.array([m.scalar(*x,refined['a'],refined['b']) for x in zip(before,after,bk,ak,u,d)])
    refined_actual=cp.asnumpy(m.decisions(cp,*(cp.asarray(x) for x in (before,after,bk,ak,u,d)),refined['a'],refined['b']))
    assert np.array_equal(refined_oracle,refined_actual)
    games['split'][200:]=2
    metadata=[{'link':i,'white_elo':2300,'black_elo':2300,'white_end':'win','black_end':'resigned'} for i in range(1200)]
    fits=[{**fit,'status':'fitted'}]+[{'status':'insufficient_data'}]*2
    accumulator=v.Validation(cp,rows,games,metadata,fits)
    accumulator.consume(rows)
    report=accumulator.finish()
    assert report['cpu_cuda_passed'] and report['per_game_count_mismatches']==0,report
    assert not report['promotion_ready']
    normalized=accumulator.metrics[0,200:,1:]/accumulator.metrics[0,200:,:1]
    cpu_calibration=calibration.compare(np,cp.asnumpy(normalized),v.BOOTSTRAPS)
    for key in ('ece','baseline_ece','ece_change'):
        assert abs(report['test'][key]-cpu_calibration[key])<1e-12
    for key in ('ece_ci95','baseline_ece_ci95','ece_change_ci95'):
        np.testing.assert_allclose(report['test']['calibration_uncertainty'][key],cpu_calibration['calibration_uncertainty'][key],atol=1e-12,rtol=0)
    cluster_meta=[{'white_name':f'w{i//2}','black_name':f'b{i//2}'} for i in range(1200)]
    mask=games['split']==2
    gpu_clusters=blunder_refinement_audit.player_intervals(cp,accumulator.metrics[0],cluster_meta,mask,repeats=64)
    cpu_clusters=blunder_refinement_audit.player_intervals(np,cp.asnumpy(accumulator.metrics[0]),cluster_meta,mask,repeats=64)
    assert gpu_clusters['status']=='estimated'
    for key in ('ece_ci95','baseline_ece_ci95','ece_change_ci95','relative_mse_change_ci95'):
        np.testing.assert_allclose(gpu_clusters[key],cpu_clusters[key],atol=1e-12,rtol=0)
    # A negative test must not silently pass the numerical gate.
    accumulator.counts[0,2]+=1
    failed=accumulator.finish()
    assert not failed['cpu_cuda_passed'] and failed['per_game_count_mismatches']==1
    json.dumps(report,allow_nan=False)
    print(json.dumps({'status':'passed','source':'synthetic_only','hardware':hardware,
        'moves':n,'decision_mismatches':int(np.count_nonzero(oracle!=actual)),
        'forced_mate_105cp':'passed','known_parameter_recovery':fit,
        'gpu_metrics_and_bootstrap':'passed','paired_calibration_cpu_cuda':'passed',
        'refinement_28_trials_cpu_cuda':'passed','refined_move_mismatches':int(np.count_nonzero(refined_oracle!=refined_actual)),
        'player_component_bootstrap_cpu_cuda':'passed',
        'negative_count_mismatch':'detected'},indent=2))


if __name__=='__main__': main()
