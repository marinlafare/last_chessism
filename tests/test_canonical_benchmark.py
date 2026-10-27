import unittest
from copy import deepcopy
import tempfile
from pathlib import Path
import numpy as np
from tests.test_canonical_blunders import fixture,report
from chessism_api.operations.canonical_blunders import compute,registry,benchmark_compute as measured
from chessism_api.operations.canonical_blunders import benchmark_job as job,benchmark_source as source


def cuda_check():
    from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
    cp,hardware=cuda_backend(0)
    _,config,_=registry.freeze(report(),registry.ACK); games,rows=fixture()
    from tests.test_canonical_binary_source import wire
    from chessism_api.operations.canonical_blunders.binary_source import pack
    packed=pack(wire([(games[int(r['game'])]['game_link'],int(r['ply']),float(r['after']),int(r['ak'])) for r in rows]),games,0)
    assert packed.tobytes()==rows.tobytes()
    records,_=compute.infer(cp,rows,games,config)
    actual,timings=measured.infer(cp,packed,games,config)
    assert measured.compare(actual,records,digest=True)==[]
    for key in ('upload','decisions','reductions','download'):
        assert timings[key+'_cuda_seconds']>0 and timings[key+'_wall_seconds']>0
    assert all(np.isfinite(v) and v>=0 for v in timings.values())
    empty,_=measured.infer(cp,np.empty(0,compute.DTYPE),[],config)
    assert empty==[]
    print('PASS actual CUDA: binary inputs, 18,000 moves, all modes, mate/missing/tablebase, flags/counts/digests, timing events; '+hardware['name'])


class BenchmarkTests(unittest.TestCase):
    def test_frozen_configuration(self):
        mid,config,_=registry.freeze(report(),registry.ACK)
        settings=job.configuration(mid,config)
        self.assertEqual(settings['games'],100000)
        self.assertEqual(settings['model']['threshold'],.15)
        config['fits'][0]['a']=99
        self.assertNotEqual(settings['model'],config)
        config['kernel_sha256']='changed'
        with self.assertRaises(ValueError):job.configuration(mid,config)

    def test_no_cpu_fallback(self):
        _,config,_=registry.freeze(report(),registry.ACK); games,rows=fixture()
        with self.assertRaisesRegex(RuntimeError,'no CPU fallback'):measured.infer(np,rows,games,config)

    def test_exact_comparison_detects_changed_flags_and_missing(self):
        _,config,_=registry.freeze(report(),registry.ACK); games,rows=fixture()
        old,_=compute.infer(np,rows,games,config,require_cuda=False); new=deepcopy(old)
        self.assertEqual(measured.compare(new,old,digest=True),[])
        new[0]['flags']['blunders'].append(999)
        self.assertEqual(measured.compare(new,old),[1])
        with self.assertRaises(ValueError):measured.compare(new[:-1],old)

    def test_frozen_batch_roundtrip(self):
        games,rows=fixture()
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory); source.spool(folder,0,rows,games,[],[])
            recovered,data=source.read(folder,0)
            self.assertEqual(recovered.tobytes(),rows.tobytes()); self.assertEqual(data['games'],games)

    def test_histograms_include_both_owners_unknown_and_zero(self):
        hist={p:{m:{'counts':[0]*7,'games_count':0} for m in ('bullet','blitz','rapid')} for p in ('a','b','zero')}
        job.add_histograms(hist,[{'mode':'blitz','white_owner':'a','black_owner':'b','white_count':9,'black_count':0},
            {'mode':'rapid','white_owner':'a','white_count':None}])
        self.assertEqual(hist['a']['blitz']['counts'],[0]*6+[1])
        self.assertEqual(hist['b']['blitz']['counts'],[1]+[0]*6)
        self.assertEqual(hist['zero']['bullet']['games_count'],0)


if __name__=='__main__':unittest.main()
