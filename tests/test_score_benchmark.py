"""One-input batch experiments; explicit fake device, no database or network."""
import io
import unittest
from unittest.mock import AsyncMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import score_benchmark as bench, score_stream as stream
from chessism_api.operations.research_algorithms.repository import RunCancelled
from tests import test_score_stream as fixtures


class BenchmarkTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for module in (stream, bench):
            override = patch.object(module, 'cuda_backend', return_value=(fixtures.FakeDevice(), {'name':'isolated test device'}))
            override.start(); self.addCleanup(override.stop)
        _, self.batch = fixtures.StreamTests().fixture()
        comparison=stream.Comparison('fixture')
        comparison.consume(self.batch)
        self.baseline=comparison.report()

    async def test_one_frozen_input_all_variants_and_timed_passes_match(self):
        result=await bench.benchmark(io.BytesIO(self.batch.tobytes()),self.baseline,{'player':'fixture'},AsyncMock())
        self.assertEqual(result['status'],'passed')
        self.assertFalse(result['canonical'])
        self.assertEqual(result['input_reuse'],'within_run_only')
        self.assertEqual(result['database_extractions'],1)
        self.assertEqual(len(result['measurement_order']),18)
        for variant in result['variants']:
            self.assertEqual(variant['validation']['input_sha256'],self.baseline['input_sha256'])
            for backend in ('cuda_cupy','numpy_cpu'):
                samples=variant[backend+'_seconds']
                self.assertEqual(len(samples),3)
                self.assertEqual(variant[backend+'_median_seconds'],sorted(samples)[1])
        orders=result['measurement_order']
        self.assertEqual([v['backend'] for v in orders[:2]],['numpy_cpu','cuda_cupy'])
        self.assertEqual([v['backend'] for v in orders[6:8]],['cuda_cupy','numpy_cpu'])
        self.assertEqual(orders[6]['batch_rows'],262144)

    async def test_cancellation_prevents_timed_passes(self):
        reporter=AsyncMock(); reporter.check.side_effect=RunCancelled('cancelled')
        with patch.object(bench,'summary_pass') as calculate:
            with self.assertRaises(RunCancelled):
                await bench.benchmark(io.BytesIO(self.batch.tobytes()),self.baseline,{'player':'fixture'},reporter)
        calculate.assert_not_called()

    async def test_corrupt_or_failed_baseline_is_never_benchmarked(self):
        with self.assertRaisesRegex(ValueError,'baseline'):
            await bench.benchmark(io.BytesIO(),{**self.baseline,'status':'failed'},{'player':'fixture'},AsyncMock())
        changed=self.batch.copy(); changed['score'][0]+=1
        with self.assertRaisesRegex(ValueError,'disagrees'):
            await bench.benchmark(io.BytesIO(changed.tobytes()),self.baseline,{'player':'fixture'},AsyncMock())

    async def test_timed_path_mismatch_does_not_publish_partial_benchmark(self):
        with patch.object(bench,'summary_pass',return_value={'rows':0}):
            with self.assertRaisesRegex(ValueError,'Timed summary pass disagrees'):
                await bench.benchmark(io.BytesIO(self.batch.tobytes()),self.baseline,{'player':'fixture'},AsyncMock())

    def test_large_batches_and_partial_tail_preserve_every_occurrence(self):
        data=np.tile(self.batch,160)[:300001]
        comparison=stream.Comparison('fixture',batch_rows=262144)
        for offset in range(0,len(data),262144): comparison.consume(data[offset:offset+262144])
        reference=comparison.report()
        self.assertEqual(reference['rows'],len(data))
        self.assertEqual(reference['peak_batch_rows'],262144)
        for size in stream.GPU_BATCH_SIZES:
            for backend in ('cuda_cupy','numpy_cpu'):
                result=bench.summary_pass(io.BytesIO(data.tobytes()),'fixture',backend=backend,batch_rows=size)
                self.assertEqual(result['rows'],len(data))
                self.assertEqual(result['groups'],bench.signature(reference))
                self.assertEqual(result['input_sha256'],reference['input_sha256'])

    def test_unbounded_or_unknown_backends_rejected(self):
        for size in (True,8192.0,8193,262145,1000000):
            with self.assertRaises(ValueError): stream.Comparison('fixture',batch_rows=size)
            with self.assertRaises(ValueError): stream.read_batch(io.BytesIO(),size)
        with self.assertRaises(ValueError):
            bench.summary_pass(io.BytesIO(),'fixture',backend='fallback',batch_rows=8192)


if __name__=='__main__': unittest.main()
