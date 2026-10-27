"""Numerical streaming contracts with an explicit fake device; no live access."""
import io
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np

from chessism_api.operations.research_algorithms import score_stream as stream, summary_validation as legacy
from scripts.validate_summary_cuda import synthetic_rows


class FakeDevice:
    """Only used by isolated tests, never a production fallback."""
    cuda = SimpleNamespace(Device=lambda _: SimpleNamespace(use=lambda: None),
        get_current_stream=lambda: SimpleNamespace(synchronize=lambda: None))
    asnumpy = staticmethod(np.asarray)

    def __getattr__(self, name): return getattr(np, name)


class StreamTests(unittest.TestCase):
    def fixture(self):
        rows = legacy.validate_rows(synthetic_rows())
        batch = stream.pack([(legacy.MODES.index(r['mode']),r['player_score'],legacy.KINDS[r['score_kind']],
            -1 if r['mate_moves'] is None else r['mate_moves']) for r in rows])
        return rows, batch

    def test_array_oracle_agrees_with_original_per_row_oracle(self):
        rows, batch = self.fixture()
        np.testing.assert_array_equal(stream.cpu_reference(batch), legacy.reference(rows))

    def test_batch_partitioning_does_not_change_results_or_fingerprint(self):
        _, batch = self.fixture()
        reports=[]
        for size in (1,257,8192):
            with patch.object(stream, 'cuda_backend', return_value=(FakeDevice(),{'name':'isolated test device'})):
                comparison = stream.Comparison('fixture')
            for start in range(0,len(batch),size): comparison.consume(batch[start:start+size])
            report = comparison.report()
            self.assertEqual(report['status'],'passed')
            self.assertLessEqual(report['peak_batch_rows'],size)
            reports.append(report)
        self.assertEqual(len({r['input_sha256'] for r in reports}),1)
        self.assertEqual(reports[0]['groups'],reports[-1]['groups'])

    def test_larger_input_never_accumulates_per_row_outputs(self):
        batch = stream.pack([(1,0.,0,-1)]*8192)
        with patch.object(stream, 'cuda_backend', return_value=(FakeDevice(),{'name':'isolated test device'})):
            comparison = stream.Comparison('fixture')
        for _ in range(25): comparison.consume(batch)
        report=comparison.report()
        self.assertEqual(report['rows'],204800)
        self.assertEqual(report['groups'][1]['cuda'][6],204800)
        self.assertEqual(report['peak_batch_rows'],8192)
        self.assertEqual(report['mismatch_examples'],[])
        self.assertEqual(report['coverage']['zero_scores'],204800)
        for value in vars(comparison).values():
            if isinstance(value,np.ndarray): self.assertLessEqual(value.size,45)

    def test_mismatches_cannot_become_a_pass(self):
        _, batch = self.fixture()
        with patch.object(stream, 'cuda_backend', return_value=(FakeDevice(),{'name':'test'})):
            comparison=stream.Comparison('fixture')
        original=stream.score_kernel
        def wrong(*args):
            result=original(*args); result[:,0]=0; return result
        with patch.object(stream,'score_kernel',wrong): comparison.consume(batch)
        result=comparison.report()
        self.assertEqual(result['status'],'failed')
        self.assertGreater(result['row_mismatches'],0)
        self.assertGreater(result['counter_mismatches'],0)
        self.assertEqual(len(result['mismatch_examples']),20)

    def test_malformed_and_oversized_streams_fail_closed(self):
        for record in ((-1,0.,0,-1),(1,float('nan'),0,-1),(1,9998.,0,-1),
                       (1,9998.,1,3),(1,0.,1,-1),(1,9998.,2,2),(1,0.,3,-1)):
            with self.assertRaises(ValueError): stream.pack([record])
        with self.assertRaisesRegex(ValueError,'Truncated'): stream.read_batch(io.BytesIO(b'broken'))
        with patch.object(stream,'cuda_backend',return_value=(FakeDevice(),{})):
            comparison=stream.Comparison('fixture')
        with self.assertRaises(ValueError): comparison.consume(np.zeros(8193,dtype=stream.DTYPE))
        comparison.rows=stream.MAX_ROWS
        with self.assertRaises(ValueError): comparison.consume(stream.pack([(1,0.,0,-1)]))


if __name__=='__main__': unittest.main()
