"""Fixed-rule numerical oracles retained by current model diagnostics."""
import io
import unittest
import numpy as np
from chessism_api.operations.research_algorithms import blunder_fixed as model
from tests.fixed_blunder_fixtures import synthetic


class FixedTests(unittest.TestCase):
    def test_scalar_parity_boundaries_mates_missing_and_tablebases(self):
        rows, _ = synthetic()
        actual = model.kernel(np, *(rows[key] for key in ('before','after','before_kind','after_kind')))
        np.testing.assert_array_equal(actual, model.reference(rows))
        self.assertEqual(actual[-16:].tolist(), [1,0,1,0,1,0,1,0,0,-1,-1,-1,-1,-1,1,1])

    def test_corrupt_stream_fails(self):
        with self.assertRaisesRegex(ValueError, 'Truncated'): model.read_batch(io.BytesIO(b'x'))
