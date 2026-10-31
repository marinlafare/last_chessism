"""Offline guards for the explicit all-player experiment; no production writes."""
from datetime import datetime, timezone
import io
import unittest
from unittest.mock import patch

import numpy as np

from scripts import benchmark_all_score_distributions as cohort
from chessism_api.operations.research_algorithms import score_benchmark, score_stream
from chessism_api.operations.research_algorithms.score_validation_job import compare_groups
from tests import test_score_stream as fixtures


class CohortTests(unittest.TestCase):
    def test_inventory_requires_every_eligible_player_current_and_bounded(self):
        row = {'player_name': 'alice', 'revision': 2, 'published_revision': 2, 'snapshot_revision': 2,
            'generated_at': datetime.now(timezone.utc), 'scored': 3_000_000}
        self.assertEqual(cohort.checked_inventory([row])[0]['scored'], 3_000_000)
        for replacement in ({'revision': None}, {'published_revision': 1}, {'snapshot_revision': 1},
                {'generated_at': None}, {'scored': cohort.MAX_PLAYER_ROWS + 1}, {'scored': -1}):
            with self.assertRaises(ValueError):
                cohort.checked_inventory([{**row, **replacement}])
        with self.assertRaises(ValueError):
            cohort.checked_inventory([])
        with self.assertRaises(ValueError):
            cohort.checked_inventory([{**row, 'scored': cohort.MAX_PLAYER_ROWS}] * 6)
        self.assertEqual(cohort.checked_inventory([{**row, 'scored': 0}])[0]['scored'], 0)

    def test_window_cannot_leak_neighbor_rows_or_read_unboundedly(self):
        data = np.zeros(10, dtype=score_stream.DTYPE)
        data['score'] = np.arange(10)
        window = cohort.Window(io.BytesIO(data.tobytes()), 3, 2)
        self.assertEqual(np.frombuffer(window.read(999999), dtype=score_stream.DTYPE)['score'].tolist(), [3, 4])
        self.assertEqual(window.read(10), b'')
        window.seek(0)
        self.assertEqual(len(window.read(32)), 32)
        for invalid in (-1, 65):
            with self.assertRaises(ValueError): window.seek(invalid)
        with self.assertRaises(ValueError): window.read(-1)
        with self.assertRaises(ValueError): cohort.Window(io.BytesIO(), 0, cohort.MAX_ROWS + 1)

    def test_multiple_windows_preserve_exact_counts_and_every_timed_pass(self):
        _, data = fixtures.StreamTests().fixture()
        data = data[:1201]
        with patch.object(score_stream, 'cuda_backend', return_value=(fixtures.FakeDevice(), {})), \
                patch.object(score_benchmark, 'cuda_backend', return_value=(fixtures.FakeDevice(), {})), \
                patch.object(cohort, 'MAX_ROWS', 500), patch.object(cohort, 'GPU_BATCH', 8192):
            full = score_stream.Comparison('fixture')
            full.consume(data)
            expected = {r['mode']: {'counts': r['cuda'], 'mates': r['mates_cuda']} for r in full.report()['groups']}
            result = cohort.measure_player(io.BytesIO(data.tobytes()), 'fixture', len(data))
            self.assertEqual(compare_groups(expected, result['groups'])['status'], 'passed')
            self.assertEqual([b['rows'] for b in result['blocks']], [500, 500, 201])
            self.assertEqual([b['offset'] for b in result['blocks']], [0, 500, 1000])
            self.assertTrue(all(b['row_mismatches'] == b['counter_mismatches'] == 0 for b in result['blocks']))
            self.assertEqual(len(result['samples_seconds']['cuda_cupy']), 3)
            empty = cohort.measure_player(io.BytesIO(), 'fixture', 0)
            self.assertEqual(empty['blocks'], [])
            for changed in (data.tobytes()[:-1], data.tobytes() + b'x'):
                with self.assertRaisesRegex(ValueError, 'length'):
                    cohort.measure_player(io.BytesIO(changed), 'fixture', len(data))
            with patch.object(cohort, 'summary_pass', return_value={'rows': 0}):
                with self.assertRaisesRegex(ValueError, 'Timed pass'):
                    cohort.measure_player(io.BytesIO(data.tobytes()), 'fixture', len(data))

    def test_merge_retains_unknown_mates_and_both_sides(self):
        target, part = cohort.empty_groups('alice'), cohort.empty_groups('alice')
        part['blitz']['counts'][13] = 3
        part['blitz']['mates'] = [
            {'side': 'for', 'moves': None, 'positions': 1},
            {'side': 'for', 'moves': 2, 'positions': 1},
            {'side': 'against', 'moves': 2, 'positions': 1}]
        cohort.merge_groups(target, part)
        cohort.merge_groups(target, part)
        self.assertEqual(target['blitz']['counts'][13], 6)
        self.assertEqual([r['positions'] for r in target['blitz']['mates']], [2, 2, 2])


if __name__ == '__main__': unittest.main()
