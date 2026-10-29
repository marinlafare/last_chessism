"""Offline transport budgets/parity. No production database, network, or GPU."""
from pathlib import Path
import unittest
from unittest.mock import patch
import numpy as np
from chessism_api.operations.cuda_summaries import batch_budget as b, blunder_compute
from chessism_api.operations.canonical_blunders import compute, registry, binary_source
from tests.test_canonical_blunders import fixture, report


class BudgetTests(unittest.TestCase):
    def test_uses_gpu_and_two_host_slots_not_fixed_two_million(self):
        budget = b.plan(8*1024**3, 8*1024**3, 8*1024**3)
        self.assertTrue(budget.fits(12_000_000, 150_000))
        self.assertFalse(budget.fits(24_000_000, 300_000))
        self.assertEqual(budget.describe()['input_slots'], 2)
        self.assertFalse(budget.smaller().fits(12_000_000, 150_000))

    def test_device_and_host_limits_both_apply(self):
        self.assertFalse(b.plan(2*1024**3, 8*1024**3, 8*1024**3).fits(6_000_000, 75_000))
        self.assertFalse(b.plan(8*1024**3, 8*1024**3, 2*1024**3).fits(4_000_000, 50_000))
        with self.assertRaisesRegex(RuntimeError, 'no CPU fallback'):
            b.plan(b.RESERVE, 8*1024**3, 8*1024**3)

    def test_actual_cgroup_remaining_memory_not_host_total(self):
        contents = {'/proc/meminfo': 'MemAvailable: 40000000 kB\n',
                    '/sys/fs/cgroup/memory.max': str(8*1024**3),
                    '/sys/fs/cgroup/memory.current': str(1024**3)}
        def read(path):
            if str(path) not in contents: raise FileNotFoundError(str(path))
            return contents[str(path)]
        with patch.object(Path, 'read_text', read):
            self.assertEqual(b.host_available(), 7*1024**3)
            contents['/sys/fs/cgroup/memory.max'] = 'max'
            self.assertEqual(b.host_available(), 40000000*1024)

    def test_large_blunders_exactly_match_frozen_outputs_and_provenance(self):
        games, rows = fixture(); _, config, _ = registry.freeze(report(), registry.ACK)
        expected, _ = compute.infer(np, rows, games, config, require_cuda=False)
        actual, info = blunder_compute.infer(np, rows, games, config, require_cuda=False)
        self.assertEqual(actual, expected)
        self.assertEqual(info['cuda_batch_rows'], len(rows))
        with self.assertRaisesRegex(RuntimeError, 'fallback'):
            blunder_compute.infer(np, rows, games, config)
        config['kernel_sha256'] = 'modified'
        with self.assertRaisesRegex(ValueError, 'implementation changed'):
            blunder_compute.infer(np, rows, games, config, require_cuda=False)

    def test_empty_and_unassessed_blunders_preserve_null_counts(self):
        _, config, _ = registry.freeze(report(), registry.ACK)
        empty = np.empty(0, compute.DTYPE)
        self.assertEqual(blunder_compute.infer(np, empty, [], config, require_cuda=False)[0], [])
        games, rows = fixture(); rows['ak'] = -1
        self.assertEqual(blunder_compute.infer(np, rows, games, config, require_cuda=False)[0],
                         compute.infer(np, rows, games, config, require_cuda=False)[0])

    def test_larger_copy_blocks_keep_games_whole_and_bounded(self):
        games = [{'total_positions': 128} for _ in range(4097)]
        blocks = list(binary_source.blocks(games, max_games=2048, max_rows=262144))
        self.assertEqual([len(block) for block in blocks], [2048, 2048, 1])


if __name__ == '__main__': unittest.main()
