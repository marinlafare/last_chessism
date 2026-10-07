"""Opt-in real-engine check; run in the isolated benchmark image without networking."""
import json
import os
from pathlib import Path
import tempfile
import unittest

import chess
from stockfish_batch.checkpoints import encode
from stockfish_batch.config import Config
from stockfish_batch.worker import run


@unittest.skipUnless(os.environ.get('STOCKFISH_VM16_INTEGRATION') == '1', 'opt-in 16-engine integration')
class VM16Integration(unittest.IsolatedAsyncioTestCase):
    async def test_sixteen_engines_with_reduced_hash_and_memory_reporting(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'input.jsonl'
            source.write_bytes(b''.join(encode({'id': str(i), 'fen': chess.STARTING_FEN}) for i in range(64)))
            config = Config(input=str(source), output=str(root / 'results'),
                workers=16, threads=1, hash_mb=256, memory_mib=12288,
                nodes=100000, max_positions=64, multipv=4, run_timeout=90, stall_timeout=30,
                upload_mode='background', batch_size=500, compact_results=True)
            await run(config, progress=lambda *a, **k: None)
            report = json.loads((root / 'results/performance.json').read_bytes())
            self.assertEqual(report['analyzed_this_attempt'], 64)
            self.assertEqual(len(report['workers']), 16)
            self.assertTrue(all(worker['positions'] > 0 for worker in report['workers']))
            memory = report['metrics']['memory']
            self.assertGreater(memory['worker']['samples'], 0)
            self.assertGreater(memory['worker']['max_bytes'], 0)
            self.assertGreater(memory['host_available']['min_bytes'], 0)
            self.assertIn(memory['cgroup_oom_kills_delta'], (0, None))
            print(json.dumps({'integration_positions': 64,
                'worker_seconds': report['metrics']['worker_seconds'], 'memory': memory}))
