import asyncio
import copy
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import Mock, patch

import chess
from benchmark_vm16 import large, large_phase
from cleaning_job.cleanup import prefixes
from stockfish_batch.checkpoints import encode


class LargeBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.ident = '20261006d200'
        self.image = large.REPOSITORY + 'vm200-' + self.ident + '@sha256:' + 'a' * 64

    def test_one_vm_two_sequential_containers_and_two_hour_total_limit(self):
        spec = large.specification(self.ident, self.image, large.SCRIPT.read_text())
        group = spec['taskGroups'][0]
        task = group['taskSpec']
        self.assertEqual(group['taskCount'], '1')
        self.assertEqual(group['parallelism'], '1')
        self.assertEqual(task['maxRunDuration'], '7200s')
        self.assertEqual(task['maxRetryCount'], 0)
        self.assertEqual(task['computeResource'], {'cpuMilli': '16000', 'memoryMib': '12288'})
        self.assertEqual(len(task['runnables']), 2)
        for phase, runnable in zip(('interrupt', 'resume'), task['runnables']):
            self.assertFalse(runnable.get('background'))
            self.assertFalse(runnable.get('ignoreExitStatus'))
            container = runnable['container']
            self.assertEqual(container['entrypoint'], 'python')
            self.assertEqual(container['commands'][2:4], [phase, '25000'])
            self.assertIn('--memory=12g', container['options'])
            config = large.configuration(self.ident, phase)
            self.assertEqual((config.workers, config.hash_mb, config.nodes, config.max_positions),
                             (16, 256, 100000, 200000))
            self.assertEqual(config.stall_timeout, 300)
        self.assertEqual(spec['allocationPolicy']['instances'][0]['policy']['machineType'], 'n2d-highcpu-16')
        self.assertEqual(prefixes(spec), [f'inputs/vm200-{self.ident}/', f'results/vm200-{self.ident}/'])

    def test_invalid_identity_and_foreign_image_refused(self):
        for ident in ('bad', '../../foo', self.ident + '1'):
            with self.assertRaises(ValueError):
                large.configuration(ident)
        with self.assertRaises(ValueError):
            large.specification(self.ident, self.image.replace('d200', 'd201'), 'pass')

    def test_both_remote_phases_and_runtime_limits_are_verified(self):
        spec = large.specification(self.ident, self.image, 'pass')
        large.check_job(copy.deepcopy(spec), spec)
        changed = copy.deepcopy(spec)
        changed['taskGroups'][0]['taskSpec']['runnables'][1]['container']['commands'][-1] = 'tampered'
        with self.assertRaises(ValueError):
            large.check_job(changed, spec)
        changed = copy.deepcopy(spec)
        changed['taskGroups'][0]['taskSpec']['maxRunDuration'] = '14400s'
        with self.assertRaises(ValueError):
            large.check_job(changed, spec)

    def test_changed_checkpoint_is_not_a_successful_recovery(self):
        with self.assertRaises(ValueError):
            large_phase.unchanged({'000000.json': {'generation': '1'}},
                                  {'000000.json': {'generation': '2'}})
        with self.assertRaises(ValueError):
            large_phase.unchanged({}, {})
        large_phase.unchanged({'a': {'generation': '1'}}, {'a': {'generation': '1'}, 'b': {}})

    def test_kill_only_own_live_subprocess_group(self):
        process = Mock(pid=123, returncode=None)
        with patch.object(large_phase.os, 'killpg') as kill:
            large_phase.stop_owned(process)
            kill.assert_called_once_with(123, signal.SIGKILL)
            process.returncode = 0
            large_phase.stop_owned(process)
            large_phase.stop_owned(None)
            self.assertEqual(kill.call_count, 1)

    def test_lost_submission_response_cannot_start_second_job(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / 'input.jsonl').write_bytes(b'input')
            client = Mock()
            client.storage.return_value.read.return_value = b'input'
            client.submit.side_effect = TimeoutError('lost response')
            client.jobs.return_value = []
            client.resources.return_value = []
            with patch.object(large, 'load', return_value={'id': self.ident, 'job': large.job_name(self.ident),
                  'specification': {}}), patch.object(large.database, 'execute'), patch.object(large, 'quota_check', return_value={}):
                with self.assertRaises(TimeoutError):
                    large.launch(directory, client)
                with self.assertRaises(FileExistsError):
                    large.launch(directory, client)
            client.submit.assert_called_once()

    def test_collection_does_not_accept_failed_job(self):
        with patch.object(large, 'load', return_value={}), patch.object(large, 'status', return_value={'state': 'FAILED'}):
            with self.assertRaises(ValueError):
                large.collect(Path('/unused'), Mock())


@unittest.skipUnless(os.environ.get('STOCKFISH_LARGE_RECOVERY_INTEGRATION') == '1', 'opt-in isolated real-engine restart')
class LocalRestartIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_actual_sigkill_and_fresh_worker_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root / 'input.jsonl', root / 'results'
            source.write_bytes(b''.join(encode({'id': str(i), 'fen': chess.STARTING_FEN}) for i in range(30)))
            args = ['--input', str(source), '--output', str(output), '--workers', '1', '--threads', '1',
                    '--hash-mb', '16', '--nodes', '100000', '--max-positions', '30', '--memory-mib', '1024',
                    '--run-timeout', '60', '--stall-timeout', '15', '--upload-mode', 'background',
                    '--batch-size', '5', '--compact-results']
            await large_phase.phase('interrupt', 5, args)
            before = json.loads((output / 'recovery/interruption.json').read_bytes())
            self.assertEqual(before['child_exit_code'], -9)
            self.assertFalse((output / 'manifest.json').exists())
            await large_phase.phase('resume', 5, args)
            after = json.loads((output / 'recovery/resume.json').read_bytes())
            self.assertTrue(after['checkpoint_recovery_verified'])
            self.assertGreaterEqual(after['worker_started']['resumed'], 5)
            self.assertLess(after['worker_started']['resumed'], 30)
            self.assertEqual(after['complete']['saved'], 30)
            self.assertTrue((output / 'manifest.json').exists())


if __name__ == '__main__':
    unittest.main()
