"""Memory regression checks; synthetic data only, no cloud or database access."""
import hashlib
import json
import os
import tracemalloc
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.cloud_codec import checksum
from chessism_api.operations.cloud_analysis import cloud_run_controller as run_controller
from chessism_api.operations.cloud_analysis import controller, importer
from test_cloud_run_analysis import fixture, RUN_ID


class ChecksumTests(unittest.TestCase):
    def test_streamed_checksum_preserves_existing_canonical_bytes(self):
        for value in [None, [], {}, [0, -0.0, 1e-20, True, False, None],
                      {'z': ['ñ', '\\', '\n', '♟'], 'a': {'value': 1.25}},
                      {1: 'one', 2: 'two'}, ('tuple', 42)]:
            expected = hashlib.sha256(json.dumps(value, sort_keys=True,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()
            self.assertEqual(checksum(value), expected)
        with self.assertRaises(ValueError):
            checksum({'value': float('nan')})

    def test_large_checksum_has_bounded_temporary_memory(self):
        value = [{'id': f'{i:064x}', 'sha256': 'a'*64, 'object': 'tasks/000001/batches/000000.json'}
                 for i in range(25000)]
        expected = hashlib.sha256(json.dumps(value, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        tracemalloc.start()
        try:
            self.assertEqual(checksum(value), expected)
            self.assertLess(tracemalloc.get_traced_memory()[1], 2*1024*1024)
        finally:
            tracemalloc.stop()

    def test_manifest_still_rejects_missing_reordered_or_changed_ids_and_hashes(self):
        positions = [{'id': 'a'}, {'id': 'b'}]
        contract = {'fingerprint': 'original'}
        receipts = {'batches/000000.json': {'records': [
            {'id': 'a', 'sha256': 'one'}, {'id': 'b', 'sha256': 'two'}]}}
        manifest = {'status': 'complete', 'fingerprint': 'original', 'position_count': 2,
                    'records': receipts['batches/000000.json']['records']}
        importer.validate_manifest(manifest, positions, contract, receipts)
        for rows in [positions[:1], list(reversed(positions)), [{'id': 'c'}, {'id': 'b'}]]:
            with self.assertRaises(ValueError):
                importer.validate_manifest(manifest, rows, contract, receipts)
        with self.assertRaises(ValueError):
            importer.validate_manifest({**manifest, 'records': [{'id': 'a', 'sha256': 'changed'},
                {'id': 'b', 'sha256': 'two'}]}, positions, contract, receipts)


class FinalizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fully_imported_success_does_not_reload_run(self):
        launch, remote, execution = fixture()
        launch['tasks'] = [{'index': 0, 'start': 0, 'count': 1, 'contract': {'fingerprint': 'root'}}]
        launch['run_execution'] = {'name': execution['name'], 'uid': execution['uid']}
        execution['completionTime'] = '2026-10-28T03:00:00Z'
        run = SimpleNamespace(id=RUN_ID, job_id='job', status='running', launch=launch,
            positions=[{'id': 'one'}], contract={'fingerprint': 'root'},
            receipts={'tasks/000000/batches/000000.json': {'records': [{'id': 'one'}]}})
        cloud = Mock()
        cloud.request.side_effect = [remote, execution]
        with patch.object(run_controller.api, 'terminal', return_value='SUCCEEDED'), \
             patch.object(run_controller, 'ingest_ready', new=AsyncMock(return_value=0)), \
             patch.object(run_controller, 'verify_tasks', return_value=[]), \
             patch.object(run_controller.StageTimings, 'save', new=AsyncMock()), \
             patch.object(run_controller, 'AsyncDBSession', side_effect=AssertionError('Duplicate run load')), \
             patch.object(controller, 'save_run', new=AsyncMock()) as save:
            await run_controller.advance(controller.Controller(cloud, 'unused'),
                SimpleNamespace(selection={}), run)
        self.assertEqual(save.call_args.kwargs['status'], 'finalizing')
        self.assertEqual(save.call_args.kwargs['launch']['manifest']['position_count'], 1)
        self.assertFalse(any(c.kwargs.get('method') == 'POST' for c in cloud.request.call_args_list))


if __name__ == '__main__': unittest.main()
