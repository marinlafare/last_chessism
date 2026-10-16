"""Column migration and bounded ingestion; no billable cloud calls."""
import asyncio
from copy import deepcopy
import json
import os
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.models import Base, CloudAnalysisJob, CloudAnalysisRun
from chessism_api.database.cloud_codec import encode_document, decode_document, checksum
from chessism_api.database import cloud_columns
from chessism_api.operations.cloud_analysis.ingestion import ingest_ready


class CodecTests(unittest.TestCase):
    def test_all_cloud_tables_have_real_columns_not_json_or_payload_text(self):
        from sqlalchemy import JSON
        for table in Base.metadata.tables.values():
            if table.name.startswith('cloud_'):
                self.assertFalse(any(isinstance(c.type, JSON) for c in table.c), table.name)
        self.assertIn('config__hash_mb', cloud_columns.TABLES['launch'].c)
        self.assertIn('sha256', cloud_columns.TABLES['receipt'].c)

    def test_empty_null_absent_numeric_types_and_nested_recovery_roundtrip(self):
        value = {'backend': 'batch_spot', 'recovery_session': 1, 'image': None,
                 'quota': {'CPUS': {'limit': 16.0, 'usage': 0, 'needed': 16}},
                 'recovery_summary': {'status': 'RETRYING', 'uids': {'job-one': 'uid-1'},
                                      'jobs': ['job-one'], 'last_exit_code': 50001},
                 'batch_status': {'state': 'FAILED', 'statusEvents': [
                     {'type': 'TASK_STATE_CHANGED', 'taskState': 'FAILED',
                      'taskExecution': {'exitCode': 50001}}]},
                 'tasks': [], 'config': {}, 'cleanup_report': {'image_blockers': {'image': ['job']}}}
        root, rows = encode_document('test', 'launch', value)
        decoded = decode_document(root, rows)
        self.assertEqual(checksum(decoded), checksum(value))
        self.assertIs(type(decoded['quota']['CPUS']['usage']), int)
        self.assertIs(type(decoded['quota']['CPUS']['limit']), float)
        bad = deepcopy(rows)
        bad['launch'][0]['recovery_session'] = 2
        with self.assertRaisesRegex(ValueError, 'checksum'):
            decode_document(root, bad)
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            encode_document('test', 'launch', {'unknown': 'must not be silently discarded'})

    def test_two_thousand_fens_use_four_temporary_identity_packs(self):
        data = [{'id': str(i), 'fen': f'fen-{i}'} for i in range(2000)]
        root, rows = encode_document('test', 'positions', data)
        self.assertEqual(len(rows['position']), 4)
        self.assertEqual(decode_document(root, rows), data)

    def test_real_cleanup_report_pending_compute_roundtrips_without_json(self):
        import sys
        from pathlib import Path
        from unittest.mock import patch
        # Use the actual cleanup producer and its fake cloud fixture, not a
        # handwritten success-only report that misses asynchronous teardown.
        fixtures = str(Path(__file__).resolve().parents[1] / 'stockfish-batch-worker/tests')
        with patch.object(sys, 'path', [fixtures, *sys.path]):
            from test_cleaning_job import FakeCloud, job
        from cleaning_job.cleanup import prepare, apply
        cloud = FakeCloud()
        cloud.resource_data = [{'name': job()['uid'] + '-group0-0', 'kind': kind,
                                'scope': 'zones/us-central1-c' if kind != 'instanceTemplates' else None}
                               for kind in ('instances', 'disks', 'instanceGroupManagers', 'instanceTemplates')]
        plan = prepare(cloud, ['batch-one'])
        pending = apply(cloud, plan, wait_seconds=0)
        self.assertFalse(pending['complete'])
        self.assertTrue(pending['data_deletion_deferred'])
        self.assertEqual([w[0] for w in cloud.writes], ['job'])
        for field in ('cleanup_report', 'cleanup_verification'):
            root, rows = encode_document('pending', 'launch', {field: pending})
            self.assertEqual(len(rows['compute_resource']), 4)
            self.assertEqual(decode_document(root, rows), {field: pending})
        cloud.resource_data = []
        finished = apply(cloud, plan, wait_seconds=0)
        self.assertTrue(finished['complete'])
        root, rows = encode_document('finished', 'launch', {'cleanup_report': finished})
        self.assertEqual(decode_document(root, rows), {'cleanup_report': finished})


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_downloads_overlap_single_writer_with_bounded_prefetch(self):
        read_batches, writes = [], []
        lock = threading.Lock()
        class Storage:
            def read(self, uri, limit):
                if uri.endswith('contract.json'): return b'{"version": 1}'
                index = int(uri.rsplit('/', 1)[1].split('.')[0])
                with lock: read_batches.append(index)
                return str(index).encode()
        peak, writing = 0, False
        async def writer(run_id, raw, index, **kwargs):
            nonlocal peak, writing
            self.assertFalse(writing)
            writing = True
            for _ in range(20):
                if len(read_batches) > len(writes) + 1: break
                await asyncio.sleep(.001)
            peak = max(peak, len(read_batches) - len(writes))
            writes.append(index)
            writing = False
            return 500
        run = SimpleNamespace(id='run', receipts={})
        count = await ingest_ready(Storage(), run, [{'index': 0, 'output': 'test', 'count': 10000,
                                                    'contract': {'version': 1}}], writer=writer)
        self.assertEqual(count, 10000)
        self.assertEqual(sorted(writes), list(range(20)))
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 5)

    async def test_missing_shard_does_not_block_another_shard_and_receipts_are_skipped(self):
        reads = []
        class Storage:
            def read(self, uri, limit):
                reads.append(uri)
                if uri.endswith('contract.json'): return b'{}'
                return None if uri.startswith('missing') else b'result'
        writer = AsyncMock(return_value=500)
        run = SimpleNamespace(id='r', receipts={'tasks/000001/batches/000000.json': {}})
        await ingest_ready(Storage(), run, [
            {'index': 0, 'output': 'missing', 'count': 1000, 'contract': {}},
            {'index': 1, 'output': 'ready', 'count': 1000, 'contract': {}}], writer=writer)
        writer.assert_awaited_once()
        self.assertEqual(writer.call_args.kwargs['task_index'], 1)
        self.assertEqual(writer.call_args.args[2], 1)
        self.assertNotIn('ready/batches/000000.json', reads)

    async def test_import_error_stops_writer_without_acknowledging_files(self):
        class Storage:
            def read(self, uri, limit): return b'{}'
        writer = AsyncMock(side_effect=ValueError('transaction rolled back'))
        with self.assertRaisesRegex(ValueError, 'rolled back'):
            await ingest_ready(Storage(), SimpleNamespace(id='r', receipts={}), [
                {'index': 0, 'output': 'x', 'count': 5000, 'contract': {}}], writer=writer)
        writer.assert_awaited_once()


if __name__ == '__main__': unittest.main()
