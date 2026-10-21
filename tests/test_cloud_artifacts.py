"""Completion reads remain bounded and never turn storage errors into absence."""
import json
import threading
import time
import unittest
from unittest.mock import Mock

from chessism_api.operations.cloud_analysis.artifacts import (
    PERFORMANCE_MAX_BYTES, read_manifest, read_performance, read_completion,
)
from stockfish_batch.storage import MAX_MANIFEST_BYTES


class ParallelCompletionTests(unittest.TestCase):
    def test_unit_artifacts_overlap_with_separate_clients_and_original_limits(self):
        barrier, clients, reads = threading.Barrier(2, timeout=5), [], []
        parent = self

        class Storage:
            def __init__(self):
                self.owner = threading.get_ident()
                clients.append(self)

            def read(self, uri, limit):
                parent.assertEqual(threading.get_ident(), self.owner)
                name = uri.rsplit('/', 1)[1]
                parent.assertEqual(limit, MAX_MANIFEST_BYTES if name == 'manifest.json' else PERFORMANCE_MAX_BYTES)
                reads.append(name)
                barrier.wait()
                return json.dumps({'name': name}).encode()

        self.assertEqual(read_completion(Storage, 'gs://test/unit'),
                         ({'name': 'manifest.json'}, {'name': 'performance.json'}))
        self.assertEqual(len(clients), 2)
        self.assertCountEqual(reads, ['manifest.json', 'performance.json'])

    def test_error_waits_for_other_reader_and_never_returns_partial_completion(self):
        barrier, finished = threading.Barrier(2, timeout=5), threading.Event()

        class Storage:
            def read(self, uri, limit):
                barrier.wait()
                if uri.endswith('manifest.json'):
                    raise PermissionError('denied')
                time.sleep(.01)
                finished.set()
                return b'{}'

        with self.assertRaises(PermissionError):
            read_completion(Storage, 'gs://test/unit')
        self.assertTrue(finished.is_set())

    def test_missing_and_malformed_unit_artifacts_are_not_success(self):
        storage = Mock()
        storage.read.return_value = None
        self.assertEqual(read_completion(lambda: storage, 'gs://test/unit'), (None, None))
        storage.read.return_value = b'[]'
        with self.assertRaisesRegex(ValueError, 'JSON object'):
            read_completion(lambda: storage, 'gs://test/unit')


class ArtifactTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_artifacts_use_their_original_size_limits(self):
        storage = Mock()
        storage.read.return_value = b'{"fingerprint":"expected"}'
        for reader, name, limit in (
            (read_manifest, 'manifest.json', MAX_MANIFEST_BYTES),
            (read_performance, 'performance.json', PERFORMANCE_MAX_BYTES),
        ):
            self.assertEqual(await reader(storage, 'gs://test/results'), {'fingerprint': 'expected'})
            storage.read.assert_called_with('gs://test/results/' + name, limit)

    async def test_only_missing_objects_return_none(self):
        storage = Mock()
        for reader in (read_manifest, read_performance):
            storage.read.return_value = None
            self.assertIsNone(await reader(storage, 'gs://test/results'))
            storage.read.return_value = b'not-json'
            with self.assertRaises(json.JSONDecodeError):
                await reader(storage, 'gs://test/results')
            for raw in (b'null', b'[]', b'0', b'"missing"'):
                storage.read.return_value = raw
                with self.assertRaisesRegex(ValueError, 'must contain a JSON object'):
                    await reader(storage, 'gs://test/results')
            for error in (PermissionError('denied'), TimeoutError('unavailable'), ValueError('too large')):
                storage.read.side_effect = error
                with self.assertRaises(type(error)):
                    await reader(storage, 'gs://test/results')
            storage.read.side_effect = None
