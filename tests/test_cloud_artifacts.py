"""Completion reads remain bounded and never turn storage errors into absence."""
import json
import unittest
from unittest.mock import Mock

from chessism_api.operations.cloud_analysis.artifacts import (
    PERFORMANCE_MAX_BYTES, read_manifest, read_performance,
)
from stockfish_batch.storage import MAX_MANIFEST_BYTES


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
