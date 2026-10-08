"""Completed-unit reuse never reopens engines; incomplete units resume normally."""
from dataclasses import replace
import json
import unittest
from unittest.mock import AsyncMock, patch

from test_worker import WorkerTests
from stockfish_batch.work_unit import completed
from stockfish_batch.checkpoints import encode
from stockfish_batch.__main__ import execute


class WorkUnitTests(unittest.IsolatedAsyncioTestCase):
    execute = WorkerTests.execute
    def setUp(self):
        WorkerTests.setUp(self)
        self.config = replace(self.config, max_positions=7, upload_mode='background',
                              batch_size=500, compact_results=True)

    async def test_completed_unit_skips_engines_after_two_replacements(self):
        await self.execute()
        self.assertTrue(completed(self.config, engine_sha='test-engine-sha'))
        original = {p: p.read_bytes() for p in self.output.rglob('*.json')}
        with patch('stockfish_batch.__main__.run', new_callable=AsyncMock) as analyse, \
             patch('stockfish_batch.work_unit.binary_digest', return_value='test-engine-sha'):
            self.assertEqual(await execute(self.config, skip_completed_unit=True), 0)
            self.assertEqual(await execute(self.config, skip_completed_unit=True), 0)
            analyse.assert_not_awaited()
        self.assertEqual(original, {p: p.read_bytes() for p in self.output.rglob('*.json')})

    async def test_partial_unit_uses_normal_checkpoint_resume(self):
        self.assertFalse(completed(self.config, engine_sha='test-engine-sha'))
        with patch('stockfish_batch.__main__.run', new_callable=AsyncMock) as analyse:
            self.assertEqual(await execute(self.config, skip_completed_unit=True), 0)
            analyse.assert_awaited_once_with(self.config)

    async def test_completed_unit_mismatched_contract_or_manifest_fails_closed(self):
        await self.execute()
        with self.assertRaisesRegex(ValueError, 'contract'):
            completed(self.config, engine_sha='different-engine')
        path = self.output / 'manifest.json'
        manifest = json.loads(path.read_bytes())
        manifest['records'][0]['id'] = 'wrong-id'
        path.write_bytes(encode(manifest))
        with self.assertRaisesRegex(ValueError, 'identity'):
            completed(self.config, engine_sha='test-engine-sha')

    async def test_manifest_without_performance_report_is_not_skipped(self):
        await self.execute()
        (self.output / 'performance.json').unlink()
        self.assertFalse(completed(self.config, engine_sha='test-engine-sha'))
