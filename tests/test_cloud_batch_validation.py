"""Parallel CPU validation preserves the exact existing checkpoint checks."""
import asyncio
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from test_cloud_analysis import fixture
from chessism_api.operations.cloud_analysis import batch_validation as validation, ingestion
from stockfish_batch.checkpoints import encode


class ValidationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await asyncio.to_thread(validation.shutdown_pool)

    async def test_parallel_matches_reference_and_rejects_changed_inputs(self):
        rows, contract, raw = fixture()
        reference = validation.validate_batch(raw, 0, rows, contract)
        prepared = await asyncio.gather(*(validation.prepare_in_process(raw, rows, contract) for _ in range(8)))
        for item in prepared:
            receipt, results, seconds = validation.checked_batch(item, raw, rows, contract)
            self.assertEqual((receipt, results), reference[1:])
            self.assertGreater(seconds, 0)
        for new_raw, new_rows, new_contract in [(raw+b' ', rows, contract),
                (raw, list(reversed(rows)), contract), (raw, rows, {**contract, 'fingerprint': 'other'})]:
            with self.assertRaisesRegex(ValueError, 'no longer matches'):
                validation.checked_batch(prepared[0], new_raw, new_rows, new_contract)

    async def test_corruption_never_reaches_writer(self):
        rows, contract, raw = fixture()
        import json
        corrupt = json.loads(raw)
        corrupt['records'][0]['result_sha256'] = 'wrong'
        class Storage:
            def read(self, uri, limit):
                return encode(contract) if uri.endswith('contract.json') else encode(corrupt)
        run = SimpleNamespace(id='test', positions=rows, receipts={})
        with patch.object(ingestion, 'import_batch', new_callable=AsyncMock) as writer:
            with self.assertRaisesRegex(ValueError, 'checksum'):
                await ingestion.ingest_ready(Storage(), run, [{'output': 'test', 'count': 2, 'contract': contract}])
            writer.assert_not_awaited()

    async def test_real_prefetch_hands_writer_only_validated_batches(self):
        rows, contract, raw = fixture(501)
        from stockfish_batch.checkpoints import BatchCheckpoints
        checkpoints = BatchCheckpoints(None, '', contract, rows, 500)
        artifacts = {'test/contract.json': encode(contract)}
        for index, group in enumerate(checkpoints.groups):
            artifacts[f'test/batches/{index:06d}.json'] = encode({'fingerprint': contract['fingerprint'],
                'records': [checkpoints.prepare(row, {'fen': row['fen'], 'is_valid': True,
                    'analysis': {'pv': [], 'score': 0}}, 1) for row in group]})
        class Storage:
            def read(self, uri, limit): return artifacts.get(uri)
        seen = []
        async def writer(run_id, raw, index, **options):
            self.assertIsInstance(options['_prepared'], validation.ValidatedBatch)
            group = rows[index*500:(index+1)*500]
            validation.checked_batch(options['_prepared'], raw, group, contract)
            seen.append(index)
            return len(group)
        with patch.object(ingestion, 'import_batch', writer):
            result = await ingestion.ingest_ready(Storage(), SimpleNamespace(id='r', positions=rows, receipts={}),
                [{'output': 'test', 'count': len(rows), 'contract': contract}])
        self.assertEqual(result, 501)
        self.assertEqual(seen, [0, 1])


if __name__ == '__main__': unittest.main()
