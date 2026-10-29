"""Opt-in disposable PostgreSQL regression tests for finalization memory fixes."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from sqlalchemy import select
import test_cloud_analysis_database as existing
from chessism_api.database import cloud_codec as codec
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudResultBatch
from chessism_api.operations.cloud_analysis import controller, importer, history
from chessism_api.operations.cloud_analysis.result_receipts import completion_manifest
from chessism_api.operations.cloud_analysis import cloud_run_controller as run_controller
from test_cloud_run_analysis import fixture


@unittest.skipUnless(existing.TEST_URL, 'requires explicitly isolated PostgreSQL')
class FinalizationDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown
    reserved_run = existing.CloudDatabaseTests.reserved_run
    results = existing.CloudDatabaseTests.results

    async def test_scalar_updates_and_verified_transition_do_not_read_payloads(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        await importer.import_batch(run.id, raw, 0)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
        manifest = completion_manifest(contract, len(saved.positions), saved.receipts)
        with patch.object(codec, 'read_documents', side_effect=AssertionError('Full document read')):
            await controller.save_run(run.id, cloud_state='SUCCEEDED', status='running')
            await controller.save_run(run.id, status='finalizing', launch={'manifest': manifest})
        async with AsyncDBSession() as session:
            state = (await session.execute(select(CloudAnalysisRun.results_verified_at,
                CloudAnalysisRun.completion_manifest_sha256, CloudAnalysisRun.imported_count)
                .where(CloudAnalysisRun.id == run.id))).one()
        self.assertIsNotNone(state.results_verified_at)
        self.assertEqual(state.completion_manifest_sha256, codec.checksum(manifest))
        self.assertEqual(state.imported_count, 2)

    async def test_normal_compaction_does_not_reload_positions_or_receipts(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        await importer.import_batch(run.id, raw, 0)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
        manifest = completion_manifest(contract, len(saved.positions), saved.receipts)
        await controller.save_run(run.id, status='finalizing', launch={'manifest': manifest})
        await controller.save_run(run.id, status='refreshing')
        original = codec.read_document
        def guarded(connection, ref):
            if ref.endswith((':positions', ':receipts')) or ':batch:' in ref:
                raise AssertionError('Unnecessary input/receipt payload read')
            return original(connection, ref)
        with patch.object(codec, 'read_document', side_effect=guarded):
            self.assertTrue(await history.compact_run(run.id))
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.status, 'complete')
            self.assertEqual(saved.imported_count, 2)
            self.assertEqual(saved.positions, [])
            self.assertEqual(saved.receipts, {})

    async def test_receipts_read_in_bounded_groups_and_checksums_remain_enforced(self):
        run = await self.reserved_run()
        async with AsyncDBSession() as session, session.begin():
            connection = await session.connection()
            for index in range(65):
                name = f'tasks/{index:06d}/batches/000000.json'
                ref = f'{run.id}:batch:{index}:0'
                receipt = {'sha256': 'a'*64, 'records': [
                    {'id': str(index), 'object': name, 'sha256': 'b'*64}]}
                await connection.run_sync(codec.write_document, ref, 'receipt', receipt)
                session.add(CloudResultBatch(run_id=run.id, task_index=index, batch_index=0,
                    object_name=name, sha256='a'*64, receipt_sha256=codec.checksum(receipt),
                    position_count=1, size_bytes=100, detail_record_id=ref))
        with patch.object(codec, 'read_documents', wraps=codec.read_documents) as reads:
            async with AsyncDBSession() as session:
                saved = await session.get(CloudAnalysisRun, run.id,
                    execution_options={'cloud_documents': ('receipts',)})
                self.assertEqual(len(saved.receipts), 65)
        self.assertEqual([len(c.args[1]) for c in reads.call_args_list], [1, 32, 32, 1])
        async with self.engine.begin() as connection:
            await connection.execute(codec.columns.ROOTS.update().where(
                codec.columns.ROOTS.c.id == ref).values(sha256='0'*64))
        with self.assertRaisesRegex(ValueError, 'checksum'):
            async with AsyncDBSession() as session:
                await session.get(CloudAnalysisRun, run.id,
                    execution_options={'cloud_documents': ('receipts',)})

    async def test_last_import_refreshes_receipts_without_reloading_positions(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        launch, remote, execution = fixture()
        launch['tasks'] = [{'index': 0, 'start': 0, 'count': 2, 'contract': contract}]
        launch['run_execution'] = {'name': execution['name'], 'uid': execution['uid']}
        execution['completionTime'] = '2026-10-28T03:00:00Z'
        await controller.save_run(run.id, status='running', launch=launch)
        async with AsyncDBSession() as session:
            current = await session.get(CloudAnalysisRun, run.id)
        self.assertFalse(current.receipts)
        cloud = Mock()
        cloud.request.side_effect = [remote, execution]
        async def ingest(*args, **kwargs):
            return await importer.import_batch(run.id, raw, 0, task_index=0)
        def verify(factory, config, tasks, positions, receipts):
            self.assertIs(positions, current.positions)
            self.assertEqual(sum(len(r['records']) for r in receipts.values()), 2)
            return []
        original = codec.read_documents
        def guarded(connection, refs):
            self.assertNotIn(run.id + ':positions', refs)
            return original(connection, refs)
        with patch.object(codec, 'read_documents', side_effect=guarded), \
             patch.object(run_controller.api, 'terminal', return_value='SUCCEEDED'), \
             patch.object(run_controller, 'ingest_ready', new=AsyncMock(side_effect=ingest)), \
             patch.object(run_controller, 'verify_tasks', side_effect=verify):
            await run_controller.advance(controller.Controller(cloud, 'unused'),
                SimpleNamespace(selection={}), current)
        async with AsyncDBSession() as session:
            state = (await session.execute(select(CloudAnalysisRun.status,
                CloudAnalysisRun.imported_count).where(CloudAnalysisRun.id == run.id))).one()
        self.assertEqual(tuple(state), ('finalizing', 2))


if __name__ == '__main__': unittest.main()
