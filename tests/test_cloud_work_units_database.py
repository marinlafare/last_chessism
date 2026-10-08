"""Opt-in real PostgreSQL tests, fake cloud only; never the application DB."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

import test_cloud_analysis_database as existing
from sqlalchemy import func, select, text, update
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (CloudAnalysisJob, CloudAnalysisRun, CloudBatchUnit,
    CloudFenClaim, CloudResultBatch, CloudControllerHeartbeat, CloudPhaseTiming, Fen)
from chessism_api.operations.cloud_analysis import controller, importer, history
from chessism_api.operations.cloud_analysis import work_units_controller as fleet
from chessism_api.operations.cloud_analysis import work_units_results as results
from chessism_api.operations.cloud_analysis.migrate_work_units import upgrade
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.routers.cloud_analysis import create_cloud_job, cloud_jobs
from stockfish_batch.checkpoints import BatchCheckpoints, encode

IMAGE = 'us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:' + 'a' * 64


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL test database')
class WorkUnitDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown

    async def root(self):
        selection = {**self.job.selection, 'execution_mode': 'batch_work_units_v1'}
        await controller.save_job(self.job.id, target=3, selection=selection)
        self.job.selection, self.job.target = selection, 3
        await controller.reserve(self.job, 3)
        async with AsyncDBSession() as session:
            return (await session.scalars(select(CloudAnalysisRun))).one()

    async def reload(self, ident):
        async with AsyncDBSession() as session:
            return await session.get(CloudAnalysisRun, ident)

    async def reserve_units(self):
        root = await self.root()
        launch = {'backend': 'batch_spot', 'workflow_version': 3, 'multi_vm': True,
                  'units': [], 'tasks': [], 'jobs': [], 'n_vms': 1}
        await controller.save_run(root.id, launch=launch)
        for count in (2, 1):
            root = await self.reload(root.id)
            self.assertEqual(await fleet.reserve_unit(self.job, root, root.launch, 0, count), count)
        return await self.reload(root.id)

    async def objects(self, unit):
        run = await self.reload(unit['run_id'])
        contract = unit['contract']
        checkpoints = BatchCheckpoints(None, '', contract, run.positions, 500)
        records = []
        for row in run.positions:
            analysis = ([{'multipv': i, 'nodes': 100000, 'score': 20-i,
                         'wdl': [200, 700, 100], 'pv': [move]}
                        for i, move in enumerate(['e2e4', 'd2d4', 'g1f3', 'c2c4'], 1)]
                        if row['fen'] == self.fens[0] else {'score': 0, 'pv': []})
            records.append(checkpoints.prepare(row, {'fen': row['fen'], 'is_valid': True, 'analysis': analysis}, 1))
        raw = encode({'fingerprint': contract['fingerprint'], 'records': records})
        _, receipt, _ = importer.validate_batch(raw, 0, run.positions, contract)
        manifest = {'status': 'complete', 'fingerprint': contract['fingerprint'],
                    'position_count': unit['count'], 'records': receipt['records']}
        performance = {'schema_version': 1, 'fingerprint': contract['fingerprint'],
                       'position_count': unit['count'], 'resumed': 0, 'analyzed_this_attempt': unit['count'],
                       'workers': [{'worker': 0, 'positions': unit['count'], 'analysis_seconds': 1, 'upload_wait_seconds': 0}],
                       'metrics': {'worker_seconds': 2, 'fen_per_second': unit['count'] / 2}}
        output = unit['config']['output']
        return {output + '/contract.json': encode(contract), output + '/batches/000000.json': raw,
                output + '/manifest.json': encode(manifest), output + '/performance.json': encode(performance)}

    async def test_units_claim_disjoint_fens_and_parent_ui_does_not_load_inputs(self):
        root = await self.reserve_units()
        self.assertEqual(root.positions, [])
        self.assertEqual(root.position_count, 3)
        self.assertEqual([u['count'] for u in root.launch['units']], [2, 1])
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 3)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudBatchUnit)), 2)
        local, fens = await existing.get_fens_for_analysis(10, raise_errors=True)
        self.assertFalse(fens)
        if local: await local.close()
        view = await cloud_jobs()
        self.assertEqual(len(view['jobs'][0]['runs']), 1)
        self.assertEqual(view['jobs'][0]['runs'][0]['positions'], 3)

    async def test_preparation_timings_are_native_completed_rows_without_closing_parent_phase(self):
        # Timeout also catches a separate timing connection deadlocking on the
        # reservation's run FK. No cloud operations are involved.
        root = await asyncio.wait_for(self.reserve_units(), timeout=20)
        async with AsyncDBSession() as session:
            rows = (await session.scalars(select(CloudPhaseTiming).where(
                CloudPhaseTiming.job_id == self.job.id))).all()
            self.assertEqual(await session.scalar(text(
                "SELECT reltuples FROM pg_class WHERE oid = 'cloud_fen_claim'::regclass")), 2)
        for unit in root.launch['units']:
            measured = [r for r in rows if r.run_id == unit['run_id']]
            self.assertEqual({r.phase for r in measured}, {
                'prepare_statistics', 'prepare_select', 'prepare_recheck', 'prepare_encode',
                'prepare_validate', 'prepare_persist', 'prepare_claims', 'prepare_commit'})
            self.assertTrue(all(r.finished_at and r.outcome == 'complete' and r.duration_seconds >= 0 for r in measured))
        parent_phase, = [r for r in rows if r.run_id == root.id]
        self.assertEqual(parent_phase.phase, 'preparing')
        self.assertIsNone(parent_phase.finished_at)

    async def test_invalid_fen_rolls_back_unit_claims_and_records_failed_validation(self):
        root = await self.root()
        async with AsyncDBSession() as session, session.begin():
            session.add(Fen(fen='invalid FEN', n_games=100, moves_counter='{}'))
        with self.assertRaises(ValueError):
            await fleet.reserve_unit(self.job, root, {'units': []}, 0, 1)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudBatchUnit)), 0)
            failure = (await session.scalars(select(CloudPhaseTiming).where(
                CloudPhaseTiming.phase == 'prepare_validate'))).one()
            self.assertEqual((failure.outcome, failure.run_id), ('failed', root.id))
        self.assertEqual((await self.reload(root.id)).position_count, 0)

    async def test_statistics_failure_stops_before_reservation(self):
        root = await self.root()
        with patch.object(fleet, 'refresh_claim_statistics', side_effect=RuntimeError('analyze failed')), \
             patch('chessism_api.database.ask_db.get_fens_for_analysis', new_callable=AsyncMock) as select_fens:
            with self.assertRaisesRegex(RuntimeError, 'analyze failed'):
                await fleet.reserve_unit(self.job, root, {'units': []}, 0, 1)
            select_fens.assert_not_awaited()
        self.assertEqual((await self.reload(root.id)).position_count, 0)

    async def test_statistics_refresh_repairs_empty_to_populated_claim_estimate(self):
        root = await self.root()
        # Synthetic keys intentionally bypass chess validation: this is a SQL
        # planner regression, isolated from the application database.
        async with self.engine.begin() as connection:
            await connection.execute(text('ALTER TABLE cloud_fen_claim SET (autovacuum_enabled = false)'))
            await connection.execute(text("""
                INSERT INTO fen(fen, n_games, moves_counter)
                SELECT repeat(md5(i::text), 2), 100000-i, '{}' FROM generate_series(1,100000) i
            """))
            await connection.execute(text("""
                INSERT INTO cloud_fen_claim(fen,run_id)
                SELECT repeat(md5(i::text), 2), :run FROM generate_series(1,10000) i
            """), {'run': root.id})
            await connection.execute(text('DELETE FROM cloud_fen_claim'))
            await connection.execute(text('ANALYZE fen'))
            await connection.execute(text('ANALYZE cloud_fen_claim'))
        async with self.engine.begin() as connection:
            await connection.execute(text("""
                INSERT INTO cloud_fen_claim(fen,run_id)
                SELECT repeat(md5(i::text), 2), :run FROM generate_series(1,5000) i
            """), {'run': root.id})
            self.assertEqual(await connection.scalar(text(
                "SELECT reltuples FROM pg_class WHERE oid = 'cloud_fen_claim'::regclass")), 0)
        await fleet.refresh_claim_statistics()
        async with self.engine.connect() as connection:
            self.assertEqual(await connection.scalar(text(
                "SELECT reltuples FROM pg_class WHERE oid = 'cloud_fen_claim'::regclass")), 5000)
        session, fens = await existing.get_fens_for_analysis(5000, raise_errors=True)
        try:
            self.assertEqual(len(fens), 5000)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim).where(
                CloudFenClaim.fen.in_(fens))), 0)
        finally:
            await session.close()

    async def test_import_updates_parent_and_job_atomically_exactly_once(self):
        root = await self.reserve_units()
        unit = root.launch['units'][1]
        objects = await self.objects(unit)
        raw = objects[unit['config']['output'] + '/batches/000000.json']
        self.assertEqual(await importer.import_batch(unit['run_id'], raw, 0), 1)
        self.assertEqual(await importer.import_batch(unit['run_id'], raw, 0), 0)
        self.assertEqual((await self.reload(root.id)).imported_count, 1)

    async def test_all_units_reserved_and_uploaded_before_any_google_submission(self):
        root, cloud = await self.root(), Mock()
        worker = controller.Controller(cloud, 'unused')
        cloud.publish.return_value = IMAGE
        cloud.storage.return_value.create.return_value = True
        cloud.start_recovery.return_value = 'owner'
        with patch.object(fleet, 'verify_clean_workspace'), patch.object(fleet, 'check_bucket'), \
             patch.object(fleet.batch_spot, 'check_quota', return_value={}), \
             patch.object(fleet, 'inspect_worker', return_value=('sha256:' + 'a' * 64, {})), \
             patch.object(fleet.work_units, 'ranges', return_value=[(0, 2), (0, 1)]):
            for _ in range(3):
                await fleet.advance(worker, self.job, await self.reload(root.id))
            current = await self.reload(root.id)
            self.assertEqual(current.status, 'publishing')
            self.assertEqual(current.position_count, 3)
            self.assertEqual(len(current.launch['units']), 2)
            cloud.start_recovery.assert_not_called()
            await fleet.advance(worker, self.job, current)
            await fleet.advance(worker, self.job, await self.reload(root.id))
            self.assertEqual(cloud.storage.return_value.create.call_count, 2)
            cloud.start_recovery.assert_not_called()
            await fleet.advance(worker, self.job, await self.reload(root.id))
        self.assertEqual((await self.reload(root.id)).status, 'running')
        cloud.start_recovery.assert_called_once()
        spec = cloud.start_recovery.call_args.args[1]['spec']
        self.assertEqual(len(spec['taskGroups'][0]['taskSpec']['runnables']), 2)

    async def test_changed_checkpoint_does_not_increment_parent_again(self):
        root = await self.reserve_units()
        unit = root.launch['units'][1]
        objects = await self.objects(unit)
        raw = objects[unit['config']['output'] + '/batches/000000.json']
        await importer.import_batch(unit['run_id'], raw, 0)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 1)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
        bad = json.loads(raw)
        bad['records'][0]['result_sha256'] = 'f' * 64
        with self.assertRaises(ValueError): await importer.import_batch(unit['run_id'], encode(bad), 0)
        self.assertEqual((await self.reload(root.id)).imported_count, 1)

    async def test_offline_catchup_proofs_cleanup_and_compaction_survive_each_restart(self):
        root = await self.reserve_units()
        objects = {}
        for unit in root.launch['units']: objects.update(await self.objects(unit))
        storage = Mock()
        storage.read.side_effect = lambda uri, *args: objects.get(uri)
        cloud = Mock()
        cloud.storage.return_value = storage
        launch = deepcopy(root.launch)
        launch['tasks'] = fleet.lanes(launch)
        for i, unit in enumerate(launch['units']):
            launch['units'][i] = await results.import_unit(cloud, unit)
        root = await self.reload(root.id)
        self.assertEqual(root.imported_count, 3)
        await results.verify_saved(root, launch)
        # Child proof survives a crash before it was saved in the parent.
        cached = await results.import_unit(cloud, root.launch['units'][0])
        self.assertTrue(cached['verified'])
        broken = deepcopy(launch)
        broken['units'][0]['manifest_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'proof changed'): await results.verify_saved(root, broken)
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(CloudAnalysisRun).where(CloudAnalysisRun.id == root.id).values(imported_count=2))
        with self.assertRaisesRegex(ValueError, 'Uncommitted'):
            await results.verify_saved(await self.reload(root.id), launch)
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(CloudAnalysisRun).where(CloudAnalysisRun.id == root.id).values(imported_count=3))
        launch = results.performance(launch)
        await controller.save_run(root.id, status='finalizing', launch=launch)
        # Parent proof is immutable, even if altered metadata still looks complete.
        root = await self.reload(root.id)
        await results.verify_saved(root, root.launch)
        self.assertEqual(root.launch['performance']['position_count'], 3)
        worker = controller.Controller(cloud, 'unused')
        await fleet.advance(worker, self.job, root)
        root = await self.reload(root.id)
        self.assertEqual(root.status, 'cleanup_planning')
        # All destructive cloud operations are mocked; gates run against PostgreSQL.
        with patch.object(fleet, 'prepare', return_value={}), \
             patch.object(fleet, 'cleaning_job', return_value={'complete': True}), \
             patch.object(fleet, 'plan_logs', return_value={}), \
             patch.object(fleet, 'remove_logs', return_value={'complete': True}):
            for expected in ('cleaning', 'log_planning', 'log_cleaning', 'refreshing'):
                await fleet.advance(worker, self.job, await self.reload(root.id))
                self.assertEqual((await self.reload(root.id)).status, expected)
        # Exercise the real SQL affected-game query (no associated games here),
        # then compact children one at a time with a fresh session every tick.
        for _ in range(4):
            await fleet.advance(worker, self.job, await self.reload(root.id))
        root = await self.reload(root.id)
        self.assertEqual(root.status, 'complete')
        self.assertTrue(root.details_pruned)
        async with AsyncDBSession() as session:
            runs = (await session.scalars(select(CloudAnalysisRun))).all()
            self.assertTrue(all(r.details_pruned and not r.positions and not r.receipts for r in runs))
            self.assertEqual(await session.scalar(select(func.sum(CloudResultBatch.position_count))), 3)
            self.assertEqual(await session.scalar(select(func.count()).select_from(Fen).where(Fen.score.is_not(None))), 3)
        with patch.object(controller, 'refresh_global_projections', new_callable=AsyncMock):
            await worker.tick()
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).status, 'complete')

    async def test_all_new_batch_requests_use_same_bounded_preparation(self):
        await controller.save_job(self.job.id, status='complete')
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
        for count, mode in ((10000, 'batch_work_units_v1'), (200000, 'batch_work_units_v1'),
                            (200001, 'batch_work_units_v1'), (500000, 'batch_work_units_v1')):
            created = await create_cloud_job(CloudJobRequest(total_fens=count), AsyncMock())
            async with AsyncDBSession() as session:
                job = await session.get(CloudAnalysisJob, created['id'])
                self.assertEqual(job.target, count)
                self.assertEqual(job.selection['execution_mode'], mode)
            await controller.save_job(created['id'], status='complete')

    async def test_additive_migration_blocks_active_work_and_is_repeatable(self):
        with self.assertRaisesRegex(ValueError, 'Finish/recover'):
            async with self.engine.begin() as connection: await connection.run_sync(upgrade)
        await controller.save_job(self.job.id, status='complete')
        async with self.engine.begin() as connection:
            await connection.execute(text('DROP TABLE cloud_batch_unit'))
            await connection.execute(text('DROP TABLE cloud_control_work_unit'))
        for _ in range(2):
            async with self.engine.begin() as connection: await connection.run_sync(upgrade)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).selection, self.job.selection)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudBatchUnit)), 0)
