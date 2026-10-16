"""Opt-in isolated PostgreSQL tests for the relational control-state migration."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
import test_cloud_analysis_database as existing
from sqlalchemy import select, text, func
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudResultBatch, CloudFenClaim, Fen
from chessism_api.database.cloud_codec import read_document, checksum
from chessism_api.database.cloud_migration import migrate, repair_cleanup_resources
from chessism_api.operations.cloud_analysis import controller, importer, history


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL test database')
class ColumnDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown
    reserved_run = existing.CloudDatabaseTests.reserved_run
    results = existing.CloudDatabaseTests.results

    async def test_pending_compute_cleanup_survives_database_restart_and_can_continue(self):
        from chessism_api.operations.cloud_analysis import batch_spot_controller as fleet
        run = await self.reserved_run()
        await controller.save_run(run.id, status='cleaning', cleanup_plan={},
                                  launch={'multi_vm': True, 'tasks': []})
        pending = {'complete': False, 'remaining_jobs': [], 'data_deletion_deferred': True,
                   'remaining_compute': [{'kind': 'instanceTemplates', 'scope': None, 'name': 'owned-template'}]}
        worker = controller.Controller(Mock(spec=[]), 'unused-image')
        with patch.object(fleet.results, 'validate_saved'), \
             patch.object(fleet, 'require_stopped', new_callable=AsyncMock), \
             patch.object(fleet, 'cleaning_job', side_effect=[pending, {'complete': True, 'remaining_compute': []}]):
            async with AsyncDBSession() as session:
                current = await session.get(CloudAnalysisRun, run.id)
            await fleet.advance(worker, self.job, current)
            # A new session/process reconstructs the structured pending report.
            async with AsyncDBSession() as session:
                current = await session.get(CloudAnalysisRun, run.id)
                self.assertEqual(current.status, 'cleaning')
                self.assertEqual(current.launch['cleanup_report'], pending)
            await fleet.advance(worker, self.job, current)
        async with AsyncDBSession() as session:
            current = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(current.status, 'log_planning')
            self.assertTrue(current.launch['cleanup_report']['complete'])
        self.assertEqual(worker.cloud.mock_calls, [])

    async def test_cleanup_schema_repair_preserves_legacy_empty_null_and_paused_job(self):
        run = await self.reserved_run()
        original = {'cleanup_report': {'remaining_compute': []},
                    'cleanup_verification': {'remaining_compute': None}}
        await controller.save_run(run.id, status='cleaning', launch=original)
        await controller.save_job(self.job.id, status='paused')
        async with self.engine.begin() as c:
            await c.execute(text('DROP TABLE cloud_control_compute_resource'))
            await c.execute(text("ALTER TABLE cloud_control_launch ADD COLUMN cleanup_report__remaining_compute TEXT[] DEFAULT '{}'"))
            await c.execute(text('ALTER TABLE cloud_control_launch ADD COLUMN cleanup_verification__remaining_compute TEXT[]'))
        async with self.engine.begin() as c:
            self.assertEqual(await c.run_sync(repair_cleanup_resources), 2)
            self.assertEqual(await c.run_sync(repair_cleanup_resources), 0)
            restored = await c.run_sync(read_document, run.id + ':launch')
            self.assertEqual(restored, original)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).status, 'paused')

    async def test_cleanup_schema_repair_rejects_nonempty_legacy_string_arrays(self):
        run = await self.reserved_run()
        async with self.engine.begin() as c:
            await c.execute(text("ALTER TABLE cloud_control_launch ADD COLUMN cleanup_report__remaining_compute TEXT[] DEFAULT ARRAY['unknown-identity']"))
        with self.assertRaisesRegex(ValueError, 'Nonempty legacy'):
            async with self.engine.begin() as c:
                await c.run_sync(repair_cleanup_resources)
        async with self.engine.connect() as c:
            self.assertEqual(await c.scalar(text('SELECT cleanup_report__remaining_compute FROM cloud_control_launch LIMIT 1')), ['unknown-identity'])

    async def test_simultaneous_redelivery_commits_once_and_records_batch_timings(self):
        run = await self.reserved_run()
        _, raw = await self.results(run)
        counts = await asyncio.gather(*(importer.import_batch(run.id, raw, 0, download_seconds=.02) for _ in range(2)))
        self.assertEqual(sorted(counts), [0, 2])
        async with AsyncDBSession() as session:
            batches = (await session.scalars(select(CloudResultBatch))).all()
            self.assertEqual(len(batches), 1)
            self.assertEqual(batches[0].position_count, 2)
            self.assertEqual(batches[0].download_seconds, .02)
            self.assertIsNotNone(batches[0].committed_at)
            self.assertGreater(batches[0].transaction_seconds, 0)
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.imported_count, 2)
            self.assertEqual(len(saved.receipts), 1)

    async def test_compaction_only_after_cleanup_keeps_counts_and_releases_details(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        await importer.import_batch(run.id, raw, 0)
        with self.assertRaisesRegex(ValueError, 'cleaned'):
            await history.compact_run(run.id)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            manifest = {'status': 'complete', 'fingerprint': contract['fingerprint'],
                        'position_count': 2, 'records': next(iter(saved.receipts.values()))['records']}
        await controller.save_run(run.id, status='finalizing', launch={'manifest': manifest})
        await controller.save_run(run.id, status='refreshing', cleanup_plan={})
        await history.compact_run(run.id)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertTrue(saved.details_pruned)
            self.assertEqual((saved.position_count, saved.imported_count), (2, 2))
            self.assertEqual(saved.positions, [])
            self.assertEqual(saved.receipts, {})
            batch = (await session.scalars(select(CloudResultBatch))).one()
            self.assertIsNone(batch.detail_record_id)
            self.assertEqual(batch.position_count, 2)
            self.assertEqual(await session.scalar(select(func.count()).select_from(Fen).where(Fen.score.is_not(None))), 2)

    async def test_shard_batches_cross_position_pack_boundaries(self):
        import chess
        from cloud_job.batch_spot import config_for
        from cloud_job.launch import expected_contract
        from stockfish_batch.checkpoints import BatchCheckpoints, encode
        # Shards need not start on a multiple of 500. Exercise both the full
        # batch and its two-position tail spanning different stored input packs.
        fens = set(self.fens)
        for white in chess.SQUARES:
            for black in chess.SQUARES:
                if white == black:
                    continue
                board = chess.Board(None)
                board.set_piece_at(white, chess.Piece(chess.KING, chess.WHITE))
                board.set_piece_at(black, chess.Piece(chess.KING, chess.BLACK))
                if board.is_valid():
                    fens.add(' '.join(board.fen().split()[:4]))
                if len(fens) >= 1003:
                    break
            if len(fens) >= 1003:
                break
        async with AsyncDBSession() as session, session.begin():
            session.add_all(Fen(fen=fen, n_games=1, moves_counter='{}') for fen in fens - set(self.fens))
        run = await self.reserved_run(1003)
        positions = run.positions[499:1001]
        config = config_for(run.id, len(positions))
        contract = expected_contract(b''.join(encode(p) for p in positions), positions, config,
            {'engine_sha': 'a' * 64, 'worker_version': '1.4.0', 'chess_version': '1.11.2'})
        task = {'index': 0, 'start': 499, 'count': 502, 'contract': contract}
        await controller.save_run(run.id, launch={'backend': 'batch_spot', 'tasks': [task]})
        checkpoints = BatchCheckpoints(None, '', contract, positions, 500)
        records = [checkpoints.prepare(p, {'fen': p['fen'], 'is_valid': True,
                   'analysis': {'score': 0, 'pv': []}}, 1) for p in positions]
        for index, expected in ((1, 2), (0, 500)):
            raw = encode({'fingerprint': contract['fingerprint'], 'records': records[index*500:(index+1)*500]})
            self.assertEqual(await importer.import_batch(run.id, raw, index, task_index=0), expected)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.imported_count, 502)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 501)

    async def test_migration_is_lossless_atomic_and_repeatable(self):
        await controller.save_job(self.job.id, status='complete')
        original = {'backend': 'batch_spot', 'n_vms': 1, 'game_links': [1, 2], 'plan_id': None}
        async with self.engine.begin() as c:
            await c.execute(text('ALTER TABLE cloud_analysis_job ADD COLUMN selection JSON'))
            await c.execute(text('UPDATE cloud_analysis_job SET selection=CAST(:value AS JSON)'), {'value': json.dumps(original)})
        async with self.engine.begin() as c:
            self.assertEqual(await c.run_sync(migrate), 1)
        async with self.engine.begin() as c:
            self.assertEqual(await c.run_sync(migrate), 0)
            restored = await c.run_sync(read_document, self.job.id + ':selection')
            self.assertEqual(checksum(restored), checksum(original))
            remaining = await c.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name LIKE 'cloud_%' AND data_type IN ('json','jsonb')"))
            self.assertEqual(remaining, 0)

    async def test_unknown_recovery_field_rolls_back_before_dropping_json(self):
        await controller.save_job(self.job.id, status='complete')
        async with self.engine.begin() as c:
            await c.execute(text('ALTER TABLE cloud_analysis_job ADD COLUMN selection JSON'))
            await c.execute(text('UPDATE cloud_analysis_job SET selection=CAST(:value AS JSON)'),
                            {'value': json.dumps({'unsupported': True})})
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            async with self.engine.begin() as c:
                await c.run_sync(migrate)
        async with self.engine.connect() as c:
            preserved = await c.scalar(text('SELECT selection FROM cloud_analysis_job'))
            self.assertEqual(preserved, {'unsupported': True})


if __name__ == '__main__': unittest.main()
