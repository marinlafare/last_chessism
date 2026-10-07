"""Opt-in tests against an EMPTY, isolated PostgreSQL database, never the app DB.

Set CHESSISM_CLOUD_TEST_DATABASE_URL to .../chessism_cloud_test. Each test creates
and drops its own random schema. Do not point this at production.
"""
import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    Base, Fen, FenContinuation, DatabaseSummary, CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim,
    CloudControllerHeartbeat,
)
from chessism_api.database.ask_db import get_fens_for_analysis
from chessism_api.operations.analysis import _format_engine_results
from chessism_api.operations.cloud_analysis import controller, importer
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from cloud_job.launch import configuration, expected_contract
from stockfish_batch.checkpoints import BatchCheckpoints, encode

TEST_URL = os.environ.get("CHESSISM_CLOUD_TEST_DATABASE_URL", "")


@unittest.skipUnless(TEST_URL, "requires explicitly isolated PostgreSQL test database")
class CloudDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not TEST_URL.endswith("/chessism_cloud_test"):
            raise RuntimeError("Refusing any database not named chessism_cloud_test")
        self.schema = "cloud_test_" + uuid4().hex
        self.admin = create_async_engine(TEST_URL)
        async with self.admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        self.engine = create_async_engine(TEST_URL, connect_args={"server_settings": {"search_path": self.schema}})
        AsyncDBSession.configure(bind=self.engine)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.fens = [
            # Production keys intentionally omit halfmove/fullmove counters.
            "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
            "8/8/8/8/8/8/4k3/6K1 w - -",
            "8/8/8/8/8/8/4k3/6K1 b - -",
        ]
        self.job = CloudAnalysisJob(id=uuid4().hex, selection=CloudJobRequest().model_dump(),
                                    target=1000, imported=0, status="running")
        async with AsyncDBSession() as session, session.begin():
            session.add(self.job)
            session.add(DatabaseSummary(id=1, n_positions=3, unscored_fens=3))
            session.add_all([Fen(fen=fen, n_games=10-index, moves_counter="{}") for index, fen in enumerate(self.fens)])

    async def asyncTearDown(self):
        await self.engine.dispose()
        async with self.admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        await self.admin.dispose()

    async def reserved_run(self, count=2):
        await controller.reserve(self.job, count)
        async with AsyncDBSession() as session:
            return (await session.scalars(select(CloudAnalysisRun))).one()

    async def test_cloud_run_out_of_order_task_import_is_atomic_and_idempotent(self):
        await self._sharded_import("cloud_run")

    async def test_batch_fleet_out_of_order_import_is_atomic_and_idempotent(self):
        await self._sharded_import("batch_spot")

    async def _sharded_import(self, backend):
        if backend == "cloud_run":
            from cloud_job.cloud_run import config_for
        else:
            from cloud_job.batch_spot import config_for
        run = await self.reserved_run(2)
        config = config_for(run.id, 1)
        tasks, payloads = [], []
        for index, row in enumerate(run.positions):
            contract = expected_contract(encode(row), [row], config, {
                "engine_sha": "a" * 64, "worker_version": "1.4.0", "chess_version": "1.11.2"})
            checkpoint = BatchCheckpoints(None, "", contract, [row], 500)
            lines = ([{"multipv": i, "nodes": 100000, "score": 20-i,
                       "wdl": [200, 700, 100], "pv": [move]}
                      for i, move in enumerate(["e2e4", "d2d4", "g1f3", "c2c4"], 1)]
                     if row["fen"] == self.fens[0] else {"score": 0, "pv": []})
            record = checkpoint.prepare(row, {"fen": row["fen"], "is_valid": True, "analysis": lines}, 1)
            tasks.append({"index": index, "start": index, "count": 1, "contract": contract})
            payloads.append(encode({"fingerprint": contract["fingerprint"], "records": [record]}))
        await controller.save_run(run.id, launch={"backend": backend, "tasks": tasks})
        self.assertEqual(await importer.import_batch(run.id, payloads[1], 0, task_index=1), 1)
        self.assertEqual(await importer.import_batch(run.id, payloads[1], 0, task_index=1), 0)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, run.job_id)).imported, 1)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 1)
        self.assertEqual(await importer.import_batch(run.id, payloads[0], 0, task_index=0), 1)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, run.job_id)).imported, 2)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(sorted(saved.receipts), ["tasks/000000/batches/000000.json", "tasks/000001/batches/000000.json"])

    async def results(self, run):
        config = configuration(run.id, len(run.positions), 1000000, 3600)
        raw_input = b"".join(encode(row) for row in run.positions)
        contract = expected_contract(raw_input, run.positions, config, {
            "engine_sha": "a" * 64, "worker_version": "1.1.0", "chess_version": "1.11.2",
        })
        checkpoint = BatchCheckpoints(None, "", contract, run.positions, 500)
        records = []
        for row in run.positions:
            if row["fen"] == self.fens[0]:
                analysis = [{"multipv": i, "nodes": 100, "score": 20-i,
                             "wdl": [200, 700, 100], "pv": [move]}
                            for i, move in enumerate(["e2e4", "d2d4", "g1f3", "c2c4"], 1)]
            else:
                analysis = {"score": 0, "pv": []}
            records.append(checkpoint.prepare(row, {"fen": row["fen"], "is_valid": True, "analysis": analysis}, 10))
        await controller.save_run(run.id, contract=contract)
        return contract, encode({"fingerprint": contract["fingerprint"], "records": records})

    async def test_cloud_claims_are_visible_before_first_result_and_local_locks_are_skipped(self):
        local, local_fens = await get_fens_for_analysis(1, raise_errors=True)
        try:
            run = await self.reserved_run(1)
            self.assertNotIn(run.positions[0]["fen"], local_fens)
            second_local, second_fens = await get_fens_for_analysis(3, raise_errors=True)
            try:
                self.assertEqual(len(second_fens), 1)
                self.assertNotIn(run.positions[0]["fen"], second_fens)
                self.assertNotIn(local_fens[0], second_fens)
                async with AsyncDBSession() as session:
                    self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 1)
            finally:
                await second_local.close()
        finally:
            await local.close()

    async def test_two_cloud_reservations_are_disjoint(self):
        await asyncio.gather(controller.reserve(self.job, 2), controller.reserve(self.job, 2))
        async with AsyncDBSession() as session:
            claims = (await session.scalars(select(CloudFenClaim))).all()
        self.assertEqual(len(claims), 3)
        self.assertEqual(len({claim.fen for claim in claims}), 3)

    async def test_fresh_recheck_excludes_scored_fens_even_after_claim_release(self):
        from chessism_api.database.fen_claims import unclaimed_locked_fens
        async with AsyncDBSession() as session, session.begin():
            row = await session.get(Fen, self.fens[0], with_for_update=True)
            row.score = 10
        async with AsyncDBSession() as session:
            self.assertEqual(await unclaimed_locked_fens(session, self.fens), self.fens[1:])

    async def test_paused_job_blocks_following_cloud_launches(self):
        await controller.save_job(self.job.id, status="paused")
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudAnalysisJob(id=uuid4().hex, selection=CloudJobRequest().model_dump(),
                                        target=1000, status="queued", imported=0))
        cloud = Mock()
        with patch.object(controller, "reserve", new_callable=AsyncMock) as reserve:
            self.assertFalse(await controller.Controller(cloud, "unused-image").tick())
            reserve.assert_not_called()
        self.assertEqual(cloud.mock_calls, [])

    async def test_daemon_heartbeat_and_shutdown_release_singleton_without_launching(self):
        from chessism_api.operations.cloud_analysis import __main__ as daemon
        await controller.save_job(self.job.id, status="paused")
        cloud = Mock()
        def test_engine(*args, **kwargs):
            return create_async_engine(TEST_URL, connect_args={"server_settings": {"search_path": self.schema}})
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(daemon, "create_async_engine", side_effect=test_engine), \
             patch.object(daemon, "Client", return_value=cloud):
            marker = Path(directory) / "heartbeat"
            args = SimpleNamespace(gcloud="not-called", local_image="unused", poll_seconds=5, heartbeat_file=marker)
            task = asyncio.create_task(daemon.serve(args))
            async def ready():
                while not marker.exists():
                    if task.done():
                        task.result()
                    await asyncio.sleep(0.02)
            try:
                await asyncio.wait_for(ready(), timeout=5)
                async with self.engine.connect() as connection:
                    lock = text("SELECT pg_try_advisory_lock(:key)")
                    self.assertFalse(await connection.scalar(lock, {"key": daemon.CONTROLLER_LOCK}))
                    self.assertIsNotNone(await connection.scalar(select(CloudControllerHeartbeat.seen_at)))
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        async with self.engine.connect() as connection:
            self.assertTrue(await connection.scalar(lock, {"key": daemon.CONTROLLER_LOCK}))
            await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": daemon.CONTROLLER_LOCK})
            self.assertEqual(await connection.scalar(select(func.count()).select_from(CloudAnalysisRun)), 0)
        self.assertEqual(cloud.mock_calls, [])

    async def test_game_jobs_accept_two_hundred_thousand_and_reject_larger_previews(self):
        from fastapi import HTTPException
        from chessism_api.routers.cloud_analysis import create_cloud_job
        await controller.save_job(self.job.id, status="complete")
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
        redis = AsyncMock()
        request = CloudJobRequest(mode="games", plan_id="c" * 32)
        for count in (10001, 199999, 200000):
            redis.get.return_value = json.dumps({"game_links": [21, 22],
                "player_name": "magnuscarlsen", "fens_to_analyze": count})
            created = await create_cloud_job(request, redis)
            async with AsyncDBSession() as session:
                saved = await session.get(CloudAnalysisJob, created["id"])
                self.assertEqual(saved.target, count)
                self.assertEqual(saved.selection["total_fens"], count)
            await controller.save_job(created["id"], status="complete")
        redis.get.return_value = json.dumps({"game_links": [21, 22], "fens_to_analyze": 200001})
        with self.assertRaises(HTTPException) as raised:
            await create_cloud_job(request, redis)
        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn("200,000", raised.exception.detail)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudAnalysisJob)), 4)

    async def test_large_job_continues_after_ten_chunks_but_stops_at_two_hundred(self):
        # Pre-upgrade requests retain their saved chunk/retry contract.
        cloud = Mock()
        instance = controller.Controller(cloud, "unused-image")
        previous = 0
        for completed in (10, 199, 200):
            async with AsyncDBSession() as session, session.begin():
                job = await session.get(CloudAnalysisJob, self.job.id)
                job.selection = CloudJobRequest(total_fens=200000).model_dump()
                job.target, job.imported = 200000, min(completed * 1000, 199999)
                session.add_all([CloudAnalysisRun(id=uuid4().hex, job_id=job.id,
                    status="complete", positions=[], launch={}, receipts={})
                    for _ in range(completed - previous)])
            with patch.object(controller, "reserve", new_callable=AsyncMock) as reserve, \
                 patch.object(controller, "refresh_global_projections", new_callable=AsyncMock):
                self.assertTrue(await instance.tick())
                if completed < 200:
                    reserve.assert_awaited_once()
                    self.assertEqual(reserve.await_args.args[1], 1000)
                else:
                    reserve.assert_not_called()
            previous = completed
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).status, "limit_reached")
        self.assertEqual(cloud.mock_calls, [])

    async def test_new_large_job_reserves_all_positions_once(self):
        async with AsyncDBSession() as session, session.begin():
            job = await session.get(CloudAnalysisJob, self.job.id)
            job.selection = {**job.selection, "execution_mode": "single_vm_v1"}
            job.target = 200000
        with patch.object(controller, "reserve", new_callable=AsyncMock) as reserve:
            await controller.Controller(Mock(), "unused").tick()
            reserve.assert_awaited_once()
            self.assertEqual(reserve.await_args.args[1], 200000)
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudAnalysisRun(id=uuid4().hex, job_id=self.job.id,
                status="complete", positions=[], receipts={}, launch={}))
        with patch.object(controller, "reserve", new_callable=AsyncMock) as reserve, \
             patch.object(controller, "refresh_global_projections", new_callable=AsyncMock):
            await controller.Controller(Mock(), "unused").tick()
            reserve.assert_not_called()

    async def test_large_reservation_claims_every_fen_and_api_returns_lightweight_summary(self):
        from chessism_api.routers.cloud_analysis import cloud_jobs
        from sqlalchemy import insert
        extra = ["8/8/8/8/8/8/4k3/6K1 w - - 0 " + str(i) for i in range(1, 1301)]
        async with AsyncDBSession() as session, session.begin():
            await session.execute(insert(Fen), [{"fen": f, "n_games": 1, "moves_counter": "{}"} for f in extra])
        await controller.reserve(self.job, 1303)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 1303)
        listing = await cloud_jobs()
        self.assertEqual(listing["jobs"][0]["runs"][0]["positions"], 1303)
        self.assertEqual(listing["jobs"][0]["runs"][0]["imported"], 0)
        self.assertNotIn("receipts", listing["jobs"][0]["runs"][0])

    async def test_performance_report_is_committed_before_resources_and_logs_are_cleaned(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        await importer.import_batch(run.id, raw, 0)
        name, receipt, _ = importer.validate_batch(raw, 0, run.positions, contract)
        manifest = {"status": "complete", "fingerprint": contract["fingerprint"],
                    "position_count": 2, "records": receipt["records"]}
        performance = {"schema_version": 1, "fingerprint": contract["fingerprint"],
            "position_count": 2, "resumed": 2, "analyzed_this_attempt": 0, "workers": [],
            "metrics": {"worker_seconds": 1, "fen_per_second": 0}}
        await controller.save_run(run.id, status="running", launch={
            "jobs": ["test-job"], "uid": "uid", "workflow_version": 2})
        cloud = Mock()
        cloud.job.return_value = {"uid": "uid", "status": {"state": "SUCCEEDED"}}
        cloud.storage.return_value.read.side_effect = [encode(contract), encode(manifest), encode(performance)]
        worker = controller.Controller(cloud, "unused")
        await worker.tick()
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.launch["performance"], performance)
            self.assertEqual(saved.status, "finalizing")
        with patch.object(controller, "refresh_projections", new_callable=AsyncMock): await worker.tick()
        with patch.object(controller, "prepare", return_value={"jobs": [{"uid": "uid"}]}): await worker.tick()
        with patch.object(controller, "cleaning_job", return_value={"complete": True}): await worker.tick()
        with patch.object(controller, "plan_logs", return_value={"streams": {}}): await worker.tick()
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.status, "log_cleaning")
            self.assertEqual(saved.launch["log_cleanup_plan"], {"streams": {}})
        with patch.object(controller, "remove_logs", return_value={"complete": True}) as clean:
            await worker.tick()
            clean.assert_called_once()
        with patch.object(controller, 'refresh_projections', new_callable=AsyncMock):
            await worker.tick()
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.status, "complete")
            self.assertEqual(saved.launch["performance"], performance)

    async def test_ui_create_requires_online_controller_and_freezes_preview(self):
        from fastapi import HTTPException
        from chessism_api.routers.cloud_analysis import create_cloud_job, cloud_jobs
        await controller.save_job(self.job.id, status="complete")
        redis = AsyncMock()
        with self.assertRaises(HTTPException) as raised:
            await create_cloud_job(CloudJobRequest(), redis)
        self.assertEqual(raised.exception.status_code, 409)
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
        redis.get.return_value = json.dumps({"game_links": [21, 22], "player_name": "player", "fens_to_analyze": 6967})
        created = await create_cloud_job(CloudJobRequest(mode="games", plan_id="c" * 32, n_vms=2), redis)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisJob, created["id"])
            self.assertEqual(saved.selection["game_links"], [21, 22])
            self.assertEqual(saved.selection["nodes"], 100_000)
            self.assertEqual(saved.selection["stall_timeout_seconds"], 300)
            self.assertEqual(saved.selection["total_fens"], 6967)
            self.assertEqual(saved.selection["execution_mode"], "batch_multi_vm_v1")
            self.assertEqual(saved.selection["n_vms"], 2)
            self.assertEqual(saved.target, 6967)
        listing = await cloud_jobs()
        self.assertTrue(listing["controller_online"])
        self.assertTrue(listing["cloud_busy"])
        self.assertEqual(listing["blocking_job_id"], created["id"])
        public = next(job for job in listing["jobs"] if job["id"] == created["id"])
        self.assertNotIn("game_links", public["selection"])

    async def test_batch_preflight_vm_count_can_change_without_releasing_or_reselecting_fens(self):
        from fastapi import HTTPException
        from chessism_api.operations.cloud_analysis.schemas import CloudVmCountRequest
        from chessism_api.routers.cloud_analysis import revise_batch_vms
        run = await self.reserved_run(2)
        async with AsyncDBSession() as session, session.begin():
            job = await session.get(CloudAnalysisJob, self.job.id)
            job.status = "paused"
            job.selection = {**job.selection, "execution_mode": "batch_multi_vm_v1", "n_vms": 2}
        await revise_batch_vms(self.job.id, CloudVmCountRequest(n_vms=1))
        async with AsyncDBSession() as session:
            job = await session.get(CloudAnalysisJob, self.job.id)
            self.assertEqual((job.status, job.selection["n_vms"]), ("queued", 1))
            self.assertEqual((await session.get(CloudAnalysisRun, run.id)).positions, run.positions)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
        # Reject modification while the controller can advance, or once any
        # durable launch configuration exists (including a paused publication).
        with self.assertRaises(HTTPException):
            await revise_batch_vms(self.job.id, CloudVmCountRequest(n_vms=2))
        await controller.save_job(self.job.id, status="paused")
        await controller.save_run(run.id, launch={"image_id": "pinned"}, status="publishing")
        with self.assertRaises(HTTPException):
            await revise_batch_vms(self.job.id, CloudVmCountRequest(n_vms=2))

    async def test_two_tabs_cannot_create_concurrent_cloud_jobs(self):
        from fastapi import HTTPException
        from chessism_api.routers.cloud_analysis import create_cloud_job
        await controller.save_job(self.job.id, status="complete")
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
        outcomes = await asyncio.gather(
            create_cloud_job(CloudJobRequest(), AsyncMock()),
            create_cloud_job(CloudJobRequest(), AsyncMock()), return_exceptions=True)
        self.assertEqual(sum(isinstance(outcome, dict) for outcome in outcomes), 1)
        errors = [outcome for outcome in outcomes if isinstance(outcome, HTTPException)]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].status_code, 409)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudAnalysisJob)), 2)

    async def test_open_job_blocks_create_resume_and_retry_from_another_job(self):
        from fastapi import HTTPException
        from chessism_api.routers.cloud_analysis import create_cloud_job, resume_cloud_job, retry_cloud_job
        other_id = uuid4().hex
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
            session.add(CloudAnalysisJob(id=other_id, selection=CloudJobRequest().model_dump(),
                target=1000, imported=0, status="paused"))
        for state in ("queued", "running", "paused", "waiting", "failed"):
            await controller.save_job(self.job.id, status=state)
            for action in (lambda: create_cloud_job(CloudJobRequest(), AsyncMock()),
                           lambda: resume_cloud_job(other_id), lambda: retry_cloud_job(other_id)):
                with self.subTest(state=state), self.assertRaises(HTTPException) as raised:
                    await action()
                self.assertEqual(raised.exception.status_code, 409)
                self.assertIn(self.job.id, raised.exception.detail)

    async def test_explicit_cloud_retry_keeps_claims_and_same_checkpoints(self):
        from chessism_api.routers.cloud_analysis import retry_cloud_job
        run = await self.reserved_run()
        await controller.save_run(run.id, status="failed", cloud_state="FAILED", launch={
            "jobs": [f"chessism-ui-{run.id}-1"], "image": "pinned-image",
        })
        await controller.save_job(self.job.id, status="failed")
        await retry_cloud_job(self.job.id)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.status, "submitting")
            self.assertEqual(len(saved.launch["jobs"]), 2)
            self.assertEqual(saved.launch["image"], "pinned-image")
            self.assertEqual(saved.positions, run.positions)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)

    async def test_import_is_atomic_idempotent_and_uses_stockfish_fields(self):
        run = await self.reserved_run()
        self.assertEqual(run.positions, [{"id": hashlib.sha256(fen.encode()).hexdigest(), "fen": fen}
                                         for fen in self.fens[:2]])
        contract, raw = await self.results(run)
        self.assertEqual(await importer.import_batch(run.id, raw, 0), 2)
        self.assertEqual(await importer.import_batch(run.id, raw, 0), 0)
        async with AsyncDBSession() as session:
            saved = await session.get(Fen, self.fens[0])
            self.assertEqual(saved.analysis_source, "stockfish")
            self.assertEqual((saved.score, saved.next_moves, saved.wdl_win), (19, "e2e4", 200))
            self.assertIsNotNone(saved.analyzed_at)
            self.assertIsNone(saved.tablebase_dtz)
            self.assertEqual(await session.scalar(select(func.count()).select_from(FenContinuation)), 3)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            self.assertEqual(set((await session.scalars(select(Fen.fen))).all()), set(self.fens))
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 2)
            summary = await session.get(DatabaseSummary, 1)
            self.assertEqual((summary.analyzed_fens, summary.unscored_fens, summary.nonzero_scored_fens), (2, 1, 1))
            self.assertEqual(len((await session.get(CloudAnalysisRun, run.id)).receipts), 1)
        await importer.refresh_projections(run.positions)
        await importer.refresh_global_projections()

    async def test_failed_transaction_keeps_claims_and_no_import_receipt(self):
        run = await self.reserved_run()
        _, raw = await self.results(run)
        original = importer.stage_fen_results
        async def fail_after_writes(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("simulated crash before commit")
        with patch.object(importer, "stage_fen_results", side_effect=fail_after_writes):
            with self.assertRaises(RuntimeError):
                await importer.import_batch(run.id, raw, 0)
        async with AsyncDBSession() as session:
            self.assertIsNone((await session.get(Fen, self.fens[0])).score)
            self.assertEqual((await session.get(CloudAnalysisRun, run.id)).receipts, {})
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 0)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
        self.assertEqual(await importer.import_batch(run.id, raw, 0), 2)

    async def test_incremental_import_then_restart_finishes_before_cleanup(self):
        run = await self.reserved_run()
        contract, raw = await self.results(run)
        await controller.save_run(run.id, status="running", launch={"jobs": ["test-job"], "uid": "uid"})
        name, receipt, _ = importer.validate_batch(raw, 0, run.positions, contract)
        manifest = {"status": "complete", "fingerprint": contract["fingerprint"],
                    "position_count": 2, "records": receipt["records"]}
        storage = Mock()
        cloud = Mock()
        cloud.job.return_value = {"uid": "uid", "status": {"state": "RUNNING"}}
        cloud.storage.return_value = storage
        storage.read.side_effect = [encode(contract), raw]
        worker = controller.Controller(cloud, "unused-local-image")
        with patch.object(controller, "cleaning_job") as cleanup:
            await worker.tick()
            cleanup.assert_not_called()
        # New controller process, same DB state: download no already-imported batches.
        worker = controller.Controller(cloud, "unused-local-image")
        cloud.job.return_value["status"]["state"] = "SUCCEEDED"
        storage.read.side_effect = [encode(contract), encode(manifest)]
        await worker.tick()
        with patch.object(controller, "refresh_projections", new_callable=AsyncMock):
            await worker.tick()
        with patch.object(controller, "prepare", return_value={"version": 2, "jobs": []}):
            await worker.tick()
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertEqual(saved.status, "cleaning")
            self.assertEqual(saved.cleanup_plan, {"version": 2, "jobs": []})
        # Cleanup can resume using its plan even after Batch metadata disappears.
        cloud.job.side_effect = AssertionError("cleanup must use the persisted plan")
        with patch.object(controller, "cleaning_job", return_value={"complete": True}) as cleanup:
            await controller.Controller(cloud, "unused-local-image").tick()
            cleanup.assert_called_once()
        with patch.object(controller, 'refresh_projections', new_callable=AsyncMock):
            await worker.tick()
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisRun, run.id)).status, "complete")
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 2)


if __name__ == "__main__":
    unittest.main()
