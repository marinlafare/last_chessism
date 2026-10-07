"""Offline validation and controller boundary tests; no Google credentials needed."""
import copy
from dataclasses import asdict
import json
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
import tempfile
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations.cloud_analysis import controller, importer
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.operations.analysis_settings import CLOUD_STALL_TIMEOUT_SECONDS
from cloud_job.launch import configuration, expected_contract, render, Client
from cloud_job import launch
from cleaning_job.cloud import CleanupError
from stockfish_batch import __version__
from stockfish_core import ENGINE_SHA256
from stockfish_batch.checkpoints import BatchCheckpoints, digest, encode, parse_input


def fixture(count=2, fen="8/8/8/8/8/8/4k3/6K1 w - - 0 1"):
    rows = [{"id": str(index), "fen": fen} for index in range(count)]
    config = configuration("a" * 32, count, 1000000, 3600)
    raw = b"".join(encode(row) for row in rows)
    contract = expected_contract(raw, rows, config, {
        "engine_sha": "a" * 64, "worker_version": "1.1.0", "chess_version": "1.11.2",
    })
    checkpoints = BatchCheckpoints(None, "", contract, rows, 500)
    records = [checkpoints.prepare(row, {"fen": row["fen"], "is_valid": True,
               "analysis": {"pv": [], "score": 0}}, 1) for row in rows]
    return rows, contract, encode({"fingerprint": contract["fingerprint"], "records": records})


class CloudValidationTests(unittest.TestCase):
    def test_four_field_keys_survive_input_checkpoint_and_import_validation(self):
        fen = "8/8/8/8/8/8/4k3/6K1 w - -"
        rows, contract, raw = fixture(1, fen)
        self.assertEqual(parse_input(encode(rows[0]), 1), rows)
        name, receipt, results = importer.validate_batch(raw, 0, rows, contract)
        self.assertEqual(results[0]["fen"], fen)
        importer.validate_manifest({"status": "complete", "fingerprint": contract["fingerprint"],
            "position_count": 1, "records": receipt["records"]}, rows, contract, {name: receipt})
        # Equivalent six-field analysis must not silently change the database key.
        changed = json.loads(raw)
        record = changed["records"][0]
        record["engine_result"]["fen"] += " 0 1"
        record["result_sha256"] = digest(encode(record["engine_result"]))
        with self.assertRaisesRegex(ValueError, "Result does not match input"):
            importer.validate_batch(encode(changed), 0, rows, contract)

    def test_launch_requires_image_with_current_fen_compatibility(self):
        image_id = "sha256:" + "a" * 64
        for version in (__version__, "1.2.0"):
            metadata = {"worker_version": version, "engine_sha": ENGINE_SHA256, "chess_version": "1.11.2"}
            with self.subTest(version=version), patch.object(launch, "docker", side_effect=[image_id, json.dumps(metadata)]):
                if version == __version__:
                    self.assertEqual(launch.inspect_worker("local-image"), (image_id, metadata))
                else:
                    with self.assertRaisesRegex(ValueError, "Build the current worker"):
                        launch.inspect_worker("local-image")

    def test_stall_timeout_is_not_a_user_parameter(self):
        self.assertEqual(CLOUD_STALL_TIMEOUT_SECONDS, 300)
        self.assertNotIn("stall_timeout_seconds", CloudJobRequest.model_json_schema()["properties"])
        self.assertNotIn("stall_timeout_seconds", CloudJobRequest().model_dump())
        for seconds in (300, 3600, 0, None):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                CloudJobRequest(stall_timeout_seconds=seconds)

    def test_game_fen_count_must_come_from_the_server_preview(self):
        request = CloudJobRequest(mode="games", plan_id="a" * 32)
        with self.assertRaisesRegex(ValueError, "saved game preview"):
            _ = request.target
        for target in (1, 6967, 10000):
            with self.subTest(target=target), self.assertRaisesRegex(ValueError, "calculated by the server"):
                CloudJobRequest(mode="games", plan_id="a" * 32, total_fens=target)
        self.assertEqual(CloudJobRequest().target, 1000)
        self.assertEqual(CloudJobRequest(total_fens=777).target, 777)

    def test_nodes_are_not_a_user_parameter(self):
        self.assertNotIn("nodes", CloudJobRequest().model_dump())
        self.assertNotIn("nodes", CloudJobRequest.model_json_schema()["properties"])
        for nodes in (100_000, 1_000_000, 1, None):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                CloudJobRequest(nodes=nodes)

    def test_bounded_request_and_player_validation(self):
        self.assertEqual(CloudJobRequest(mode="loop", runs=3, positions_per_run=500).target, 1500)
        for kwargs in ({"total_fens": 200001}, {"runs": 201}, {"positions_per_run": 1001}, {"nodes": 10000001},
                       {"stall_timeout_seconds": 3601}, {"max_task_seconds": 900},
                       {"mode": "player"}, {"mode": "games"}):
            with self.assertRaises(ValueError):
                CloudJobRequest(**kwargs)

    def test_cloud_jobs_accept_up_to_two_hundred_thousand_in_one_vm(self):
        for mode in ("all", "player"):
            for count in (10001, 199999, 200000):
                with self.subTest(mode=mode, count=count):
                    request = CloudJobRequest(mode=mode, player_name="magnuscarlsen", total_fens=count)
                    self.assertEqual(request.target, count)
        self.assertEqual(CloudJobRequest(mode="loop", runs=200, positions_per_run=1000).target, 200000)
        config = configuration("a" * 32, 200000, 100000, 300, stall_only=True)
        self.assertEqual(config.max_positions, 200000)
        self.assertTrue(config.compact_results)
        with self.assertRaises(ValueError):
            configuration("a" * 32, 200001, 100000, 300, stall_only=True)

    def test_render_one_spot_vm_with_bounded_retries_and_background_uploads(self):
        config = configuration("a" * 32, 1000, 1000000, 3600)
        spec = render(config, "us-central1-docker.pkg.dev/chessism-production/chessism-workers/ui-test@sha256:" + "a" * 64, 3600)
        group = spec["taskGroups"][0]
        self.assertEqual((group["taskCount"], group["parallelism"]), ("1", "1"))
        self.assertEqual(group["taskSpec"]["maxRunDuration"], "3600s")
        self.assertEqual(group["taskSpec"]["maxRetryCount"], 1)
        commands = group["taskSpec"]["runnables"][0]["container"]["commands"]
        self.assertEqual(commands[commands.index("--batch-size") + 1], "500")
        self.assertEqual(commands[commands.index("--upload-mode") + 1], "background")
        self.assertNotIn("logsPolicy", spec)
        self.assertEqual(spec["allocationPolicy"]["instances"][0]["policy"]["provisioningModel"], "SPOT")

    def test_progress_timeout_does_not_set_an_absolute_task_limit(self):
        seconds = CLOUD_STALL_TIMEOUT_SECONDS
        self.assertEqual(seconds, 300)
        config = configuration("a" * 32, 1000, 100000, seconds, stall_only=True)
        spec = render(config, "us-central1-docker.pkg.dev/chessism-production/chessism-workers/ui-test@sha256:" + "a" * 64, seconds)
        task = spec["taskGroups"][0]["taskSpec"]
        self.assertNotIn("maxRunDuration", task)
        self.assertEqual(task["maxRetryCount"], 1)
        commands = task["runnables"][0]["container"]["commands"]
        self.assertEqual(commands[commands.index("--run-timeout") + 1], "0")
        self.assertEqual(commands[commands.index("--stall-timeout") + 1], "300")

    def test_manifest_requires_exact_imported_ids_hashes_and_count(self):
        rows, contract, raw = fixture()
        name, receipt, results = importer.validate_batch(raw, 0, rows, contract)
        manifest = {"status": "complete", "fingerprint": contract["fingerprint"],
                    "position_count": len(rows), "records": receipt["records"]}
        importer.validate_manifest(manifest, rows, contract, {name: receipt})
        for field, value in (("position_count", 99), ("status", "running"), ("fingerprint", "wrong"),
                             ("records", list(reversed(receipt["records"])))):
            bad = {**manifest, field: value}
            with self.assertRaises(ValueError):
                importer.validate_manifest(bad, rows, contract, {name: receipt})
        with self.assertRaises(ValueError):
            importer.validate_manifest(manifest, rows, contract, {})
        self.assertEqual(results[0]["analysis"]["score"], 0)

    def test_corrupt_foreign_incomplete_batch_rejected(self):
        import json
        rows, contract, raw = fixture()
        original = json.loads(raw)
        for mutate in (
            lambda data: data.update(fingerprint="foreign"),
            lambda data: data["records"].pop(),
            lambda data: data["records"][0].update(result_sha256="bad"),
            lambda data: data["records"][0].update(id="unknown"),
        ):
            data = copy.deepcopy(original)
            mutate(data)
            with self.assertRaises(ValueError):
                importer.validate_batch(encode(data), 0, rows, contract)

    def test_adoption_rejects_different_existing_batch_job(self):
        expected = {"taskGroups": [{"taskSpec": {"runnables": [{"container": {"imageUri": "same", "commands": []}}]}}]}
        Client.check_job(expected, expected)
        other = copy.deepcopy(expected)
        other["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["imageUri"] = "different"
        with self.assertRaises(ValueError):
            Client.check_job(other, expected)


class ControllerGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_game_workload_is_rejected_before_creating_a_job(self):
        import json
        from fastapi import HTTPException
        from chessism_api.routers import cloud_analysis
        redis = AsyncMock()
        request = CloudJobRequest(mode="games", plan_id="a" * 32)
        with patch.object(cloud_analysis, "AsyncDBSession") as database:
            for count in (None, "6967", -1, 0, 200001, True):
                redis.get.return_value = json.dumps({"game_links": [1], "fens_to_analyze": count})
                with self.subTest(count=count), self.assertRaises(HTTPException) as raised:
                    await cloud_analysis.create_cloud_job(request, redis)
                self.assertEqual(raised.exception.status_code, 409)
            database.assert_not_called()

    async def test_publishing_refuses_soft_delete_before_uploading_image(self):
        rows, contract, _ = fixture()
        run = SimpleNamespace(id="a" * 32, positions=rows, status="publishing",
                              launch={"image_id": "local-image"}, contract=contract)
        job = SimpleNamespace(selection={"max_task_seconds": 3600, "nodes": 1000000})
        cloud = Mock()
        cloud.bucket.return_value = {"softDeletePolicy": {"retentionDurationSeconds": "604800"}}
        with self.assertRaises(ValueError):
            await controller.Controller(cloud, "test-image").advance(job, run)
        cloud.publish.assert_not_called()

    async def test_upload_recovery_accepts_only_identical_existing_input(self):
        rows, contract, _ = fixture()
        run = SimpleNamespace(id="a" * 32, positions=rows, status="uploading", launch={})
        job = SimpleNamespace(selection={"max_task_seconds": 3600, "nodes": 1000000})
        storage, cloud = Mock(), Mock()
        cloud.storage.return_value = storage
        storage.create.return_value = False
        storage.read.return_value = b"".join(encode(row) for row in rows)
        with patch.object(controller, "save_run", new_callable=AsyncMock) as save:
            await controller.Controller(cloud, "test-image").advance(job, run)
        self.assertEqual(save.await_args.kwargs["status"], "submitting")
        storage.read.return_value = b"foreign input"
        with self.assertRaises(ValueError):
            await controller.Controller(cloud, "test-image").advance(job, run)

    async def test_submit_uses_persisted_spec_and_records_returned_uid(self):
        rows, _, _ = fixture()
        config = configuration("a" * 32, len(rows), 1000000, 3600)
        run = SimpleNamespace(id="a" * 32, positions=rows, status="submitting", launch={
            "jobs": ["job"], "spec": {"saved": "exact-spec"}, "config": asdict(config),
        })
        job = SimpleNamespace(selection={"max_task_seconds": 3600, "nodes": 1000000})
        cloud = Mock()
        cloud.submit.return_value = {"uid": "immutable-uid"}
        with patch.object(controller, "save_run", new_callable=AsyncMock) as save:
            await controller.Controller(cloud, "test-image").advance(job, run)
        cloud.submit.assert_called_once_with("job", {"saved": "exact-spec"})
        self.assertEqual(save.await_args.kwargs["launch"]["uid"], "immutable-uid")

    async def test_cleanup_never_called_without_complete_committed_receipts(self):
        rows, contract, raw = fixture()
        run = SimpleNamespace(id="a" * 32, positions=rows, contract=contract, receipts={},
                              status="cleaning", launch={"manifest": {}}, cleanup_plan={})
        job = SimpleNamespace(selection={"max_task_seconds": 3600, "nodes": 1000000})
        with patch.object(controller, "cleaning_job") as cleanup:
            with self.assertRaises(ValueError):
                await controller.Controller(Mock(), "test-image").advance(job, run)
            cleanup.assert_not_called()

    async def test_new_cleanup_requires_saved_performance_report(self):
        rows, contract, raw = fixture()
        name, receipt, _ = importer.validate_batch(raw, 0, rows, contract)
        run = SimpleNamespace(id="a" * 32, positions=rows, contract=contract, receipts={name: receipt},
            status="cleanup_planning", launch={"workflow_version": 2, "jobs": ["job"],
            "manifest": {"status": "complete", "fingerprint": contract["fingerprint"],
                         "position_count": len(rows), "records": receipt["records"]}})
        with patch.object(controller, "prepare") as cleanup:
            with self.assertRaisesRegex(ValueError, "Performance report"):
                await controller.Controller(Mock(), "unused").advance(None, run)
            cleanup.assert_not_called()

    async def test_preparing_checks_clean_workspace_without_cloud_writes(self):
        rows, _, _ = fixture()
        run = SimpleNamespace(id="a" * 32, positions=rows, status="preparing", launch={})
        job = SimpleNamespace(selection={**CloudJobRequest().model_dump(), "nodes": 1000000,
                                         "stall_timeout_seconds": 3600})
        cloud = Mock()
        for method in ("jobs", "resources", "images", "objects", "run_resources"):
            getattr(cloud, method).return_value = []
        with patch.object(controller, "inspect_worker", return_value=("sha256:" + "b" * 64,
                  {"engine_sha": "a" * 64, "worker_version": "1.1.0", "chess_version": "1.11.2"})), \
             patch.object(controller, "save_run", new_callable=AsyncMock) as save:
            await controller.Controller(cloud, "test-image").advance(job, run)
        self.assertEqual(save.await_args.kwargs["status"], "publishing")
        saved_config = save.await_args.kwargs["launch"]["config"]
        self.assertEqual(saved_config["nodes"], 100_000)
        self.assertEqual(saved_config["run_timeout"], 0)
        self.assertEqual(saved_config["stall_timeout"], 300)
        spec = render(controller.Config(**saved_config),
            "us-central1-docker.pkg.dev/chessism-production/chessism-workers/ui-test@sha256:" + "a" * 64, 300)
        commands = spec["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["commands"]
        self.assertEqual(commands[commands.index("--nodes") + 1], "100000")
        self.assertEqual([call[0] for call in cloud.mock_calls], ["jobs", "run_resources", "resources", "images", "objects"])

    async def test_leftovers_prevent_image_publication_and_preserve_recovery_data(self):
        rows, _, _ = fixture()
        run = SimpleNamespace(id="a" * 32, positions=rows, status="preparing", launch={})
        for kind in ("jobs", "resources", "images", "objects", "run_resources"):
            cloud = Mock()
            for method in ("jobs", "resources", "images", "objects", "run_resources"):
                getattr(cloud, method).return_value = [{}] if method == kind else []
            with self.subTest(kind=kind), patch.object(controller, "inspect_worker") as inspect:
                with self.assertRaisesRegex(CleanupError, "Previous cloud resources"):
                    await controller.Controller(cloud, "unused-image").advance(None, run)
                inspect.assert_not_called()
                cloud.publish.assert_not_called()
                cloud.delete_object.assert_not_called()

    async def test_real_cleanup_writes_tmp_plan_and_can_recreate_it_after_restart(self):
        from cleaning_job.cleanup import prepare
        file = Path(__file__).resolve().parents[1] / "stockfish-batch-worker/tests/test_cleaning_job.py"
        spec = importlib.util.spec_from_file_location("cleanup_fixtures", file)
        fixtures = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixtures)
        cloud = fixtures.FakeCloud()
        plan = prepare(cloud, ["batch-one"])
        rows, contract, raw = fixture()
        name, receipt, _ = importer.validate_batch(raw, 0, rows, contract)
        run = SimpleNamespace(id="a" * 32, positions=rows, contract=contract, receipts={name: receipt},
            status="cleaning", cleanup_plan=plan, launch={"manifest": {"status": "complete",
                "fingerprint": contract["fingerprint"], "position_count": 2, "records": receipt["records"]}})
        self.assertFalse(controller.runtime.CLEANUP_ROOT.is_relative_to(controller.runtime.WORKER_ROOT))
        for attempt in range(2):
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(controller.runtime, "CLEANUP_ROOT", Path(directory)), \
                 patch.object(controller, "save_run", new_callable=AsyncMock) as save:
                await controller.Controller(cloud, "unused-image").advance(None, run)
                self.assertTrue((Path(directory) / (plan["sha256"] + ".json")).is_file())
                self.assertEqual(save.await_args.kwargs["status"], "refreshing")
        self.assertFalse(cloud.job_data)
        self.assertFalse(cloud.object_data)

    async def test_existing_run_keeps_its_checkpoint_configuration(self):
        rows, _, _ = fixture()
        config = configuration("a" * 32, len(rows), 1_000_000, 3600)
        run = SimpleNamespace(id="a" * 32, positions=rows, status="running",
                              launch={"config": asdict(config)})
        job = SimpleNamespace(selection={"max_task_seconds": 3600})
        instance = controller.Controller(Mock(), "test-image")
        with patch.object(instance, "poll", new_callable=AsyncMock) as poll:
            await instance.advance(job, run)
        self.assertEqual(poll.await_args.args[1].nodes, 1_000_000)

    async def test_running_imports_available_batch_without_cleanup(self):
        rows, contract, raw = fixture()
        config = configuration("a" * 32, len(rows), 1000000, 3600)
        run = SimpleNamespace(id="a" * 32, job_id="b" * 32, positions=rows, contract=contract,
                              receipts={}, launch={"jobs": ["job"], "uid": "uid"})
        storage = Mock()
        storage.read.side_effect = [encode(contract), raw]
        cloud = Mock()
        cloud.job.return_value = {"uid": "uid", "status": {"state": "RUNNING"}}
        cloud.storage.return_value = storage
        with patch.object(controller, "save_run", new_callable=AsyncMock), \
             patch.object(controller, "import_batch", new_callable=AsyncMock) as import_result, \
             patch.object(controller, "cleaning_job") as cleanup:
            await controller.Controller(cloud, "test-image").poll(run, config)
        import_result.assert_awaited_once_with(run.id, raw, 0)
        cleanup.assert_not_called()

    async def test_missing_remote_job_preserves_results(self):
        cloud = Mock()
        cloud.job.return_value = None
        run = SimpleNamespace(launch={"jobs": ["job"], "uid": "uid"})
        with self.assertRaises(ValueError):
            await controller.Controller(cloud, "test-image").poll(run, None)
        cloud.storage.assert_not_called()


if __name__ == "__main__":
    unittest.main()
