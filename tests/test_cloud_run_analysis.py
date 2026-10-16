"""Offline Cloud Run backend tests: no credentials, cloud writes or database."""
from copy import deepcopy
from dataclasses import asdict
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations.cloud_analysis import controller, importer
from chessism_api.operations.cloud_analysis import cloud_run_controller as run_controller
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from cloud_job import cloud_run as api
from cloud_job.launch import expected_contract
from cleaning_job.cloud import CleanupError, REPOSITORY
from stockfish_batch.checkpoints import BatchCheckpoints, encode
from stockfish_batch.cloud_run_task import task_config
from stockfish_core import ENGINE_SHA256

RUN_ID = "a" * 32
IMAGE = REPOSITORY + "ui-" + RUN_ID + "@sha256:" + "b" * 64
META = {"engine_sha": ENGINE_SHA256, "worker_version": "1.4.0", "chess_version": "1.11.2"}


def fixture():
    config = api.config_for(RUN_ID, 500)
    launch = {"backend": "cloud_run", "config": asdict(config), "run_job": api.job_name(RUN_ID),
              "run_uid": "job-uid", "run_executions": [], "spec": api.spec_for(RUN_ID, config, IMAGE, 2, 2)}
    remote = {**deepcopy(launch["spec"]), "uid": "job-uid", "etag": "etag"}
    execution = {"name": launch["run_job"] + "/executions/test-one", "uid": "execution-uid", "etag": "etag",
                 **deepcopy(launch["spec"]["template"])}
    return launch, remote, execution


class PlanningTests(unittest.TestCase):
    def test_backend_and_cpu_validation(self):
        self.assertEqual(CloudJobRequest().backend, "batch_spot")
        for cpus in (1, 4, 64):
            self.assertEqual(CloudJobRequest(backend="cloud_run", n_cpus=cpus).n_cpus, cpus)
        for value in (0, 65, 2.5, "4", True):
            with self.assertRaises(ValueError):
                CloudJobRequest(backend="cloud_run", n_cpus=value)
        with self.assertRaises(ValueError):
            CloudJobRequest(backend="anything")

    def test_shards_cover_selection_once_with_bounded_memory(self):
        for count in (1, 20, 499, 500, 501, 2082, 25000, 200000):
            for cpus in (1, 4, 16, 64):
                parts = api.partitions(count, cpus)
                self.assertLessEqual(len(parts), 400)
                self.assertEqual([i for start, size in parts for i in range(start, start + size)], list(range(count)))
                self.assertTrue(all(start % 500 == 0 and 1 <= size <= 5000 for start, size in parts))
        for args in ((0, 1), (200001, 1), (1, 0), (1, 65)):
            with self.assertRaises(ValueError): api.partitions(*args)

    def test_fixed_single_core_profile_and_task_paths(self):
        config = api.config_for(RUN_ID, 500)
        self.assertEqual((config.nodes, config.threads, config.hash_mb, config.multipv), (100000, 1, 256, 4))
        self.assertEqual((config.run_timeout, config.stall_timeout), (0, 300))
        spec = api.spec_for(RUN_ID, config, IMAGE, 2, 4)["template"]
        self.assertEqual((spec["taskCount"], spec["parallelism"]), (2, 2))
        template = spec["template"]
        self.assertEqual((template["maxRetries"], template["timeout"]), (1, "604800s"))
        self.assertEqual(template["containers"][0]["resources"]["limits"], {"cpu": "1", "memory": "1Gi"})
        chosen = api.task_config(config, 1, 2)
        self.assertTrue(chosen.input.endswith("/tasks/000001/input.jsonl"))
        self.assertTrue(chosen.output.endswith("/tasks/000001"))
        for index in (-1, 2):
            with self.assertRaises(ValueError): api.task_config(config, index, 2)
        with self.assertRaises(ValueError): task_config(config, {})

    def test_quota_requires_both_metrics_in_correct_region(self):
        cloud = Mock()
        def metric(name, limit, region="us-central1"):
            return {"metric": "run.googleapis.com/" + name, "consumerQuotaLimits": [
                {"quotaBuckets": [{"dimensions": {"region": region}, "effectiveLimit": str(limit)}]}]}
        cloud.pages.return_value = [{"metrics": [metric("cpu_allocation", 200000), metric("mem_allocation", 400 * 1024**3)]}]
        self.assertEqual(api.check_quota(cloud, 64)["cpu"], 200000)
        for metrics in ([metric("cpu_allocation", 200000)],
                        [metric("cpu_allocation", 1000), metric("mem_allocation", 400 * 1024**3)],
                        [metric("cpu_allocation", 200000), metric("mem_allocation", 1024**3)],
                        [metric("cpu_allocation", 200000, "other"), metric("mem_allocation", 400 * 1024**3)]):
            cloud.pages.return_value = [{"metrics": metrics}]
            with self.assertRaises(CleanupError): api.check_quota(cloud, 4)
            cloud.request.assert_not_called()

    def test_execution_configuration_must_match(self):
        launch, _, execution = fixture()
        run_controller.verify_execution(execution, launch)
        for key, value in (("parallelism", 8), ("taskCount", 99), ("template", {}), ("name", "other")):
            with self.assertRaises(ValueError):
                run_controller.verify_execution({**execution, key: value}, launch)

    def test_sharded_receipts_validate_identity_and_root_completion(self):
        rows = [{"id": str(i), "fen": "8/8/8/8/8/8/4k3/6K1 w - -"} for i in range(4)]
        cfg = api.config_for(RUN_ID, 2)
        root = expected_contract(b"".join(map(encode, rows)), rows, cfg, META)
        receipts = {}
        for number in range(2):
            subset = rows[number * 2:number * 2 + 2]
            contract = expected_contract(b"".join(map(encode, subset)), subset, cfg, META)
            task = {"index": number, "start": number * 2, "count": 2, "contract": contract}
            checkpoints = BatchCheckpoints(None, "", contract, subset, 500)
            records = [checkpoints.prepare(row, {"fen": row["fen"], "is_valid": True,
                       "analysis": {"pv": [], "score": 0}}, 1) for row in subset]
            raw = encode({"fingerprint": contract["fingerprint"], "records": records})
            key, receipt, results = importer.validate_batch(raw, 0, rows, root, task)
            self.assertTrue(key.startswith(f"tasks/{number:06d}/batches/"))
            self.assertEqual(len(results), 2)
            receipts[key] = receipt
            for wrong in ({**task, "start": 2 if number == 0 else 0}, {**task, "contract": root}):
                with self.assertRaises(ValueError): importer.validate_batch(raw, 0, rows, root, wrong)
        manifest = {"status": "complete", "position_count": 4, "fingerprint": root["fingerprint"],
                    "records": [r for key in sorted(receipts) for r in receipts[key]["records"]]}
        importer.validate_manifest(manifest, rows, root, receipts)
        with self.assertRaises(ValueError): importer.validate_manifest(manifest, rows, root, {})


class RestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_lost_run_response_never_replayed_on_resume(self):
        launch, remote, execution = fixture()
        run = SimpleNamespace(id=RUN_ID, status="run_start", launch=launch)
        job = SimpleNamespace(selection={"backend": "cloud_run"})
        cloud = Mock()
        cloud.pages.return_value = [{"executions": []}]
        writes = []
        async def save(_, **fields):
            for key, value in fields.items(): setattr(run, key, value)
        def request(service, path, **kwargs):
            if kwargs.get("method") == "POST":
                self.assertEqual(run.status, "run_discover")
                writes.append(path)
                raise CleanupError("lost response")
            return remote
        cloud.request.side_effect = request
        with patch.object(controller, "save_run", side_effect=save):
            instance = controller.Controller(cloud, "unused")
            with self.assertRaises(CleanupError): await instance.advance(job, run)
            self.assertEqual(len(writes), 1)
            with self.assertRaisesRegex(ValueError, "unresolved"): await instance.advance(job, run)
            self.assertEqual(len(writes), 1)
            cloud.pages.return_value = [{"executions": [execution]}]
            await instance.advance(job, run)
            self.assertEqual(run.status, "running")
            self.assertEqual(run.launch["run_execution"]["uid"], "execution-uid")
            self.assertEqual(len(writes), 1)

    async def test_unknown_execution_prevents_second_start(self):
        launch, remote, execution = fixture()
        run = SimpleNamespace(id=RUN_ID, status="run_start", launch=launch)
        cloud = Mock()
        cloud.request.return_value = remote
        cloud.pages.return_value = [{"executions": [execution]}]
        with patch.object(controller, "save_run", new_callable=AsyncMock) as save:
            with self.assertRaisesRegex(ValueError, "Unexpected/running"):
                await run_controller.advance(controller.Controller(cloud, "unused"), SimpleNamespace(selection={}), run)
            save.assert_not_called()
            self.assertFalse(any(call.kwargs.get("method") == "POST" for call in cloud.request.call_args_list))

    async def test_cleanup_refused_without_committed_receipts(self):
        launch, _, _ = fixture()
        launch["manifest"] = {}
        run = SimpleNamespace(id=RUN_ID, status="cleanup_planning", launch=launch,
                              positions=[{"id": "one"}], contract={"fingerprint": "a"}, receipts={})
        with patch("cloud_job.cloud_run_cleanup.prepare") as prepare:
            with self.assertRaises(ValueError):
                await run_controller.advance(controller.Controller(Mock(), "unused"), None, run)
            prepare.assert_not_called()


if __name__ == "__main__": unittest.main()
