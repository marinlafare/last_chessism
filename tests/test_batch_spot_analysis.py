"""Offline fleet tests. All cloud calls and DB state changes are fakes."""
from copy import deepcopy
from dataclasses import asdict
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations.cloud_analysis import controller, batch_spot_controller as fleet
from chessism_api.operations.cloud_analysis import batch_spot_results as results
from chessism_api.operations.cloud_analysis.batch_spot_retry import retry_launch
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.operations.cloud_analysis.importer import validate_batch
from cloud_job import batch_spot as api
from cloud_job.launch import expected_contract
from cleaning_job.cleanup import prefixes
from cleaning_job.cloud import CleanupError, REPOSITORY
from stockfish_batch.checkpoints import encode, BatchCheckpoints
from stockfish_core import ENGINE_SHA256

RUN = "a" * 32
IMAGE = REPOSITORY + "ui-" + RUN + "@sha256:" + "b" * 64
META = {"engine_sha": ENGINE_SHA256, "worker_version": "1.4.0", "chess_version": "1.11.2"}


def fixture(count=5, n_vms=2):
    rows = [{"id": str(i), "fen": "8/8/8/8/8/8/4k3/6K1 w - -"} for i in range(count)]
    root = expected_contract(b"".join(map(encode, rows)), rows, api.config_for(RUN, count), META)
    tasks = []
    for index, (start, size) in enumerate(api.partitions(count, n_vms)):
        ident = api.shard_id(RUN, index)
        cfg = api.config_for(ident, size)
        subset = rows[start:start + size]
        tasks.append({"index": index, "start": start, "count": size, "run_id": ident,
                      "config": asdict(cfg), "spec": api.spec_for(cfg, IMAGE),
                      "contract": expected_contract(b"".join(map(encode, subset)), subset, cfg, META),
                      "status": "PENDING", "jobs": [], "recovery_session": 1})
    return SimpleNamespace(id=RUN, job_id="job", status="submitting", positions=rows,
        contract=root, receipts={}, launch={"backend": "batch_spot", "multi_vm": True,
            "recovery_session": 1, "tasks": tasks, "jobs": [], "image": IMAGE})


def quota_cloud(cpus=32, spot=0, usage=0):
    regional = [{"metric": m, "limit": v, "usage": usage if m == "N2D_CPUS" else 0} for m, v in (
        ("N2D_CPUS", cpus), ("PREEMPTIBLE_CPUS", spot), ("INSTANCES", 24),
        ("SSD_TOTAL_GB", 500), ("IN_USE_ADDRESSES", 8))]
    cloud = Mock()
    cloud.request.side_effect = lambda service, path: {"quotas": regional if "/regions/" in path else
        [{"metric": "CPUS_ALL_REGIONS", "limit": 160, "usage": 0}]}
    return cloud


class PlanTests(unittest.TestCase):
    def test_n_vms_is_bounded_and_engine_settings_stay_system_owned(self):
        for count in (1, 2, 10):
            self.assertEqual(CloudJobRequest(n_vms=count).n_vms, count)
        for value in (0, 11, -1, True, 1.5, "2", None):
            with self.assertRaises(ValueError): CloudJobRequest(n_vms=value)
        for key in ("machine_type", "threads", "hash_mb", "nodes", "memory_mib"):
            with self.assertRaises(ValueError): CloudJobRequest(**{key: 16})

    def test_even_disjoint_shards_and_unique_identities(self):
        for count in (1, 3, 499, 500, 501, 13456, 200000):
            for n in (1, 2, 3, 10):
                parts = api.partitions(count, n)
                self.assertEqual([p for start, size in parts for p in range(start, start + size)], list(range(count)))
                self.assertEqual(len(parts), min(count, n))
                self.assertLessEqual(max(size for _, size in parts) - min(size for _, size in parts), 1)
        self.assertEqual(len({api.shard_id(RUN, i) for i in range(10)}), 10)
        self.assertNotEqual(api.shard_id(RUN, 0), api.shard_id("b" * 32, 0))

    def test_fixed_profile_spot_only_and_independent_cleanup_scopes(self):
        run = fixture()
        scopes = set()
        for task in run.launch["tasks"]:
            cfg, spec = api.config_for(task["run_id"], task["count"]), task["spec"]
            self.assertEqual((cfg.workers, cfg.threads, cfg.nodes, cfg.hash_mb), (16, 1, 100000, 256))
            self.assertEqual((cfg.memory_mib, cfg.stall_timeout, cfg.run_timeout, cfg.batch_size), (12288, 300, 0, 500))
            self.assertEqual(spec["taskGroups"][0]["taskSpec"]["computeResource"], {"cpuMilli": "16000", "memoryMib": "12288"})
            self.assertNotIn("maxRunDuration", spec["taskGroups"][0]["taskSpec"])
            self.assertIn("--memory=12g", spec["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["options"])
            api.verify_job(spec, spec)
            self.assertFalse(scopes.intersection(prefixes(spec)))
            scopes.update(prefixes(spec))
            for machine, model in (("n2d-standard-4", "SPOT"), (api.MACHINE, "STANDARD")):
                changed = deepcopy(spec)
                changed["allocationPolicy"]["instances"][0]["policy"].update(machineType=machine, provisioningModel=model)
                with self.assertRaises(ValueError): api.verify_job(changed, spec)

    def test_quota_uses_correct_scope_and_available_capacity(self):
        self.assertIn("N2D_CPUS", api.check_quota(quota_cloud(), 2))
        self.assertIn("PREEMPTIBLE_CPUS", api.check_quota(quota_cloud(cpus=0, spot=32), 2))
        for cloud in (quota_cloud(16), quota_cloud(32, usage=1), quota_cloud(64, spot=16)):
            with self.assertRaisesRegex(CleanupError, "Insufficient"): api.check_quota(cloud, 2)
            self.assertTrue(all(c.args[0] == "compute" and not c.kwargs for c in cloud.request.call_args_list))
        cloud = quota_cloud()
        cloud.request.side_effect = None
        cloud.request.return_value = {"quotas": []}
        with self.assertRaisesRegex(CleanupError, "Cannot verify"): api.check_quota(cloud, 1)

    def test_retry_only_unfinished_shards_preserves_checkpoint_and_prior_job_inventory(self):
        run = fixture()
        for index, task in enumerate(run.launch["tasks"]):
            task.update(status="SUCCEEDED" if index == 0 else "FAILED", recovery_execution="owner")
            task["recovery_summary"] = {"status": task["status"], "jobs": ["old"], "uids": {"old": "uid"}}
        before = deepcopy(run.launch)
        new = retry_launch(run.launch)
        self.assertEqual(run.launch, before)
        self.assertEqual(new["tasks"][0], before["tasks"][0])
        self.assertEqual(new["tasks"][1]["recovery_session"], 2)
        self.assertEqual(new["tasks"][1]["prior_uids"], {"old": "uid"})
        self.assertNotIn("recovery_execution", new["tasks"][1])
        with self.assertRaises(ValueError): retry_launch({**run.launch, "recovery_session": 3})
        run.launch["tasks"][1]["status"] = "RUNNING"
        with self.assertRaises(ValueError): retry_launch(run.launch)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_quota_failure_precedes_image_inspection_or_publish(self):
        run = fixture()
        run.status, run.launch = "preparing", {}
        cloud = quota_cloud(16)
        with patch.object(fleet, "verify_clean_workspace"), patch.object(fleet, "inspect_worker") as inspect:
            with self.assertRaises(CleanupError):
                await fleet.advance(controller.Controller(cloud, "local"), SimpleNamespace(selection={"n_vms": 2}), run)
            inspect.assert_not_called()
            cloud.publish.assert_not_called()
            cloud.start_recovery.assert_not_called()

    async def test_partial_submission_resumes_owners_without_restarting_first_vm(self):
        run, cloud = fixture(), Mock()
        async def save(_, **fields):
            for key, value in fields.items(): setattr(run, key, deepcopy(value))
        cloud.start_recovery.side_effect = ["owner0", CleanupError("lost reply")]
        with patch.object(controller, "save_run", side_effect=save), patch.object(api, "check_quota"):
            with self.assertRaises(CleanupError):
                await fleet.advance(controller.Controller(cloud, "local"), SimpleNamespace(selection={}), run)
            self.assertEqual(run.launch["tasks"][0]["recovery_execution"], "owner0")
            cloud.start_recovery.reset_mock(side_effect=True)
            cloud.start_recovery.return_value = "owner1"
            await fleet.advance(controller.Controller(cloud, "local"), SimpleNamespace(selection={}), run)
            self.assertEqual(run.status, "running")
            self.assertEqual(cloud.start_recovery.call_count, 1)
            self.assertEqual(cloud.start_recovery.call_args.args[0], run.launch["tasks"][1]["run_id"])

    async def test_cancel_before_partial_submission_fences_every_unfinished_vm(self):
        run, cloud = fixture(), Mock()
        with patch.object(controller, "save_run", new_callable=AsyncMock), patch.object(api, "check_quota") as quota:
            await fleet.advance(controller.Controller(cloud, "local"), SimpleNamespace(selection={"cancel_requested": True}), run)
            self.assertEqual(cloud.cancel_recovery.call_count, 2)
            self.assertEqual(cloud.start_recovery.call_count, 2)
            quota.assert_not_called()

    async def test_preempted_vm_does_not_block_other_vm_import_or_fail_whole_job(self):
        run, cloud = fixture(), Mock()
        for task in run.launch["tasks"]:
            task["recovery_execution"] = "owner"
        def snapshot(ident, task):
            name = "job" + str(task["index"])
            return {"jobs": [name], "uids": {name: "uid"}, "current_job": name,
                    "execution": "owner", "status": "RETRYING" if task["index"] == 0 else "RUNNING",
                    "preemptions": 1, "application_failures": 0}, {"state": "ACTIVE"}
        cloud.recovery_snapshot.side_effect = snapshot
        def remote(name):
            t = run.launch["tasks"][int(name[-1])]
            return {**deepcopy(t["spec"]), "uid": "uid", "status": {"state": "FAILED" if name == "job0" else "RUNNING"}}
        cloud.job.side_effect = remote
        with patch.object(results, "import_ready", new_callable=AsyncMock) as imports, \
             patch.object(controller, "save_run", new_callable=AsyncMock) as save, \
             patch.object(controller, "save_job", new_callable=AsyncMock) as save_job:
            await fleet.poll(controller.Controller(cloud, "local"), SimpleNamespace(selection={}), run, deepcopy(run.launch))
            self.assertEqual(imports.await_count, 1)
            self.assertEqual(len(imports.call_args.args[2]), 2)
            self.assertEqual(save.call_args.kwargs["launch"]["vm_statuses"][0]["state"], "RECOVERING")
            self.assertEqual(save.call_args.kwargs["cloud_state"], "RUNNING")
            save_job.assert_not_called()

    async def test_cleanup_waits_for_receipts_reports_and_every_supervisor(self):
        run, cloud = fixture(), Mock()
        run.status = "cleanup_planning"
        with patch.object(fleet, "prepare") as prepare:
            with self.assertRaises(KeyError):
                await fleet.advance(controller.Controller(cloud, "local"), None, run)
            prepare.assert_not_called()
        cloud.require_recovery_stopped.side_effect = [None, ValueError("still active")]
        with patch.object(results, "validate_saved"), patch.object(fleet, "prepare") as prepare:
            with self.assertRaisesRegex(ValueError, "still active"):
                await fleet.advance(controller.Controller(cloud, "local"), None, run)
            self.assertEqual(cloud.require_recovery_stopped.call_count, 2)
            prepare.assert_not_called()

    async def test_all_shard_manifests_and_reports_saved_before_joint_cleanup(self):
        run = fixture()
        objects = {}
        for task in run.launch["tasks"]:
            subset = run.positions[task["start"]:task["start"] + task["count"]]
            checkpoints = BatchCheckpoints(None, "", task["contract"], subset, 500)
            records = [checkpoints.prepare(row, {"fen": row["fen"], "is_valid": True,
                       "analysis": {"pv": [], "score": 0}}, 1) for row in subset]
            raw = encode({"fingerprint": task["contract"]["fingerprint"], "records": records})
            key, receipt, _ = validate_batch(raw, 0, run.positions, run.contract, task)
            run.receipts[key] = receipt
            manifest = {"status": "complete", "fingerprint": task["contract"]["fingerprint"],
                        "position_count": task["count"], "records": [{**r, "object": "batches/000000.json"} for r in receipt["records"]]}
            report = {"schema_version": 1, "fingerprint": task["contract"]["fingerprint"],
                      "position_count": task["count"], "resumed": 0, "analyzed_this_attempt": task["count"],
                      "workers": [{"worker": 0, "positions": task["count"], "analysis_seconds": 1, "upload_wait_seconds": 0}],
                      "metrics": {"worker_seconds": 2, "fen_per_second": 1}}
            objects[task["config"]["output"] + "/manifest.json"] = encode(manifest)
            objects[task["config"]["output"] + "/performance.json"] = encode(report)
        storage = Mock()
        storage.read.side_effect = lambda uri, *args: objects.get(uri)
        launch = await results.collect(storage, run, deepcopy(run.launch))
        self.assertEqual(launch["performance"]["position_count"], 5)
        self.assertEqual(launch["performance"]["vm_count"], 2)
        results.validate_saved(run, launch)
        launch["task_performance"][1]["fingerprint"] = "foreign"
        with self.assertRaises(ValueError): results.validate_saved(run, launch)


if __name__ == "__main__": unittest.main()
