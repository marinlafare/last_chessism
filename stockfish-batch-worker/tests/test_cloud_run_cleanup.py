from copy import deepcopy
import threading
import time
import unittest

from test_cleaning_job import FakeCloud, obj
from cloud_job import cloud_run as api, cloud_run_cleanup as cleanup
from cleaning_job.cloud import CleanupError, PROJECT, REGION, REPOSITORY, Cloud
from unittest.mock import Mock, patch

RUN = "a" * 32
IMAGE = REPOSITORY + "ui-" + RUN + "@sha256:" + "b" * 64


class RunCloud(FakeCloud):
    def __init__(self):
        super().__init__()
        self.job_data = {}
        config = api.config_for(RUN, 500)
        spec = api.spec_for(RUN, config, IMAGE, 1, 1)
        self.remote = {**deepcopy(spec), "uid": "run-uid", "etag": "etag"}
        self.execution = {"name": api.job_name(RUN) + "/executions/exec-one", "uid": "exec-uid",
                          "conditions": [{"type": "Completed", "state": "CONDITION_SUCCEEDED"}]}
        self.launch = {"run_job": api.job_name(RUN), "run_uid": "run-uid", "spec": spec,
                       "run_executions": [{"name": self.execution["name"], "uid": "exec-uid"}], "image": IMAGE}
        self.object_data = [obj(f"inputs/ui-{RUN}/tasks/000000/input.jsonl"), obj(f"results/ui-{RUN}/tasks/000000/batches/000000.json")]
        self.image_data = [{"uri": IMAGE, "uploadTime": "time-one"}]
        self.others = []

    def request(self, service, path, **kwargs):
        if kwargs.get("method") == "DELETE":
            assert path == api.job_name(RUN)
            assert kwargs["params"]["etag"] == "etag"
            self.writes.append(("run", path))
            self.remote = None
            return {"name": "operation"}
        assert path == api.job_name(RUN)
        return deepcopy(self.remote)

    operation = staticmethod(Cloud.operation)

    def pages(self, service, path):
        assert path == api.job_name(RUN) + "/executions"
        return [{"executions": [deepcopy(self.execution)]}]

    def run_resources(self): return deepcopy(self.others)

    def log_entries(self, query, **kwargs): return []


class CleanupTests(unittest.TestCase):
    def test_parallel_objects_keep_exact_generations_and_resume_after_failure(self):
        cloud = RunCloud()
        cloud.object_data = [obj(f'results/ui-{RUN}/batches/{i:06d}.json', str(i + 10)) for i in range(20)]
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        self.assertFalse(cleanup.apply(cloud, plan)['complete'])
        unrelated = obj('results/someone-else/keep.json')
        cloud.object_data.append(unrelated)
        remove, lock = cloud.delete_object, threading.Lock()
        active, peak, fail = 0, 0, True

        def delete(value):
            nonlocal active, peak, fail
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(.02)
                with lock:
                    if fail and value['generation'] == '13':
                        fail = False
                        raise CleanupError('temporary deletion failure')
                    remove(value)
            finally:
                with lock:
                    active -= 1

        cloud.delete_object = delete
        with self.assertRaisesRegex(CleanupError, 'temporary deletion'):
            cleanup.apply(cloud, plan)
        self.assertEqual(active, 0)
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 8)
        self.assertFalse(any(w[0] in {'image', 'log'} for w in cloud.writes))
        self.assertTrue(cleanup.apply(cloud, plan)['complete'])
        removed = [(w[1], w[2]) for w in cloud.writes if w[0] == 'object']
        self.assertCountEqual(removed, [(o['name'], o['generation']) for o in plan['objects']])
        self.assertEqual(cloud.object_data, [unrelated])

    def test_failed_parallel_precondition_blocks_every_delete(self):
        cloud = RunCloud()
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        barrier, completed = threading.Barrier(4, timeout=5), []

        def read(name, fail=False):
            def call(*args):
                barrier.wait()
                self.assertFalse(cloud.writes)
                completed.append(name)
                if fail:
                    raise CleanupError('bucket changed')
            return call

        with patch.object(cleanup, 'other_references', side_effect=read('references')), \
             patch.object(cleanup, 'check_bucket', side_effect=read('bucket', True)), \
             patch.object(cleanup, 'check_inventory', side_effect=read('objects')), \
             patch.object(cleanup.images, 'remaining', side_effect=read('images')):
            with self.assertRaisesRegex(CleanupError, 'bucket changed'):
                cleanup.apply(cloud, plan)
        self.assertEqual(len(completed), 4)
        self.assertEqual(cloud.writes, [])

    def test_cleanup_exact_scope_and_restart_after_job_deletion(self):
        cloud = RunCloud()
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        cloud.object_data.append(obj("results/other/keep.json"))
        self.assertFalse(cleanup.apply(cloud, plan)["complete"])
        self.assertEqual([w[0] for w in cloud.writes], ["run"])
        self.assertTrue(cleanup.apply(cloud, plan)["complete"])
        self.assertEqual([w[0] for w in cloud.writes], ["run", "object", "object", "image"])
        self.assertEqual(len(cloud.object_data), 1)
        self.assertTrue(cleanup.apply(cloud, plan)["complete"])

    def test_unknown_or_running_execution_blocks_all_deletion(self):
        for mutation in ({"uid": "foreign"}, {"conditions": []}):
            cloud = RunCloud()
            plan = cleanup.prepare(cloud, cloud.launch, RUN)
            cloud.execution.update(mutation)
            with self.assertRaises(CleanupError): cleanup.apply(cloud, plan)
            self.assertEqual(cloud.writes, [])

    def test_new_objects_reuploaded_image_or_replaced_job_block(self):
        for change in ("object", "image", "job"):
            cloud = RunCloud()
            plan = cleanup.prepare(cloud, cloud.launch, RUN)
            if change == "object": cloud.object_data.append(obj(f"results/ui-{RUN}/new.json", "99"))
            if change == "image": cloud.image_data[0]["uploadTime"] = "time-two"
            if change == "job": cloud.remote["uid"] = "replacement"
            with self.assertRaises(CleanupError): cleanup.apply(cloud, plan)
            self.assertEqual(cloud.writes, [])

    def test_other_region_service_revision_protects_image(self):
        cloud = RunCloud()
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        cloud.others = [{"kind": "Revision", "name": "other-region/revisions/old", "image": IMAGE}]
        with self.assertRaisesRegex(CleanupError, "references this image"): cleanup.apply(cloud, plan)
        self.assertEqual(cloud.writes, [])

    def test_deleted_job_tombstone_does_not_block_cleanup_forever(self):
        cloud = RunCloud()
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        cloud.remote.update(deleteTime="now", reconciling=False)
        self.assertTrue(cleanup.apply(cloud, plan)["complete"])
        self.assertFalse(any(w[0] == "run" for w in cloud.writes))

    def test_other_run_resource_prevents_whole_stream_log_deletion(self):
        cloud = RunCloud()
        plan = cleanup.prepare(cloud, cloud.launch, RUN)
        cloud.others = [{"kind": "Service", "metadata": {"uid": "other-owner"}}]
        report = cleanup.clean_logs(cloud, plan)
        self.assertEqual(set(report["retained_shared"]), cleanup.STREAMS)
        self.assertFalse(any(w[0] == "log" for w in cloud.writes))

    def test_global_inventory_uses_v1_continuation_and_keeps_snapshots(self):
        cloud = Cloud()
        row = {"kind": "Job", "metadata": {"name": "other", "uid": "uid", "labels": {"cloud.googleapis.com/location": "us-east1"}}}
        def reply(service, path, **kwargs):
            if path.endswith('/jobs') and not kwargs.get('params'):
                return {"items": [row], "metadata": {"continue": "next"}}
            return {}
        with patch.object(cloud, "request", side_effect=reply) as request:
            values = cloud.run_resources()
        self.assertEqual(values[0]["name"], f"projects/{PROJECT}/locations/us-east1/jobs/other")
        job_calls = [c for c in request.call_args_list if c.args[1].endswith('/jobs')]
        self.assertEqual(len(request.call_args_list), 5)
        self.assertEqual(job_calls[1].kwargs["params"], {"continue": "next"})
        with patch.object(cloud, "request", return_value={"items": [{"metadata": {}}]}):
            with self.assertRaises(CleanupError): cloud.run_resources()


if __name__ == "__main__": unittest.main()
