"""Cloud Run cancellation can become terminal before its final uploads arrive.

The phase is durable; the observation window intentionally is not. A controller
restart must observe a fresh quiet window and revalidate receipts before deleting
anything. Saved deletion plans retain previously seen generations, even after a
partial cleanup, so restarting cannot legitimize a replacement.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from time import monotonic

from cleaning_job import cancelled_artifacts, images
from cleaning_job.cleanup import digest, inventory, require
from cleaning_job.cloud import BUCKET, CleanupError
from cloud_job import cloud_run_cleanup
from stockfish_batch.checkpoints import BatchCheckpoints, digest as content_digest
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from .result_receipts import task_receipts, completion_manifest

MIN_DRAIN_SECONDS = 120
QUIET_SECONDS = 30
MAX_DRAIN_SECONDS = 600


class LateUploads(CleanupError):
    """Expected, new task artifacts require another observation/import pass."""


class Drain:
    def __init__(self, roots, artifact_plan, all_ids, tasks_by_run):
        self.prefixes = cancelled_artifacts.prefixes([r.id for r in roots])
        self.allowed, self.seen, self.current = {}, {}, None
        self.started = self.changed = monotonic()
        self.approved = None
        for root in roots:
            for task in tasks_by_run[root.id]:
                output = f'results/ui-{root.id}/tasks/{task["index"]:06d}/'
                require(task['output'] == f'gs://{BUCKET}/{output[:-1]}', 'Unexpected cancellation task output')
                self.allowed[f'inputs/ui-{root.id}/tasks/{task["index"]:06d}/input.jsonl'] = 'input'
                for filename in ('contract.json', 'manifest.json', 'performance.json'):
                    self.allowed[output + filename] = filename
                for index in range((task['count'] + 499) // 500):
                    self.allowed[output + f'batches/{index:06d}.json'] = 'batch'
            if root.cleanup_plan:
                cloud_run_cleanup.validate(root.cleanup_plan)
                self.remember(root.cleanup_plan['objects'])
        if artifact_plan:
            cancelled_artifacts.validate(artifact_plan, all_ids)
            self.remember([o for o in artifact_plan['objects'] if self.owns(o)])

    def owns(self, obj):
        return any(obj['name'].startswith(p) for p in self.prefixes)

    def remember(self, objects):
        for obj in objects:
            name = obj['name']
            require(name in self.allowed, 'Unexpected cancellation artifact; cleanup withheld: ' + name)
            require(name not in self.seen or self.seen[name] == obj,
                    'Cancellation artifact replaced or changed; cleanup withheld: ' + name)
            self.seen[name] = deepcopy(obj)

    def observe(self, cloud):
        objects = inventory(cloud, self.prefixes)
        self.remember(objects)
        current = {o['name']: o for o in objects}
        # Missing objects are permitted after partial deletion, but any addition
        # (even an old generation reappearing) invalidates the receipt gate.
        if self.current is None or any(self.current.get(k) != v for k, v in current.items()):
            self.changed = monotonic()
            self.approved = None
        self.current = current
        return objects

    def ready(self):
        now = monotonic()
        ready = self.current is not None and now - self.started >= MIN_DRAIN_SECONDS and now - self.changed >= QUIET_SECONDS
        if not ready and now - self.started >= MAX_DRAIN_SECONDS:
            raise CleanupError('Late uploads did not settle within ten minutes; checkpoints retained. Resume to observe again.')
        return ready

    def approve(self, before, after):
        if before != after:
            self.approved = None
            raise LateUploads('Uploads changed during receipt validation; waiting again')
        self.approved = deepcopy(self.current)

    def require_approved(self, objects):
        if self.approved is None:
            raise LateUploads('Cancellation uploads need another receipt-validation pass')
        for obj in objects:
            if self.owns(obj) and self.approved.get(obj['name']) != obj:
                # Check replacements/unexpected paths before treating this as a
                # recoverable upload. Never broaden approval automatically.
                self.remember([obj])
                raise LateUploads('New task uploads require another import pass')

def verify_artifacts(cloud, run, tasks, objects):
    """Validate every present result against its durable receipt, in four lanes.

    Missing completion metadata is normal for cancellation. If present, it must
    match the reserved task. Full manifests also prove all their batches saved.
    No cloud writes and no DB writes happen in these reader threads.
    """
    present = {o['name'] for o in objects}
    def verify(task):
        storage = cloud.storage()
        base = task['output'].removeprefix(f'gs://{BUCKET}/') + '/'
        receipts = task_receipts(run.receipts, task['index'])
        for name in sorted(present):
            if not name.startswith(base):
                continue
            filename = name[len(base):]
            limit = BatchCheckpoints.MAX_BYTES if filename.startswith('batches/') else MAX_MANIFEST_BYTES if filename == 'manifest.json' else 2 * 1024 * 1024
            raw = storage.read(f'gs://{BUCKET}/{name}', limit)
            require(raw is not None, 'Cancellation artifact disappeared during validation')
            if filename.startswith('batches/'):
                require(filename in receipts and content_digest(raw) == receipts[filename]['sha256'],
                        'Cloud batch lacks a matching durable receipt; cleanup withheld')
            elif filename == 'contract.json':
                require(json.loads(raw) == task['contract'], 'Cancellation task contract changed')
            elif filename == 'manifest.json':
                expected = completion_manifest(task['contract'], task['count'], receipts)
                require(len(expected['records']) == task['count'] and json.loads(raw) == expected,
                        'Cancellation manifest differs from saved results')
            elif filename == 'performance.json':
                validate_performance(json.loads(raw), task['contract'], max_workers=1)
            else:
                raise CleanupError('Unexpected cancellation result artifact')
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='cancel-verify') as pool:
        list(pool.map(verify, tasks))


def replan_run(cloud, root, drain):
    drain.require_approved(list(drain.current.values()))
    if root.cleanup_plan:
        plan = root.cleanup_plan
        cloud_run_cleanup.validate(plan)
        cloud_run_cleanup.current_job(cloud, plan)
        cloud_run_cleanup.other_references(cloud, plan)
        images.remaining(cloud, plan)  # Never adopt a new image upload or UID.
    else:
        plan = cloud_run_cleanup.prepare(cloud, root.launch, root.id, cancelled=True)
        drain.require_approved(plan['objects'])
    # Historical, already-deleted objects need not be in the current snapshot.
    result = deepcopy(plan)
    result['objects'] = [o for o in drain.seen.values() if any(o['name'].startswith(p) for p in plan['prefixes'])]
    result['objects'].sort(key=lambda o: (o['name'], o['generation']))
    result['sha256'] = digest(result)
    cloud_run_cleanup.validate(result)
    return result
