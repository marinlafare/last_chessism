"""Bounded per-shard completion reads; every task must match durable receipts."""
from concurrent.futures import ThreadPoolExecutor
import threading

from .artifacts import read_json, PERFORMANCE_MAX_BYTES
from .importer import validate_manifest
from .result_receipts import task_receipts
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_MANIFEST_BYTES


def verify_task_artifacts(storage_factory, tasks, positions, receipts, *, engine_workers,
                          retain_manifests=False, concurrency=4):
    """Return ordered manifests/reports only after ALL tasks validate.

    Each thread owns its Storage/HTTP session. At most four task manifests are
    being read. Cloud Run discards them after comparison; legacy Batch retains
    its saved per-shard proofs. On error in-flight reads drain before returning.
    """
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise ValueError('Verification concurrency must be 1–4')
    thread_state = threading.local()

    def verify(task):
        if not hasattr(thread_state, 'storage'):
            thread_state.storage = storage_factory()
        storage = thread_state.storage
        manifest = read_json(storage, task['output'], 'manifest.json', MAX_MANIFEST_BYTES)
        if manifest is None:
            raise ValueError('Successful cloud task has no completion manifest; cleanup withheld')
        validate_manifest(manifest, positions[task['start']:task['start'] + task['count']],
                          task['contract'], task_receipts(receipts, task['index']))
        if not retain_manifests:
            manifest = None
        report = read_json(storage, task['output'], 'performance.json', PERFORMANCE_MAX_BYTES)
        if report is None:
            raise ValueError('Successful cloud task has no performance report; cleanup withheld')
        return manifest, validate_performance(report, task['contract'], engine_workers)

    manifests, reports = [], []
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix='cloud-verify') as pool:
        for offset in range(0, len(tasks), concurrency):
            # A bounded batch avoids scheduling every task after a failed read.
            for manifest, report in pool.map(verify, tasks[offset:offset + concurrency]):
                if retain_manifests:
                    manifests.append(manifest)
                reports.append(report)
    return manifests, reports
