"""Cloud Run's fixed-single-core adapter for shared completion verification."""
from cloud_job import cloud_run as api
from .task_results import verify_task_artifacts


def verify_tasks(storage_factory, config, tasks, positions, receipts, *, concurrency=4):
    items = [{**task, 'output': api.task_config(config, task['index'], len(tasks)).output}
             for task in tasks]
    _, reports = verify_task_artifacts(storage_factory, items, positions, receipts,
                                      engine_workers=1, concurrency=concurrency)
    return reports
