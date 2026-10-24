"""Explicitly approved backup -> private restore test -> incremental FEN backup.

Run inside the API container after deploying and checking it is idle:
  python - --approve-live-backups < scripts/run_backup_chain.py

Uses normal application queues/ownership checks. Keeps older recovery points.
No restore targets the live database; no cloud task is started.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import time
import uuid


async def run():
    from arq.jobs import Job, JobStatus
    from chessism_api.redis_client import get_redis_pool, close_redis_pool
    from chessism_api.routers.backups import api_create_database_backup
    from chessism_api.routers.analysis import api_create_fen_analysis_backup
    from chessism_api.operations.backup_coordination import reserve_backup, release_backup
    from chessism_api.operations.database_restore_tests import (
        latest_backup_for_restore_test, mark_restore_test_queued, mark_restore_test_queue_failed,
    )
    from chessism_api.routers.jobs import api_get_active_jobs

    redis = await get_redis_pool()
    started = time.monotonic()

    def report(**values):
        print(json.dumps({"at": datetime.now(timezone.utc).isoformat(),
                          "elapsed_seconds": round(time.monotonic() - started, 1), **values},
                         default=str), flush=True)

    async def wait(job_id, queue, stage):
        job = Job(job_id, redis, _queue_name=queue)
        while True:
            status = await job.status()
            raw = await redis.get(f"chessism:job_progress:{job_id}")
            progress = json.loads(raw) if raw else {}
            report(stage=stage, job_id=job_id, status=status.value,
                   progress={k: progress.get(k) for k in
                             ("phase", "detail", "processed", "total", "bytes")})
            if status == JobStatus.complete:
                return await job.result(timeout=5)
            if status == JobStatus.not_found:
                raise RuntimeError(f"{stage} job disappeared; inspect before resubmitting")
            await asyncio.sleep(30)

    try:
        active = json.loads((await api_get_active_jobs(redis)).body).get("jobs", [])
        if active:
            raise RuntimeError("Active jobs found; do not start maintenance chain")
        submitted = json.loads((await api_create_database_backup(redis)).body)
        backup = await wait(submitted["job_id"], "backup_queue", "database_backup")
        latest = latest_backup_for_restore_test()
        if latest["backup_id"] != backup["backup_id"]:
            raise RuntimeError("Latest backup changed; refusing to test the wrong snapshot")
        job_id = str(uuid.uuid4())
        reserved, owner = await reserve_backup(redis, job_id)
        if not reserved:
            raise RuntimeError(f"Backup lane occupied by {owner}")
        try:
            mark_restore_test_queued(job_id=job_id, backup=latest)
            job = await redis.enqueue_job("run_database_restore_test_job",
                backup_id=backup["backup_id"], retire_previous=False,
                _job_id=job_id, _queue_name="backup_queue")
            if job is None:
                raise RuntimeError("Restore test was not queued")
        except Exception as error:
            mark_restore_test_queue_failed(job_id=job_id, backup_id=backup["backup_id"], error=error)
            await release_backup(redis, job_id)
            raise
        tested = await wait(job_id, "backup_queue", "private_restore_test")
        if not tested.get("restore_tested"):
            raise RuntimeError("Database restore verification did not pass")
        submitted = json.loads((await api_create_fen_analysis_backup(redis)).body)
        fens = await wait(submitted["job_id"], "pipeline_queue", "fen_backup")
        report(state="succeeded", backup_id=backup["backup_id"],
               compressed_bytes=backup.get("repository_bytes"),
               restore_tested=True, restore_seconds=tested.get("elapsed_seconds"),
               retained_previous=(tested.get("cleanup") or {}).get("state") == "preserved",
               fen_backup=fens)
    finally:
        await close_redis_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approve-live-backups", action="store_true", required=True)
    parser.parse_args()
    asyncio.run(run())
