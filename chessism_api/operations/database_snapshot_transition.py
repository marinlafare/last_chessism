"""Explicit one-shot save/test/retire operation, never run at application startup.

Run manually: python -m chessism_api.operations.database_snapshot_transition
It touches only Chessism backups. Live archiving must already be disabled.
"""
import asyncio
import argparse
from contextlib import suppress
import os
import time
import uuid

from sqlalchemy import text

from . import database_backups as b
from .database_backup_job import run_database_backup_job
from .database_restore_tests import run_database_restore_test_job, database_restore_test_status


async def main(*, resume_backup_id=None):
    from chessism_api.database.runtime import bind_runtime_engine
    from chessism_api.redis_client import get_redis_pool, close_redis_pool
    path = b.APP_BACKUP_ROOT / "snapshot-transition.json"
    previous = b._read_json(path, {})
    if previous.get("state") in {"running", "succeeded"}:
        raise RuntimeError("Transition already started; inspect its status before requesting another")
    b.require_storage(include_app_usage=True)
    engine = await bind_runtime_engine(os.environ["DATABASE_URL"])
    started = time.monotonic()
    status = {"state": "running", "stage": "preflight", "started_at": b._iso_now()}
    heartbeat = None
    def publish():
        progress = (database_restore_test_status() if status["stage"] == "restore_test"
                    else b._read_json(b.BACKUP_STATUS_PATH, {}))
        status.update(elapsed_seconds=round(time.monotonic()-started, 1), updated_at=b._iso_now(),
                      phase=progress.get("phase"), detail=progress.get("detail"),
                      compressed_bytes=progress.get("compressed_bytes_written") or status.get("compressed_bytes"))
        if status["state"] == "succeeded":
            status.update(phase="complete", detail="Compressed snapshot restored successfully; obsolete Chessism backup data removed.")
        b._atomic_write_json(path, status)
    async def monitor():
        while True:
            publish()
            await asyncio.sleep(5)
    try:
        async with engine.connect() as connection:
            if await connection.scalar(text("SHOW archive_mode")) != "off":
                raise RuntimeError("Disable live WAL archiving before this transition; no cleanup performed")
        redis = await get_redis_pool()
        heartbeat = asyncio.create_task(monitor())
        if resume_backup_id:
            from .database_restore_tests import latest_backup_for_restore_test
            from .database_snapshot import STORAGE_MODE
            result = latest_backup_for_restore_test()
            if result.get("backup_id") != resume_backup_id or result.get("storage_mode") != STORAGE_MODE:
                raise RuntimeError("Resume requires the exact latest compressed snapshot")
            status["reused_existing_archive"] = True
        else:
            status["stage"] = "backup"
            job_id = "simple-backup-" + uuid.uuid4().hex
            result = await run_database_backup_job({"redis": redis, "job_id": job_id})
        status.update(stage="restore_test", backup_id=result["backup_id"], compressed_bytes=result["repository_bytes"])
        publish()
        tested = await run_database_restore_test_job(
            {"redis": redis, "job_id": "simple-restore-" + uuid.uuid4().hex}, backup_id=result["backup_id"])
        cleanup = tested.get("cleanup") or {}
        if cleanup.get("state") != "complete" or cleanup.get("legacy_retained"):
            raise RuntimeError("Snapshot passed restore; retiring old backup data still requires attention")
        status.update(state="succeeded", stage="compressed_backup_verified_and_old_data_retired",
                      removed_bytes=cleanup["removed_bytes"], application_bytes=cleanup["application_bytes"],
                      compressed_bytes=result["repository_bytes"])
    except BaseException as error:
        status.update(state="failed", failed_stage=status["stage"], error_type=type(error).__name__, error=str(error))
        raise
    finally:
        if heartbeat:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        publish()
        await engine.dispose()
        await close_redis_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume-backup-id", help="Retry verification of this exact latest archive; do not create another dump")
    asyncio.run(main(resume_backup_id=parser.parse_args().resume_backup_id))
