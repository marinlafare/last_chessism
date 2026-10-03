"""Superuser API for complete PostgreSQL backup management."""

from __future__ import annotations

import uuid

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from chessism_api.operations.backup_coordination import release_backup, reserve_backup
from chessism_api.operations.database_backups import (
    BackupCapacityError,
    BackupUnavailableError,
    database_backup_overview,
    require_storage,
)
from chessism_api.operations.database_restore_tests import (
    RestoreTestUnavailableError,
    latest_backup_for_restore_test,
    mark_restore_test_queued,
    mark_restore_test_queue_failed,
)
from chessism_api.redis_client import get_redis_pool


router = APIRouter()


@router.get("/database")
async def api_database_backup_overview() -> dict:
    """Return backup storage, policy, current status, and verified history."""
    return database_backup_overview()


@router.post("/database")
async def api_create_database_backup(
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Queue one manually requested full or incremental database backup."""
    try:
        # Keep the HTTP request fast. The worker performs the exact recursive
        # quota measurement immediately before it starts writing.
        require_storage(include_app_usage=False)
    except BackupUnavailableError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except BackupCapacityError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error

    job_id = str(uuid.uuid4())
    reserved, owner = await reserve_backup(redis, job_id)
    if not reserved:
        raise HTTPException(
            status_code=409,
            detail=f"Backup storage is already being used by job {owner}.",
        )

    try:
        job = await redis.enqueue_job(
            "run_database_backup_job",
            _job_id=job_id,
            _queue_name="backup_queue",
        )
        if job is None:
            raise RuntimeError("The backup job could not be queued.")
    except Exception:
        await release_backup(redis, job_id)
        raise

    return JSONResponse(
        status_code=202,
        content={
            "message": "Database backup queued.",
            "job_id": job_id,
            "storage_location": database_backup_overview()["storage"]["database_location"],
        },
    )


@router.post("/database/test")
async def api_test_latest_database_backup(
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Queue an explicit restore rehearsal for the latest recovery point."""
    try:
        backup = latest_backup_for_restore_test()
    except RestoreTestUnavailableError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error

    backup_id = str(backup["backup_id"])
    job_id = str(uuid.uuid4())
    reserved, owner = await reserve_backup(redis, job_id)
    if not reserved:
        raise HTTPException(
            status_code=409,
            detail=f"Backup storage is already being used by job {owner}.",
        )

    mark_restore_test_queued(job_id=job_id, backup=backup)
    try:
        job = await redis.enqueue_job(
            "run_database_restore_test_job",
            backup_id=backup_id,
            _job_id=job_id,
            _queue_name="backup_queue",
        )
        if job is None:
            raise RuntimeError("The restore-test job could not be queued.")
    except Exception as error:
        mark_restore_test_queue_failed(
            job_id=job_id,
            backup_id=backup_id,
            error=error,
        )
        await release_backup(redis, job_id)
        raise

    return JSONResponse(
        status_code=202,
        content={
            "message": f"Restore test queued for {backup_id}.",
            "job_id": job_id,
            "backup_id": backup_id,
        },
    )
