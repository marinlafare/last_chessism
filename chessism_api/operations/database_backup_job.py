"""Manual database backups, including recipe fingerprints and legacy matrices."""

import asyncio
import uuid
from typing import Any

from chessism_api.operations import database_backups as backup
from chessism_api.operations.backup_coordination import ensure_backup_reservation, release_backup
from chessism_api.operations.matrix_backups import backup_completed_matrices
from chessism_api.operations.matrix_constructor.storage import matrix_catalog_lock
from chessism_api.operations.matrix_constructor.definition_backups import definition_backup_manifest
from chessism_api.operations.research_algorithms.backups import algorithm_backup_manifest


async def run_database_backup_job(ctx: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
    # Hold through both the file copy and pgBackRest's recovery point. Only
    # recipe changes/legacy publication wait; Stockfish/ingestion continue.
    ctx = {**ctx, "job_id": str(ctx.get("job_id") or uuid.uuid4())}
    await ensure_backup_reservation(ctx.get("redis"), ctx["job_id"])
    try:
        async with matrix_catalog_lock() as session:
            return await _run_database_backup_job(ctx, session)
    finally:
        await release_backup(ctx.get("redis"), ctx["job_id"])


async def _run_database_backup_job(ctx: dict[str, Any], session) -> dict[str, Any]:
    """Create a full or incremental pgBackRest backup after explicit UI request."""
    job_id = str(ctx.get("job_id") or uuid.uuid4())
    redis = ctx.get("redis")
    started_at = backup._iso_now()

    try:
        storage = backup.require_storage(include_app_usage=True)
        baseline_app_bytes = int(storage.get("application_bytes") or 0)
        backup.DATABASE_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        backup.MANIFEST_DIRECTORY.mkdir(parents=True, exist_ok=True)

        running_status = {
            **backup._default_status(storage),
            "status": "running",
            "phase": "preparing",
            "trigger": "manual",
            "started_at": started_at,
            "progress_total_bytes": 0,
            "progress_completed_bytes": 0,
            "progress_eta_seconds": None,
            "progress_rate_bytes_per_second": None,
            "detail": "Inspecting the pgBackRest repository.",
            "error_code": None,
            "error_message": None,
        }

        async def publish_progress(
            *,
            phase: str,
            detail: str,
            total: int | None = None,
            processed: int | None = None,
            eta_seconds: float | None = None,
            rate_bytes_per_second: float | None = None,
        ) -> None:
            if total is not None:
                running_status["progress_total_bytes"] = max(0, int(total))
            if processed is not None:
                running_status["progress_completed_bytes"] = max(0, int(processed))
            running_status.update({
                "phase": phase,
                "detail": detail,
                "progress_eta_seconds": eta_seconds,
                "progress_rate_bytes_per_second": rate_bytes_per_second,
                "progress_updated_at": backup._iso_now(),
            })
            backup._atomic_write_json(backup.BACKUP_STATUS_PATH, running_status)
            await backup._write_progress(
                redis,
                job_id,
                phase=phase,
                detail=detail,
                total=int(running_status.get("progress_total_bytes") or 0),
                processed=int(running_status.get("progress_completed_bytes") or 0),
                eta_seconds=eta_seconds,
                rate_bytes_per_second=rate_bytes_per_second,
            )

        await publish_progress(phase="preparing", detail=running_status["detail"])

        definitions = await definition_backup_manifest(session)
        algorithms = await algorithm_backup_manifest(session)
        matrices = await backup_completed_matrices(session, publish_progress=publish_progress)
        info = await backup._pgbackrest_info(tolerate_missing=True)
        stanza_ready = bool(
            info
            and isinstance(info[0], dict)
            and int((info[0].get("status") or {}).get("code") or 0) == 0
        )
        if not stanza_ready:
            await publish_progress(
                phase="initializing",
                detail="Creating the Chessism backup stanza.",
            )
            await backup._run_command_capture(
                "pgbackrest",
                f"--stanza={backup.PGBACKREST_STANZA}",
                "stanza-create",
            )
            info = await backup._pgbackrest_info()

        existing_catalog = backup._read_json(backup.BACKUP_CATALOG_PATH, {"backups": []})
        known_backups = (
            existing_catalog.get("backups", [])
            if isinstance(existing_catalog, dict)
            else []
        )
        if not isinstance(known_backups, list):
            known_backups = []
        repository_backups = backup._backup_rows(info)
        previous_backup_ids = {
            str(row.get("label"))
            for row in repository_backups
            if row.get("label")
        }
        active_full_storage_mode = backup._active_full_storage_mode(
            repository_backups,
            known_backups,
        )
        backup_type, selection_reason = backup.choose_backup_type(
            repository_backups,
            archive_gap=backup.PGBACKREST_GAP_PATH.exists(),
            active_full_storage_mode=active_full_storage_mode,
        )
        migration_full = bool(repository_backups) and (
            active_full_storage_mode != backup.BACKUP_STORAGE_MODE
        ) and backup_type == "full"
        await publish_progress(
            phase="checking",
            detail="Checking PostgreSQL and WAL archiving.",
        )
        await backup._run_command_capture(
            "pgbackrest",
            f"--stanza={backup.PGBACKREST_STANZA}",
            "check",
        )

        running_status["backup_type"] = backup_type
        await publish_progress(phase="backing_up", detail=selection_reason)
        await backup._run_backup_command(
            backup_type,
            baseline_app_bytes=baseline_app_bytes + matrices["bytes_added"],
            publish_progress=publish_progress,
        )

        verified_info = await backup._pgbackrest_info()
        catalog_rows = backup._catalog_rows(verified_info, known_backups=known_backups)
        if not catalog_rows:
            raise RuntimeError("pgBackRest completed without publishing a backup record.")
        newest = catalog_rows[0]
        if str(newest.get("backup_id")) in previous_backup_ids:
            raise RuntimeError("pgBackRest did not publish a new backup record.")
        if newest.get("type") != backup_type:
            raise RuntimeError("The completed pgBackRest backup type did not match the request.")
        newest["storage_mode"] = backup.BACKUP_STORAGE_MODE
        newest["backup_format_version"] = backup.BACKUP_FORMAT_VERSION

        await publish_progress(
            phase="verifying",
            detail=f"Verifying backup {newest['backup_id']} and its required WAL.",
        )
        await backup._run_command_capture(
            "pgbackrest",
            f"--stanza={backup.PGBACKREST_STANZA}",
            f"--set={newest['backup_id']}",
            "verify",
        )

        completed_at = backup._iso_now()
        manifest = {
            "schema_version": 5,
            "algorithms": algorithms,
            "matrix_definitions": definitions,
            "matrices": matrices,
            "format": "chessism-pgbackrest",
            "backup_format_version": backup.BACKUP_FORMAT_VERSION,
            "storage_mode": backup.BACKUP_STORAGE_MODE,
            "app_id": backup.APP_ID,
            "database_engine": "postgresql",
            "backup_id": newest["backup_id"],
            "backup_type": newest["type"],
            "prior": newest.get("prior"),
            "trigger": "manual",
            "started_at": started_at,
            "completed_at": completed_at,
            "verified_at": completed_at,
            "selection_reason": selection_reason,
            "database_bytes": newest["database_bytes"],
            "database_delta_bytes": newest["database_delta_bytes"],
            "repository_bytes": newest["repository_bytes"],
            "repository_delta_bytes": newest["repository_delta_bytes"],
            "destination_uuid": backup.EXPECTED_VOLUME_UUID,
            "restore_tested": False,
        }
        manifest_path = backup.MANIFEST_DIRECTORY / f"{newest['backup_id']}.json"
        backup._atomic_write_json(manifest_path, manifest)
        manifest_sha256 = backup._sha256(manifest_path)
        backup._write_catalog(catalog_rows, updated_at=completed_at)

        removed_backup_ids: list[str] = []
        if migration_full:
            catalog_rows, removed_backup_ids = await backup._expire_legacy_backup_chains(
                catalog_rows,
                protected_backup_id=str(newest["backup_id"]),
                redis=redis,
                job_id=job_id,
            )
        current_app_bytes = backup.record_application_usage()

        if backup_type == "full":
            backup.PGBACKREST_GAP_PATH.unlink(missing_ok=True)

        result = {
            **newest,
            "manifest": str(manifest_path),
            "manifest_sha256": manifest_sha256,
            "application_bytes": current_app_bytes,
            "storage_mode": backup.BACKUP_STORAGE_MODE,
            "backup_format_version": backup.BACKUP_FORMAT_VERSION,
            "removed_backup_ids": removed_backup_ids,
            "matrices": matrices,
            "matrix_definitions": definitions,
        }
        success_status = {
            **running_status,
            "status": "success",
            "phase": "complete",
            "backup_id": newest["backup_id"],
            "completed_at": completed_at,
            "last_verified_at": completed_at,
            "artifact_count": 1 + matrices["count"],
            "matrix_count": matrices["count"],
            "matrix_definition_count": definitions["count"],
            "total_bytes": current_app_bytes,
            "bytes_added": max(0, current_app_bytes - baseline_app_bytes),
            "manifest_sha256": manifest_sha256,
            "storage_mode": backup.BACKUP_STORAGE_MODE,
            "backup_format_version": backup.BACKUP_FORMAT_VERSION,
            "detail": (
                f"Verified {backup_type} database backup {newest['backup_id']}; "
                f"{definitions['count']} matrix definitions protected in PostgreSQL; "
                f"{matrices['count']} legacy matrix snapshots saved "
                f"({matrices['copied_count']} new, {matrices['reused_count']} unchanged)"
                + (
                    f" and removed {len(removed_backup_ids)} superseded legacy backups."
                    if removed_backup_ids
                    else "."
                )
            ),
        }
        backup._atomic_write_json(backup.BACKUP_STATUS_PATH, success_status)
        await backup._write_progress(redis, job_id, phase="complete", detail=success_status["detail"], result=result)
        return result
    except (Exception, asyncio.CancelledError) as error:
        storage = backup.storage_snapshot(include_app_usage=False)
        failure_status = {
            **backup._default_status(storage),
            "status": "unavailable" if isinstance(error, backup.BackupUnavailableError) else "failed",
            "phase": "unavailable" if isinstance(error, backup.BackupUnavailableError) else "failed",
            "trigger": "manual",
            "started_at": started_at,
            "completed_at": backup._iso_now(),
            "error_code": error.__class__.__name__,
            "error_message": str(error),
            "detail": str(error),
        }
        if storage.get("marker_matches"):
            backup._atomic_write_json(backup.BACKUP_STATUS_PATH, failure_status)
        await backup._write_progress(
            redis,
            job_id,
            phase="unavailable" if isinstance(error, backup.BackupUnavailableError) else "failed",
            detail=str(error),
        )
        raise
