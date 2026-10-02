"""On-demand, incremental PostgreSQL backups stored outside the repository."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chessism_api.operations.backup_coordination import (
    ensure_backup_reservation,
    release_backup,
)
from chessism_api.operations.database_restore_tests import database_restore_test_status


APP_ID = os.getenv("DATABASE_BACKUP_APP_ID", "chessism")
BACKUP_VOLUME_ROOT = Path(os.getenv("BACKUP_VOLUME_ROOT", "/backup-volume"))
EXPECTED_VOLUME_UUID = os.getenv(
    "BACKUP_VOLUME_UUID",
    "1ebffac6-4b7a-4906-9e07-9586060d3825",
)
VOLUME_MARKER = BACKUP_VOLUME_ROOT / ".chessism-backup-volume"
APP_BACKUP_ROOT = BACKUP_VOLUME_ROOT / APP_ID
DATABASE_BACKUP_ROOT = APP_BACKUP_ROOT / "database"
PGBACKREST_REPOSITORY = DATABASE_BACKUP_ROOT / "pgbackrest"
MANIFEST_DIRECTORY = DATABASE_BACKUP_ROOT / "manifests"
BACKUP_STATUS_PATH = APP_BACKUP_ROOT / "backup-status.json"
BACKUP_USAGE_PATH = APP_BACKUP_ROOT / "backup-usage.json"
BACKUP_CATALOG_PATH = DATABASE_BACKUP_ROOT / "catalog.json"
PGBACKREST_GAP_PATH = Path(os.getenv(
    "CHESSISM_ARCHIVE_GAP_PATH",
    "/var/spool/pgbackrest/chessism-archive-gap",
))
PGBACKREST_STANZA = os.getenv("PGBACKREST_STANZA", "chessism")
DISPLAY_ROOT = os.getenv(
    "DATABASE_BACKUP_DISPLAY_DIR",
    "/main-monitor-db-backups/chessism/database",
)

FREE_FLOOR_BYTES = int(os.getenv("BACKUP_FREE_FLOOR_BYTES", "150000000000"))
APP_QUOTA_BYTES = int(os.getenv("DATABASE_BACKUP_QUOTA_BYTES", "200000000000"))
STOP_MARGIN_BYTES = int(os.getenv("BACKUP_STOP_MARGIN_BYTES", "2000000000"))
FULL_AFTER_INCREMENTALS = int(os.getenv("DATABASE_BACKUP_FULL_AFTER_INCREMENTALS", "30"))
FULL_AFTER_DAYS = int(os.getenv("DATABASE_BACKUP_FULL_AFTER_DAYS", "30"))
PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
BACKUP_FORMAT_VERSION = 2
BACKUP_STORAGE_MODE = "block_incremental_v1"
LEGACY_STORAGE_MODE = "file_incremental_legacy"

TERMINAL_PHASES = {"complete", "failed", "unavailable"}


class BackupUnavailableError(RuntimeError):
    """Raised when the dedicated external repository cannot be used safely."""


class BackupCapacityError(RuntimeError):
    """Raised before or during a backup that would violate storage limits."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return utc_now().isoformat()


def _read_json(path: Path, default: Any) -> Any:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _directories, filenames in os.walk(path):
        for filename in filenames:
            try:
                total += (Path(root) / filename).stat().st_size
            except FileNotFoundError:
                continue
    return total


def record_application_usage() -> int:
    """Persist an exact quota measurement after a backup writer completes."""
    total_bytes = _directory_size(APP_BACKUP_ROOT)
    _atomic_write_json(BACKUP_USAGE_PATH, {
        "schema_version": 1,
        "updated_at": _iso_now(),
        "total_bytes": total_bytes,
    })
    return total_bytes


def _volume_marker_value() -> str | None:
    try:
        return VOLUME_MARKER.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def storage_snapshot(*, include_app_usage: bool = False) -> dict[str, Any]:
    """Return safe storage information without creating any paths."""
    marker = _volume_marker_value()
    marker_matches = marker == EXPECTED_VOLUME_UUID
    snapshot: dict[str, Any] = {
        "available": False,
        "expected_uuid": EXPECTED_VOLUME_UUID,
        "marker_matches": marker_matches,
        "storage_location": f"/main-monitor-db-backups/{APP_ID}",
        "database_location": DISPLAY_ROOT,
        "free_floor_bytes": FREE_FLOOR_BYTES,
        "quota_bytes": APP_QUOTA_BYTES,
        "total_bytes": None,
        "used_bytes": None,
        "free_bytes": None,
        "application_bytes": None,
        "error": None,
    }
    if not marker_matches:
        snapshot["error"] = "The dedicated backup volume is not mounted or its UUID marker is invalid."
        return snapshot

    try:
        usage = shutil.disk_usage(BACKUP_VOLUME_ROOT)
    except OSError as error:
        snapshot["error"] = f"Unable to read backup-volume capacity: {error}"
        return snapshot

    snapshot.update({
        "available": os.access(BACKUP_VOLUME_ROOT, os.R_OK),
        "writable": os.access(APP_BACKUP_ROOT, os.W_OK),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    })
    if include_app_usage:
        snapshot["application_bytes"] = _directory_size(APP_BACKUP_ROOT)
    if not snapshot["writable"]:
        snapshot["available"] = False
        snapshot["error"] = "The Chessism backup directory is not writable."
    elif usage.free <= FREE_FLOOR_BYTES:
        snapshot["error"] = "The backup volume has reached the protected free-space floor."
    return snapshot


def require_storage(*, include_app_usage: bool = True) -> dict[str, Any]:
    snapshot = storage_snapshot(include_app_usage=include_app_usage)
    if not snapshot["available"]:
        raise BackupUnavailableError(str(snapshot.get("error") or "Backup storage is unavailable."))
    if int(snapshot.get("free_bytes") or 0) <= FREE_FLOOR_BYTES + STOP_MARGIN_BYTES:
        raise BackupCapacityError(
            "The backup would enter the protected 150 GB free-space reserve."
        )
    if int(snapshot.get("application_bytes") or 0) >= APP_QUOTA_BYTES:
        raise BackupCapacityError("Chessism has reached its 200 GB backup allocation.")
    return snapshot


def _default_status(storage: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "app_id": APP_ID,
        "database_engine": "postgresql",
        "status": "unavailable" if not storage.get("available") else "stale",
        "trigger": None,
        "backup_id": None,
        "backup_type": None,
        "started_at": None,
        "completed_at": None,
        "last_verified_at": None,
        "restore_tested": False,
        "artifact_count": 0,
        "total_bytes": 0,
        "bytes_added": 0,
        "destination_uuid": EXPECTED_VOLUME_UUID,
        "relative_path": f"main-monitor-db-backups/{APP_ID}/database",
        "manifest_sha256": None,
        "error_code": "volume_unavailable" if not storage.get("available") else None,
        "error_message": storage.get("error") if not storage.get("available") else None,
        "detail": "Backup storage unavailable." if not storage.get("available") else "No database backup has run yet.",
    }


def database_backup_overview() -> dict[str, Any]:
    # Do not walk a repository that may grow to hundreds of gigabytes merely
    # because the UI is polling. Exact usage is measured by the backup worker
    # before and after each write and persisted in backup-status.json.
    storage = storage_snapshot(include_app_usage=False)
    saved_status = _read_json(BACKUP_STATUS_PATH, {}) if storage["marker_matches"] else {}
    status = saved_status if isinstance(saved_status, dict) and saved_status else _default_status(storage)
    if not saved_status and storage.get("available"):
        _atomic_write_json(BACKUP_STATUS_PATH, status)
    saved_usage = _read_json(BACKUP_USAGE_PATH, {}) if storage["marker_matches"] else {}
    storage["application_bytes"] = int(
        saved_usage.get("total_bytes")
        if isinstance(saved_usage, dict) and saved_usage.get("total_bytes") is not None
        else status.get("total_bytes") or 0
    )
    if not storage["available"]:
        status = {
            **status,
            "status": "unavailable",
            "error_code": "volume_unavailable",
            "error_message": storage.get("error"),
            "detail": storage.get("error"),
        }
    catalog = _read_json(BACKUP_CATALOG_PATH, {"backups": []}) if storage["marker_matches"] else {"backups": []}
    backups = catalog.get("backups", []) if isinstance(catalog, dict) else []
    backups = backups if isinstance(backups, list) else []
    active_storage_mode = _latest_catalog_full_storage_mode(backups)
    migration_required = bool(backups) and active_storage_mode != BACKUP_STORAGE_MODE
    return {
        "storage": storage,
        "status": status,
        "restore_test": database_restore_test_status(),
        "policy": {
            "manual_only": True,
            "full_after_incrementals": FULL_AFTER_INCREMENTALS,
            "full_after_days": FULL_AFTER_DAYS,
            "retained_full_chains": 2,
            "storage_mode": BACKUP_STORAGE_MODE,
            "active_storage_mode": active_storage_mode or (
                LEGACY_STORAGE_MODE if backups else None
            ),
            "migration_required": migration_required,
        },
        "backups": backups,
    }


async def _write_progress(
    redis: Any,
    job_id: str,
    *,
    phase: str,
    detail: str,
    result: dict[str, Any] | None = None,
) -> None:
    if redis is None:
        return
    payload = {
        "job_id": job_id,
        "kind": "database_backup",
        "phase": phase,
        "total": 0,
        "processed": 0,
        "detail": detail,
        "result": result,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )


def _backup_rows(info: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not info or not isinstance(info[0], dict):
        return []
    rows = info[0].get("backup") or []
    return [row for row in rows if isinstance(row, dict)]


def _latest_catalog_full_storage_mode(backups: list[dict[str, Any]]) -> str | None:
    """Return the storage mode attached to the newest cataloged full backup."""
    full_rows = [row for row in backups if str(row.get("type")) == "full"]
    if not full_rows:
        return None
    latest_full = max(
        full_rows,
        key=lambda row: str(row.get("completed_at") or ""),
    )
    mode = latest_full.get("storage_mode")
    return str(mode) if mode else None


def _active_full_storage_mode(
    repository_backups: list[dict[str, Any]],
    catalog_backups: list[dict[str, Any]],
) -> str | None:
    """Match pgBackRest's newest full to our independently verified catalog."""
    full_rows = [row for row in repository_backups if str(row.get("type")) == "full"]
    if not full_rows:
        return None
    latest_full = max(
        full_rows,
        key=lambda row: int((row.get("timestamp") or {}).get("stop") or 0),
    )
    label = str(latest_full.get("label") or "")
    for row in catalog_backups:
        if str(row.get("backup_id") or "") == label:
            mode = row.get("storage_mode")
            return str(mode) if mode else None
    return None


def _legacy_full_backup_ids(
    catalog_backups: list[dict[str, Any]],
    *,
    protected_backup_id: str,
) -> list[str]:
    """Return exact legacy full labels that may be expired after verification."""
    return [
        str(row["backup_id"])
        for row in catalog_backups
        if str(row.get("type")) == "full"
        and row.get("backup_id")
        and str(row.get("backup_id")) != protected_backup_id
        and row.get("storage_mode") != BACKUP_STORAGE_MODE
    ]


def choose_backup_type(
    backups: list[dict[str, Any]],
    *,
    archive_gap: bool,
    active_full_storage_mode: str | None = BACKUP_STORAGE_MODE,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Select a safe backup type using the agreed 30/30 chain policy."""
    if archive_gap:
        return "full", "WAL archiving was interrupted, so a new full chain is required."
    if not backups:
        return "full", "No verified base backup exists yet."

    full_rows = [row for row in backups if str(row.get("type")) == "full"]
    if not full_rows:
        return "full", "The repository has no full base backup."
    if active_full_storage_mode != BACKUP_STORAGE_MODE:
        return (
            "full",
            "Block-incremental storage is enabled, so one new full base is required; "
            "the existing chain remains protected until the new full is verified.",
        )
    latest_full = max(
        full_rows,
        key=lambda row: int((row.get("timestamp") or {}).get("stop") or 0),
    )
    full_stop = int((latest_full.get("timestamp") or {}).get("stop") or 0)
    incrementals = sum(
        1
        for row in backups
        if str(row.get("type")) == "incr"
        and int((row.get("timestamp") or {}).get("stop") or 0) > full_stop
    )
    if incrementals >= FULL_AFTER_INCREMENTALS:
        return "full", f"The current chain already has {incrementals} incrementals."

    reference_now = now or utc_now()
    age_seconds = max(0, int(reference_now.timestamp()) - full_stop)
    if age_seconds >= FULL_AFTER_DAYS * 24 * 60 * 60:
        return "full", f"The current full base is at least {FULL_AFTER_DAYS} days old."
    return "incr", "A healthy block-enabled full base exists, so only changed blocks will be stored."


async def _run_command_capture(*arguments: str, allow_failure: bool = False) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await process.communicate()
    output = stdout.decode("utf-8", errors="replace") if stdout else ""
    if process.returncode and not allow_failure:
        sanitized = output[-4000:].strip()
        raise RuntimeError(sanitized or f"Command {arguments[0]} failed with exit code {process.returncode}.")
    return int(process.returncode or 0), output


async def _pgbackrest_info(*, tolerate_missing: bool = False) -> list[dict[str, Any]]:
    code, output = await _run_command_capture(
        "pgbackrest",
        f"--stanza={PGBACKREST_STANZA}",
        "--output=json",
        "info",
        allow_failure=tolerate_missing,
    )
    if code and tolerate_missing:
        return []
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError("pgBackRest returned invalid repository information.") from error
    return payload if isinstance(payload, list) else []


async def _run_backup_command(
    backup_type: str,
    *,
    baseline_app_bytes: int,
    redis: Any,
    job_id: str,
) -> str:
    process = await asyncio.create_subprocess_exec(
        "pgbackrest",
        f"--stanza={PGBACKREST_STANZA}",
        f"--type={backup_type}",
        "--repo1-bundle=y",
        "--repo1-block=y",
        "--log-level-console=info",
        "backup",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    recent_lines: list[str] = []
    last_storage_check = 0.0
    start_free = shutil.disk_usage(BACKUP_VOLUME_ROOT).free

    assert process.stdout is not None
    while True:
        try:
            raw_line = await asyncio.wait_for(process.stdout.readline(), timeout=2.0)
        except asyncio.TimeoutError:
            raw_line = b""
        if raw_line:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if line:
                recent_lines.append(line)
                recent_lines = recent_lines[-40:]
                await _write_progress(redis, job_id, phase="backing_up", detail=line[-500:])

        now_monotonic = time.monotonic()
        if now_monotonic - last_storage_check >= 2.0:
            current_free = shutil.disk_usage(BACKUP_VOLUME_ROOT).free
            estimated_app_bytes = baseline_app_bytes + max(0, start_free - current_free)
            if current_free <= FREE_FLOOR_BYTES + STOP_MARGIN_BYTES:
                process.terminate()
                await process.wait()
                raise BackupCapacityError(
                    "Backup stopped before entering the protected 150 GB free-space reserve."
                )
            if estimated_app_bytes >= APP_QUOTA_BYTES:
                process.terminate()
                await process.wait()
                raise BackupCapacityError("Backup stopped at Chessism's 200 GB allocation.")
            last_storage_check = now_monotonic

        if process.returncode is not None:
            break
        if not raw_line:
            try:
                await asyncio.wait_for(process.wait(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

    return_code = await process.wait()
    if return_code:
        raise RuntimeError("\n".join(recent_lines[-15:]) or f"pgBackRest failed with exit code {return_code}.")
    return "\n".join(recent_lines)


def _catalog_rows(
    info: list[dict[str, Any]],
    *,
    known_backups: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    known_by_id = {
        str(row.get("backup_id")): row
        for row in (known_backups or [])
        if row.get("backup_id")
    }
    rows = []
    for backup in reversed(_backup_rows(info)):
        timestamps = backup.get("timestamp") or {}
        repository = (backup.get("info") or {}).get("repository") or {}
        backup_id = backup.get("label")
        known = known_by_id.get(str(backup_id), {})
        row = {
            "backup_id": backup_id,
            "type": backup.get("type"),
            "prior": backup.get("prior"),
            "started_at": datetime.fromtimestamp(
                int(timestamps.get("start") or 0), timezone.utc
            ).isoformat() if timestamps.get("start") else None,
            "completed_at": datetime.fromtimestamp(
                int(timestamps.get("stop") or 0), timezone.utc
            ).isoformat() if timestamps.get("stop") else None,
            "database_bytes": int((backup.get("info") or {}).get("size") or 0),
            "database_delta_bytes": int((backup.get("info") or {}).get("delta") or 0),
            "repository_bytes": int(repository.get("size") or 0),
            "repository_delta_bytes": int(repository.get("delta") or 0),
            "restore_tested": bool(known.get("restore_tested", False)),
        }
        for field in (
            "restore_test_status",
            "restore_tested_at",
            "restore_test_elapsed_seconds",
            "restore_test_error",
        ):
            if field in known:
                row[field] = known[field]
        if known.get("storage_mode"):
            row["storage_mode"] = known["storage_mode"]
        if known.get("backup_format_version"):
            row["backup_format_version"] = known["backup_format_version"]
        rows.append(row)
    return rows


def _write_catalog(backups: list[dict[str, Any]], *, updated_at: str) -> None:
    _atomic_write_json(BACKUP_CATALOG_PATH, {
        "schema_version": 2,
        "updated_at": updated_at,
        "backup_format_version": BACKUP_FORMAT_VERSION,
        "storage_mode": BACKUP_STORAGE_MODE,
        "backups": backups,
    })


async def _expire_legacy_backup_chains(
    catalog_rows: list[dict[str, Any]],
    *,
    protected_backup_id: str,
    redis: Any = None,
    job_id: str = "legacy-backup-cleanup",
) -> tuple[list[dict[str, Any]], list[str]]:
    """Expire legacy chains only after a verified block-enabled full exists."""
    legacy_full_ids = _legacy_full_backup_ids(
        catalog_rows,
        protected_backup_id=protected_backup_id,
    )
    if not legacy_full_ids:
        return catalog_rows, []

    known_ids = {
        str(row.get("backup_id"))
        for row in catalog_rows
        if row.get("backup_id")
    }
    for backup_id in legacy_full_ids:
        await _run_command_capture(
            "pgbackrest",
            f"--stanza={PGBACKREST_STANZA}",
            f"--set={backup_id}",
            "--dry-run",
            "expire",
        )
        await _write_progress(
            redis,
            job_id,
            phase="cleaning_legacy",
            detail=f"Removing superseded legacy recovery chain {backup_id}.",
        )
        await _run_command_capture(
            "pgbackrest",
            f"--stanza={PGBACKREST_STANZA}",
            f"--set={backup_id}",
            "expire",
        )

    refreshed_info = await _pgbackrest_info()
    remaining_repository_ids = {
        str(row.get("label"))
        for row in _backup_rows(refreshed_info)
        if row.get("label")
    }
    if protected_backup_id not in remaining_repository_ids:
        raise RuntimeError("Legacy cleanup could not confirm the protected block backup.")
    legacy_still_present = set(legacy_full_ids) & remaining_repository_ids
    if legacy_still_present:
        raise RuntimeError(
            "Legacy cleanup did not remove: " + ", ".join(sorted(legacy_still_present))
        )

    refreshed_rows = _catalog_rows(refreshed_info, known_backups=catalog_rows)
    _write_catalog(refreshed_rows, updated_at=_iso_now())
    removed_ids = sorted(known_ids - remaining_repository_ids)
    for removed_id in removed_ids:
        (MANIFEST_DIRECTORY / f"{removed_id}.json").unlink(missing_ok=True)
    return refreshed_rows, removed_ids


async def cleanup_legacy_backup_chains() -> dict[str, Any]:
    """Safely finish a pending legacy cleanup after a verified migration."""
    info = await _pgbackrest_info()
    catalog = _read_json(BACKUP_CATALOG_PATH, {"backups": []})
    catalog_rows = catalog.get("backups", []) if isinstance(catalog, dict) else []
    if not isinstance(catalog_rows, list):
        catalog_rows = []
    repository_ids = {
        str(row.get("label"))
        for row in _backup_rows(info)
        if row.get("label")
    }
    block_fulls = [
        row for row in catalog_rows
        if str(row.get("type")) == "full"
        and row.get("storage_mode") == BACKUP_STORAGE_MODE
        and str(row.get("backup_id")) in repository_ids
    ]
    if not block_fulls:
        raise RuntimeError("No verified block-enabled full backup exists; cleanup refused.")
    protected = max(block_fulls, key=lambda row: str(row.get("completed_at") or ""))
    protected_backup_id = str(protected["backup_id"])
    await _run_command_capture(
        "pgbackrest",
        f"--stanza={PGBACKREST_STANZA}",
        f"--set={protected_backup_id}",
        "verify",
    )
    refreshed_rows, removed_ids = await _expire_legacy_backup_chains(
        catalog_rows,
        protected_backup_id=protected_backup_id,
    )
    total_bytes = record_application_usage()
    saved_status = _read_json(BACKUP_STATUS_PATH, {})
    if isinstance(saved_status, dict) and saved_status:
        saved_status.update({
            "total_bytes": total_bytes,
            "detail": (
                f"Verified block backup {protected_backup_id} and removed "
                f"{len(removed_ids)} superseded legacy backups."
            ),
        })
        _atomic_write_json(BACKUP_STATUS_PATH, saved_status)
    return {
        "protected_backup_id": protected_backup_id,
        "removed_backup_ids": removed_ids,
        "remaining_backup_ids": [row.get("backup_id") for row in refreshed_rows],
        "application_bytes": total_bytes,
    }


async def run_database_backup_job(ctx: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
    """Create a full or incremental pgBackRest backup after explicit UI request."""
    job_id = str(ctx.get("job_id") or uuid.uuid4())
    redis = ctx.get("redis")
    started_at = _iso_now()
    await ensure_backup_reservation(redis, job_id)

    try:
        storage = require_storage(include_app_usage=True)
        baseline_app_bytes = int(storage.get("application_bytes") or 0)
        DATABASE_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        MANIFEST_DIRECTORY.mkdir(parents=True, exist_ok=True)

        running_status = {
            **_default_status(storage),
            "status": "running",
            "trigger": "manual",
            "started_at": started_at,
            "detail": "Inspecting the pgBackRest repository.",
            "error_code": None,
            "error_message": None,
        }
        _atomic_write_json(BACKUP_STATUS_PATH, running_status)
        await _write_progress(redis, job_id, phase="preparing", detail=running_status["detail"])

        info = await _pgbackrest_info(tolerate_missing=True)
        stanza_ready = bool(
            info
            and isinstance(info[0], dict)
            and int((info[0].get("status") or {}).get("code") or 0) == 0
        )
        if not stanza_ready:
            await _write_progress(redis, job_id, phase="initializing", detail="Creating the Chessism backup stanza.")
            await _run_command_capture(
                "pgbackrest",
                f"--stanza={PGBACKREST_STANZA}",
                "stanza-create",
            )
            info = await _pgbackrest_info()

        existing_catalog = _read_json(BACKUP_CATALOG_PATH, {"backups": []})
        known_backups = (
            existing_catalog.get("backups", [])
            if isinstance(existing_catalog, dict)
            else []
        )
        if not isinstance(known_backups, list):
            known_backups = []
        repository_backups = _backup_rows(info)
        previous_backup_ids = {
            str(row.get("label"))
            for row in repository_backups
            if row.get("label")
        }
        active_full_storage_mode = _active_full_storage_mode(
            repository_backups,
            known_backups,
        )
        backup_type, selection_reason = choose_backup_type(
            repository_backups,
            archive_gap=PGBACKREST_GAP_PATH.exists(),
            active_full_storage_mode=active_full_storage_mode,
        )
        migration_full = bool(repository_backups) and (
            active_full_storage_mode != BACKUP_STORAGE_MODE
        ) and backup_type == "full"
        await _write_progress(redis, job_id, phase="checking", detail="Checking PostgreSQL and WAL archiving.")
        await _run_command_capture(
            "pgbackrest",
            f"--stanza={PGBACKREST_STANZA}",
            "check",
        )

        running_status.update({
            "backup_type": backup_type,
            "detail": selection_reason,
        })
        _atomic_write_json(BACKUP_STATUS_PATH, running_status)
        await _write_progress(redis, job_id, phase="backing_up", detail=selection_reason)
        await _run_backup_command(
            backup_type,
            baseline_app_bytes=baseline_app_bytes,
            redis=redis,
            job_id=job_id,
        )

        verified_info = await _pgbackrest_info()
        catalog_rows = _catalog_rows(verified_info, known_backups=known_backups)
        if not catalog_rows:
            raise RuntimeError("pgBackRest completed without publishing a backup record.")
        newest = catalog_rows[0]
        if str(newest.get("backup_id")) in previous_backup_ids:
            raise RuntimeError("pgBackRest did not publish a new backup record.")
        if newest.get("type") != backup_type:
            raise RuntimeError("The completed pgBackRest backup type did not match the request.")
        newest["storage_mode"] = BACKUP_STORAGE_MODE
        newest["backup_format_version"] = BACKUP_FORMAT_VERSION

        await _write_progress(
            redis,
            job_id,
            phase="verifying",
            detail=f"Verifying backup {newest['backup_id']} and its required WAL.",
        )
        await _run_command_capture(
            "pgbackrest",
            f"--stanza={PGBACKREST_STANZA}",
            f"--set={newest['backup_id']}",
            "verify",
        )

        completed_at = _iso_now()
        manifest = {
            "schema_version": 2,
            "format": "chessism-pgbackrest",
            "backup_format_version": BACKUP_FORMAT_VERSION,
            "storage_mode": BACKUP_STORAGE_MODE,
            "app_id": APP_ID,
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
            "destination_uuid": EXPECTED_VOLUME_UUID,
            "restore_tested": False,
        }
        manifest_path = MANIFEST_DIRECTORY / f"{newest['backup_id']}.json"
        _atomic_write_json(manifest_path, manifest)
        manifest_sha256 = _sha256(manifest_path)
        _write_catalog(catalog_rows, updated_at=completed_at)

        removed_backup_ids: list[str] = []
        if migration_full:
            catalog_rows, removed_backup_ids = await _expire_legacy_backup_chains(
                catalog_rows,
                protected_backup_id=str(newest["backup_id"]),
                redis=redis,
                job_id=job_id,
            )
        current_app_bytes = record_application_usage()

        if backup_type == "full":
            PGBACKREST_GAP_PATH.unlink(missing_ok=True)

        result = {
            **newest,
            "manifest": str(manifest_path),
            "manifest_sha256": manifest_sha256,
            "application_bytes": current_app_bytes,
            "storage_mode": BACKUP_STORAGE_MODE,
            "backup_format_version": BACKUP_FORMAT_VERSION,
            "removed_backup_ids": removed_backup_ids,
        }
        success_status = {
            **running_status,
            "status": "success",
            "backup_id": newest["backup_id"],
            "completed_at": completed_at,
            "last_verified_at": completed_at,
            "artifact_count": 1,
            "total_bytes": current_app_bytes,
            "bytes_added": max(0, current_app_bytes - baseline_app_bytes),
            "manifest_sha256": manifest_sha256,
            "storage_mode": BACKUP_STORAGE_MODE,
            "backup_format_version": BACKUP_FORMAT_VERSION,
            "detail": (
                f"Verified {backup_type} database backup {newest['backup_id']}"
                + (
                    f" and removed {len(removed_backup_ids)} superseded legacy backups."
                    if removed_backup_ids
                    else "."
                )
            ),
        }
        _atomic_write_json(BACKUP_STATUS_PATH, success_status)
        await _write_progress(redis, job_id, phase="complete", detail=success_status["detail"], result=result)
        return result
    except Exception as error:
        storage = storage_snapshot(include_app_usage=False)
        failure_status = {
            **_default_status(storage),
            "status": "unavailable" if isinstance(error, BackupUnavailableError) else "failed",
            "trigger": "manual",
            "started_at": started_at,
            "completed_at": _iso_now(),
            "error_code": error.__class__.__name__,
            "error_message": str(error),
            "detail": str(error),
        }
        if storage.get("marker_matches"):
            _atomic_write_json(BACKUP_STATUS_PATH, failure_status)
        await _write_progress(
            redis,
            job_id,
            phase="unavailable" if isinstance(error, BackupUnavailableError) else "failed",
            detail=str(error),
        )
        raise
    finally:
        await release_backup(redis, job_id)
