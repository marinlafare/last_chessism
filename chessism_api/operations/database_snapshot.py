"""Standalone compressed database snapshots; never depend on archived WAL.

The caller owns the backup lane and a repeatable-read matrix catalog lock.
Old recovery points remain intact until the new snapshot passes a real restore.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
import os
from pathlib import Path
import re
import shutil
import time
import uuid

from sqlalchemy import text

STORAGE_MODE = "compressed_snapshot_v1"
FORMAT_VERSION = 3
SAFE_ID = re.compile(r"^snapshot-[0-9]{8}T[0-9]{6}-[a-f0-9]{12}$")
TIMEOUT = 6 * 60 * 60


def snapshot_path(backup_id: str, *, partial: bool = False) -> Path:
    from . import database_backups as b
    if not SAFE_ID.fullmatch(backup_id):
        raise ValueError("Invalid standalone snapshot ID")
    root = b.DATABASE_BACKUP_ROOT / "snapshots"
    if (b.APP_BACKUP_ROOT.is_symlink() or b.DATABASE_BACKUP_ROOT.is_symlink()
            or root.is_symlink() or root.resolve().parent != b.DATABASE_BACKUP_ROOT.resolve()):
        raise ValueError("Snapshot directory must be directly inside the database backup directory")
    path = root / f"{backup_id}.dump{'.partial' if partial else ''}"
    if path.is_symlink():
        raise ValueError("Refusing a symlink snapshot")
    return path


def pg_environment(url) -> dict[str, str]:
    """Credentials stay in the child's environment, never argv or progress."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env.update(PGHOST=url.host or "/var/run/postgresql", PGPORT=str(url.port or 5432),
               PGUSER=url.username or "chessism_user", PGDATABASE=url.database or "chessism_db",
               PGPASSWORD=url.password or "", PGAPPNAME="chessism-compressed-backup",
               PGCONNECT_TIMEOUT="15")
    return env


async def run_progress_command(*args, progress, env=None, capacity_check=None):
    """Bounded stderr, periodic progress, cancellation kills/reaps the child."""
    process = await asyncio.create_subprocess_exec(
        *map(str, args), env=env, stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    lines = deque(maxlen=12)

    async def read_lines():
        async for line in process.stderr:
            lines.append(line.decode("utf-8", errors="replace").strip()[:1000])

    reader = asyncio.create_task(read_lines())
    waiter = asyncio.create_task(process.wait())
    started = time.monotonic()
    try:
        while not waiter.done():
            if time.monotonic() - started > TIMEOUT:
                raise TimeoutError("Database snapshot operation exceeded six hours")
            if capacity_check:
                capacity_check()
            await progress(lines[-1] if lines else "Starting PostgreSQL operation")
            await asyncio.wait({waiter}, timeout=5)
        await reader
        if process.returncode:
            raise RuntimeError(f"{Path(str(args[0])).name} failed: " + "\n".join(lines))
        await progress(lines[-1] if lines else "PostgreSQL operation completed")
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.terminate()
            try:
                await asyncio.wait_for(asyncio.shield(waiter), 10)
            except asyncio.TimeoutError:
                with suppress(ProcessLookupError):
                    process.kill()
                await waiter
        reader.cancel()
        with suppress(asyncio.CancelledError):
            await reader


async def snapshot_metadata(session, publish):
    from .matrix_constructor.definition_backups import definition_backup_manifest
    from .research_algorithms.backups import algorithm_backup_manifest
    from .matrix_backups import backup_completed_matrices
    from chessism_api.database.fen_identity import SHAPE_SQL, schema_version
    from chessism_api.database.relational_storage import storage_for, describe_contract
    from chessism_api.database.fen_reference_identity import (
        SHAPE_SQL as REF_SQL, schema_version as reference_version,
    )
    layout = storage_for(session)
    search_path = await session.scalar(text("SHOW search_path"))
    await session.execute(text("SELECT set_config('search_path','public',true)"))
    identity = schema_version((await session.execute(text(SHAPE_SQL))).mappings().all())
    await session.execute(text("SELECT set_config('search_path',:value,true)"), {"value": search_path})
    if identity != "numeric_v2":
        raise RuntimeError("Refusing a snapshot of a partially migrated FEN schema")
    contract = await describe_contract(session, layout) if layout else None
    references = layout.version if layout else reference_version(
        (await session.execute(text(REF_SQL))).mappings().all())
    if references not in {"relational_v2", "relational_v3", "numeric_v1"}:
        raise RuntimeError("Refusing a snapshot of partially migrated FEN references")
    # pg_dump is database-scoped, not a cluster/credentials backup. Refuse to
    # silently lose extra roles or external tablespaces; add support explicitly
    # if Chessism's single-owner layout ever changes.
    roles = list((await session.execute(text(
        "SELECT rolname FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY rolname"
    ))).scalars())
    owner = await session.scalar(text("SELECT current_user"))
    if roles != [owner] or await session.scalar(text(
        "SELECT count(*) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')"
    )):
        raise RuntimeError("Snapshot requires the supported single-owner/default-tablespace layout")
    locale = (await session.execute(text("""SELECT datname AS name, pg_encoding_to_char(encoding) AS encoding,
        datcollate AS collate, datctype AS ctype FROM pg_database WHERE datname=current_database()"""))).mappings().one()
    metadata = {
        "schema_version": 8 if contract else 6, "fen_identity_version": identity, "fen_reference_version": references,
        "matrix_definitions": await definition_backup_manifest(session),
        "algorithms": await algorithm_backup_manifest(session),
        "matrices": await backup_completed_matrices(session, publish_progress=publish),
        "database": {**dict(locale), "owner": owner},
        "database_bytes": int(await session.scalar(text("SELECT pg_database_size(current_database())"))),
    }
    if contract:
        metadata["relational_storage"] = contract
    return metadata


def _fsync_publish(partial: Path, target: Path):
    if target.exists():
        raise RuntimeError("Refusing to overwrite an existing database snapshot")
    with partial.open("rb") as stream:
        os.fsync(stream.fileno())
    os.rename(partial, target)
    fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def run_snapshot_backup(ctx, session):
    from . import database_backups as b
    backup_id = "snapshot-" + b.utc_now().strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:12]
    target, partial = snapshot_path(backup_id), snapshot_path(backup_id, partial=True)
    started = b._iso_now()
    status = {**b._default_status(b.storage_snapshot()), "status": "running", "phase": "preparing",
              "started_at": started, "backup_id": backup_id, "storage_mode": STORAGE_MODE,
              "backup_type": "full", "progress_total_bytes": 0, "progress_completed_bytes": 0}

    async def publish(*, phase, detail, **_ignored):
        status.update(phase=phase, detail=detail, progress_updated_at=b._iso_now())
        b._atomic_write_json(b.BACKUP_STATUS_PATH, status)
        await b._write_progress(ctx.get("redis"), ctx["job_id"], phase=phase, detail=detail)

    try:
        storage = b.require_storage(include_app_usage=True)
        baseline_bytes = int(storage["application_bytes"])
        catalog = b._read_json(b.BACKUP_CATALOG_PATH, {"backups": []})
        known = catalog.get("backups", [])
        cleanup = b._read_json(b.DATABASE_BACKUP_ROOT / "snapshot-cleanup.json", {})
        if cleanup.get("state") in {"running", "failed"}:
            raise RuntimeError("Retest the latest snapshot to finish interrupted backup cleanup before creating another")
        if any(row.get("storage_mode") == STORAGE_MODE and not row.get("restore_tested") for row in known):
            raise RuntimeError("Test the existing compressed snapshot before creating another one; the previous verified backup is protected")
        target.parent.mkdir(parents=True, exist_ok=True)
        await publish(phase="preparing", detail="Preparing one consistent, compressed PostgreSQL snapshot.")
        snapshot = await session.scalar(text("SELECT pg_export_snapshot()"))
        metadata = await snapshot_metadata(session, publish)
        baseline_bytes += metadata["matrices"]["bytes_added"]

        def capacity():
            size = partial.stat().st_size if partial.exists() else 0
            if shutil.disk_usage(b.BACKUP_VOLUME_ROOT).free <= b.FREE_FLOOR_BYTES + b.STOP_MARGIN_BYTES:
                raise b.BackupCapacityError("Snapshot reached the protected disk free-space floor")
            if baseline_bytes + size >= b.APP_QUOTA_BYTES:
                raise b.BackupCapacityError("Snapshot reached the Chessism backup allocation")

        async def dump_progress(line):
            size = partial.stat().st_size if partial.exists() else 0
            status["compressed_bytes_written"] = size
            await publish(phase="backing_up", detail=f"Compressed snapshot: {size / 1e9:.2f} GB written. {line}")

        await run_progress_command(
            "pg_dump", "--format=custom", "--compress=6", "--create", "--verbose", "--no-password",
            "--lock-wait-timeout=60s", f"--snapshot={snapshot}", f"--file={partial}",
            env=pg_environment(session.bind.url), progress=dump_progress, capacity_check=capacity,
        )
        await publish(phase="verifying", detail="Checking archive structure and recording its SHA-256 checksum.")
        await b._run_command_capture("pg_restore", "--list", str(partial))
        digest = await asyncio.to_thread(b._sha256, partial)
        _fsync_publish(partial, target)
        completed = b._iso_now()
        size = target.stat().st_size
        row = {"backup_id": backup_id, "type": "full", "prior": None, "started_at": started,
               "completed_at": completed, "verified_at": completed, "storage_mode": STORAGE_MODE,
               "backup_format_version": FORMAT_VERSION, "database_bytes": metadata["database_bytes"],
               "database_delta_bytes": metadata["database_bytes"], "repository_bytes": size,
               "repository_delta_bytes": size, "restore_tested": False, "archive_sha256": digest}
        manifest = {**metadata, **row, "format": "chessism-pg-dump-custom", "app_id": b.APP_ID,
                    "trigger": "manual", "destination_uuid": b.EXPECTED_VOLUME_UUID,
                    "archive_file": target.name, "compression": "gzip-6",
                    "role_passwords_included": False}
        manifest_path = b.MANIFEST_DIRECTORY / f"{backup_id}.json"
        b._atomic_write_json(manifest_path, manifest)
        b._write_catalog([row, *known], updated_at=completed)
        usage = await asyncio.to_thread(b.record_application_usage)
        result = {**row, "manifest": str(manifest_path), "manifest_sha256": b._sha256(manifest_path),
                  "application_bytes": usage, "matrices": metadata["matrices"],
                  "matrix_definitions": metadata["matrix_definitions"], "removed_backup_ids": []}
        status.update(status="success", completed_at=completed, total_bytes=usage,
                      bytes_added=size, artifact_count=1 + metadata["matrices"]["count"],
                      manifest_sha256=result["manifest_sha256"], last_verified_at=completed)
        await publish(phase="complete", detail="Compressed snapshot saved. Test backup before retiring the previous recovery point.")
        await b._write_progress(ctx.get("redis"), ctx["job_id"], phase="complete", detail=status["detail"], result=result)
        return result
    except BaseException as error:
        # The only disposable file here is THIS operation's unpublished output.
        partial.unlink(missing_ok=True)
        if b.storage_snapshot().get("marker_matches"):
            status.update(status="failed", completed_at=b._iso_now(), error_code=type(error).__name__, error_message=str(error))
            await publish(phase="failed", detail=str(error))
        raise
