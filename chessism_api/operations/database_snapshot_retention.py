"""Retire only verified Chessism recovery data, never a shared backup volume."""
import asyncio
from pathlib import Path
import re
import shutil

from sqlalchemy import text

from .database_snapshot import STORAGE_MODE, snapshot_path

LEGACY_ID = re.compile(r"^[0-9]{8}-[0-9]{6}F(?:_[0-9]{8}-[0-9]{6}[ID])?$")


def _checked_child(parent: Path, name: str) -> Path:
    child = parent / name
    if parent.is_symlink() or child.is_symlink() or child.resolve().parent != parent.resolve():
        raise RuntimeError("Refusing cleanup outside the exact Chessism backup directory")
    return child


async def retire_superseded(backup_id: str, *, archive_mode: str) -> dict:
    """Caller holds the exclusive backup lane. Test success is a hard gate.

    Legacy chains can be retired only after live archiving is OFF. This never
    turns archiving off, restarts PostgreSQL, or selects unrelated directories.
    """
    from . import database_backups as b
    if not b.storage_snapshot().get("marker_matches"):
        raise RuntimeError("Backup volume identity mismatch; cleanup refused")
    for parent in (b.APP_BACKUP_ROOT, b.DATABASE_BACKUP_ROOT, b.MANIFEST_DIRECTORY):
        if parent.is_symlink():
            raise RuntimeError("Refusing a symlink backup directory")
    rows = b._read_json(b.BACKUP_CATALOG_PATH, {}).get("backups", [])
    if not rows or rows[0].get("backup_id") != backup_id:
        raise RuntimeError("Cleanup requires the latest cataloged backup")
    newest = rows[0]
    manifest_path = b.MANIFEST_DIRECTORY / f"{backup_id}.json"
    manifest = b._read_json(manifest_path, {})
    if (newest.get("storage_mode") != STORAGE_MODE or not newest.get("restore_tested")
            or newest.get("restore_test_status") != "success"
            or not manifest.get("restore_tested") or manifest.get("restore_test_status") != "success"):
        raise RuntimeError("Cleanup requires a successfully restored compressed snapshot")
    archive = snapshot_path(backup_id)
    if (not archive.is_file() or archive.stat().st_size != int(newest["repository_bytes"])
            or await asyncio.to_thread(b._sha256, archive) != newest.get("archive_sha256")
            or newest.get("archive_sha256") != manifest.get("archive_sha256")):
        raise RuntimeError("Verified replacement archive failed its cleanup integrity check")

    journal_path = b.DATABASE_BACKUP_ROOT / "snapshot-cleanup.json"
    previous = b._read_json(journal_path, {})
    if previous.get("state") in {"running", "failed"}:
        if previous.get("replacement_backup_id") != backup_id:
            raise RuntimeError("An earlier cleanup needs to finish before another backup can be retired")
        known_ids = {row.get("backup_id") for row in rows}
        rows += [row for row in previous.get("retiring_catalog_rows", []) if row.get("backup_id") not in known_ids]
    # Resolve and validate ALL deletion targets before changing the catalog.
    obsolete = []
    files = []
    legacy_rows = []
    for row in rows[1:]:
        key = str(row.get("backup_id") or "")
        if row.get("storage_mode") == STORAGE_MODE:
            files.append(snapshot_path(key))
            obsolete.append(key)
        elif LEGACY_ID.fullmatch(key):
            legacy_rows.append(row)
            if archive_mode == "off":
                obsolete.append(key)
        else:
            raise RuntimeError("Unknown backup catalog entry; automatic cleanup refused")
    repository = _checked_child(b.DATABASE_BACKUP_ROOT, "pgbackrest")
    for key in obsolete:
        files.append(_checked_child(b.MANIFEST_DIRECTORY, f"{key}.json"))
    remove_repository = archive_mode == "off" and repository.exists()
    expected_root = b.BACKUP_VOLUME_ROOT / "chessism" / "database"
    if remove_repository and (b.APP_ID != "chessism" or b.DATABASE_BACKUP_ROOT.resolve() != expected_root.resolve()):
        raise RuntimeError("Legacy cleanup is restricted to the Chessism database repository")
    # A durable journal makes a partial filesystem failure explicit/retryable.
    plan = {"state": "running", "replacement_backup_id": backup_id,
            "removed_backup_ids": obsolete, "archive_mode": archive_mode,
            "retiring_catalog_rows": [row for row in rows if row.get("backup_id") in obsolete],
            "paths": [str(p) for p in files] + ([str(repository)] if remove_repository else []),
            "started_at": b._iso_now()}
    b._atomic_write_json(journal_path, plan)
    before = await asyncio.to_thread(b._directory_size, b.APP_BACKUP_ROOT)
    # Withdraw retiring entries BEFORE deleting so interrupted cleanup cannot
    # advertise a partially deleted chain as recoverable.
    b._write_catalog([row for row in rows if row.get("backup_id") not in obsolete], updated_at=b._iso_now())
    try:
        for path in files:
            path.unlink(missing_ok=True)
        if remove_repository:
            await asyncio.to_thread(shutil.rmtree, repository)
        after = await asyncio.to_thread(b.record_application_usage)
        plan.update(state="complete", completed_at=b._iso_now(), removed_bytes=max(0, before-after),
                    application_bytes=after, legacy_retained=archive_mode != "off" and bool(legacy_rows))
        b._atomic_write_json(journal_path, plan)
        status = b._read_json(b.BACKUP_STATUS_PATH, {})
        if status.get("backup_id") == backup_id:
            status.update(total_bytes=after, cleanup=plan)
            b._atomic_write_json(b.BACKUP_STATUS_PATH, status)
        return plan
    except Exception as error:
        b._atomic_write_json(journal_path, {**plan, "state": "failed", "error": str(error)})
        raise


async def retire_after_restore(backup_id):
    from chessism_api.database.engine import AsyncDBSession
    async with AsyncDBSession() as session:
        mode = await session.scalar(text("SHOW archive_mode"))
    return await retire_superseded(backup_id, archive_mode=mode)
