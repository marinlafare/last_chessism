"""Legacy snapshot companions; new recipe records live in PostgreSQL itself."""

from pathlib import Path
import shutil

from sqlalchemy import select

from chessism_api.database.models import MatrixArtifact
from chessism_api.operations.matrix_constructor.artifact_files import (
    copy_snapshot, file_operation, inspect_snapshot,
)
from chessism_api.operations.matrix_constructor.storage import ARTIFACT_ROOT


async def backup_completed_matrices(session, *, publish_progress) -> dict:
    """Caller holds matrix_catalog_lock until the database backup is finished."""
    from chessism_api.operations import database_backups as backup

    rows = list((await session.execute(
        select(MatrixArtifact.id).where(MatrixArtifact.status == "complete").order_by(MatrixArtifact.id)
    )).scalars())
    destination = backup.RESEARCH_BACKUP_ROOT / "matrices"
    if ARTIFACT_ROOT.resolve() == destination.resolve():
        raise ValueError("Working matrices must be separate from the backup directory.")
    # Measure once, then track bytes reserved for each newly published copy.
    storage = backup.require_storage(include_app_usage=True)
    application_bytes = int(storage.get("application_bytes") or 0)
    starting_free = shutil.disk_usage(backup.BACKUP_VOLUME_ROOT).free
    copied_bytes = 0
    snapshots = []
    await publish_progress(phase="saving_matrices", total=0, processed=0,
                           detail=f"Checking {len(rows)} legacy matrix snapshots; definitions are stored in PostgreSQL.")
    for index, artifact_id in enumerate(rows):
        def capacity_check(required: int):
            free = shutil.disk_usage(backup.BACKUP_VOLUME_ROOT).free
            if free - required <= backup.FREE_FLOOR_BYTES + backup.STOP_MARGIN_BYTES:
                raise backup.BackupCapacityError("Matrix backup would cross the protected free-space floor.")
            estimated_usage = application_bytes + max(copied_bytes, starting_free - free, 0)
            if estimated_usage + required >= backup.APP_QUOTA_BYTES:
                raise backup.BackupCapacityError("Matrix backup would exceed Chessism's backup allocation.")

        copied = await file_operation(copy_snapshot, ARTIFACT_ROOT, destination, artifact_id,
                                      capacity_check=capacity_check)
        if copied["copied"]:
            copied_bytes += copied["size_bytes"]
        snapshots.append({**copied, "relative_path": f"research/matrices/{artifact_id}/manifest.json"})
        await publish_progress(
            phase="saving_matrices", total=0, processed=0,
            detail=f"Legacy snapshots saved: {index + 1}/{len(rows)}; unchanged copies are reused.",
        )
    return {
        "schema_version": 1, "count": len(snapshots),
        "copied_count": sum(item["copied"] for item in snapshots),
        "reused_count": sum(not item["copied"] for item in snapshots),
        "bytes_added": copied_bytes, "snapshots": snapshots,
    }


def validate_matrix_bundle(bundle: dict, root: Path, restored_ids: list[str]) -> dict:
    """Manual restore test: verify restored metadata against every backup file."""
    if bundle.get("schema_version") != 1 or not isinstance(bundle.get("snapshots"), list):
        raise ValueError("Invalid matrix companion backup manifest.")
    snapshots = bundle["snapshots"]
    ids = [item["artifact_id"] for item in snapshots]
    if len(ids) != len(set(ids)) or len(ids) != bundle.get("count") or sorted(ids) != sorted(restored_ids):
        raise ValueError("Restored matrix metadata does not match the backup's snapshot list.")
    total = 0
    for item in snapshots:
        inspected = inspect_snapshot(root, item["artifact_id"], verify=True)
        if (inspected["manifest_sha256"] != item["manifest_sha256"]
                or inspected["size_bytes"] != item["size_bytes"]):
            raise ValueError(f"Changed matrix backup: {item['artifact_id']}")
        total += inspected["size_bytes"]
    return {"status": "passed", "count": len(snapshots), "size_bytes": total}
