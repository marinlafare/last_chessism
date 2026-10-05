"""Copy external matrices into working storage, without deleting any backup.

Run `python -m chessism_api.operations.matrix_constructor.storage_cli migrate`
for the one-time split. After restoring PostgreSQL, use `restore --backup-id ID`
to restore the companion files recorded by that recovery point.
"""

import argparse
import asyncio
import json
import re
import shutil

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine

import constants
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import MatrixArtifact
from chessism_api.operations import database_backups as backup
from chessism_api.operations.matrix_backups import validate_matrix_bundle
from .artifact_files import copy_snapshot, file_operation
from .storage import ARTIFACT_ROOT, FREE_SPACE_FLOOR, matrix_catalog_lock, relative_manifest_path


async def restore_working_copies(*, backup_id: str | None = None) -> dict:
    if backup.VOLUME_MARKER.read_text(encoding="utf-8").strip() != backup.EXPECTED_VOLUME_UUID:
        raise ValueError("The backup volume UUID marker does not match.")
    source = backup.RESEARCH_BACKUP_ROOT / "matrices"
    if source.resolve() == ARTIFACT_ROOT.resolve():
        raise ValueError("Working and backup paths must be separate.")
    async with matrix_catalog_lock() as session:
        ids = list((await session.execute(
            select(MatrixArtifact.id).where(MatrixArtifact.status == "complete").order_by(MatrixArtifact.id)
        )).scalars())
        if backup_id:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", backup_id):
                raise ValueError("Invalid backup identifier.")
            manifest = json.loads((backup.MANIFEST_DIRECTORY / f"{backup_id}.json").read_text())
            await file_operation(validate_matrix_bundle, manifest["matrices"], source, ids)
        ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)

        def capacity_check(required):
            if shutil.disk_usage(ARTIFACT_ROOT).free - required < FREE_SPACE_FLOOR:
                raise ValueError("Insufficient local space for matrix working copies.")

        results = []
        for artifact_id in ids:
            result = await file_operation(
                copy_snapshot, source, ARTIFACT_ROOT, artifact_id,
                capacity_check=capacity_check, verify_existing=True,
            )
            results.append(result)
        # Rebase metadata only after every copy has been verified. Relative
        # paths survive moving the project or restoring on another machine.
        for artifact_id in ids:
            await session.execute(update(MatrixArtifact).where(MatrixArtifact.id == artifact_id).values(
                artifact_path=relative_manifest_path(artifact_id),
            ))
    return {"working_root": str(ARTIFACT_ROOT), "copied": sum(item["copied"] for item in results),
            "verified": len(results), "bytes": sum(item["size_bytes"] for item in results),
            "backups_preserved": True}


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("migrate", "restore"))
    parser.add_argument("--backup-id")
    args = parser.parse_args()
    if args.action == "restore" and not args.backup_id:
        parser.error("restore requires --backup-id")
    engine = create_async_engine(constants.CONN_STRING.replace("postgresql://", "postgresql+asyncpg://", 1))
    AsyncDBSession.configure(bind=engine)
    try:
        print(json.dumps(await restore_working_copies(backup_id=args.backup_id), indent=2))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
