"""Companion archives and the private backup-controller protocol.

The API and PostgreSQL worker never receive Docker access. Only the dedicated
controller can stop the graph service or create isolated restore containers.
"""
import asyncio
import os
from pathlib import Path
import re
import time
import uuid

from chessism_api.operations.backup_files import atomic_write_json, read_json, sha256

SAFE_ID = re.compile(r"snapshot-[0-9]{8}T[0-9]{6}-[a-f0-9]{12}\Z")
DATABASES = ("neo4j", "system")


def archive_directory(root: Path, backup_id: str) -> Path:
    if not SAFE_ID.fullmatch(backup_id):
        raise ValueError("Invalid Neo4j backup ID")
    target = root / "neo4j" / backup_id
    for path in (root.parent, root, target.parent, target):
        if path.is_symlink():
            raise ValueError("Symlink Neo4j backup path refused")
    return target


def archive_files(root: Path, backup_id: str) -> list[Path]:
    directory = archive_directory(root, backup_id)
    files = [directory / f"{name}.dump" for name in DATABASES]
    if any(path.is_symlink() for path in files):
        raise ValueError("Symlink Neo4j archive refused")
    return files


def verify_archives(root: Path, backup_id: str, metadata: dict) -> None:
    if metadata.get("format") != "neo4j-admin-dump-v1":
        raise RuntimeError("Missing Neo4j backup format")
    archives = metadata.get("archives", {})
    if set(archives) != set(DATABASES):
        raise RuntimeError("Neo4j backup must contain both graph and system databases")
    for name, path in zip(DATABASES, archive_files(root, backup_id)):
        item = archives[name]
        if (item.get("file") != path.name or not path.is_file()
                or path.stat().st_size != item.get("bytes") or sha256(path) != item.get("sha256")):
            raise RuntimeError(f"Neo4j {name} archive checksum/size mismatch")


async def request(operation: str, backup_id: str, *, metadata=None, progress=None,
                  baseline_bytes=0) -> dict:
    """Private file mailbox; liveness leases prevent abandoned requests running."""
    if operation not in {"dump", "test"} or not SAFE_ID.fullmatch(backup_id):
        raise ValueError("Invalid Neo4j backup request")
    rpc = Path(os.environ.get("CHESSISM_NEO4J_BACKUP_RPC", "/neo4j-backup-rpc"))
    heartbeat = rpc / "heartbeat"
    if not heartbeat.is_file() or time.time() - heartbeat.stat().st_mtime > 30:
        raise RuntimeError("Neo4j backup controller is unavailable; no complete backup was created")
    token = uuid.uuid4().hex
    request_path = rpc / f"{token}.request.json"
    lease = rpc / f"{token}.lease"
    response = rpc / f"{token}.response.json"
    state = rpc / f"{token}.progress.json"
    lease.touch()
    atomic_write_json(request_path, {"operation": operation, "backup_id": backup_id,
                                    "metadata": metadata, "baseline_bytes": baseline_bytes})
    started = time.monotonic()
    try:
        while True:
            lease.touch()
            result = read_json(response, None)
            if result is not None:
                if not result.get("ok"):
                    raise RuntimeError(result.get("error", "Neo4j backup operation failed"))
                return result["result"]
            if time.monotonic() - started > 6 * 60 * 60:
                raise TimeoutError("Neo4j backup operation exceeded six hours")
            if progress:
                await progress(read_json(state, {}).get("detail", "Waiting for Neo4j backup controller"))
            await asyncio.sleep(2)
    finally:
        # Removing the lease cancels work even if the worker itself was stopped.
        lease.unlink(missing_ok=True)
        request_path.unlink(missing_ok=True)
        for path in (response, state):
            path.unlink(missing_ok=True)


async def test_companion(manifest: dict, progress) -> dict:
    metadata = manifest.get("neo4j")
    if not metadata:
        if int(manifest.get("backup_format_version", 0)) >= 4:
            raise RuntimeError("This backup is missing its required Neo4j archives")
        return {"status": "not_included", "detail": "Legacy PostgreSQL-only backup; Neo4j was not backed up."}
    from chessism_api.operations import database_backups as b
    await asyncio.to_thread(verify_archives, b.DATABASE_BACKUP_ROOT, manifest["backup_id"], metadata)
    return await request("test", manifest["backup_id"], metadata=metadata, progress=progress)


def discard_unpublished(root: Path, backup_id: str):
    """Remove only this failed operation's known, unpublished companion files."""
    for path in archive_files(root, backup_id):
        path.unlink(missing_ok=True)
    directory = archive_directory(root, backup_id)
    if directory.exists():
        directory.rmdir()
