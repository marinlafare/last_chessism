"""Verified, non-overwriting copies of immutable matrix directories."""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

from .storage import relative_manifest_path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_directory(root: Path, artifact_id: str) -> Path:
    relative_manifest_path(artifact_id)
    folder = root / artifact_id
    if folder.is_symlink() or folder.resolve().parent != root.resolve():
        raise ValueError("Unsafe matrix directory.")
    return folder


def inspect_snapshot(root: Path, artifact_id: str, *, verify: bool = False) -> dict:
    """Check structure/sizes cheaply; full hashes are optional for existing copies."""
    folder = artifact_directory(root, artifact_id)
    return inspect_directory(folder, artifact_id, verify=verify)


def inspect_directory(folder: Path, artifact_id: str, *, verify: bool) -> dict:
    if folder.is_symlink() or not folder.is_dir():
        raise ValueError(f"Missing or unsafe matrix folder: {artifact_id}")
    manifest_path = folder / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("Unsafe matrix manifest.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("format") != "chessism_matrix" or manifest.get("version") != 2
            or manifest.get("artifact_id") != artifact_id
            or manifest.get("validation", {}).get("status") != "passed"):
        raise ValueError(f"Invalid matrix manifest: {artifact_id}")
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Matrix manifest contains no files.")
    names = set()
    total = manifest_path.stat().st_size
    for entry in rows:
        name = entry["name"]
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name)
                or name == "manifest.json" or name in names):
            raise ValueError("Unsafe or duplicate matrix filename.")
        path = folder / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size != entry["bytes"]:
            raise ValueError(f"Missing or changed matrix file: {artifact_id}/{name}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("sha256", ""))):
            raise ValueError("Invalid matrix file checksum.")
        if verify and sha256(path) != entry["sha256"]:
            raise ValueError(f"Matrix checksum mismatch: {artifact_id}/{name}")
        names.add(name)
        total += entry["bytes"]
    required = {"row_keys.jsonl.gz", "dictionaries.json.gz"}
    for layout in manifest["columns"].values():
        for column in layout:
            required.update((column["array_file"], column["missing_mask_file"]))
    if not required.issubset(names) or {entry.name for entry in folder.iterdir()} != names | {"manifest.json"}:
        raise ValueError("Matrix files do not match the manifest.")
    return {
        "artifact_id": artifact_id, "manifest_sha256": sha256(manifest_path),
        "size_bytes": total, "manifest": manifest,
    }


def copy_snapshot(source_root: Path, destination_root: Path, artifact_id: str, *,
                  expected_sha256: str | None = None, capacity_check=None,
                  verify_existing: bool = False) -> dict:
    """Copy once, verify before atomic publication, never prune or overwrite.

    Existing copies use manifest hash + file sizes during normal backups. The
    manual restore rehearsal verifies all payload hashes, including old copies.
    """
    if source_root.resolve() == destination_root.resolve():
        raise ValueError("Working storage and backup storage must be different directories.")
    source = inspect_snapshot(source_root, artifact_id)
    if expected_sha256 and source["manifest_sha256"] != expected_sha256:
        raise ValueError(f"Matrix manifest changed: {artifact_id}")
    target = artifact_directory(destination_root, artifact_id)
    result = {key: source[key] for key in ("artifact_id", "manifest_sha256", "size_bytes")}
    if target.exists():
        existing = inspect_snapshot(destination_root, artifact_id, verify=verify_existing)
        if existing["manifest_sha256"] != source["manifest_sha256"]:
            raise ValueError(f"Refusing to overwrite a different matrix snapshot: {artifact_id}")
        return {**result, "copied": False}
    if capacity_check:
        capacity_check(source["size_bytes"])
    destination_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{artifact_id}.copy-", dir=destination_root))
    try:
        source_folder = artifact_directory(source_root, artifact_id)
        # Copy and hash in chunks, checking capacity throughout large transfers.
        for entry in source["manifest"]["files"]:
            source_file, target_file = source_folder / entry["name"], temporary / entry["name"]
            digest = hashlib.sha256()
            with source_file.open("rb") as incoming, target_file.open("xb") as output:
                for block in iter(lambda: incoming.read(4 * 1024 * 1024), b""):
                    if capacity_check:
                        capacity_check(len(block))
                    output.write(block)
                    digest.update(block)
                output.flush()
                os.fsync(output.fileno())
            if digest.hexdigest() != entry["sha256"]:
                raise ValueError(f"Source matrix checksum mismatch: {artifact_id}/{entry['name']}")
        shutil.copyfile(source_folder / "manifest.json", temporary / "manifest.json")
        with (temporary / "manifest.json").open("rb") as manifest_file:
            os.fsync(manifest_file.fileno())
        copied = inspect_directory(temporary, artifact_id, verify=True)
        if copied["manifest_sha256"] != source["manifest_sha256"]:
            raise ValueError("Matrix manifest changed during copy.")
        temporary.chmod(0o755)  # Backup worker must be able to read root-created copies.
        descriptor = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if target.exists():
            raise ValueError("A matrix snapshot with this UUID was published concurrently.")
        os.rename(temporary, target)
        descriptor = os.open(destination_root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return {**result, "copied": True}
    finally:
        # Only this invocation's validated temporary directory; existing copies
        # (including backup copies of locally deleted artifacts) are untouched.
        if temporary.exists():
            shutil.rmtree(temporary)


async def file_operation(function, *args, **kwargs):
    """Keep I/O off the event loop; cancellation waits for file handles to close."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise
