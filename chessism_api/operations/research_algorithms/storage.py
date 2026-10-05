"""Disposable workspaces, bounded disk allocation, and single-worker ownership."""

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

WORK_ROOT = Path(os.getenv("ALGORITHM_WORK_DIR", str(Path(__file__).resolve().parents[3] / "research_data" / "algorithms" / "tmp")))
FREE_FLOOR = int(os.getenv("ALGORITHM_FREE_FLOOR_BYTES", "20000000000"))
WORKSPACE_PATTERN = re.compile(r"run-[0-9a-f-]{36}-[A-Za-z0-9_]+$")


def capacity(config):
    required = config["max_rows"] * (len(config["columns"]) * 8 + 128) + 1024 * 1024
    ancestor = WORK_ROOT
    while not ancestor.exists():
        ancestor = ancestor.parent
    free = shutil.disk_usage(ancestor).free
    return {"temporary_bytes_upper_estimate": required, "free_bytes": free,
            "free_floor_bytes": FREE_FLOOR, "safe_to_run": free - required >= FREE_FLOOR}


def ensure_capacity(config):
    if not capacity(config)["safe_to_run"]:
        raise ValueError("Insufficient local space above the protected reserve for this algorithm.")


@contextmanager
def workspace(run_id):
    uuid.UUID(run_id)
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix=f"run-{run_id}-", dir=WORK_ROOT))
    try:
        yield folder
    finally:
        shutil.rmtree(folder)


def acquire_worker_lock():
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    handle = (WORK_ROOT / ".worker.lock").open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        handle.close()
        raise RuntimeError("Another algorithm worker owns this working directory.")
    return handle


def cleanup_interrupted_workspaces():
    # Called only by the worker holding .worker.lock, never by API startup.
    removed = []
    for folder in WORK_ROOT.iterdir():
        if folder.is_dir() and not folder.is_symlink() and WORKSPACE_PATTERN.fullmatch(folder.name):
            shutil.rmtree(folder)
            removed.append(folder.name)
    return removed
