"""Exclusive, narrowly scoped maintenance of disposable restore workspaces."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import shutil


@contextmanager
def workspace_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    # Do not unlink this file: doing so permits two locks on different inodes.
    fd = os.open(root / ".restore-workspace.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            try:
                yield True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def remove_workspace(root: Path, candidate: Path) -> bool:
    """Caller must hold workspace_lock. Never remove a possibly live cluster."""
    if candidate.parent.resolve() != root.resolve() or not candidate.name.startswith("rehearsal-"):
        raise ValueError("Not a direct restore rehearsal workspace")
    if candidate.is_symlink():
        candidate.unlink()
        return True
    # Fail closed even for stale PIDs: an operator can inspect these after a
    # crash. PIDs cannot reliably establish liveness across container namespaces.
    if (candidate / "pgdata" / "postmaster.pid").exists():
        return False
    if candidate.is_dir():
        shutil.rmtree(candidate)
    else:
        candidate.unlink(missing_ok=True)
    return True
