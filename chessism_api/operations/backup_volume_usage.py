"""Read-only, cached disk map. Never runs in the backup/retention write path."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import threading
import time
from datetime import datetime, timezone

CACHE_SECONDS = 300
SCAN_TIMEOUT_SECONDS = 30
_lock = threading.Lock()
_states: dict[tuple, dict] = {}


def _metadata(path: Path) -> dict:
    """Only small regular metadata files; never follow links or read artifacts."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                return {}
            value = json.loads(source.read(65537))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _reservation(path: Path) -> int | None:
    # Only explicitly published byte quotas, never a backup's original DB size
    # or a disk-wide free-space floor. No configuration or credential scraping.
    for name in ('backup-allocation.json', 'backup-policy.json', 'setup.json',
                 'backup-usage.json', 'backup-status.json'):
        metadata = _metadata(path / name)
        for document in (metadata, metadata.get('storage'), metadata.get('policy')):
            if not isinstance(document, dict):
                continue
            for key in ('quota_bytes', 'allocation_bytes', 'reserved_bytes'):
                value = document.get(key)
                if isinstance(value, str) and value.isascii() and value.isdecimal():
                    value = int(value)
                if type(value) is int and 0 < value <= 2**53 - 1:
                    return value
    return None


def _check_volume(root: Path, expected_uuid: str) -> tuple[int, int]:
    if root.is_symlink() or not root.is_mount():
        raise OSError('Backup volume unavailable')
    marker = root / '.chessism-backup-volume'
    if marker.is_symlink() or marker.read_text().strip() != expected_uuid:
        raise OSError('Backup volume identity changed')
    info = root.stat()
    return info.st_dev, info.st_ino


def measure_volume(root: Path, expected_uuid: str, app_id: str, quota: int) -> dict:
    identity = _check_volume(root, expected_uuid)
    directories = sorted(
        (path for path in root.iterdir()
         if not path.name.startswith('.') and path.name != 'lost+found'
         and not path.is_symlink() and path.is_dir()
         and not path.is_mount() and path.stat().st_dev == identity[0]),
        key=lambda path: (path.name != app_id, path.name),
    )
    if len(directories) > 100:
        raise OSError('Too many backup directories to measure safely')
    sizes = {}
    if directories:
        # One invocation counts hard-linked snapshots once, including links
        # shared across directories. Read directory metadata, not file contents.
        result = subprocess.run(
            ['du', '--summarize', '--one-file-system', '--no-dereference',
             '--block-size=1', '--null', '--', *map(str, directories)],
            capture_output=True, timeout=SCAN_TIMEOUT_SECONDS, check=True,
        )
        for record in result.stdout.split(b'\0'):
            if record:
                count, path = record.split(b'\t', 1)
                sizes[os.fsdecode(path)] = int(count)
        if set(sizes) != {str(path) for path in directories}:
            raise OSError('Incomplete disk usage measurement')
    rows = [{
        'id': path.name,
        'label': 'Chessism' if path.name == app_id else path.name,
        'used_bytes': sizes[str(path)],
        'reserved_bytes': quota if path.name == app_id else _reservation(path),
        'is_application': path.name == app_id,
    } for path in directories]
    if _check_volume(root, expected_uuid) != identity:
        raise OSError('Backup volume changed during measurement')
    return {'allocations': rows, 'measured_at': datetime.now(timezone.utc).isoformat()}


def _refresh(key: tuple) -> None:
    root, expected_uuid, app_id, quota = key
    try:
        result = measure_volume(Path(root), expected_uuid, app_id, quota)
        error = None
    except Exception:
        # Never send raw subprocess errors, filesystem paths or file contents.
        result = None
        error = 'Could not measure all backup folders. The previous measurement, if any, is retained.'
    with _lock:
        state = _states[key]
        if result is not None:
            state['snapshot'] = result
        state.update(refreshing=False, error=error, checked_at=time.monotonic())


def volume_usage(root: Path, expected_uuid: str, app_id: str, quota: int) -> dict:
    """Return immediately; at most one 30-second scan per five minutes/process."""
    key = (str(root), expected_uuid, app_id, quota)
    with _lock:
        state = _states.setdefault(key, {
            'snapshot': {}, 'refreshing': False, 'checked_at': None, 'error': None,
        })
        expired = state['checked_at'] is None or time.monotonic() - state['checked_at'] >= CACHE_SECONDS
        if expired and not state['refreshing']:
            state['refreshing'] = True
            threading.Thread(target=_refresh, args=(key,), daemon=True).start()
        return {
            **state['snapshot'], 'refreshing': state['refreshing'],
            'error': state['error'], 'stale': bool(state['error'] or state['refreshing']),
        }
