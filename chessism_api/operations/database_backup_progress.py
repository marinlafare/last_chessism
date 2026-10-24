"""Factual pgBackRest byte progress for database-backup jobs."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any


PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60


async def write_database_backup_progress(
    redis: Any,
    job_id: str,
    *,
    phase: str,
    detail: str,
    result: dict[str, Any] | None = None,
    total: int = 0,
    processed: int = 0,
    eta_seconds: float | None = None,
    rate_bytes_per_second: float | None = None,
) -> None:
    if redis is None:
        return
    payload = {
        "job_id": job_id,
        "kind": "database_backup",
        "phase": phase,
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "unit": "bytes" if total else None,
        "eta_seconds": eta_seconds,
        "rate_bytes_per_second": rate_bytes_per_second,
        "detail": detail,
        "result": result,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )


def parse_pgbackrest_progress(payload: Any) -> tuple[int, int] | None:
    """Extract factual byte progress from pgBackRest's stable JSON info output."""
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        return None
    backup_lock = (
        (payload[0].get("status") or {})
        .get("lock", {})
        .get("backup", {})
    )
    if not isinstance(backup_lock, dict) or not backup_lock.get("held"):
        return None

    try:
        total = int(backup_lock.get("size") or 0)
        completed = int(backup_lock.get("size-cplt") or 0)
    except (TypeError, ValueError):
        return None
    if total <= 0:
        return None
    return total, min(max(0, completed), total)


async def read_pgbackrest_progress(stanza: str) -> tuple[int, int] | None:
    """Read lightweight in-flight backup progress without scanning the repository."""
    process = await asyncio.create_subprocess_exec(
        "pgbackrest",
        f"--stanza={stanza}",
        "--output=json",
        "--detail-level=progress",
        "--log-level-console=error",
        "info",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await process.communicate()
    if process.returncode:
        return None
    try:
        payload = json.loads(stdout.decode("utf-8", errors="replace"))
    except (AttributeError, json.JSONDecodeError):
        return None
    return parse_pgbackrest_progress(payload)
