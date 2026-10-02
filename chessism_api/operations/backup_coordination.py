"""Shared coordination for operations that read or write backup storage."""

from __future__ import annotations

from arq.connections import ArqRedis


BACKUP_LOCK_KEY = "chessism:backup:active"
BACKUP_LOCK_TTL_SECONDS = 7 * 24 * 60 * 60

_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def redis_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


async def reserve_backup(redis: ArqRedis, job_id: str) -> tuple[bool, str | None]:
    """Reserve the single backup lane for a specific ARQ job."""
    reserved = await redis.set(
        BACKUP_LOCK_KEY,
        job_id,
        ex=BACKUP_LOCK_TTL_SECONDS,
        nx=True,
    )
    if reserved:
        return True, job_id
    return False, redis_text(await redis.get(BACKUP_LOCK_KEY))


async def ensure_backup_reservation(redis: ArqRedis, job_id: str) -> None:
    """Ensure a directly invoked worker job owns the backup lane."""
    current = redis_text(await redis.get(BACKUP_LOCK_KEY))
    if current == job_id:
        await redis.expire(BACKUP_LOCK_KEY, BACKUP_LOCK_TTL_SECONDS)
        return
    if current:
        raise RuntimeError(f"Backup storage is already reserved by job {current}.")
    reserved, owner = await reserve_backup(redis, job_id)
    if not reserved:
        raise RuntimeError(f"Backup storage is already reserved by job {owner}.")


async def release_backup(redis: ArqRedis | None, job_id: str) -> None:
    """Release the backup lane only when it is still owned by this job."""
    if redis is None:
        return
    await redis.eval(_RELEASE_LOCK_SCRIPT, 1, BACKUP_LOCK_KEY, job_id)
