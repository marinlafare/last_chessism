"""Small durable acknowledgements. Restart truth always comes from cloud inventory."""
import asyncio
from datetime import datetime, timezone
import re

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudCleanupProgress


async def record(job_id, plan_sha256, phase, total, removed):
    if (not re.fullmatch(r'[0-9a-f]{64}', plan_sha256) or phase not in {'compute', 'objects', 'image', 'logs'}
            or type(total) is not int or total < 0
            or (removed is not None and (type(removed) is not int or not 0 <= removed <= total))):
        raise ValueError('Invalid cleanup progress acknowledgement')
    table = CloudCleanupProgress.__table__
    stmt = insert(table).values(job_id=job_id, plan_sha256=plan_sha256, phase=phase,
        total=total, removed=removed, updated_at=datetime.now(timezone.utc))
    stmt = stmt.on_conflict_do_update(index_elements=['job_id', 'plan_sha256'], set_={
        'phase': stmt.excluded.phase, 'updated_at': stmt.excluded.updated_at,
        'removed': func.coalesce(stmt.excluded.removed, table.c.removed)},
        where=table.c.total == stmt.excluded.total)
    async with AsyncDBSession() as session, session.begin():
        result = await session.execute(stmt)
        if result.rowcount != 1:
            raise ValueError('Cleanup progress plan changed')


def observe(job_id):
    loop = asyncio.get_running_loop()
    def callback(plan_sha256, phase, total, removed):
        future = asyncio.run_coroutine_threadsafe(record(job_id, plan_sha256, phase, total, removed), loop)
        try:
            future.result(timeout=15)
        except BaseException:
            future.cancel()
            raise
    return callback


def bind(timings, job_id):
    timings.cleanup_progress = observe(job_id)
    return timings
