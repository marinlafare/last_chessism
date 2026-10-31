"""Structured preview reads and the shared, owner-scoped preview lease."""
from contextlib import asynccontextmanager
import logging
import uuid

from fastapi import HTTPException
from redis.exceptions import RedisError
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from .catalog import SOURCES, tables_for
from .query import compile_query, effective_order
from .writer import preview_value

LOG = logging.getLogger(__name__)
RELEASE_LEASE = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


@asynccontextmanager
async def preview_slot(redis, client_id, *, analytical=False):
    """Both preview modes share one slot; never release another owner's lease.

    The TTL is longer than either preview deadline. A release outage must not
    discard a completed response or replace the original query error.
    """
    key, owner = f'export-preview:{client_id}', uuid.uuid4().hex
    try:
        acquired = await redis.set(key, owner, nx=True, ex=30)
    except RedisError:
        message = 'Preview admission is temporarily unavailable.'
        detail = {'code': 'PREVIEW_UNAVAILABLE', 'message': message} if analytical else message
        raise HTTPException(503, detail) from None
    if not acquired:
        message = 'A preview is already running for this integration.'
        detail = {'code': 'PREVIEW_BUSY', 'message': message} if analytical else message
        raise HTTPException(429, detail, headers={'Retry-After': '3'})
    try:
        yield
    finally:
        try:
            await redis.eval(RELEASE_LEASE, 1, key, owner)
        except RedisError:
            LOG.warning('Preview lease release unavailable; lease will expire.')


async def structured_preview(spec):
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        await session.execute(text("SET LOCAL statement_timeout = '12s'"))
        await session.execute(text("SET LOCAL lock_timeout = '1s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather = 1'))
        snapshot_at = await session.scalar(text('SELECT transaction_timestamp()'))
        limit = min(spec.limit, 100)
        statement, params = compile_query(spec, tables_for(session), limit=limit+1)
        rows = (await session.execute(statement, params)).mappings().all()
    return {'kind': 'live_preview', 'source': spec.source, 'snapshot_at': snapshot_at,
        'columns': [{'name': key, 'type': SOURCES[spec.source].columns[key].dtype} for key in spec.select],
        'rows': [[preview_value(row[key]) for key in spec.select] for row in rows[:limit]],
        'row_count': min(len(rows), limit), 'has_more': len(rows) > limit,
        'selection_order': effective_order(spec), 'json_integer_encoding': 'decimal string when outside JavaScript safe range'}
