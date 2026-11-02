"""Freeze graph publication across the two database snapshot boundaries."""
from contextlib import asynccontextmanager
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from .sync_schema import LOCK


@asynccontextmanager
async def publication_barrier():
    # Acquire BEFORE opening the repeatable-read PostgreSQL snapshot. The
    # outbox in that snapshot then contains everything the graph still needs.
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL lock_timeout='120s'"))
        await session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key':LOCK})
        released = False

        async def release():
            nonlocal released
            if not released:
                await session.rollback()
                released = True

        try:
            yield release
        finally:
            await release()
