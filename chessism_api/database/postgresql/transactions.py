"""Explicit transaction boundaries used by ingestion application operations."""
from contextlib import asynccontextmanager
import logging

from sqlalchemy import text

from .analysis_cleanup import finish_analysis_cleanup

logger = logging.getLogger(__name__)


async def driver_connection(session):
    """Borrow COPY support only after the operation has begun its transaction."""
    if not session.in_transaction():
        raise ValueError('A caller-owned transaction is required')
    # SQLAlchemy BEGIN is lazy; this starts the physical driver transaction.
    await session.execute(text('SELECT 1'))
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    if not driver.is_in_transaction():
        raise RuntimeError('PostgreSQL transaction was not started')
    return driver


@asynccontextmanager
async def write_transaction(session_factory):
    """Commit the operation's unit, or finish rollback/close before propagating.

    Uses the accepted P1 cleanup mechanism unchanged. Cleanup failures cannot
    replace a primary operation error or interrupt closure after rollback fails.
    """
    session = session_factory()
    try:
        await session.begin()
        yield session
        await session.commit()
    except BaseException:
        async def cleanup():
            try:
                await session.rollback()
            finally:
                await session.close()
        try:
            await finish_analysis_cleanup(cleanup())
        except BaseException:
            logger.exception('Ingestion cleanup failed; preserving the operation error')
        raise
    else:
        await finish_analysis_cleanup(session.close())
