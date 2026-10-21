"""Add only the stop/cleanup checkpoint table; no existing data is rewritten."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudJobCancellation


def upgrade(connection):
    if not connection.scalar(text('SELECT pg_try_advisory_xact_lock(731946219)')):
        raise ValueError('Stop the idle cloud controller before installing cancellation checkpoints')
    if connection.scalar(text("SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached')")):
        raise ValueError('Finish existing cloud work before installing cancellation checkpoints')
    CloudJobCancellation.__table__.create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='5s'"))
            await connection.run_sync(upgrade)
        print('Cancellation checkpoint table ready. No existing data changed; no cloud work started.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
