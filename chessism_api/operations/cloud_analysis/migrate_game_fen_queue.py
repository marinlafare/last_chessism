"""Install the reservation queue only after cloud jobs/controller are stopped.

No automatic startup migration and no deployment while a cloud job is active.
"""
import asyncio
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudGameFenQueue, CloudGameFenCandidate


def upgrade(connection):
    # Fail immediately if the running controller owns its singleton lock.
    if not connection.scalar(text('SELECT pg_try_advisory_xact_lock(731946219)')):
        raise ValueError('Cloud controller is running; do not migrate during an active process')
    if not connection.scalar(text('SELECT pg_try_advisory_xact_lock(731946220)')):
        raise ValueError('A cloud submission is in progress; retry after it finishes')
    if connection.scalar(text("SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached')")):
        raise ValueError('Finish/recover cloud jobs before enabling the FEN queue')
    CloudGameFenQueue.__table__.create(connection, checkfirst=True)
    CloudGameFenCandidate.__table__.create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='5s'"))
            await connection.run_sync(upgrade)
        print('Game FEN queue schema ready. No existing jobs, claims or FEN results changed.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
