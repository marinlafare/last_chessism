"""Add cloud-owned sequence control without changing existing saved jobs."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudSequenceControl


def upgrade(connection):
    connection.execute(text('SELECT pg_advisory_xact_lock(731946219)'))
    if connection.scalar(text("SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached')")):
        raise ValueError('Finish/recover existing cloud requests before activating automatic sequences')
    CloudSequenceControl.__table__.create(connection, checkfirst=True)
    connection.execute(text('ALTER TABLE cloud_control_work_unit ADD COLUMN IF NOT EXISTS uploaded BOOLEAN'))


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='10s'"))
            await connection.run_sync(upgrade)
        print('Cloud sequence control ready; saved jobs unchanged. No Google resources created.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
