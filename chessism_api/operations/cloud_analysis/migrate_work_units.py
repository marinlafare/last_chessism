"""Additive work-unit schema, no Google calls. Stop API/controller first."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudBatchUnit
from chessism_api.database import cloud_columns


def upgrade(connection):
    connection.execute(text('SELECT pg_advisory_xact_lock(731946219)'))
    if connection.scalar(text("SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached')")):
        raise ValueError('Finish/recover existing cloud requests before enabling work units')
    CloudBatchUnit.__table__.create(connection, checkfirst=True)
    cloud_columns.TABLES['work_unit'].create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='10s'"))
            await connection.run_sync(upgrade)
        print('Work-unit tables ready; existing jobs/results unchanged. No cloud resources created.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
