"""Add reservation checkpoints; no changes to existing jobs or evaluations."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.postgresql.models import CloudInputPreparation, CloudInputChunk


def upgrade(connection):
    connection.execute(text('SELECT pg_advisory_xact_lock(731946225)'))
    for model in (CloudInputPreparation, CloudInputChunk):
        model.__table__.create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='3s'"))
            await connection.execute(text("SET LOCAL statement_timeout='30s'"))
            await connection.run_sync(upgrade)
        print('Input-preparation checkpoint tables ready; existing jobs unchanged.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
