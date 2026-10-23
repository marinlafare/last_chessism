"""Add disposable combined-builder caches. No changes to cloud jobs or FEN data."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudGameDraftCache, CloudGameDraftEntryFen, CloudGameDraftFen


def upgrade(connection):
    connection.execute(text('SELECT pg_advisory_xact_lock(731946225)'))
    for model in (CloudGameDraftCache, CloudGameDraftEntryFen, CloudGameDraftFen):
        model.__table__.create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='5s'"))
            await connection.run_sync(upgrade)
        print('Combined draft cache tables ready; jobs and FEN results unchanged.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
