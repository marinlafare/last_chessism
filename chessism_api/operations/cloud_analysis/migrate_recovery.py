"""Add recovery history and the pending game queue while the controller is stopped."""
import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.models import CloudRecoveryEvent
from .migrate_game_fen_queue import upgrade as queue_upgrade


def upgrade(connection):
    queue_upgrade(connection)  # Checks controller/submission locks and terminal jobs.
    CloudRecoveryEvent.__table__.create(connection, checkfirst=True)


async def main():
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout='5s'"))
            await connection.run_sync(upgrade)
        print('Recovery history and game queue installed; existing results unchanged.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
