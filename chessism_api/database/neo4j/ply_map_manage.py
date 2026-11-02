"""Additive installation only; never starts a summary run."""
import asyncio
import os
from chessism_api.database.postgresql.runtime import bind_runtime_engine
from chessism_api.database.postgresql.engine import AsyncDBSession
from .ply_map_store import install, status


async def main():
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with AsyncDBSession() as session,session.begin():
            await install(session)
            print(await status(session))
    finally:
        await engine.dispose()


if __name__=='__main__':
    asyncio.run(main())
