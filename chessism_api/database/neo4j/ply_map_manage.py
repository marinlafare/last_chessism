"""Read canonical map status; schema initialization is a separate operation."""
import asyncio
import os
from chessism_api.database.postgresql.runtime import bind_runtime_engine
from chessism_api.database.postgresql.engine import AsyncDBSession
from .ply_map_store import status
from chessism_api.database.postgresql.transactions import read_transaction


async def main():
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with read_transaction(AsyncDBSession) as session:
            print(await status(session))
    finally:
        await engine.dispose()


if __name__=='__main__':
    asyncio.run(main())
