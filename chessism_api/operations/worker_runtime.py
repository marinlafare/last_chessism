"""No-DDL database lifecycle for independently deployed queue workers."""
import os
from chessism_api.database.runtime import bind_runtime_engine


async def startup(ctx):
    ctx['database_engine'] = await bind_runtime_engine(os.environ['DATABASE_URL'])


async def shutdown(ctx):
    engine = ctx.pop('database_engine', None)
    if engine is not None:
        await engine.dispose()
