# chessism_api/redis_client.py
import os
from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
redis_settings = RedisSettings(host=REDIS_HOST, port=6379)

# This will be our shared client pool
redis_pool: ArqRedis = None

async def get_redis_pool() -> ArqRedis:
    """
    Returns the shared ArqRedis client pool.
    """
    global redis_pool
    if not redis_pool:
        redis_pool = await create_pool(redis_settings)
    return redis_pool

async def close_redis_pool():
    """
    Closes the shared client pool.
    """
    if redis_pool:
        await redis_pool.close()