# worker.py
import os
import constants
from arq.connections import RedisSettings
from arq import create_pool 

# --- Import the actual job functions from your operations ---
# These are the tasks the worker is allowed to run.
from chessism_api.operations.analysis import (
    run_analysis_job, 
    run_player_analysis_job
)
# --- MODIFIED: Import ALL FEN jobs ---
from chessism_api.operations.fens import (
    run_fen_generation_job,
    run_fen_pipeline,
    run_fen_insertion_job,
    run_association_insertion_job
)

# --- NEW: Import the database initializer ---
from chessism_api.database.engine import init_db
# --- NEW: Import Redis client for the boss job ---
from chessism_api.redis_client import redis_settings


# --- NEW: Read queue name from environment ---
# This allows docker-compose to assign different queues to different workers
WORKER_QUEUE = os.environ.get("QUEUE_NAME", "analysis_queue")
print(f"--- [WORKER] Starting up, listening on queue: {WORKER_QUEUE} ---", flush=True)


# --- NEW: Worker startup function ---
async def startup(ctx):
    """
    This function is run by arq when the worker starts.
    It initializes the database connection for this process.
    """
    print(f"--- [WORKER] Initializing database connection... ---", flush=True)
    if not constants.CONN_STRING:
        raise ValueError("DATABASE_URL environment variable is not set for worker.")
    await init_db(constants.CONN_STRING)
    print(f"--- [WORKER] Database connection initialized. ---", flush=True)
    
    # --- NEW: Add a redis pool to the 'boss' worker's context ---
    # This allows run_fen_pipeline to enqueue jobs
    ctx['redis'] = await create_pool(redis_settings)


# --- NEW: Worker shutdown function ---
async def shutdown(ctx):
    """
    Closes the redis pool on shutdown.
    """
    print(f"--- [WORKDEM] Shutting down... ---", flush=True)
    redis = ctx.get('redis')
    if redis:
        await redis.close()
    print(f"--- [WORKER] Shutdown complete. ---", flush=True)


# --- This is the main configuration class for ARQ ---
class WorkerSettings:
    """
    Defines the worker's settings.
    ARQ reads this class to know what functions to listen for
    and where to connect.
    """
    
    # --- MODIFIED: Added all FEN jobs ---
    functions = [
        run_analysis_job, 
        run_player_analysis_job,
        run_fen_generation_job,
        run_fen_pipeline,
        run_fen_insertion_job,
        run_association_insertion_job
    ]
    
    redis_settings = redis_settings
    
    # --- MODIFIED: Use the dynamic queue name ---
    queue_name = WORKER_QUEUE

    # --- NEW: Tell arq to run the startup/shutdown functions ---
    on_startup = startup
    on_shutdown = shutdown

    # --- Force the worker to run only one job at a time ---
    max_jobs = 1

    # --- Set the worker's global timeout to 24 hours ---
    job_timeout = 86400
