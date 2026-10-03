# worker.py
import os
import constants
from arq import create_pool
from arq.worker import func

from chessism_api.operations.analysis import (
    run_analysis_job, 
    run_player_analysis_job,
    run_analysis_loop_job,
    run_player_games_analysis_job,
)
from chessism_api.operations.analysis_backups import (
    run_fen_analysis_backup_job,
    run_fen_analysis_restore_job,
)
from chessism_api.operations.ingestion_pipeline.fen_orchestrator import (
    run_fen_pipeline,
)
from chessism_api.operations.ingestion_pipeline.fen_workers import (
    run_fen_generation_job,
    run_fen_insertion_job,
    run_association_insertion_job,
)
from chessism_api.operations.ingestion_pipeline.jobs import (
    run_create_player_games_job,
    run_update_player_games_job,
)
from chessism_api.operations.tablebase import run_tablebase_analysis_job
from chessism_api.operations.player_deletion import run_delete_player_job
from chessism_api.operations.coefficient_research import run_chessism_coefficient_experiment
from chessism_api.operations.player_salience import run_player_salience_job
from chessism_api.operations.database_backups import run_database_backup_job
from chessism_api.operations.database_restore_tests import (
    cleanup_stale_restore_workspaces,
    run_database_restore_test_job,
)

from chessism_api.database.engine import init_db
from chessism_api.redis_client import redis_settings


WORKER_QUEUE = os.environ.get("QUEUE_NAME", "analysis_queue")
print(f"--- [WORKER] Starting up, listening on queue: {WORKER_QUEUE} ---", flush=True)


async def startup(ctx):
    """
    This function is run by arq when the worker starts.
    It initializes the database connection for this process.
    """
    print(f"--- [WORKER] Initializing database connection... ---", flush=True)
    if WORKER_QUEUE == "backup_queue":
        removed = cleanup_stale_restore_workspaces()
        if removed:
            print(
                "--- [WORKER] Removed interrupted restore workspaces: "
                + ", ".join(removed)
                + " ---",
                flush=True,
            )
    if not constants.CONN_STRING:
        raise ValueError("DATABASE_URL environment variable is not set for worker.")
    await init_db(constants.CONN_STRING)
    print(f"--- [WORKER] Database connection initialized. ---", flush=True)
    
    ctx['redis'] = await create_pool(redis_settings)


async def shutdown(ctx):
    """
    Closes the redis pool on shutdown.
    """
    print(f"--- [WORKDEM] Shutting down... ---", flush=True)
    redis = ctx.get('redis')
    if redis:
        await redis.close()
    print(f"--- [WORKER] Shutdown complete. ---", flush=True)


class WorkerSettings:
    """
    Defines the worker's settings.
    ARQ reads this class to know what functions to listen for
    and where to connect.
    """
    
    functions = [
        run_analysis_job, 
        run_player_analysis_job,
        run_analysis_loop_job,
        run_player_games_analysis_job,
        run_fen_generation_job,
        run_fen_pipeline,
        run_fen_insertion_job,
        run_association_insertion_job,
        run_create_player_games_job,
        run_update_player_games_job,
        run_fen_analysis_backup_job,
        run_fen_analysis_restore_job,
        run_tablebase_analysis_job,
        run_delete_player_job,
        run_chessism_coefficient_experiment,
        run_player_salience_job,
        func(run_database_backup_job, timeout=7 * 24 * 60 * 60),
        func(run_database_restore_test_job, timeout=7 * 24 * 60 * 60),
    ]
    
    redis_settings = redis_settings
    
    queue_name = WORKER_QUEUE

    on_startup = startup
    on_shutdown = shutdown

    max_jobs = 1

    job_timeout = 86400
