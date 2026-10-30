"""Dedicated serial FEN archive worker; existing backup ownership still applies."""
from arq.worker import func
from chessism_api.operations.analysis_backups import run_fen_analysis_backup_job, run_fen_analysis_restore_job
from chessism_api.operations.worker_runtime import startup, shutdown
from chessism_api.redis_client import redis_settings
from .queue import FEN_BACKUP_QUEUE, FEN_BACKUP_TIMEOUT


class WorkerSettings:
    functions = [func(run_fen_analysis_backup_job, timeout=FEN_BACKUP_TIMEOUT),
                 func(run_fen_analysis_restore_job, timeout=FEN_BACKUP_TIMEOUT)]
    queue_name = FEN_BACKUP_QUEUE
    redis_settings = redis_settings
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 1
    job_timeout = FEN_BACKUP_TIMEOUT
