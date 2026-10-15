"""Resource bounds shared by the HTTP service and dedicated export worker."""
import os
from pathlib import Path

ROOT = Path(os.environ.get('CHESSISM_EXPORT_ROOT', '/research-data/exports')).resolve()
MAX_ROWS = 5_000_000
MAX_COLUMNS = 48
BATCH_ROWS = 2048
MAX_BYTES = int(os.environ.get('CHESSISM_EXPORT_MAX_BYTES', 2 * 1024**3))
FREE_FLOOR = int(os.environ.get('CHESSISM_EXPORT_FREE_FLOOR_BYTES', 20_000_000_000))
TIMEOUT = int(os.environ.get('CHESSISM_EXPORT_TIMEOUT_SECONDS', 900))
RETENTION = int(os.environ.get('CHESSISM_EXPORT_RETENTION_SECONDS', 86400))
QUEUE = 'export_queue'
TERMINAL = ('complete', 'failed', 'cancelled', 'expired', 'deleted')
