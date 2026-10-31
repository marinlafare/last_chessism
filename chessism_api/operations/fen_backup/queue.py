"""FEN archives must never occupy the ingestion coordinator's queue."""
FEN_BACKUP_QUEUE = 'fen_backup_queue'
FEN_BACKUP_TIMEOUT = 7 * 24 * 60 * 60
