"""Non-blocking, read-only telemetry for the explicit FEN migration."""
import asyncio
import json
import time
from datetime import datetime, timezone

from chessism_api.operations.backup_files import atomic_write_json

STAGES = (
    ('counting_original_positions', 'Count original positions'),
    ('adding_fen_identifiers', 'Add numeric FEN IDs / rebuild parent indexes'),
    ('copying_continuations', 'Copy continuations to numeric references'),
    ('indexing_numeric_continuations', 'Build indexes and validate foreign key'),
    ('verifying_every_continuation', 'Compare every continuation with the original'),
    ('switching_tables', 'Switch tables atomically'),
    ('analyzing_statistics', 'Refresh database statistics'),
)

REFERENCE_STAGES = (
    ('copying_game_associations', 'Copy game/position links'),
    ('copying_player_frequencies', 'Copy player position frequencies'),
    ('indexing_numeric_references', 'Build numeric indexes'),
    ('validating_foreign_keys', 'Validate game, player and FEN references'),
    ('comparing_all_reference_rows', 'Compare every row with the original'),
    ('swapping_reference_tables', 'Switch both tables atomically'),
    ('checking_numeric_reference_identity', 'Check constraints and sequence'),
    ('analyzing_reference_tables', 'Refresh database statistics'),
)


async def database_progress(connection, pid, initial_lsn):
    # Catalog/statistics only: never count or lock the large application tables.
    activity = await connection.fetchrow("""
      SELECT state, wait_event_type, wait_event,
        extract(epoch FROM clock_timestamp()-query_start)::float8 AS query_seconds,
        CASE WHEN query LIKE '%ADD COLUMN fen_id%' THEN 'Adding IDs and rebuilding the FEN table'
             WHEN query LIKE '%ADD CONSTRAINT%UNIQUE%' THEN 'Building a unique index'
             WHEN query LIKE '%ADD CONSTRAINT%PRIMARY KEY%' THEN 'Building primary-key index'
             WHEN query LIKE '%VALIDATE CONSTRAINT%' THEN 'Validating foreign-key references'
             WHEN ltrim(query) LIKE 'INSERT%' THEN 'Copying rows'
             WHEN ltrim(query) LIKE 'SELECT%' THEN 'Counting / verifying data'
             WHEN ltrim(query) LIKE 'ANALYZE%' THEN 'Collecting table statistics'
             ELSE 'Migration statement' END AS operation
      FROM pg_stat_activity WHERE pid=$1
    """, pid)
    index = await connection.fetchrow("""
      SELECT phase,blocks_done,blocks_total,tuples_done,tuples_total
      FROM pg_stat_progress_create_index WHERE pid=$1
    """, pid)
    analyze = await connection.fetchrow("""
      SELECT phase,sample_blks_scanned AS blocks_done,sample_blks_total AS blocks_total
      FROM pg_stat_progress_analyze WHERE pid=$1
    """, pid)
    wal = await connection.fetchval(
        'SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),$1::text::pg_lsn)::bigint', initial_lsn)
    return {'activity': dict(activity) if activity else {},
            'counters': dict(index or analyze) if index or analyze else None,
            'wal_bytes_since_start': wal}


class ProgressReporter:
    def __init__(self, path, backup_id, rehearsal):
        self.path = path
        self.started = self.phase_started = time.monotonic()
        self.reference = {p['phase']: p['seconds'] for p in rehearsal.get('phases', [])}
        self.status = {'state': 'running', 'mode': 'live_migration', 'backup_id': backup_id,
                       'started_at': datetime.now(timezone.utc).isoformat(), 'phases': [],
                       'migration_kind': rehearsal.get('migration_kind', 'continuations')}

    def flush(self):
        now = time.monotonic()
        self.status.update(elapsed_seconds=round(now-self.started, 1),
                           phase_elapsed_seconds=round(now-self.phase_started, 1),
                           updated_at=datetime.now(timezone.utc).isoformat())
        atomic_write_json(self.path, self.status)

    async def publish(self, phase, **detail):
        now = time.monotonic()
        if self.status.get('phase'):
            self.status['phases'].append({'phase': self.status['phase'],
                                         'seconds': round(now-self.phase_started, 3)})
        self.phase_started = now
        self.status.update(phase=phase, database=None,
                           rehearsal_stage_seconds=self.reference.get(phase), **detail)
        self.flush()
        print(json.dumps({'phase': phase, 'elapsed_seconds': self.status['elapsed_seconds']}), flush=True)

    async def monitor(self, connection, pid, initial_lsn):
        while True:
            phase = self.status.get('phase')
            try:
                snapshot = await database_progress(connection, pid, initial_lsn)
                if self.status.get('phase') == phase:
                    self.status['database'] = snapshot
                    self.status.pop('monitor_error', None)
            except Exception as error:
                # A failed stats sample must not cancel an in-flight migration.
                self.status.update(database=None, monitor_error=type(error).__name__)
            self.flush()
            await asyncio.sleep(5)

    async def finish(self, state, phase, **detail):
        self.status['state'] = state
        await self.publish(phase, **detail)
