"""Explicit live cutover, only after a passing full-size isolated rehearsal.

Application/worker clients must already be stopped. The script refuses any
other client connection, reserves the backup lane, and retains the old table.
It never starts/stops application services or expires/deletes backups.
"""
import argparse
import asyncio
import json
import os
import sys
from contextlib import suppress
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
from chessism_api.database.fen_identity_migration import migrate
from scripts.fen_migration_progress import ProgressReporter
from chessism_api.operations.backup_coordination import ensure_backup_reservation, release_backup
from chessism_api.operations.database_restore_tests import latest_backup_for_restore_test
from chessism_api.redis_client import get_redis_pool, close_redis_pool


async def run(args):
    if not args.apply:
        raise RuntimeError('Explicit --apply is required')
    rehearsal = json.loads(args.rehearsal_report.read_text())
    references = args.reference_tables
    kind = 'references' if references else 'continuations'
    comparisons = rehearsal.get('migration', {}).get('comparison', {})
    if references:
        compared = (set(comparisons) == {'game_fen_association', 'player_position_frequency'}
                    and all(c.get('mismatches') == 0 and c.get('original_rows') == c.get('numeric_rows')
                            for c in comparisons.values()))
        validated = rehearsal.get('restore_validation', {}).get('references', {}).get('version') == 'numeric_v1'
    else:
        compared = comparisons.get('mismatches') == 0
        validated = True
    if (rehearsal.get('state') != 'succeeded' or rehearsal.get('mode') != 'isolated_rehearsal'
            or rehearsal.get('migration_kind', 'continuations') != kind
            or rehearsal.get('backup_id') != args.backup_id
            or not compared or not validated
            or rehearsal.get('restore_validation', {}).get('version') != 'numeric_v2'):
        raise RuntimeError('A passing full-size rehearsal for this backup is required')
    backup = latest_backup_for_restore_test()
    if backup['backup_id'] != args.backup_id or not backup.get('restore_tested'):
        raise RuntimeError('The verified rollback backup is no longer the selected recovery point')
    redis = await get_redis_pool()
    job_id = 'live-fen-' + kind + '-' + args.backup_id
    await ensure_backup_reservation(redis, job_id)
    connection = observer = monitor = None
    progress = ProgressReporter(args.report, args.backup_id, rehearsal)

    try:
        dsn = os.environ['DATABASE_URL'].replace('postgresql+asyncpg://', 'postgresql://', 1)
        connection = await asyncpg.connect(dsn)
        if await connection.fetchval('SELECT current_database()') != args.expect_database:
            raise RuntimeError('Database does not match --expect-database')
        clients = await connection.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'")
        if clients:
            raise RuntimeError('Application/database clients remain connected; stop them before the cutover')
        running = await connection.fetchval("SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','failed')")
        if running:
            raise RuntimeError('Cloud jobs are still active')
        await progress.publish('preflight_passed')
        # Open only AFTER the exclusive-client preflight. Its queries cannot
        # modify data, and never touch or lock application relations.
        observer = await asyncpg.connect(dsn, server_settings={
            'application_name': 'chessism-fen-migration-progress',
            'default_transaction_read_only': 'on', 'statement_timeout': '2000', 'lock_timeout': '500'})
        initial_lsn = await observer.fetchval('SELECT pg_current_wal_lsn()::text')
        monitor = asyncio.create_task(progress.monitor(observer, connection.get_server_pid(), initial_lsn))
        migration = migrate
        if references:
            from chessism_api.database.fen_reference_migration import migrate as migration
        result = await migration(connection, progress.publish)
        monitor.cancel()
        with suppress(asyncio.CancelledError):
            await monitor
        monitor = None
        await progress.finish('succeeded', 'migration_complete_awaiting_application_deployment', migration=result)
    except BaseException as error:
        await progress.finish('failed', 'migration_failed_application_must_stay_stopped', error_type=type(error).__name__)
        raise
    finally:
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if observer is not None:
            await observer.close()
        if connection is not None:
            await connection.close()
        await release_backup(redis, job_id)
        await close_redis_pool()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--reference-tables', action='store_true', help='Migrate game associations and player frequencies together')
    parser.add_argument('--backup-id', required=True)
    parser.add_argument('--expect-database', required=True)
    parser.add_argument('--rehearsal-report', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
