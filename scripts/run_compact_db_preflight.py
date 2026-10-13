"""Restore one verified compressed snapshot and prove proposed changes offline.

Network must be disabled. Mount backups read-only, not PGDATA/live sockets.
The restored database is stopped and retained for the candidate rehearsal.
"""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


async def run(args):
    # Block accidental default app connections, including dotenv loading.
    os.environ['DATABASE_URL'] = 'postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'
    os.environ['PYTHON_DOTENV_DISABLED'] = '1'
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.database_snapshot_restore import prepare_cluster, restore_archive
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.operations.backup_files import atomic_write_json
    from scripts.fen_migration_progress import ProgressReporter
    from scripts.compact_db_preflight import prove_data, require_private

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Production data/socket must not be mounted')
    backup = restore.latest_backup_for_restore_test()
    if (backup['backup_id'] != args.backup_id or not backup.get('restore_tested')
            or backup.get('restore_test_status') != 'success'):
        raise RuntimeError('Select the latest restore-tested snapshot explicitly')
    if args.output.exists():
        raise RuntimeError('Reports must use a new directory')
    args.output.mkdir(parents=True, mode=0o755)
    manifest = json.loads((restore.MANIFEST_DIRECTORY / (args.backup_id + '.json')).read_text())
    workspace = restore.RESTORE_TEST_ROOT / ('compact-db-' + uuid.uuid4().hex[:12])
    pgdata, socket = workspace / 'pgdata', workspace / 'socket'
    reporter = ProgressReporter(args.output / 'status.json', args.backup_id, {})
    reporter.status.update(mode='isolated_compact_preflight', live_database_changed=False,
                           workspace=str(workspace), ready_for_live_migration=False)
    connection = heartbeat = None
    async def pulse():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    async def detail(message):
        reporter.status['detail'] = message
        reporter.flush()
    with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
        if not acquired:
            raise RuntimeError('Another restore/rehearsal owns the workspace')
        try:
            required = int(backup['database_bytes']) * 2 + 25_000_000_000
            free = shutil.disk_usage(restore.RESTORE_TEST_ROOT).free
            if free < required:
                raise RuntimeError(f'Need {required} free bytes for restore and later compaction')
            for directory in (workspace, pgdata, socket):
                directory.mkdir(mode=0o700)
            heartbeat = asyncio.create_task(pulse())
            await reporter.publish('verifying_and_initializing_snapshot', free_bytes=free)
            await prepare_cluster(backup, manifest, pgdata, detail)
            await restore._run_command_capture(restore.PG_CTL, '-D', str(pgdata), '-l',
                str(workspace / 'postgres.log'), '-w', '-t', '300', '-o',
                restore._postgres_start_options(socket), 'start')
            await reporter.publish('restoring_snapshot')
            await restore_archive(args.backup_id, socket, pgdata, detail)
            connection = await asyncpg.connect(host=str(socket), port=55432,
                user=restore.RESTORE_USER, database=restore.RESTORE_DATABASE)
            await require_private(connection, pgdata)
            await reporter.publish('analyzing_restored_tables')
            for table in ('game_fen_association','fen','fen_continuation','moves','player'):
                await connection.execute(f'ANALYZE public.{table}')
            proof = await prove_data(connection, pgdata, reporter.publish)
            atomic_write_json(args.output / 'proofs.json', proof)
            await reporter.finish('succeeded', 'structural_preflight_complete',
                detail='Every-row proofs passed. Private database retained; application/candidate tests still required.')
        except BaseException as error:
            await reporter.finish('failed', 'structural_preflight_failed', error_type=type(error).__name__, detail=str(error))
            raise
        finally:
            if heartbeat:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
            if connection:
                await connection.close()
            await restore._stop_restored_postgres(pgdata)
            reporter.status['private_postgres_stopped'] = not (pgdata / 'postmaster.pid').exists()
            reporter.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-id', required=True)
    parser.add_argument('--output', required=True, type=Path)
    asyncio.run(run(parser.parse_args()))
