"""Restore a verified recovery point and rehearse FEN-ID migration off-line.

Run inside a one-off worker-backup container, with current source mounted at
/app and a writable report directory. No live database connection is made.
"""
import argparse
import asyncio
import json
import shutil
import sys
import uuid
from contextlib import suppress
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.fen_identity_migration import migrate
from chessism_api.operations import database_restore_tests as restore
from chessism_api.operations.backup_coordination import ensure_backup_reservation, release_backup
from scripts.fen_migration_progress import ProgressReporter, database_progress
from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
from chessism_api.operations.restore_workspace import workspace_lock, remove_workspace
from chessism_api.operations.fen_backup.database import export_batches, restore_batch
from chessism_api.redis_client import get_redis_pool, close_redis_pool


async def run(args):
    if not restore._SAFE_BACKUP_ID.fullmatch(args.backup_id):
        raise ValueError('Invalid backup identifier')
    backup = restore.latest_backup_for_restore_test()
    if backup['backup_id'] != args.backup_id or not backup.get('restore_tested'):
        raise RuntimeError('The selected latest backup must already have passed a restore test')
    kind = 'references' if args.reference_tables else 'continuations'
    # Each attempt owns its own lock token and workspace. Failed restored
    # copies remain available for diagnosis and are never overwritten on retry.
    job_id = 'fen-' + kind + '-' + args.backup_id + '-' + uuid.uuid4().hex[:8]
    redis = await get_redis_pool()
    await ensure_backup_reservation(redis, job_id)
    reporter = ProgressReporter(args.report, args.backup_id, {'migration_kind': kind})
    status = reporter.status
    status['mode'] = 'isolated_rehearsal'
    publish = reporter.publish

    workspace = restore.RESTORE_TEST_ROOT / ('rehearsal-' + job_id)
    status.update(job_id=job_id, workspace_name=workspace.name)
    pgdata, socket = workspace / 'pgdata', workspace / 'socket'
    connection = observer = None
    initial_lsn = None
    owned = False

    async def heartbeat():
        while True:
            phase = status.get('phase')
            if observer is not None and connection is not None and initial_lsn is not None:
                try:
                    snapshot = await database_progress(observer, connection.get_server_pid(), initial_lsn)
                    if status.get('phase') == phase:
                        status['database'] = snapshot
                        status.pop('monitor_error', None)
                except Exception as error:
                    status['monitor_error'] = type(error).__name__
            reporter.flush()
            await asyncio.sleep(5)

    monitor = asyncio.create_task(heartbeat())
    try:
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Restore workspace is busy')
            free = shutil.disk_usage(restore.RESTORE_TEST_ROOT).free
            required = int(backup['database_bytes']) + 80_000_000_000
            if free < required:
                raise RuntimeError('Insufficient free space for restored database, new tables, indexes and WAL')
            workspace.mkdir(mode=0o700)
            owned = True
            pgdata.mkdir(mode=0o700); socket.mkdir(mode=0o700)
            await publish('restoring_verified_backup', required_free_bytes=required, free_bytes=free)
            await restore._run_restore(backup_id=args.backup_id, pgdata=pgdata, redis=None, job_id=job_id)
            await publish('starting_isolated_postgres')
            await restore._run_command_capture(restore.PG_CTL, '-D', str(pgdata), '-l', str(workspace / 'postgres.log'),
                '-w', '-t', '300', '-o', restore._postgres_start_options(socket), 'start')
            connection = await asyncpg.connect(host=str(socket), port=55432, user=restore.RESTORE_USER, database=restore.RESTORE_DATABASE)
            await publish('waiting_for_recovery')
            await wait_for_recovery_completion(connection)
            await publish('checking_restored_database')
            observer = await asyncpg.connect(host=str(socket), port=55432, user=restore.RESTORE_USER,
                database=restore.RESTORE_DATABASE, server_settings={'default_transaction_read_only':'on',
                'statement_timeout':'2000','lock_timeout':'500','application_name':'fen-reference-rehearsal-progress'})
            initial_lsn = await observer.fetchval('SELECT pg_current_wal_lsn()::text')
            probe = json.loads(await connection.fetchval(restore._validation_query()))
            restore._validate_probe(probe)
            migration = migrate
            if args.reference_tables:
                from chessism_api.database.fen_reference_migration import migrate as migration
            result = await migration(connection, publish)
            await publish('testing_portable_backup_roundtrip', migration=result)
            engine = create_async_engine(URL.create('postgresql+asyncpg', username=restore.RESTORE_USER,
                                                   host=str(socket), port=55432, database=restore.RESTORE_DATABASE))
            AsyncDBSession.configure(bind=engine)
            try:
                async with AsyncDBSession() as session, session.begin():
                    batches = export_batches(session)
                    try:
                        records = await anext(batches, [])
                    finally:
                        await batches.aclose()
                assert all('fen_id' not in record and '_fen_id' not in record for record in records)
                saved, missing = await restore_batch(records)
                if saved != len(records) or missing:
                    raise RuntimeError('Portable backup roundtrip did not restore every sampled position')
                # Restoring exactly the exported values must leave alternatives
                # identical even though continuation row IDs may be regenerated.
                from sqlalchemy import select
                from chessism_api.database.models import Fen, FenContinuation
                async with AsyncDBSession() as session:
                    rows = (await session.execute(select(Fen.fen, FenContinuation.rank, FenContinuation.move, FenContinuation.score)
                        .join(FenContinuation, FenContinuation.fen_id == Fen.fen_id)
                        .where(Fen.fen.in_([r['fen'] for r in records])))).all()
                expected = {(r['fen'], c['rank'], c['move'], c['score']) for r in records for c in r['continuations']}
                if set(tuple(r) for r in rows) != expected:
                    raise RuntimeError('Portable backup changed continuation values')
            finally:
                await engine.dispose()
            await publish('checking_restored_identity', portable_roundtrip_positions=saved)
            expected = {'fen_identity_version': 'numeric_v2'}
            if args.reference_tables:
                expected['fen_reference_version'] = 'numeric_v1'
            status['restore_validation'] = await restore._validate_fen_identity(socket, expected)
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
            await observer.close(); observer = None
            await connection.close(); connection = None
            await restore._stop_restored_postgres(pgdata)
            if not remove_workspace(restore.RESTORE_TEST_ROOT, workspace):
                raise RuntimeError('Rehearsal cluster did not stop; files retained')
            owned = False
            status['state'] = 'succeeded'
            await publish('rehearsal_complete')
    except BaseException as error:
        status.update(state='failed', error_type=type(error).__name__, failed_phase=status.get('phase'))
        # Never persist driver errors, DSNs, or sampled record values.
        await publish('rehearsal_failed')
        raise
    finally:
        monitor.cancel()
        with suppress(asyncio.CancelledError):
            await monitor
        if observer is not None:
            await observer.close()
        if connection is not None:
            await connection.close()
        if owned:
            await restore._stop_restored_postgres(pgdata)
        await release_backup(redis, job_id)
        await close_redis_pool()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-id', required=True)
    parser.add_argument('--reference-tables', action='store_true', help='Rehearse game associations and player frequencies together')
    parser.add_argument('--report', type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
