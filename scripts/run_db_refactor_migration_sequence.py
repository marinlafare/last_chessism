"""Rebuild and migrate an independent legacy snapshot, with no live access."""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import traceback
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED'] = '1'
os.environ['DATABASE_URL'] = 'postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


def verified_source(baseline, acceptance):
    from scripts.run_db_refactor_candidate import verified_workspace
    status = json.loads((baseline/'status.json').read_text())
    manifest = json.loads((baseline/'manifest.json').read_text())
    workspace = verified_workspace(status, manifest, Path('/restore-test'))
    gate = json.loads((acceptance/'status.json').read_text())
    result = json.loads((acceptance/'acceptance.json').read_text())
    if (status.get('live_database_changed') is not False
        or gate.get('mode') != 'isolated_corpus_finalization' or gate.get('state') != 'succeeded'
        or gate.get('backup_id') != status['backup_id'] or gate.get('rollback_verified') is not True
        or gate.get('private_postgres_stopped') is not True or gate.get('source_unchanged') is not True
        or gate.get('live_database_changed') is not False or result.get('state') != 'succeeded'
        or result.get('atomic_summary_refresh') is not True or result.get('restart_validated') is not True):
        raise ValueError('Need accepted corpus-summary rehearsal for the same baseline backup')
    return workspace, status['backup_id'], manifest['backup']['completed_at']


async def run(args):
    import asyncpg
    from sqlalchemy.ext.asyncio import create_async_engine
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from scripts.db_refactor_e2e_support import source_control
    from scripts.run_db_refactor_release import clone_stopped_source
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.db_refactor_candidate import prepare_reference, build_candidate
    from scripts.db_refactor_tuning import apply_tuning
    from scripts.db_refactor_application_checks import prepare_application_copy
    from scripts.db_refactor_migration_sequence import reset_cloned_shadows, validate_application_copies, finish_private_sequence
    from scripts.db_refactor_switch_rehearsal import exercise_switch
    from scripts.db_refactor_release_rehearsal import private_url, check_runtime
    from scripts.db_refactor_corpus import snapshot, validate_summaries, InjectedFinalizationFailure
    from scripts.db_refactor_workload import digest
    from scripts.fen_migration_progress import ProgressReporter

    if (Path('/var/lib/postgresql/data/PG_VERSION').exists()
        or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount()
        or Path('/var/run/docker.sock').exists()
        or {p.name for p in Path('/sys/class/net').iterdir()} != {'lo'}):
        raise RuntimeError('Migration rehearsal requires network isolation and no live mounts')
    source_workspace, backup_id, snapshot_at = verified_source(args.baseline, args.acceptance)
    if args.output.exists():
        raise RuntimeError('Use a fresh report directory')
    args.output.mkdir(parents=True)
    source = source_workspace/'pgdata'
    workspace = Path('/restore-test')/('db-refactor-baseline-sequence-'+uuid.uuid4().hex[:12])
    pgdata = workspace/'pgdata'
    reporter = ProgressReporter(args.output/'status.json', backup_id, {'migration_kind':'game_player_refactor'})
    reporter.status.update(mode='isolated_migration_sequence',live_database_changed=False,
        production_ready=False,source_workspace=str(source_workspace),workspace=str(workspace),
        snapshot_at=snapshot_at,row_checks=0,steps_completed=0,steps_total=8)
    await reporter.publish('checking_whole_migration_source')
    root = Path(__file__).resolve().parents[1]
    fingerprints = source_fingerprint(root)
    atomic_write_json(args.output/'source-files.json', fingerprints)
    monitor = telemetry = conn = observer = engine = socket = None
    started = False

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    async def stop():
        nonlocal started
        await restore._stop_restored_postgres(pgdata)
        if (pgdata/'postmaster.pid').exists():
            raise RuntimeError('Private PostgreSQL did not stop; workspace retained')
        started = False

    async def start():
        nonlocal started
        started = True
        await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'postgres.log'),
            '-w','-t','300','-o',restore._postgres_start_options(socket),'start')

    try:
        monitor = asyncio.create_task(heartbeat())
        with workspace_lock(Path('/restore-test')) as locked:
            if not locked:
                raise RuntimeError('Private restore volume is already in use')
            original = source_control(source)
            _, raw_size = await restore._run_command_capture('du','-sb',str(source))
            size = int(raw_size.split()[0])
            free = shutil.disk_usage(source).free
            if free < size + 80_000_000_000:
                raise RuntimeError('Need space for independent clone plus 80 GB build/WAL reserve')
            reporter.status.update(source_bytes=size,free_bytes=free)
            workspace.mkdir(mode=0o700)
            socket = restore._create_restore_socket_directory()
            atomic_write_json(workspace/'migration-sequence-owner.json',{'report':str(args.output),'source':str(source),'socket':str(socket)})
            await reporter.publish('copying_whole_migration_database')
            async def copy_progress():
                if pgdata.exists():
                    _, copied = await restore._run_command_capture('du','-sb',str(pgdata))
                    reporter.status['copy_bytes_done'] = min(size,int(copied.split()[0]))
            await clone_stopped_source(source,pgdata,copy_progress)
            reporter.status['copy_bytes_done'] = size
            if source_control(source) != original:
                raise RuntimeError('Source changed during cloning')
            try:
                await start()
                conn = await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='chessism_db',command_timeout=7200)
                observer = await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='chessism_db')
                telemetry = asyncio.create_task(reporter.monitor(observer,conn.get_server_pid(),await conn.fetchval('SELECT pg_current_wal_lsn()::text')))
                await reporter.publish('resetting_shadows_in_new_private_clone')
                reset = await reset_cloned_shadows(conn,pgdata)
                atomic_write_json(args.output/'clone-reset.json',reset)
                reference = await prepare_reference(conn,pgdata)
                atomic_write_json(args.output/'reference.json',reference)
                rows = await build_candidate(conn,pgdata,reporter.publish)
                reporter.status['row_checks'] = len(rows)
                await apply_tuning(conn,pgdata,reporter.publish)
                await prepare_application_copy(conn,pgdata,reporter.publish)
                rows.update(await validate_application_copies(conn,pgdata,reporter.publish))
                reporter.status['row_checks'] = len(rows)
                atomic_write_json(args.output/'row-validation.json',rows)
                telemetry.cancel()
                with suppress(asyncio.CancelledError): await telemetry
                telemetry = None
                await observer.close(); observer = None
                await conn.close(); conn = None
                switch = await exercise_switch(socket,pgdata,reporter.publish)
                atomic_write_json(args.output/'switch-rollback.json',switch)
                url = private_url(socket,'chessism_db')
                engine = create_async_engine(url)
                async with engine.connect() as connection:
                    before = await snapshot(connection)
                await engine.dispose(); engine = None
                try:
                    await finish_private_sequence(socket,pgdata,reporter.publish,inject=True)
                except InjectedFinalizationFailure:
                    pass
                else:
                    raise RuntimeError('Combined switch/summary failure injection was not reached')
                engine = create_async_engine(url)
                async with engine.connect() as connection:
                    if await snapshot(connection) != before:
                        raise RuntimeError('Injected cutover failure changed summary caches')
                await engine.dispose(); engine = None
                reporter.status['rollback_verified'] = True
                result = await finish_private_sequence(socket,pgdata,reporter.publish)
                atomic_write_json(args.output/'migration-result.json',result)
                runtime = await check_runtime(socket,pgdata)
                engine = create_async_engine(url)
                async with engine.connect() as connection:
                    after = await snapshot(connection)
                await engine.dispose(); engine = None
                await reporter.publish('restarting_whole_migration_database')
                await stop(); await start()
                restarted = await check_runtime(socket,pgdata)
                if digest(restarted['contract']) != digest(runtime['contract']):
                    raise RuntimeError('Migrated storage contract changed after restart')
                engine = create_async_engine(url,connect_args={'server_settings':{'default_transaction_read_only':'on','statement_timeout':'7200000'}})
                async with engine.connect() as connection:
                    if await validate_summaries(connection) != result['summaries'] or await snapshot(connection) != after:
                        raise RuntimeError('Migrated summaries changed after restart')
                atomic_write_json(args.output/'restart.json',restarted)
            finally:
                if telemetry:
                    telemetry.cancel()
                    with suppress(asyncio.CancelledError): await telemetry
                if observer: await observer.close()
                if conn: await conn.close()
                if engine: await engine.dispose(); engine = None
                if started: await stop()
                if socket and not (pgdata/'postmaster.pid').exists():
                    socket.rmdir(); socket = None
            if source_control(source) != original:
                raise RuntimeError('Original source was modified')
        if source_fingerprint(root) != fingerprints:
            raise RuntimeError('Application source changed during rehearsal')
        atomic_write_json(args.output/'acceptance.json',{'state':'succeeded','live_database_changed':False,
            'production_ready':False,'fresh_shadow_build':True,'row_checks':len(rows),
            'atomic_cutover_and_summaries':True,'injected_failure_rollback':True,'restart_validated':True,
            'snapshot_at':snapshot_at,'remaining':['Review rehearsal evidence and prepare live deployment runbook.',
                'Fresh tested production backup and explicit maintenance-window approval.'],
            'limitations':['New migration of an older restored snapshot, not the current live database.',
                'No live services deployed, no production tables deleted, no cloud jobs.']})
        await reporter.finish('succeeded','whole_migration_complete',private_postgres_stopped=True,source_unchanged=True)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','whole_migration_failed',error_type=type(error).__name__,failed_phase=reporter.status.get('phase'))
        raise
    finally:
        if socket and not started: socket.rmdir()
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError): await monitor


async def main(args):
    task = asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(run(args),timeout=4*3600)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--acceptance',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(main(parser.parse_args()))
