"""Refresh/rollback/restart gate on a NEW isolated canonical database copy.

No live target, application deployment, queue consumers or cloud networking.
"""
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


def verify_acceptance(path, source):
    status = json.loads((path/'status.json').read_text())
    acceptance = json.loads((path/'acceptance.json').read_text())
    if (status.get('mode') != 'isolated_http_backup' or status.get('state') != 'succeeded'
        or status.get('source_workspace') != str(source) or status.get('live_database_changed') is not False
        or status.get('private_postgres_stopped') is not True or status.get('source_unchanged') is not True
        or status.get('completed_jobs') != 3 or acceptance.get('state') != 'succeeded'
        or acceptance.get('real_arq_jobs') != 3 or acceptance.get('http_checks', 0) < 18):
        raise ValueError('Need successful HTTP/queue/restore acceptance of this canonical source')


async def run(args):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from scripts.db_refactor_e2e_support import verified_source, source_control
    from scripts.run_db_refactor_release import clone_stopped_source
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.db_refactor_release_rehearsal import private_url, rejected_startup, check_runtime
    from scripts.db_refactor_corpus import finalize, snapshot, validate_summaries, InjectedFinalizationFailure
    from scripts.db_refactor_workload import digest
    from scripts.fen_migration_progress import ProgressReporter

    if (Path('/var/lib/postgresql/data/PG_VERSION').exists()
        or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount()
        or Path('/var/run/docker.sock').exists()
        or {p.name for p in Path('/sys/class/net').iterdir()} != {'lo'}):
        raise RuntimeError('Corpus rehearsal requires network isolation and no live mounts')
    source_workspace, backup_id, contract = verified_source(args.release)
    verify_acceptance(args.acceptance, source_workspace)
    if args.output.exists():
        raise RuntimeError('Use a fresh report directory')
    args.output.mkdir(parents=True)
    source = source_workspace/'pgdata'
    workspace = Path('/restore-test')/('db-refactor-baseline-corpus-'+uuid.uuid4().hex[:12])
    pgdata = workspace/'pgdata'
    reporter = ProgressReporter(args.output/'status.json', backup_id, {})
    reporter.status.update(mode='isolated_corpus_finalization',live_database_changed=False,
        production_ready=False,source_workspace=str(source_workspace),workspace=str(workspace),steps_completed=0,steps_total=8)
    await reporter.publish('checking_corpus_rehearsal_source')
    root = Path(__file__).resolve().parents[1]
    fingerprints = source_fingerprint(root)
    atomic_write_json(args.output/'source-files.json', fingerprints)
    monitor = engine = socket = None
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
            _, size_text = await restore._run_command_capture('du','-sb',str(source))
            size = int(size_text.split()[0])
            if shutil.disk_usage(source).free < size + 50_000_000_000:
                raise RuntimeError('Need space for independent clone plus 50 GB working reserve')
            reporter.status['source_bytes'] = size
            workspace.mkdir(mode=0o700)
            socket = restore._create_restore_socket_directory()
            atomic_write_json(workspace/'corpus-rehearsal-owner.json', {'report':str(args.output),'socket':str(socket)})
            await reporter.publish('copying_corpus_rehearsal_database')
            async def copy_progress():
                if pgdata.exists():
                    _, done = await restore._run_command_capture('du','-sb',str(pgdata))
                    reporter.status['copy_bytes_done'] = min(size,int(done.split()[0]))
            await clone_stopped_source(source,pgdata,copy_progress)
            if source_control(source) != original:
                raise RuntimeError('Accepted source changed during cloning')
            try:
                await start()
                url = private_url(socket,'chessism_db')
                engine = create_async_engine(url,isolation_level='REPEATABLE READ',connect_args={
                    'server_settings':{'statement_timeout':'7200000','application_name':'private-corpus-finalization'}})
                async with engine.connect() as connection:
                    from scripts.db_refactor_corpus import check_private_corpus
                    async with connection.begin():
                        await check_private_corpus(connection,pgdata)
                        await connection.execute(text("UPDATE public.relational_storage_release SET application_ready=false WHERE rehearsal_only"))
                    await rejected_startup(url,'relational_v2','release has not been approved')
                    reporter.status['unapproved_startup_rejected'] = True
                    async with connection.begin():
                        before = await snapshot(connection)
                    atomic_write_json(args.output/'before-summaries.json',before)
                    await reporter.publish('testing_summary_failure_rollback')
                    try:
                        async with connection.begin():
                            await finalize(connection,pgdata,reporter.publish,inject=True)
                    except InjectedFinalizationFailure:
                        pass
                    else:
                        raise RuntimeError('Rollback injection did not execute')
                    async with connection.begin():
                        if await snapshot(connection) != before:
                            raise RuntimeError('Summary cache changed after injected failure rollback')
                    reporter.status['rollback_verified'] = True
                    atomic_write_json(args.output/'rollback.json',{'injected_failure':True,'summary_fingerprints_unchanged':True})
                    async with connection.begin():
                        result = await finalize(connection,pgdata,reporter.publish)
                        # Approval applies only to this private rehearsal copy,
                        # in the SAME commit as every validated summary.
                        await connection.execute(text("""UPDATE public.relational_storage_release
                            SET application_ready=true WHERE version='relational_v2' AND rehearsal_only"""))
                    async with connection.begin():
                        after = await snapshot(connection)
                    atomic_write_json(args.output/'corpus-validation.json',result)
                    atomic_write_json(args.output/'after-summaries.json',after)
                await engine.dispose(); engine = None
                await reporter.publish('restarting_private_finalized_database')
                await stop(); await start()
                runtime = await check_runtime(socket,pgdata)
                if digest(runtime['contract']) != digest(contract):
                    raise RuntimeError('Summary refresh changed the accepted schema contract')
                engine = create_async_engine(url,connect_args={'server_settings':{'default_transaction_read_only':'on','statement_timeout':'7200000'}})
                async with engine.connect() as connection:
                    restarted = await validate_summaries(connection)
                    if restarted != result or await snapshot(connection) != after:
                        raise RuntimeError('Finalized summaries changed after private restart')
                atomic_write_json(args.output/'restart.json',{'summary_fingerprints_unchanged':True,
                    'validation':restarted,'runtime_checks':runtime['checks']})
            finally:
                if engine:
                    await engine.dispose(); engine = None
                if started:
                    await stop()
                if not (pgdata/'postmaster.pid').exists() and socket:
                    socket.rmdir(); socket = None
            if source_control(source) != original:
                raise RuntimeError('Accepted source was modified')
        if fingerprints != source_fingerprint(root):
            raise RuntimeError('Application source changed during rehearsal')
        atomic_write_json(args.output/'acceptance.json',{'state':'succeeded','live_database_changed':False,
            'production_ready':False,'atomic_summary_refresh':True,'injected_failure_rollback':True,
            'restart_validated':True,'remaining':['Final fresh-source copy/cutover orchestration and review.',
                'Fresh tested production backup and explicit maintenance-window approval.'],
            'limitations':['Uses an accepted private canonical copy, not a fresh live migration.',
                'No API/worker deployment; no queue consumers or cloud jobs.']})
        await reporter.finish('succeeded','corpus_finalization_complete',private_postgres_stopped=True,source_unchanged=True)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','corpus_finalization_failed',error_type=type(error).__name__,failed_phase=reporter.status.get('phase'))
        raise
    finally:
        if socket and not started:
            socket.rmdir()
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
    parser.add_argument('--release',type=Path,required=True)
    parser.add_argument('--acceptance',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(main(parser.parse_args()))
