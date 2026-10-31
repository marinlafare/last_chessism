"""Inventory cutover dependencies on the accepted PRIVATE online-restored copy."""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
import sys
import traceback
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


def verified_source(application, root):
    status=json.loads((application/'status.json').read_text())
    acceptance=json.loads((application/'acceptance.json').read_text())
    target=Path(status['restored_workspace'])
    if (status.get('mode')!='isolated_application'
        or status.get('phase')!='application_rehearsal_complete'
        or status.get('state') not in ('succeeded','needs_review')
        or status.get('live_database_changed') is not False
        or acceptance.get('application_write_checks')!='passed'
        or acceptance.get('online_backup_worker_and_wal_restore')!='passed'
        or target.parent!=root or not target.name.startswith('db-refactor-baseline-online-')
        or target.is_symlink() or target.resolve()!=target or str(target)==status.get('workspace')):
        raise ValueError('Need the distinct accepted private online-restored copy')
    return target,status['backup_id']


async def run(args):
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.db_refactor_cutover import inspect_cutover
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from scripts.fen_migration_progress import ProgressReporter

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount():
        raise RuntimeError('Live database, socket and existing backups must not be mounted')
    workspace,backup_id=verified_source(args.application,restore.RESTORE_TEST_ROOT)
    if args.output.exists():
        raise RuntimeError('Use a new report directory')
    args.output.mkdir(parents=True)
    reporter=ProgressReporter(args.output/'status.json',backup_id,{})
    reporter.status.update(mode='isolated_cutover_preflight',live_database_changed=False,
        production_ready=False,workspace=str(workspace),source_application=str(args.application),
        rehearse_switch=args.rehearse_switch)
    await reporter.publish('checking_cutover_inventory_workspace')
    fingerprints=source_fingerprint(Path(__file__).resolve().parents[1])
    atomic_write_json(args.output/'source-files.json',fingerprints)
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    redis=conn=monitor=None
    reserved=started=False
    job_id='db-refactor-preflight-'+uuid.uuid4().hex[:12]
    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    try:
        redis=await get_redis_pool()
        await ensure_backup_reservation(redis,job_id)
        reserved=True
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Rehearsal workspace is busy')
            if not (pgdata/'PG_VERSION').exists() or (pgdata/'postmaster.pid').exists():
                raise RuntimeError('Private copy must exist and be stopped')
            try:
                started=True
                await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'cutover-preflight-postgres.log'),
                    '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
                conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,
                    database=restore.RESTORE_DATABASE,server_settings={'application_name':'cutover-preflight','statement_timeout':'60000'})
                await wait_for_recovery_completion(conn)
                await reporter.publish('inspecting_cutover_dependencies')
                async with conn.transaction(readonly=True,isolation='repeatable_read'):
                    inventory=await inspect_cutover(conn,pgdata)
                atomic_write_json(args.output/'dependencies.json',inventory)
                reporter.status.update(relations=len(inventory['relations']),foreign_keys=len(inventory['foreign_keys']),
                    triggers=len(inventory['triggers']),dependent_views=len(inventory['dependent_views']),
                    sequences=len(inventory['sequences']),functions=len(inventory['functions']))
                if args.rehearse_switch:
                    await conn.close();conn=None
                    from scripts.db_refactor_switch_rehearsal import exercise_switch
                    rehearsal=await exercise_switch(socket,pgdata,reporter.publish)
                    atomic_write_json(args.output/'switch-rehearsal.json',rehearsal)
                    reporter.status.update(switch_checks=len(rehearsal['checks']),switch_rolled_back=True)
            finally:
                if conn:
                    await conn.close();conn=None
                if started:
                    await restore._stop_restored_postgres(pgdata)
                    if (pgdata/'postmaster.pid').exists():
                        raise RuntimeError('Private PostgreSQL did not stop')
                    started=False
        if fingerprints!=source_fingerprint(Path(__file__).resolve().parents[1]):
            raise RuntimeError('Source changed during cutover rehearsal')
        await reporter.finish('succeeded','cutover_switch_rehearsal_complete' if args.rehearse_switch else 'cutover_inventory_complete',production_ready=False)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
                      for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','cutover_inventory_failed',error_type=type(error).__name__,
                              failed_phase=reporter.status.get('phase'))
        raise RuntimeError('Private inventory failed; see report') from None
    finally:
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError): await monitor
        if reserved: await release_backup(redis,job_id)
        if redis: await close_redis_pool()


async def main(args):
    task=asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(run(args),timeout=15*60)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--application',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--rehearse-switch',action='store_true',help='Exercise the switch on the private copy, always rolling back; never commits')
    asyncio.run(main(parser.parse_args()))
