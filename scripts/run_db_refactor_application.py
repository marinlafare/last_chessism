"""Production-sized APPLICATION rehearsal, private online backups, no live cutover."""
import argparse
import asyncio
from contextlib import suppress
import json
import hashlib
import os
from pathlib import Path
import shutil
import signal
import sys
import traceback
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


def source_fingerprint(root):
    hashes={}
    for directory in ('chessism_api','scripts'):
        for path in sorted((root/directory).rglob('*.py')):
            hashes[str(path.relative_to(root))]=hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ('main.py','worker.py','docker-compose.yml'):
        hashes[name]=hashlib.sha256((root/name).read_bytes()).hexdigest()
    return hashes


def verified_application_source(report, baseline, candidate, root):
    """Use only the restored copy from our accepted offline roundtrip."""
    status=json.loads((report/'status.json').read_text())
    acceptance=json.loads((report/'acceptance.json').read_text())
    backup=json.loads((report/'backup-roundtrip.json').read_text())
    workspace=Path(status['restored_workspace'])
    if (status.get('state')!='succeeded' or status.get('mode')!='isolated_write_validation'
        or status.get('backup_id')!=baseline['backup']['backup_id']
        or status.get('live_database_changed') is not False
        or status.get('source_comparison')!=str(candidate)
        or acceptance.get('offline_pgbackrest_roundtrip')!='passed'
        or acceptance.get('write_contracts')!='passed'
        or backup.get('restored_pgdata')!=str(workspace/'pgdata')
        or workspace.parent!=root or not workspace.name.startswith('db-refactor-baseline-verify-')
        or workspace.resolve()!=workspace or workspace.is_symlink()
        or workspace==Path(status['workspace'])):
        raise ValueError('Need an accepted, distinct private restored application copy')
    return workspace


async def run(args):
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.run_db_refactor_candidate import verified_workspace,verified_retune
    from scripts.db_refactor_candidate import guard
    from scripts.db_refactor_application_checks import prepare_application_copy,measure_application_reads,application_writes
    from scripts.db_refactor_online_backup import OnlineApplicationBackup
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from scripts.fen_migration_progress import ProgressReporter

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount():
        raise RuntimeError('Do not mount the live database, socket, or production backup repository')
    baseline=json.loads((args.baseline/'manifest.json').read_text())
    original=verified_workspace(json.loads((args.baseline/'status.json').read_text()),baseline,restore.RESTORE_TEST_ROOT)
    candidate=verified_retune(args.candidate,baseline,original)
    if candidate['status']['state']!='succeeded' or candidate['status'].get('slowdown_flags')!=0:
        raise RuntimeError('The previous numeric read rehearsal must have passed')
    workspace=verified_application_source(args.write_validation,baseline,args.candidate,restore.RESTORE_TEST_ROOT)
    if os.environ.get('BENCHMARK_RESOURCE_PROFILE')!=baseline['resource_profile']:
        raise RuntimeError('Match the prior rehearsal resource profile')
    if args.output.exists():
        raise RuntimeError('Never overwrite an existing report directory')
    args.output.mkdir(mode=0o755,parents=True)
    token=uuid.uuid4().hex[:12]
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    work=restore.RESTORE_TEST_ROOT/('db-refactor-acceptance-'+token)
    restored=restore.RESTORE_TEST_ROOT/('db-refactor-baseline-online-'+token)/'pgdata'
    reporter=ProgressReporter(args.output/'status.json',baseline['backup']['backup_id'],{})
    reporter.status.update(mode='isolated_application',live_database_changed=False,production_ready=False,
        workspace=str(workspace),restored_workspace=str(restored.parent),repository_workspace=str(work),
        source_comparison=str(args.candidate),resource_profile=baseline['resource_profile'])
    await reporter.publish('checking_application_rehearsal_workspace')
    source_root=Path(__file__).resolve().parents[1]
    source_hashes=source_fingerprint(source_root)
    atomic_write_json(args.output/'source-files.json',source_hashes)
    backup=OnlineApplicationBackup(pgdata,socket,work,restored,reporter.publish,reporter.status)
    redis=conn=monitor=None
    owned=set()
    reserved=False
    job_id='db-refactor-app-'+token

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    async def start(data,sockets,*,online=False):
        if (data/'postmaster.pid').exists():
            raise RuntimeError('Private database is already running')
        owned.add(data)
        await restore._run_command_capture(restore.PG_CTL,'-D',str(data),'-l',str(data.parent/'application-postgres.log'),
            '-w','-t','300','-o',backup.start_options() if online else restore._postgres_start_options(sockets),'start')
        connection=await asyncpg.connect(host=str(sockets),port=55432,user=restore.RESTORE_USER,
            database=restore.RESTORE_DATABASE,server_settings={'statement_timeout':'7200000','application_name':'application-rehearsal-control'})
        try:
            await wait_for_recovery_completion(connection)
            await guard(connection,data)
        except BaseException:
            await connection.close()
            raise
        return connection

    async def stop(data):
        await restore._stop_restored_postgres(data)
        if (data/'postmaster.pid').exists():
            raise RuntimeError('Owned private database did not stop')
        owned.discard(data)

    try:
        redis=await get_redis_pool()
        await ensure_backup_reservation(redis,job_id)
        reserved=True
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Restore workspace is busy')
            try:
                if not (pgdata/'PG_VERSION').is_file() or (pgdata/'postmaster.pid').exists():
                    raise RuntimeError('Restored application source must be retained and stopped')
                _,size_text=await restore._run_command_capture('du','-sb',str(pgdata))
                size=int(size_text.split()[0])
                free=shutil.disk_usage(workspace).free
                if free<2*size+50_000_000_000:
                    raise RuntimeError('Need room for moves, private online backup and independent restored database')
                # One-shot marker prevents interpreting modified probes as a new
                # pristine rehearsal. The earlier source copy is never touched.
                marker=workspace/'application-rehearsal-owner.json'
                with marker.open('x') as target:
                    json.dump({'report':str(args.output),'job_id':job_id},target)
                reporter.status.update(source_bytes=size,free_bytes=free)
                conn=await start(pgdata,socket)
                await prepare_application_copy(conn,pgdata,reporter.publish)
                results=await measure_application_reads(socket,candidate['manifest']['inputs']['players'],reporter.publish,
                    lambda rows:atomic_write_json(args.output/'application-reads.json',{'results':rows}),repeats=args.repeats)
                mismatches=sum(not row['matches'] for row in results)
                slowdowns=sum(row['slowdown'] for row in results)
                reporter.status.update(queries_completed=len(results),queries_total=len(results),
                    result_mismatches=mismatches,slowdown_flags=slowdowns,
                    query_errors=sum(bool(row['errors']) for row in results))
                writes=await application_writes(conn,socket,reporter.publish)
                atomic_write_json(args.output/'application-writes.json',writes)
                await conn.close(); conn=None
                await stop(pgdata)
                backup.prepare()
                await backup.command('creating_online_backup_stanza','--no-online','stanza-create')
                conn=await start(pgdata,socket,online=True)
                result=await backup.backup(conn)
                atomic_write_json(args.output/'online-backup.json',result)
                await conn.close(); conn=None
                await stop(pgdata)
                await backup.restore(result)
                conn=await start(restored,restored.parent/'socket')
                validated=await backup.validate(conn,result)
                atomic_write_json(args.output/'online-restore.json',validated)
                await conn.close(); conn=None
                await stop(restored)
                if source_fingerprint(source_root)!=source_hashes:
                    raise RuntimeError('Application source changed during rehearsal; cannot accept mixed versions')
                state='needs_review' if mismatches or slowdowns else 'succeeded'
                atomic_write_json(args.output/'acceptance.json',{
                    'application_reads':state,'application_write_checks':'passed',
                    'online_backup_worker_and_wal_restore':'passed', 'production_ready':False,
                    'remaining_gates':['Review all application latency and result differences.',
                        'Rehearse the canonical schema cutover, dependency/trigger moves and rollback boundary.',
                        'Wire coordinated API/worker/controller configuration and validate startup paths.',
                        'Validate the real HTTP/ARQ backup control flow; private worker transport was substituted.',
                        'Obtain a fresh verified backup and separate live-cutover approval.']})
            finally:
                if conn is not None:
                    await conn.close(); conn=None
                for data in tuple(owned):
                    await stop(data)
        await reporter.finish(state,'application_rehearsal_complete',production_ready=False,
            next_step='Review application/online-backup reports before preparing canonical cutover')
    except BaseException as error:
        # Line/function names aid diagnosis without dumping SQL parameters,
        # record contents or connection credentials into the status display.
        atomic_write_json(args.output/'failure-location.json',{
            'type':type(error).__name__,
            'frames':[{'file':Path(frame.filename).name,'line':frame.lineno,'function':frame.name}
                      for frame in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','application_rehearsal_failed',failed_phase=reporter.status.get('phase'),
                              error_type=type(error).__name__,production_ready=False)
        raise RuntimeError('Isolated application rehearsal failed; review private reports') from None
    finally:
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if reserved:
            await release_backup(redis,job_id)
        if redis:
            await close_redis_pool()


async def main(args):
    task=asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(run(args),timeout=6*60*60)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--candidate',type=Path,required=True)
    parser.add_argument('--write-validation',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--repeats',type=int,choices=range(3,6),default=3)
    asyncio.run(main(parser.parse_args()))
