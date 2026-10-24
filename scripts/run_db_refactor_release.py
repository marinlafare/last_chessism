"""Canonical startup/restart rehearsal on a NEW copy of a private restored DB.

No live URL, deployment option, production mounts, cloud jobs, or queue workers.
The accepted source remains stopped and unchanged. Retain both copies/reports.
"""
import argparse
import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import traceback
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


def check_source(source):
    if (source.name!='pgdata' or source.parent.parent!=Path('/restore-test')
        or not source.parent.name.startswith('db-refactor-baseline-online-')
        or source.resolve()!=source or source.is_symlink()
        or not (source/'PG_VERSION').is_file() or (source/'postmaster.pid').exists()):
        raise RuntimeError('Need a stopped, accepted private PostgreSQL source')
    if any(path.is_symlink() for path in source.rglob('*')):
        raise RuntimeError('External tablespaces/WAL/symlinks need a separate copy plan')
    return hashlib.sha256((source/'global/pg_control').read_bytes()).hexdigest()


async def clone_stopped_source(source, target, progress=None):
    """No hard links: modifying the clone must never change the source files."""
    if target.exists() or target.is_symlink():
        raise RuntimeError('Rehearsal target already exists; nothing will be overwritten')
    proc=await asyncio.create_subprocess_exec('cp','--archive','--reflink=auto','--',str(source),str(target),
        stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
    waiter=asyncio.create_task(proc.wait())
    try:
        while not waiter.done():
            try:
                await asyncio.wait_for(asyncio.shield(waiter),timeout=5)
            except asyncio.TimeoutError:
                if progress:
                    await progress()
        if waiter.result():
            raise RuntimeError('Private clone failed; partial workspace retained for review')
    except BaseException:
        if proc.returncode is None:
            proc.terminate()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(),30)
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
        raise
    finally:
        await waiter


async def run(args):
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.run_db_refactor_cutover_preflight import verified_source
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.db_refactor_release_rehearsal import commit_private_release
    from scripts.db_refactor_workload import digest
    from scripts.fen_migration_progress import ProgressReporter

    if (Path('/var/lib/postgresql/data/PG_VERSION').exists()
        or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount()):
        raise RuntimeError('Live database, socket and existing backup repository must not be mounted')
    source_workspace,backup_id=verified_source(args.application,restore.RESTORE_TEST_ROOT)
    if args.output.exists():
        raise RuntimeError('Use a new report directory; earlier evidence is preserved')
    args.output.mkdir(parents=True)
    token=uuid.uuid4().hex[:12]
    workspace=restore.RESTORE_TEST_ROOT/('db-refactor-baseline-release-'+token)
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    source=source_workspace/'pgdata'
    reporter=ProgressReporter(args.output/'status.json',backup_id,{})
    reporter.status.update(mode='isolated_canonical_release',live_database_changed=False,production_ready=False,
        source_workspace=str(source_workspace),workspace=str(workspace),checks_completed=0)
    await reporter.publish('checking_canonical_rehearsal_source')
    root=Path(__file__).resolve().parents[1]
    fingerprints=source_fingerprint(root)
    atomic_write_json(args.output/'source-files.json',fingerprints)
    redis=monitor=None
    reserved=started=False
    job_id='db-refactor-release-'+token

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    async def start():
        nonlocal started
        if (pgdata/'postmaster.pid').exists():
            raise RuntimeError('Rehearsal PostgreSQL unexpectedly already running')
        started=True
        await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'release-postgres.log'),
            '-w','-t','300','-o',restore._postgres_start_options(socket),'start')

    async def stop():
        nonlocal started
        await restore._stop_restored_postgres(pgdata)
        if (pgdata/'postmaster.pid').exists():
            raise RuntimeError('Private PostgreSQL did not stop')
        started=False

    try:
        redis=await get_redis_pool()
        await ensure_backup_reservation(redis,job_id)
        reserved=True
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Restore workspace is busy')
            source_control=check_source(source)
            _,size_text=await restore._run_command_capture('du','-sb',str(source))
            size=int(size_text.split()[0])
            free=shutil.disk_usage(source_workspace).free
            if free<size+20_000_000_000:
                raise RuntimeError('Need room for an independent private copy and 20 GB reserve')
            reporter.status.update(source_bytes=size,free_bytes=free)
            workspace.mkdir(mode=0o700)
            socket.mkdir(mode=0o700)
            atomic_write_json(workspace/'release-rehearsal-owner.json',{'report':str(args.output),'job_id':job_id})
            await reporter.publish('copying_private_release_workspace')
            async def copy_progress():
                if pgdata.exists():
                    _,done_text=await restore._run_command_capture('du','-sb',str(pgdata))
                    reporter.status['copy_bytes_done']=min(size,int(done_text.split()[0]))
                    reporter.flush()
            await clone_stopped_source(source,pgdata,copy_progress)
            reporter.status['copy_bytes_done']=size
            if check_source(source)!=source_control:
                raise RuntimeError('Accepted source changed during private copy')
            try:
                await start()
                result=await commit_private_release(socket,pgdata,reporter.publish)
                atomic_write_json(args.output/'canonical-release.json',result)
                reporter.status['checks_completed']=len(result['checks'])
                await reporter.publish('restarting_private_canonical_postgres')
                await stop()
                await start()
                _,raw=await restore._run_command_capture(sys.executable,str(root/'scripts/db_refactor_runtime_probe.py'),
                    '--workspace',str(workspace))
                restarted=json.loads(raw)
                if restarted['contract_hash']!=digest(result['contract']):
                    raise RuntimeError('Canonical storage contract changed after restart')
                atomic_write_json(args.output/'canonical-restart.json',restarted)
                reporter.status['checks_completed']+=len(restarted['checks'])
            finally:
                if started:
                    await stop()
            if source_control!=check_source(source):
                raise RuntimeError('Accepted private source was modified')
        if fingerprints!=source_fingerprint(root):
            raise RuntimeError('Source changed during canonical startup rehearsal')
        await reporter.finish('succeeded','canonical_release_rehearsal_complete',
            production_ready=False,source_unchanged=True,private_postgres_stopped=True)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
                      for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','canonical_release_rehearsal_failed',
            failed_phase=reporter.status.get('phase'),error_type=type(error).__name__)
        raise RuntimeError('Private release rehearsal failed; see report') from None
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
    await asyncio.wait_for(run(args),timeout=3600)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--application',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(main(parser.parse_args()))
