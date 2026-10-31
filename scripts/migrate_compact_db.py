"""Explicit approved live v3 maintenance; no automatic service restart or cleanup."""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import signal
import sys
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED']='1'
from scripts.compact_db_live_evidence import prepare,verify,validate_snapshot,sha


async def apply(args):
    if not args.approve_live_migration or not args.confirm_writers_stopped:
        raise ValueError('Explicit live approval and stopped-writers confirmation required')
    root=Path(__file__).resolve().parents[1]
    plan=json.loads(args.plan.read_text())
    verify(plan,root)
    if args.output.exists():
        raise ValueError('Choose a fresh report directory; never overwrite migration evidence')
    from sqlalchemy.ext.asyncio import create_async_engine
    import asyncpg
    from chessism_api.operations import database_backups as backups
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup,BACKUP_LOCK_KEY,redis_text
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.fen_migration_progress import ProgressReporter
    from scripts.compact_db_live import migrate

    approved=plan['preflight']
    target=approved['live_target']
    expected={k:target[k] for k in ('database','pgdata','system_identifier')}
    expected['port']='5432'
    pgdata=Path(target['pgdata'])
    if not (pgdata/'PG_VERSION').is_file():
        raise RuntimeError('Approved PGDATA must be mounted read-only for capacity checks')
    args.output.mkdir(parents=True)
    reporter=ProgressReporter(args.output/'status.json',approved['backup']['backup_id'],{})
    reporter.status.update(mode='live_compact',migration_kind='compact_v3',live_database_changed=False,
        rollback_verified=False,committed=False,services_stopped=True)
    atomic_write_json(args.output/'approved-inputs.json',plan)
    backup_expected=approved['backup']
    manifest_path=backups.MANIFEST_DIRECTORY/(backup_expected['backup_id']+'.json')
    archive=backups.DATABASE_BACKUP_ROOT/'snapshots'/(backup_expected['backup_id']+'.dump')
    def backup_gate():
        catalog=json.loads(backups.BACKUP_CATALOG_PATH.read_text())
        latest=max(catalog['backups'],key=lambda r:r['completed_at'])
        manifest=json.loads(manifest_path.read_text())
        validate_snapshot(latest,manifest,backup_expected)
        if sha(manifest_path)!=backup_expected['manifest_sha256']:
            raise RuntimeError('Rollback manifest changed')
        if archive.is_symlink() or archive.stat().st_size!=backup_expected['archive_bytes']:
            raise RuntimeError('Rollback archive changed')
        if manifest['database']['name']!=target['database']:
            raise RuntimeError('Rollback database identity mismatch')
        return manifest
    def capacity():
        if shutil.disk_usage(pgdata).free<100_000_000_000:
            raise RuntimeError('Need 100 GB free on the live database filesystem')
    redis=engine=observer=monitor=watchdog=None
    owner='compact-maintenance-'+uuid4().hex
    reserved=False
    guard_error=None
    try:
        await reporter.publish('verifying_compressed_rollback_backup')
        manifest=backup_gate()
        if await asyncio.to_thread(sha,archive)!=backup_expected['archive_sha256']:
            raise RuntimeError('Rollback archive checksum failed')
        capacity()
        redis=await get_redis_pool()
        for queue in ('analysis_queue','games_queue','research_queue','salience_queue','backup_queue',
                      'fen_queue','pipeline_queue','algorithms_queue','arq:queue'):
            if await redis.zcard(queue):
                raise RuntimeError('Work remains in '+queue)
        async for _ in redis.scan_iter(match='arq:in-progress:*',count=100):
            raise RuntimeError('An ARQ job is still in progress')
        await ensure_backup_reservation(redis,owner);reserved=True
        dsn=os.environ['DATABASE_URL']
        engine=create_async_engine(dsn,isolation_level='REPEATABLE READ',connect_args={
            'server_settings':{'statement_timeout':'7200000','application_name':'approved-compact-migration'}})
        observer=await asyncpg.connect(dsn.replace('postgresql+asyncpg://','postgresql://',1),server_settings={
            'default_transaction_read_only':'on','statement_timeout':'2000','lock_timeout':'500',
            'application_name':'compact-migration-progress'})
        async with engine.connect() as connection:
            driver=(await connection.get_raw_connection()).driver_connection
            monitor=asyncio.create_task(reporter.monitor(observer,driver.get_server_pid(),
                                                       await observer.fetchval('SELECT pg_current_wal_lsn()::text')))
            async def checkpoint():
                capacity()
                if redis_text(await redis.get(BACKUP_LOCK_KEY))!=owner:
                    raise RuntimeError('Maintenance reservation lost')
            owner_task=asyncio.current_task()
            async def watch():
                nonlocal guard_error
                try:
                    while True:
                        await asyncio.sleep(5)
                        await checkpoint()
                except asyncio.CancelledError:raise
                except Exception as error:
                    guard_error=str(error)
                    owner_task.cancel()
            watchdog=asyncio.create_task(watch())
            async def publish(phase,**details):
                if phase=='ready_to_commit_compact_migration':
                    verify(plan,root)
                    backup_gate()
                await reporter.publish(phase,**details)
            candidate=json.loads((root/plan['acceptance']/'candidate-contract.json').read_text())
            result=await migrate(connection,expected,observer.get_server_pid(),manifest['relational_storage'],
                                 candidate,publish,checkpoint)
            reporter.status.update(committed=True,live_database_changed=True)
            atomic_write_json(args.output/'migration-result.json',result)
        await reporter.finish('succeeded','compact_migration_committed_services_still_stopped',
            detail='Activate tested v3 API, workers and controller together after reviewing this result. Keep the rollback backup.')
    except BaseException as error:
        await reporter.finish('failed','keep_services_stopped_inspect_compact_outcome',
            error_type=type(error).__name__,detail=str(error),guard_error=guard_error)
        raise
    finally:
        for task in (watchdog,monitor):
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):await task
        if observer:await observer.close()
        if engine:await engine.dispose()
        if reserved:await release_backup(redis,owner)
        await close_redis_pool()


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    offline=sub.add_parser('prepare')
    offline.add_argument('--acceptance',type=Path,required=True)
    offline.add_argument('--preflight',type=Path,required=True)
    offline.add_argument('--plan',type=Path,required=True)
    live=sub.add_parser('apply')
    live.add_argument('--plan',type=Path,required=True)
    live.add_argument('--output',type=Path,required=True)
    live.add_argument('--approve-live-migration',action='store_true')
    live.add_argument('--confirm-writers-stopped',action='store_true')
    return p


async def bounded(args):
    task=asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(apply(args),4*3600)


if __name__=='__main__':
    args=parser().parse_args()
    if args.command=='prepare':
        plan=prepare(Path(__file__).resolve().parents[1],args.acceptance,args.preflight)
        with args.plan.open('x') as stream:json.dump(plan,stream,indent=2)
        print('Prepared offline. No live changes.')
    else:asyncio.run(bounded(args))
