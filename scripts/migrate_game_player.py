"""Prepare offline; apply only with explicit approval and stopped live writers.

Never starts/stops services. Never deletes original tables, backups or results.
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
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED']='1'

from scripts.db_refactor_live_evidence import prepare_plan,verify_plan,validate_backup


async def apply(args):
    # Nothing connects or reserves a resource before these explicit gates.
    if not args.approve_live_migration or not args.confirm_writers_stopped:
        raise ValueError('Explicit --approve-live-migration and --confirm-writers-stopped required')
    from sqlalchemy.ext.asyncio import create_async_engine
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup,BACKUP_LOCK_KEY,redis_text
    from chessism_api.operations.database_restore_tests import latest_backup_for_restore_test
    from chessism_api.operations import database_backups as backups
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.db_refactor_live import migrate
    from scripts.db_refactor_live_target import require_strict_archiving
    from scripts.fen_migration_progress import ProgressReporter
    import asyncpg

    root = Path(__file__).resolve().parents[1]
    plan = json.loads(args.plan.read_text())
    verify_plan(plan,root)
    if args.output.exists():
        raise ValueError('Use a new output directory; previous migration evidence is retained')
    pgdata = Path(args.expect_pgdata)
    if not pgdata.is_absolute() or not (pgdata/'PG_VERSION').is_file():
        raise ValueError('The explicitly approved PGDATA must be mounted read-only for capacity checks')
    backup = latest_backup_for_restore_test()
    validate_backup(backup,args.backup_id,args.expect_system_id,await backups._pgbackrest_info())
    reserve_bytes = max(80_000_000_000,2*int(backup['database_bytes']))
    def capacity():
        if shutil.disk_usage(pgdata).free < reserve_bytes:
            raise RuntimeError('Insufficient database disk reserve for copies, indexes and WAL')
        if shutil.disk_usage(backups.BACKUP_VOLUME_ROOT).free < backups.FREE_FLOOR_BYTES + 20_000_000_000:
            raise RuntimeError('Insufficient backup-volume headroom for migration WAL; archiving must stay enabled')
        if backups.PGBACKREST_GAP_PATH.exists():
            raise RuntimeError('WAL archive-gap marker exists; repair backup protection first')
    capacity()
    dsn = os.environ['DATABASE_URL']
    expected = {'database':args.expect_database,'pgdata':args.expect_pgdata,
                'port':str(args.expect_port),'system_identifier':args.expect_system_id}
    args.output.mkdir(parents=True)
    reporter = ProgressReporter(args.output/'status.json',args.backup_id,{'migration_kind':'game_player_refactor'})
    reporter.status.update(mode='live_game_player',rollback_verified=False,live_database_changed=False,
                           row_checks=0,steps_completed=0,steps_total=8)
    atomic_write_json(args.output/'approved-inputs.json',{'target':expected,'backup':backup,'plan':plan})
    redis = observer = monitor = watchdog = engine = None
    reserved = committed = False
    watchdog_error = None
    owner = 'live-game-player-'+uuid.uuid4().hex
    try:
        redis = await get_redis_pool()
        await ensure_backup_reservation(redis,owner);reserved=True
        validate_backup(latest_backup_for_restore_test(),args.backup_id,args.expect_system_id,await backups._pgbackrest_info())
        engine = create_async_engine(dsn,isolation_level='REPEATABLE READ',connect_args={
            'server_settings':{'statement_timeout':'7200000','application_name':'approved-game-player-migration'}})
        observer = await asyncpg.connect(dsn.replace('postgresql+asyncpg://','postgresql://',1),server_settings={
            'application_name':'game-player-migration-progress','default_transaction_read_only':'on',
            'statement_timeout':'2000','lock_timeout':'500'})
        await require_strict_archiving(observer)
        async with engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            monitor = asyncio.create_task(reporter.monitor(observer,driver.get_server_pid(),await observer.fetchval('SELECT pg_current_wal_lsn()::text')))
            async def checkpoint():
                capacity()
                if redis_text(await redis.get(BACKUP_LOCK_KEY)) != owner:
                    raise RuntimeError('Maintenance backup reservation was lost')
            owner_task = asyncio.current_task()
            async def watch_capacity():
                nonlocal watchdog_error
                try:
                    while True:
                        await asyncio.sleep(5)
                        await checkpoint()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    watchdog_error = str(error)
                    owner_task.cancel()
            watchdog = asyncio.create_task(watch_capacity())
            async def publish(phase,**details):
                if phase=='ready_to_commit_live_migration':
                    verify_plan(plan,root)
                    await require_strict_archiving(driver)
                    if latest_backup_for_restore_test()['backup_id']!=args.backup_id:
                        raise RuntimeError('Rollback recovery point changed during migration')
                await reporter.publish(phase,**details)
            await reporter.publish('live_migration_preflight')
            result = await migrate(connection,expected,observer.get_server_pid(),publish,checkpoint)
            committed = True
            atomic_write_json(args.output/'migration-result.json',result)
        await reporter.finish('succeeded','migration_committed_services_still_stopped',live_database_changed=True,
                              committed=True,legacy_tables_retained=True)
    except BaseException as error:
        await reporter.finish('failed','keep_services_stopped_inspect_migration_outcome',
            error_type=type(error).__name__,guard_error=watchdog_error,committed=committed,
            outcome='committed' if committed else 'rolled_back' if reporter.status.get('rollback_verified') else 'inspect_database')
        raise
    finally:
        if watchdog:
            watchdog.cancel()
            with suppress(asyncio.CancelledError):await watchdog
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError):await monitor
        if observer:await observer.close()
        if engine:await engine.dispose()
        if reserved:await release_backup(redis,owner)
        await close_redis_pool()


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest='command',required=True)
    prepare = commands.add_parser('prepare',help='Offline plan only: no database/Docker/Redis connections')
    prepare.add_argument('--rehearsal',type=Path,required=True)
    prepare.add_argument('--plan',type=Path,required=True)
    live = commands.add_parser('apply',help='Separately approved downtime operation')
    live.add_argument('--approve-live-migration',action='store_true')
    live.add_argument('--confirm-writers-stopped',action='store_true')
    live.add_argument('--plan',type=Path,required=True)
    live.add_argument('--backup-id',required=True)
    live.add_argument('--expect-database',required=True)
    live.add_argument('--expect-system-id',required=True)
    live.add_argument('--expect-pgdata',required=True)
    live.add_argument('--expect-port',type=int,default=5432)
    live.add_argument('--output',type=Path,required=True)
    return result


async def bounded_apply(args):
    task = asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(apply(args),timeout=4*3600)


if __name__=='__main__':
    args=parser().parse_args()
    if args.command=='prepare':
        if args.plan.exists():raise ValueError('Plan already exists; use a new filename')
        plan=prepare_plan(Path(__file__).resolve().parents[1],args.rehearsal)
        # Atomic local file write only. Preparing is NOT live approval.
        from chessism_api.operations.backup_files import atomic_write_json
        atomic_write_json(args.plan,plan)
        print('Prepared offline. Live migration NOT started or authorized.')
    else:
        asyncio.run(bounded_apply(args))
