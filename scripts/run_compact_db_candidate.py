"""Continue a completed isolated preflight with the candidate schema only.

No live deployment, service restart, cloud calls or production release marker.
Keep the source backup read-only and the private restored copy for acceptance.
"""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


async def run(args):
    os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'
    os.environ['PYTHON_DOTENV_DISABLED']='1'
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.restore_workspace import workspace_lock
    from scripts.fen_migration_progress import ProgressReporter
    from scripts.compact_db_migration import migrate_private
    from scripts.compact_db_preflight import require_private
    from chessism_api.operations.backup_files import atomic_write_json
    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Live database data/socket must not be mounted')
    previous=json.loads((args.preflight/'status.json').read_text())
    if (previous.get('state')!='succeeded' or previous.get('mode')!='isolated_compact_preflight'
            or previous.get('live_database_changed') is not False or not previous.get('private_postgres_stopped')):
        raise RuntimeError('A successful stopped isolated preflight is required')
    workspace=Path(previous['workspace'])
    if (workspace.parent!=Path('/restore-test') or not workspace.name.startswith('compact-db-')
            or workspace.is_symlink() or workspace.resolve()!=workspace):
        raise RuntimeError('Invalid private workspace')
    if args.output.exists():
        raise RuntimeError('Choose a new report directory')
    args.output.mkdir(parents=True,mode=0o755)
    reporter=ProgressReporter(args.output/'status.json',previous['backup_id'],{})
    reporter.status.update(mode='isolated_compact_candidate',live_database_changed=False,
        ready_for_live_migration=False,workspace=str(workspace))
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    conn=heartbeat=None
    async def pulse():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
        if not acquired:
            raise RuntimeError('Restore workspace is busy')
        if (pgdata/'postmaster.pid').exists() or not (pgdata/'PG_VERSION').is_file():
            raise RuntimeError('Private preflight database must be retained and stopped')
        if shutil.disk_usage(workspace).free<100_000_000_000:
            raise RuntimeError('Need 100 GB headroom for table/index rewrites')
        try:
            heartbeat=asyncio.create_task(pulse())
            await reporter.publish('starting_private_candidate')
            await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'candidate.log'),
                '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
            conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=restore.RESTORE_DATABASE)
            await require_private(conn,pgdata)
            proofs=await migrate_private(conn,pgdata,reporter.publish)
            atomic_write_json(args.output/'proofs.json',proofs)
            await reporter.publish('analyzing_candidate')
            for table in ('fen','fen_continuation','moves','game_fen_association','months','player_stats',
                          'main_character_mode_summary','home_titled_player_summary'):
                await conn.execute(f'ANALYZE public.{table}')
            await reporter.finish('needs_review','private_candidate_complete',
                detail='Private schema changed. Full application/performance/backup acceptance is still required; do not deploy.')
        except BaseException as error:
            await reporter.finish('failed','private_candidate_failed',error_type=type(error).__name__,detail=str(error))
            raise
        finally:
            if heartbeat:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
            if conn:
                await conn.close()
            await restore._stop_restored_postgres(pgdata)
            reporter.status['private_postgres_stopped']=not (pgdata/'postmaster.pid').exists()
            reporter.flush()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preflight',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    asyncio.run(run(parser.parse_args()))
