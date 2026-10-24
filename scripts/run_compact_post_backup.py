"""One compressed v3 backup and real private restore after approved migration.

Uses the normal backup/restore functions and their ownership, disk and retention
guards. Does not stop services or restore into the live database.
"""
import argparse
import asyncio
from contextlib import suppress
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def require_migration(status):
    if (status.get('mode')!='live_compact' or status.get('state')!='succeeded'
        or status.get('committed') is not True or status.get('live_database_changed') is not True):
        raise ValueError('A successfully committed live compact migration is required')


async def run(args):
    require_migration(json.loads((args.migration/'status.json').read_text()))
    if args.output.exists():
        raise ValueError('Choose a fresh report directory; previous results must remain intact')
    from chessism_api.database.runtime import bind_runtime_engine,configured_storage,COMPACT
    from chessism_api.operations import database_backups as backup
    from chessism_api.operations.database_backup_job import run_database_backup_job
    from chessism_api.operations.database_restore_tests import run_database_restore_test_job,latest_backup_for_restore_test
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    if configured_storage()!=COMPACT:
        raise RuntimeError('The complete app must be on relational_v3 before the new backup')
    args.output.mkdir(parents=True)
    started=time.monotonic()
    state={'state':'running','stage':'preflight','started_at':datetime.now(timezone.utc).isoformat(),
           'storage_profile':'relational_v3','live_restore':False,'cloud_jobs_started':0}
    engine=pulse=None
    def publish():
        progress={}
        if state['stage']=='backup':
            progress=backup._read_json(backup.BACKUP_STATUS_PATH,{})
        elif state['stage']=='restore_test':
            progress=backup._read_json(backup.APP_BACKUP_ROOT/'restore-test-status.json',{})
        state.update(elapsed_seconds=round(time.monotonic()-started,1),
            updated_at=datetime.now(timezone.utc).isoformat(),phase=progress.get('phase') or state['stage'],
            detail=progress.get('detail'),compressed_bytes=progress.get('compressed_bytes_written') or state.get('compressed_bytes'))
        atomic_write_json(args.output/'status.json',state)
    async def heartbeat():
        while True:
            publish()
            await asyncio.sleep(5)
    try:
        publish()
        engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
        redis=await get_redis_pool()
        pulse=asyncio.create_task(heartbeat())
        state['stage']='backup'
        publish()
        saved=await run_database_backup_job({'redis':redis,'job_id':'compact-post-backup-001'})
        atomic_write_json(args.output/'backup.json',saved)
        manifest=json.loads(Path(saved['manifest']).read_text())
        if manifest.get('relational_storage',{}).get('version')!='relational_v3':
            raise RuntimeError('New backup does not contain the complete v3 schema contract')
        state.update(stage='restore_test',backup_id=saved['backup_id'],compressed_bytes=saved['repository_bytes'])
        publish()
        tested=await run_database_restore_test_job(
            {'redis':redis,'job_id':'compact-post-restore-001'},backup_id=saved['backup_id'])
        atomic_write_json(args.output/'restore-test.json',tested)
        latest=latest_backup_for_restore_test()
        if (latest['backup_id']!=saved['backup_id'] or latest.get('restore_tested') is not True
            or latest.get('restore_test_status')!='success'):
            raise RuntimeError('Latest compressed backup failed its restore verification')
        cleanup=tested.get('cleanup') or {}
        if cleanup.get('state')!='complete':
            state.update(state='needs_review',stage='backup_verified_cleanup_needs_review',cleanup=cleanup)
        else:
            state.update(state='succeeded',stage='backup_and_restore_verified',cleanup=cleanup)
    except BaseException as error:
        state.update(state='failed',failed_stage=state['stage'],stage='inspect_post_migration_backup',
            error_type=type(error).__name__,error=str(error))
        raise
    finally:
        if pulse:
            pulse.cancel()
            with suppress(asyncio.CancelledError):await pulse
        publish()
        if engine:await engine.dispose()
        await close_redis_pool()


async def main(args):
    task=asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(run(args),6*3600)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--migration',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    asyncio.run(main(p.parse_args()))
