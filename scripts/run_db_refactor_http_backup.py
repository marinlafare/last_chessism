"""Real HTTP + ARQ + pgBackRest acceptance on a PRIVATE canonical DB clone.

Run only through launch_db_refactor_http_backup.py: network=none, separate Redis
in the same network namespace, no live database/backups/Docker/cloud mounts.
"""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import sys
import time
import traceback
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def run(args):
    import asyncpg
    import httpx
    from redis.asyncio import Redis
    from arq.jobs import Job,JobStatus
    from arq.connections import create_pool,RedisSettings
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from scripts.db_refactor_candidate import guard
    from scripts.db_refactor_release_rehearsal import check_runtime
    from scripts.run_db_refactor_release import clone_stopped_source
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.db_refactor_workload import digest
    from scripts.fen_migration_progress import ProgressReporter
    from scripts.db_refactor_e2e_support import (verified_source,source_control,configure_backup,
        private_environment,start_process,stop_process,assert_services_alive)

    if (Path('/var/lib/postgresql/data/PG_VERSION').exists()
        or Path('/var/run/postgresql/.s.PGSQL.5432').exists()
        or Path('/backup-volume').is_mount() or Path('/var/run/docker.sock').exists()):
        raise RuntimeError('Live data, backup repositories, and Docker socket are forbidden')
    if {p.name for p in Path('/sys/class/net').iterdir()}!={'lo'}:
        raise RuntimeError('Rehearsal container must have loopback networking only')
    source_workspace,backup_id,expected_contract=verified_source(args.release)
    if args.output.exists():
        raise RuntimeError('Use a new report directory')
    args.output.mkdir(parents=True)
    workspace=Path('/restore-test')/('db-refactor-baseline-http-'+uuid.uuid4().hex[:12])
    source=source_workspace/'pgdata'
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    reporter=ProgressReporter(args.output/'status.json',backup_id,{})
    reporter.status.update(mode='isolated_http_backup',live_database_changed=False,production_ready=False,
        workspace=str(workspace),source_workspace=str(source_workspace),http_checks=0,completed_jobs=0)
    await reporter.publish('checking_isolated_http_workspace')
    root=Path(__file__).resolve().parents[1]
    fingerprints=source_fingerprint(root)
    atomic_write_json(args.output/'source-files.json',fingerprints)
    api=worker=conn=redis=monitor=None
    started=False
    checks=[]

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    async def start_postgres(config):
        nonlocal started
        if (pgdata/'postmaster.pid').exists():
            raise RuntimeError('Private PostgreSQL is already running')
        started=True
        options=(f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}' "
            "-c unix_socket_permissions=0700 -c archive_mode=on "
            f'-c "archive_command=pgbackrest --config={config} --stanza=refactor archive-push %p"')
        await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'postgres.log'),
            '-w','-t','300','-o',options,'start')

    async def stop_databases():
        nonlocal started
        # Only clusters created inside this run's workspace are eligible.
        candidates=[p/'pgdata' for p in (workspace/'restore-tests').glob('rehearsal-*') if not p.is_symlink()]
        if started:
            candidates.append(pgdata)
        for data in candidates:
            if (data/'postmaster.pid').exists():
                await restore._stop_restored_postgres(data)
                if (data/'postmaster.pid').exists():
                    raise RuntimeError('Private PostgreSQL did not stop; workspace retained')
        started=False

    try:
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(Path('/restore-test')) as acquired:
            if not acquired:
                raise RuntimeError('Restore workspace is already in use')
            original_control=source_control(source)
            _,size_text=await restore._run_command_capture('du','-sb',str(source))
            size=int(size_text.split()[0])
            # Conservative even if copy-on-write is available: clone, backup,
            # restored database, and a protected 30 GB free floor.
            if shutil.disk_usage(source).free<3*size+30_000_000_000:
                raise RuntimeError('Insufficient space for isolated clone, backup and restore')
            workspace.mkdir(mode=0o700);socket.mkdir(mode=0o700)
            atomic_write_json(workspace/'http-rehearsal-owner.json',{'report':str(args.output)})
            reporter.status['source_bytes']=size
            await reporter.publish('copying_http_rehearsal_database')
            async def copy_progress():
                if pgdata.exists():
                    _,value=await restore._run_command_capture('du','-sb',str(pgdata))
                    reporter.status['copy_bytes_done']=min(size,int(value.split()[0]));reporter.flush()
            await clone_stopped_source(source,pgdata,copy_progress)
            if source_control(source)!=original_control:
                raise RuntimeError('Accepted source changed during copying')
            config=configure_backup(workspace)
            gate=secrets.token_urlsafe(32)
            env=private_environment(workspace,config,gate)
            await reporter.publish('waiting_for_private_redis')
            probe=Redis(host='127.0.0.1',socket_connect_timeout=1,socket_timeout=1)
            try:
                async with asyncio.timeout(90):
                    while True:
                        try:
                            if await probe.ping(): break
                        except (OSError,ConnectionError): pass
                        except Exception as error:
                            if error.__class__.__module__.startswith('redis'): pass
                            else: raise
                        await asyncio.sleep(1)
                if await probe.dbsize()!=0:
                    raise RuntimeError('Private Redis must start empty')
            finally:
                await probe.aclose()
            redis=await create_pool(RedisSettings(host='127.0.0.1'))
            await reporter.publish('starting_private_canonical_database')
            await restore._run_command_capture('pgbackrest',f'--config={config}','--stanza=refactor','--no-online','stanza-create')
            try:
                await start_postgres(config)
                conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='chessism_db')
                await guard(conn,pgdata)
                runtime=await check_runtime(socket,pgdata)
                if runtime['contract']!=expected_contract:
                    raise RuntimeError('Canonical source contract drifted')
                # Supply known committed writes between full/incremental
                # backups, without altering existing user rows.
                await conn.execute('CREATE SCHEMA http_rehearsal; CREATE TABLE http_rehearsal.probe(id integer PRIMARY KEY); INSERT INTO http_rehearsal.probe VALUES(1)')
                await reporter.publish('starting_real_api_and_backup_worker')
                worker=await start_process([sys.executable,'-m','arq','worker.WorkerSettings'],env,args.output/'worker.log')
                api=await start_process([sys.executable,'-m','uvicorn','main:app','--host','127.0.0.1','--port','18000',
                    '--no-access-log'],env,args.output/'api.log')
                async with httpx.AsyncClient(base_url='http://127.0.0.1:18000',timeout=120,trust_env=False) as client:
                    async def wait_api():
                        async with asyncio.timeout(180):
                            while True:
                                assert_services_alive([api,worker])
                                try:
                                    if (await client.get('/')).status_code==200: return
                                except httpx.TransportError: pass
                                await asyncio.sleep(1)

                    async def request(method,path,*,expected=200,record=True,**kwargs):
                        assert_services_alive([api,worker])
                        start=time.monotonic()
                        response=await client.request(method,path,**kwargs)
                        if response.status_code!=expected:
                            raise RuntimeError(f'Private HTTP check failed: {method} {path} status {response.status_code}')
                        if record:
                            checks.append({'method':method,'path':path,'status':response.status_code,
                                           'seconds':round(time.monotonic()-start,3)})
                            reporter.status['http_checks']=len(checks)
                            atomic_write_json(args.output/'http-checks.json',checks)
                        return response.json()

                    await wait_api()
                    await reporter.publish('checking_authenticated_http_readers')
                    await request('GET','/backups/database',expected=401)
                    await request('POST','/auth/gate',json={'code':gate})
                    password=secrets.token_urlsafe(32)
                    identity={'name':'Isolated migration rehearsal','email':uuid.uuid4().hex+'@example.invalid',
                              'password':password,'repeat_password':password}
                    await request('POST','/auth/admins/signup',json=identity)
                    await request('GET','/auth/me')
                    for path in ('/games/database/generalities','/fens/scored/overview','/fens/pipeline/overview',
                        '/players/main_characters/top?time_control=blitz&limit=4',
                        '/games/magnuscarlsen/recent?page_size=4',
                        '/players/magnuscarlsen/analysis/behavioural/ratings?mode=blitz',
                        '/database/schema','/backups/database'):
                        await request('GET',path)

                    async def job_finished(job_id,phase):
                        job=Job(job_id,redis,_queue_name='backup_queue')
                        async with asyncio.timeout(7200):
                            while True:
                                assert_services_alive([api,worker])
                                status=await job.status()
                                progress=await redis.get('chessism:job_progress:'+job_id)
                                if progress:
                                    progress=json.loads(progress)
                                    reporter.status.update(worker_phase=progress.get('phase'),
                                        worker_processed=progress.get('processed',0),worker_total=progress.get('total',0))
                                if status==JobStatus.complete:
                                    result=await job.result(timeout=1)
                                    reporter.status['completed_jobs']+=1
                                    return result
                                if status==JobStatus.not_found:
                                    raise RuntimeError('Private ARQ job disappeared')
                                reporter.flush()
                                await asyncio.sleep(3)

                    await reporter.publish('http_arq_full_backup')
                    queued=await request('POST','/backups/database',expected=202)
                    full=await job_finished(queued['job_id'],'full')
                    if full['type']!='full': raise RuntimeError('Expected a new full backup')
                    await conn.execute('INSERT INTO http_rehearsal.probe VALUES(2)')
                    await reporter.publish('http_arq_incremental_backup')
                    queued=await request('POST','/backups/database',expected=202)
                    incremental=await job_finished(queued['job_id'],'incremental')
                    if incremental['type']!='incr' or incremental.get('prior')!=full['backup_id']:
                        raise RuntimeError('Expected incremental of the new private full backup')
                    manifest=json.loads(Path(incremental['manifest']).read_text())
                    if manifest.get('schema_version')!=7 or manifest.get('relational_storage')!=expected_contract:
                        raise RuntimeError('Backup omitted the canonical normalized contract')
                    await reporter.publish('http_arq_restore_test')
                    queued=await request('POST','/backups/database/test',expected=202)
                    if queued['backup_id']!=incremental['backup_id']:
                        raise RuntimeError('Wrong private recovery point queued for restore')
                    result=await job_finished(queued['job_id'],'restore')
                    if not result.get('restore_tested') or result['validation']['fen_identity']['references'].get('schema_matches') is not True:
                        raise RuntimeError('The real restore worker did not validate the normalized schema')
                    atomic_write_json(args.output/'backup-restore.json',{
                        'full_id':full['backup_id'],'incremental_id':incremental['backup_id'],
                        'restore_result':result,'manifest':manifest,
                        'transport':'real authenticated HTTP, private Redis, real ARQ worker; no dependency overrides'})
                    await reporter.publish('restarting_real_api_and_worker')
                    await stop_process(api);api=None
                    await stop_process(worker);worker=None
                    await redis.delete('backup_queue:health-check')
                    worker=await start_process([sys.executable,'-m','arq','worker.WorkerSettings'],env,args.output/'worker-restart.log')
                    api=await start_process([sys.executable,'-m','uvicorn','main:app','--host','127.0.0.1','--port','18000',
                        '--no-access-log'],env,args.output/'api-restart.log')
                    await wait_api()
                    async with asyncio.timeout(60):
                        while not await redis.exists('backup_queue:health-check'):
                            assert_services_alive([api,worker])
                            await asyncio.sleep(1)
                    await request('GET','/auth/me')
                    overview=await request('GET','/backups/database')
                    if overview['restore_test']['status']!='success':
                        raise RuntimeError('Restore result did not survive app restart')
                    await request('GET','/games/magnuscarlsen/recent?page_size=4')
                    if await redis.get('chessism:backup:active') is not None:
                        raise RuntimeError('Private worker did not release the backup reservation')
            finally:
                await stop_process(api);api=None
                await stop_process(worker);worker=None
                if conn: await conn.close();conn=None
                await stop_databases()
            if source_control(source)!=original_control:
                raise RuntimeError('Accepted source was modified')
        if fingerprints!=source_fingerprint(root):
            raise RuntimeError('Application source changed during acceptance run')
        atomic_write_json(args.output/'acceptance.json',{'state':'succeeded','live_database_changed':False,
            'production_ready':False,'http_checks':len(checks),'real_arq_jobs':3,
            'remaining':['Active-corpus summary refresh and final live copy/cutover runner.',
                         'Maintenance-window approval before production activation.'],
            'limitations':['No browser rendering test. No cloud controller or analysis queues started.',
                           'Uses the previously normalized production-sized copy, not a fresh live migration.']})
        await reporter.finish('succeeded','http_backup_acceptance_complete',private_postgres_stopped=True,
            source_unchanged=True,temporary_restore_removed=True)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
                      for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','http_backup_acceptance_failed',failed_phase=reporter.status.get('phase'),
            error_type=type(error).__name__)
        raise RuntimeError('Private HTTP/backup rehearsal failed; see report and private service logs') from None
    finally:
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError): await monitor
        if redis: await redis.aclose()


async def main(args):
    task=asyncio.current_task()
    for sig in (signal.SIGTERM,signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig,task.cancel)
    await asyncio.wait_for(run(args),timeout=4*3600)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(main(parser.parse_args()))
