"""Private write-contract checks and offline backup/restore; never a live cutover."""
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
from scripts.db_refactor_candidate import C,A,guard,qi
from scripts.db_refactor_workload import digest
from scripts.run_db_refactor_candidate import verified_workspace,verified_retune
from scripts.db_refactor_roundtrip import OfflineRoundTrip


async def snapshot(conn, publish):
    relations=await conn.fetch('''SELECT n.nspname,c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=ANY($1::text[]) AND c.relkind='r' ORDER BY n.nspname,c.relname''',[C,A])
    counts=[]
    for i,row in enumerate(relations):
        await publish('checking_normalized_backup_contents',current_table=row['relname'],
                      tables_completed=i,tables_total=len(relations))
        count=await conn.fetchval(f'SELECT count(*) FROM {qi(row["nspname"])}.{qi(row["relname"])}')
        counts.append({'schema':row['nspname'],'table':row['relname'],'rows':count})
    columns=[dict(r) for r in await conn.fetch('''SELECT table_schema,table_name,column_name,data_type,
        is_nullable,column_default,is_identity,identity_generation FROM information_schema.columns
        WHERE table_schema=ANY($1::text[]) ORDER BY table_schema,table_name,ordinal_position''',[C,A])]
    constraints=[dict(r) for r in await conn.fetch('''SELECT n.nspname,c.relname,k.conname,k.convalidated,
        pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1::text[])
        ORDER BY n.nspname,c.relname,k.conname''',[C,A])]
    indexes=[dict(r) for r in await conn.fetch('''SELECT n.nspname,c.relname,i.indisvalid,i.indisready,
        pg_get_indexdef(i.indexrelid) AS definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1::text[])
        ORDER BY n.nspname,c.relname''',[C,A])]
    if any(not r['convalidated'] for r in constraints) or any(not r['indisvalid'] or not r['indisready'] for r in indexes):
        raise RuntimeError('Normalized constraints or indexes are invalid')
    return {'tables':counts,'columns':columns,'constraints':constraints,'indexes':indexes}


async def cloud_import_checks(conn, socket, output, restore):
    # Existing cloud-import regressions run in their own EMPTY database and
    # generated schemas, not in either production or the normalized corpus.
    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='chessism_cloud_test')"):
        await conn.execute('CREATE DATABASE chessism_cloud_test')
    import asyncpg
    check=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database='chessism_cloud_test')
    try:
        if await check.fetchval("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind='r')"):
            raise RuntimeError('Cloud test database is not empty; refusing to reuse it')
    finally:
        await check.close()
    names=('test_cloud_run_out_of_order_task_import_is_atomic_and_idempotent',
           'test_batch_fleet_out_of_order_import_is_atomic_and_idempotent',
           'test_import_is_atomic_idempotent_and_uses_stockfish_fields',
           'test_failed_transaction_keeps_claims_and_no_import_receipt')
    env=dict(os.environ,PGHOST=str(socket),PGPORT='55432',
        PYTHONPATH='/candidate:/candidate/tests:/candidate/stockfish-batch-worker',
        CHESSISM_CLOUD_TEST_DATABASE_URL='postgresql+asyncpg://chessism_user@/chessism_cloud_test')
    proc=await asyncio.create_subprocess_exec(sys.executable,'-m','unittest',
        *['tests.test_cloud_analysis_database.CloudDatabaseTests.'+name for name in names],
        cwd='/candidate',env=env,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    try:
        data,_=await asyncio.wait_for(proc.communicate(),timeout=300)
    except BaseException:
        if proc.returncode is None:
            proc.terminate()
            await proc.wait()
        raise
    (output/'cloud-import-tests.log').write_bytes(data)
    if proc.returncode:
        raise RuntimeError('Cloud import regressions failed; see the private test log')
    return {'passed':len(names),'tests':list(names),'scope':'unchanged import/receipt core in an isolated test database'}


async def run(args):
    os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from scripts.fen_migration_progress import ProgressReporter
    from scripts.db_refactor_write_checks import run_checks

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Live data/socket must not be mounted')
    baseline=json.loads((args.baseline/'manifest.json').read_text())
    workspace=verified_workspace(json.loads((args.baseline/'status.json').read_text()),baseline,restore.RESTORE_TEST_ROOT)
    prior=verified_retune(args.candidate,baseline,workspace)
    if (prior['status']['state']!='succeeded' or prior['status'].get('result_mismatches')!=0
            or prior['status'].get('slowdown_flags')!=0):
        raise RuntimeError('Accepted isolated read comparison is required')
    if os.environ.get('BENCHMARK_RESOURCE_PROFILE')!=baseline['resource_profile']:
        raise RuntimeError('Resource profile must match the prior rehearsal')
    if args.output.exists():
        raise RuntimeError('Use a new report directory')
    args.output.mkdir(parents=True,mode=0o755)
    token=uuid.uuid4().hex[:12]
    work=restore.RESTORE_TEST_ROOT/('db-refactor-acceptance-'+token)
    restored=restore.RESTORE_TEST_ROOT/('db-refactor-baseline-verify-'+token)/'pgdata'
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    reporter=ProgressReporter(args.output/'status.json',baseline['backup']['backup_id'],{})
    reporter.status.update(mode='isolated_write_validation',live_database_changed=False,
        production_ready=False,workspace=str(workspace),repository_workspace=str(work),
        restored_workspace=str(restored.parent),source_comparison=str(args.candidate),
        checks_completed=0,checks_total=7,backup_mode='offline')
    await reporter.publish('checking_private_write_workspace')
    roundtrip=OfflineRoundTrip(pgdata,socket,work,restored,reporter.publish,reporter.status)
    redis=monitor=conn=None
    reserved=False
    owned=set()
    job_id='db-refactor-validation-'+token
    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    async def start(data, sockets):
        if (data/'postmaster.pid').exists():
            raise RuntimeError('The selected private database is already running')
        owned.add(data)
        await restore._run_command_capture(restore.PG_CTL,'-D',str(data),'-l',str(data.parent/'write-validation-postgres.log'),
            '-w','-t','300','-o',restore._postgres_start_options(sockets),'start')
        connection=await asyncpg.connect(host=str(sockets),port=55432,user=restore.RESTORE_USER,
            database=restore.RESTORE_DATABASE,server_settings={'statement_timeout':'600000',
                                                              'application_name':'refactor-write-validation'})
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
            raise RuntimeError('Private server did not stop')
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
                if (pgdata/'postmaster.pid').exists():
                    raise RuntimeError('Source candidate must be stopped')
                _,size_text=await restore._run_command_capture('du','-sb',str(pgdata))
                size=int(size_text.split()[0])
                free=shutil.disk_usage(workspace).free
                if free<2*size+20_000_000_000:
                    raise RuntimeError('Insufficient room for independent backup and restored copy')
                reporter.status.update(source_bytes=size,free_bytes=free)
                roundtrip.prepare()
                await reporter.publish('starting_private_write_checks')
                conn=await start(pgdata,socket)
                writes=await run_checks(socket,pgdata,publish=reporter.publish)
                atomic_write_json(args.output/'write-checks.json',writes)
                await reporter.publish('testing_local_cloud_import_transactions')
                cloud=await cloud_import_checks(conn,socket,args.output,restore)
                atomic_write_json(args.output/'cloud-import-checks.json',cloud)
                before=await snapshot(conn,reporter.publish)
                atomic_write_json(args.output/'before-backup.json',before)
                await conn.close(); conn=None
                await reporter.publish('stopping_source_for_offline_backup')
                await stop(pgdata)
                backup=await roundtrip.run()
                atomic_write_json(args.output/'backup-roundtrip.json',backup)
                await reporter.publish('starting_restored_candidate')
                conn=await start(restored,restored.parent/'socket')
                after=await snapshot(conn,reporter.publish)
                atomic_write_json(args.output/'after-restore.json',after)
                if digest(before)!=digest(after):
                    raise RuntimeError('Restored normalized counts/schema/constraints/indexes differ')
                await reporter.publish('retesting_restored_writes',checks_completed=0)
                restored_writes=await run_checks(restored.parent/'socket',restored,publish=reporter.publish)
                atomic_write_json(args.output/'restored-write-checks.json',restored_writes)
                if digest(writes)!=digest(restored_writes):
                    raise RuntimeError('Restored write behavior differs')
                await conn.close(); conn=None
                await stop(restored)
                atomic_write_json(args.output/'acceptance.json',{'write_contracts':'passed',
                    'cloud_import_core':'passed','offline_pgbackrest_roundtrip':'passed',
                    'normalized_contents_sha256':digest(after),'production_ready':False,
                    'remaining_gates':['Wire the normalized writers/readers into API, workers and controller.',
                        'Validate global/engine summaries, deletion and saved-matrix contracts.',
                        'Validate ingestion-month ledgers, concurrency and bulk write performance.',
                        'Validate online WAL archiving and application backup-manifest/UI orchestration.',
                        'Obtain a fresh tested backup and explicitly approve the live cutover.']})
            finally:
                if conn is not None:
                    await conn.close(); conn=None
                for data in tuple(owned):
                    await stop(data)
        await reporter.finish('succeeded','write_backup_validation_complete',
            write_checks=len(writes['checks']),cloud_import_checks=cloud['passed'],
            restored_write_checks=len(restored_writes['checks']),backup_restore_match=True,
            production_ready=False,next_step='Review the acceptance report; finish application integration before any live cutover')
    except BaseException as error:
        await reporter.finish('failed','write_backup_validation_failed',error_type=type(error).__name__,
            failed_phase=reporter.status.get('phase'),production_ready=False)
        raise RuntimeError('Isolated write/backup validation failed; inspect its private reports') from None
    finally:
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if reserved:
            await release_backup(redis,job_id)
        if redis is not None:
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
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(main(parser.parse_args()))
