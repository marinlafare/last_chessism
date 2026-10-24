"""Restore and measure the OLD database without migrating or contacting live PG.

Run in a separate backup-worker image. Mount this repository at /candidate,
the archive read-only, and a new output directory at /reports. Never mount
production PGDATA or its socket. The image's original /app supplies old SQL.
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.db_refactor_workload import TABLES, digest, discover_inputs, make_cases, measure_case


async def assert_private_database(connection, pgdata):
    state = await connection.fetchrow('''
        SELECT current_setting('data_directory') AS data_directory,
               current_setting('port') AS port, inet_server_addr()::text AS address,
               current_setting('transaction_read_only') AS read_only
    ''')
    if (state is None or Path(state['data_directory']).resolve() != pgdata.resolve()
            or state['port'] != '55432' or state['address'] is not None
            or state['read_only'] != 'on'):
        raise RuntimeError('Benchmark connection is not the private read-only restore')


async def inventory(connection):
    return {
        'columns': [dict(row) for row in await connection.fetch('''
            SELECT table_name,column_name,data_type,is_nullable,column_default
            FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY($1::text[])
            ORDER BY table_name,ordinal_position
        ''', list(TABLES))],
        'indexes': [dict(row) for row in await connection.fetch('''
            SELECT tablename,indexname,indexdef FROM pg_indexes
            WHERE schemaname='public' AND tablename=ANY($1::text[]) ORDER BY tablename,indexname
        ''', list(TABLES))],
        'sizes': [dict(row) for row in await connection.fetch('''
            SELECT c.relname AS table_name,c.reltuples::bigint AS estimated_rows,
                   pg_table_size(c.oid) AS table_bytes,pg_indexes_size(c.oid) AS index_bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND c.relname=ANY($1::text[]) ORDER BY c.relname
        ''', list(TABLES))],
        'settings': [dict(row) for row in await connection.fetch('''
            SELECT name,setting,unit FROM pg_settings WHERE name=ANY($1::text[]) ORDER BY name
        ''', ['server_version_num','shared_buffers','work_mem','effective_cache_size',
              'max_parallel_workers_per_gather','jit','random_page_cost','seq_page_cost'])],
    }


async def run(args):
    # No caller-provided live DSN is ever used. No application init_db call.
    os.environ['DATABASE_URL'] = 'postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation, release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool, close_redis_pool
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from scripts.fen_migration_progress import ProgressReporter

    if not 1 <= args.repeats <= 20 or not 1 <= args.query_timeout <= 300:
        raise ValueError('Bounded repetitions/timeout required')
    if not restore._SAFE_BACKUP_ID.fullmatch(args.backup_id):
        raise ValueError('Invalid backup ID')
    # Fail if a caller mounts the usual live locations despite the instructions.
    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Live PostgreSQL data/socket must not be mounted')
    if args.output.exists():
        raise RuntimeError('Choose a new output directory; reports are never overwritten')
    # Parent is a dedicated local report mount. Readable reports let the host
    # monitor progress even when Docker maps the postgres UID differently.
    args.output.mkdir(parents=True, mode=0o755)
    reporter = ProgressReporter(args.output / 'status.json', args.backup_id, {})
    reporter.status.update(mode='isolated_baseline', migration_kind='game_player_refactor',
                           live_database_changed=False, queries_completed=0, queries_total=0)
    await reporter.publish('checking_backup')
    redis = connection = monitor = None
    reserved = False
    job_id = 'db-refactor-baseline-' + uuid.uuid4().hex[:12]
    workspace = restore.RESTORE_TEST_ROOT / job_id
    pgdata, socket = workspace / 'pgdata', workspace / 'socket'
    reporter.status.update(job_id=job_id, workspace=str(workspace),
                           resource_profile=os.environ.get('BENCHMARK_RESOURCE_PROFILE', 'unspecified'))

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    try:
        redis = await get_redis_pool()
        await ensure_backup_reservation(redis, job_id)
        reserved = True
        monitor = asyncio.create_task(heartbeat())
        # Keep the filesystem lock until PostgreSQL has stopped, even on failure.
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Another restore owns the workspace')
            backup = restore.latest_backup_for_restore_test()
            if (backup['backup_id'] != args.backup_id or not backup.get('restore_tested')
                    or backup.get('restore_test_status') != 'success'):
                raise RuntimeError('Latest selected recovery point must have a passing restore test')
            required = int(backup['database_bytes']) + 80_000_000_000
            free = shutil.disk_usage(restore.RESTORE_TEST_ROOT).free
            if free < required:
                raise RuntimeError('Insufficient free space for isolated restore and later candidate')
            try:
                workspace.mkdir(mode=0o700)
                pgdata.mkdir(mode=0o700)
                socket.mkdir(mode=0o700)
                await reporter.publish('restoring_verified_backup', free_bytes=free, required_free_bytes=required)
                await restore._run_restore(backup_id=args.backup_id, pgdata=pgdata, redis=None, job_id=job_id)
                await reporter.publish('starting_isolated_postgres')
                await restore._run_command_capture(restore.PG_CTL, '-D', str(pgdata), '-l',
                    str(workspace/'postgres.log'), '-w', '-t', '300', '-o',
                    restore._postgres_start_options(socket), 'start')
                connection = await asyncpg.connect(host=str(socket), port=55432,
                    user=restore.RESTORE_USER, database=restore.RESTORE_DATABASE)
                await reporter.publish('waiting_for_recovery')
                await wait_for_recovery_completion(connection)
                await connection.close()
                connection = await asyncpg.connect(host=str(socket), port=55432,
                    user=restore.RESTORE_USER, database=restore.RESTORE_DATABASE,
                    server_settings={'default_transaction_read_only':'on',
                                     'statement_timeout':str(args.query_timeout * 1000),
                                     'lock_timeout':'1000', 'application_name':'db-refactor-baseline'})
                await assert_private_database(connection, pgdata)
                await reporter.publish('checking_legacy_schema')
                schema = await inventory(connection)
                columns = {(row['table_name'],row['column_name']) for row in schema['columns']}
                if not {('game_fen_association','fen_fen'),('player_position_frequency','fen_fen'),
                        ('game_player','opponent_rating'),('game','rules')} <= columns:
                    raise RuntimeError('Expected legacy schema; refusing to measure a different layout')
                probe = json.loads(await connection.fetchval(restore._validation_query()))
                restore._validate_probe(probe)
                await reporter.publish('selecting_fixed_inputs')
                inputs = await discover_inputs(connection)
                cases = make_cases(inputs, args.legacy_source_root)
                manifest = {'version':1, 'schema_contract':'legacy_game_player_text_fen_v1',
                    'backup':backup, 'inputs':inputs, 'cases':cases, 'schema':schema,
                    'restore_validation':probe,
                    'resource_profile':reporter.status['resource_profile'],
                    'case_sha256':digest(cases), 'repeats':args.repeats,
                    'query_timeout_seconds':args.query_timeout,
                    'limitations':['Database reads only; not HTTP latency or write throughput.',
                        'First observation is not a guaranteed cold-cache measurement.',
                        'Small-sample p95 is not a production tail-latency estimate.',
                        'Candidate performance, archive correctness, write tests and backup/restore remain required.']}
                atomic_write_json(args.output/'manifest.json', manifest)
                results = []
                reporter.status['queries_total'] = len(cases)
                for case in cases:
                    await reporter.publish('measuring_queries', current_query=case['name'], sample=0)
                    async def sample(attempt, total):
                        reporter.status.update(sample=attempt, samples_per_query=total)
                        reporter.flush()
                    try:
                        result = await measure_case(connection, case, args.repeats, sample)
                    except asyncpg.QueryCanceledError:
                        result = {'name':case['name'],'status':'timeout','timeout_seconds':args.query_timeout}
                    results.append(result)
                    atomic_write_json(args.output/'results.json', {'case_sha256':manifest['case_sha256'],
                                                                 'results':results})
                    reporter.status['queries_completed'] = len(results)
                    reporter.status['query_timeouts'] = sum(r['status']=='timeout' for r in results)
                await reporter.publish('stopping_private_database')
            finally:
                if connection is not None:
                    await connection.close()
                    connection = None
                if workspace.exists():
                    await restore._stop_restored_postgres(pgdata)
                    if (pgdata/'postmaster.pid').exists():
                        raise RuntimeError('Private PostgreSQL did not stop; inspect retained workspace')
        # Keep the stopped copy for the candidate. Its prefix deliberately avoids
        # the regular backup worker's rehearsal-* stale-workspace cleanup.
        state = 'completed_with_timeouts' if reporter.status.get('query_timeouts') else 'succeeded'
        await reporter.finish(state, 'baseline_complete', retained_workspace=True,
                              next_step='Prepare candidate migration; live migration is NOT authorized by this result')
    except BaseException as error:
        await reporter.finish('failed','baseline_failed', error_type=type(error).__name__,
                              failed_phase=reporter.status.get('phase'), retained_workspace=workspace.exists())
        # Do not print database exceptions: they can contain record values/DSNs.
        raise RuntimeError('Baseline failed; consult its sanitized status report') from None
    finally:
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if reserved:
            await release_backup(redis, job_id)
        if redis is not None:
            await close_redis_pool()


async def main(args):
    task = asyncio.current_task()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, task.cancel)
    await asyncio.wait_for(run(args), timeout=args.max_seconds)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--legacy-source-root', type=Path, default=Path('/app'))
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--query-timeout', type=int, default=60)
    parser.add_argument('--max-seconds', type=int, default=7200)
    asyncio.run(main(parser.parse_args()))
