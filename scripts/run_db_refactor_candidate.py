"""Continue a successful PRIVATE baseline with shadow tables and read comparisons.

Never restores over, connects to, or migrates production. No app deployment.
The old public schema and the original baseline reports remain intact.
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
from scripts.db_refactor_workload import digest,make_cases,measure_case
from scripts.db_refactor_comparison import add_verified_medium,candidate_case,compare_results
from scripts.db_refactor_candidate import C,A,R,V,guard,prepare_reference,build_candidate
from scripts.run_db_refactor_baseline import assert_private_database
from scripts.db_refactor_tuning import apply_tuning,tuned_candidate_case,verify_original_mismatches


def verified_workspace(status,manifest,root):
    if (status.get('state')!='succeeded' or status.get('mode')!='isolated_baseline'
            or status.get('backup_id')!=manifest.get('backup',{}).get('backup_id')
            or manifest.get('case_sha256')!=digest(manifest.get('cases'))):
        raise ValueError('Successful matching baseline and intact workload are required')
    workspace = Path(status['workspace'])
    if (workspace.parent!=root or not workspace.name.startswith('db-refactor-baseline-')
            or workspace.is_symlink() or workspace.resolve()!=workspace):
        raise ValueError('Not a retained private baseline workspace')
    return workspace


def verified_retune(path, baseline, workspace):
    """Only reuse a completed, fully row-validated candidate for this baseline."""
    prior = {key:json.loads((path/file).read_text()) for key,file in (
        ('status','status.json'),('manifest','manifest.json'),
        ('validation','row-validation.json'),('reference','scoped_reference.json'),
        ('candidate','candidate.json'),('comparison','comparison.json'))}
    status,manifest = prior['status'],prior['manifest']
    required = {'game','game_player','game_opening','game_analysis_summary',
        'game_player_salience','game_player_engine_summary','player_salience_pending_game',
        'player_salience_summary','player','archive_game','archive_moves',
        'game_fen_association','player_position_frequency'}
    if (status.get('state') not in ('needs_review','succeeded')
            or status.get('phase')!='candidate_rehearsal_complete'
            or status.get('mode')!='isolated_candidate'
            or status.get('live_database_changed') is not False
            or status.get('workspace')!=str(workspace)
            or status.get('backup_id')!=baseline['backup']['backup_id']
            or manifest.get('baseline_case_sha256')!=baseline['case_sha256']
            or manifest.get('resource_profile')!=baseline['resource_profile']
            or set(prior['validation'])!=required
            or any(r.get('mismatches')!=0 for r in prior['validation'].values())):
        raise ValueError('Retuning requires a completed, matching, fully validated private candidate')
    for label,case_key in (('reference','reference_cases'),('candidate','candidate_cases')):
        if prior[label].get('case_sha256')!=digest(manifest[case_key]):
            raise ValueError('Prior query manifest does not match its measured report')
        if {r['name'] for r in prior[label]['results']}!={c['name'] for c in manifest[case_key]}:
            raise ValueError('Prior query suite is incomplete')
    return prior


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

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Production data/socket must not be mounted')
    baseline_status=json.loads((args.baseline/'status.json').read_text())
    baseline=json.loads((args.baseline/'manifest.json').read_text())
    workspace=verified_workspace(baseline_status,baseline,restore.RESTORE_TEST_ROOT)
    prior=verified_retune(args.retune_from,baseline,workspace) if args.retune_from else None
    if os.environ.get('BENCHMARK_RESOURCE_PROFILE')!=baseline['resource_profile']:
        raise RuntimeError('Use the same resource limits as the baseline')
    if args.output.exists():
        raise RuntimeError('Use a new report directory; prior reports are retained')
    args.output.mkdir(parents=True,mode=0o755)
    reporter=ProgressReporter(args.output/'status.json',baseline_status['backup_id'],{})
    reporter.status.update(mode='isolated_candidate',migration_kind='game_player_refactor',
                           live_database_changed=False,queries_completed=0,queries_total=0,
                           workspace=str(workspace),comparison_acceptance='pending')
    await reporter.publish('checking_private_workspace')
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    redis=conn=monitor=None
    reserved=False
    job_id='db-refactor-candidate-'+uuid.uuid4().hex[:12]

    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)

    async def suite(label,cases,search_schema):
        await conn.execute('SELECT set_config($1,$2,false)','search_path',search_schema+',public')
        await conn.reload_schema_state()
        await conn.execute("SET default_transaction_read_only='on'")
        await conn.execute('SELECT set_config($1,$2,false)','statement_timeout',str(baseline['query_timeout_seconds']*1000))
        results=[]
        reporter.status.update(queries_completed=0,queries_total=len(cases),query_timeouts=0,suite=label)
        for case in cases:
            await reporter.publish('measuring_'+label,current_query=case['name'],sample=0)
            async def sample(attempt,total):
                reporter.status.update(sample=attempt,samples_per_query=total)
                reporter.flush()
            try:
                result=await measure_case(conn,case,baseline['repeats'],sample)
            except asyncpg.QueryCanceledError:
                result={'name':case['name'],'status':'timeout','timeout_seconds':baseline['query_timeout_seconds']}
            results.append(result)
            atomic_write_json(args.output/(label+'.json'),{'case_sha256':digest(cases),'results':results})
            reporter.status.update(queries_completed=len(results),query_timeouts=sum(r['status']=='timeout' for r in results))
        return results

    try:
        redis=await get_redis_pool()
        await ensure_backup_reservation(redis,job_id)
        reserved=True
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Restore workspace is busy')
            if not (pgdata/'PG_VERSION').is_file() or (pgdata/'postmaster.pid').exists():
                raise RuntimeError('Baseline must be retained and stopped')
            free=shutil.disk_usage(workspace).free
            if free<100_000_000_000:
                raise RuntimeError('Need 100 GB of free rehearsal disk headroom')
            started=False
            try:
                await reporter.publish('starting_private_database',free_bytes=free)
                # Mark ownership before pg_ctl: even a startup timeout may leave
                # a server that must be stopped before releasing the lock.
                started=True
                await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'candidate-postgres.log'),
                    '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
                conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,
                    database=restore.RESTORE_DATABASE,server_settings={'application_name':'db-refactor-candidate'})
                await wait_for_recovery_completion(conn)
                await guard(conn,pgdata)
                await conn.execute("SET default_transaction_read_only='on'")
                await assert_private_database(conn,pgdata)
                await reporter.publish('selecting_verified_medium_player')
                inputs=prior['manifest']['inputs'] if prior else await add_verified_medium(conn,baseline['inputs'])
                cases=make_cases(inputs,args.legacy_source_root)
                # Existing query cases must stay byte-for-byte identical.
                originals={c['name']:c for c in baseline['cases']}
                if any(case!=originals[case['name']] for case in cases if case['name'] in originals):
                    raise RuntimeError('Deployed SQL changed since baseline; use the pinned image')
                if prior and cases!=prior['manifest']['reference_cases']:
                    raise RuntimeError('Retuned run must keep every reference case and input unchanged')
                adapted=[tuned_candidate_case(c) if prior else candidate_case(c) for c in cases]
                atomic_write_json(args.output/'manifest.json',{'baseline_case_sha256':baseline['case_sha256'],
                    'inputs':inputs,'reference_cases':cases,'candidate_cases':adapted,
                    'resource_profile':baseline['resource_profile'],
                    'retuned_from':str(args.retune_from) if prior else None,
                    'limitations':['Read/schema rehearsal only; NOT a production-ready migration.',
                        'Production ingestion/import and archive routing are not deployed.',
                        'Salience corpus-summary rebuild and new backup/restore acceptance remain pending.']})
                reporter.status['added_player']=inputs['players'][-1]
                if prior:
                    for schema in (C,A,R,V):
                        if not await conn.fetchval('SELECT to_regnamespace($1) IS NOT NULL',schema):
                            raise RuntimeError('The previously validated private candidate is missing')
                    await conn.execute("SET statement_timeout='60s'")
                    await reporter.publish('verifying_original_output_mismatches')
                    diagnosis=await verify_original_mismatches(conn,prior)
                    atomic_write_json(args.output/'mismatch-diagnosis.json',{'cases':diagnosis})
                    reporter.status.update(original_mismatches_explained=len(diagnosis),
                        validation_reused_from=str(args.retune_from/'row-validation.json'))
                    scoped=prior['comparison']['scope']
                else:
                    await suite('added_player_baseline',[c for c in cases if c['name'].startswith('medium_verified/')],'public')
                    await conn.execute("SET default_transaction_read_only='off'")
                    await reporter.publish('preparing_standard_game_reference')
                    scoped=await prepare_reference(conn,pgdata)
                reporter.status.update(scoped)
                if prior:
                    # Catch contract mistakes on the real snapshot before
                    # spending minutes remeasuring the expensive reference.
                    await conn.execute("SET default_transaction_read_only='off'")
                    await apply_tuning(conn,pgdata,reporter.publish)
                    await conn.execute("SET default_transaction_read_only='on'")
                    await conn.execute("SET statement_timeout='60s'")
                    await conn.execute('SELECT set_config($1,$2,false)','search_path',V+',public')
                    await conn.reload_schema_state()
                    expected={r['name']:r for r in prior['reference']['results']}
                    focused=[c for c in adapted if c['source'].startswith('candidate v2:')]
                    checks=[]
                    reporter.status.update(queries_completed=0,queries_total=len(focused))
                    from scripts.db_refactor_workload import row_digest
                    for case in focused:
                        await reporter.publish('verifying_tuned_queries',current_query=case['name'])
                        async with conn.transaction(readonly=True):
                            rows=await conn.fetch(case['sql'],*case['args'])
                        matches=row_digest(rows)==expected[case['name']].get('result_sha256')
                        checks.append({'name':case['name'],'rows':len(rows),'results_match':matches})
                        atomic_write_json(args.output/'focused-validation.json',{'queries':checks})
                        reporter.status['queries_completed']=len(checks)
                        if not matches:
                            raise RuntimeError('Retuned query does not match the exact saved reference')
                before=await suite('scoped_reference',cases,R)
                await conn.execute("SET default_transaction_read_only='off'")
                await conn.execute("SET search_path='public'")
                await conn.execute("SET statement_timeout='2h'")
                if prior:
                    comparisons=prior['validation']
                else:
                    comparisons=await build_candidate(conn,pgdata,reporter.publish)
                atomic_write_json(args.output/'row-validation.json',comparisons)
                sizes=[dict(row) for row in await conn.fetch('''
                    SELECT n.nspname AS schema,c.relname AS name,c.reltuples::bigint AS estimated_rows,
                        pg_table_size(c.oid) AS table_bytes,pg_indexes_size(c.oid) AS index_bytes
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname=ANY($1::text[]) AND c.relkind='r' ORDER BY n.nspname,c.relname
                ''',[C,A])]
                atomic_write_json(args.output/'sizes.json',{'relations':sizes})
                after=await suite('candidate',adapted,V)
                comparison=compare_results(before,after)
                atomic_write_json(args.output/'comparison.json',{'comparisons':comparison,'scope':scoped,
                    'production_ready':False,'candidate_storage':sizes})
                mismatches=sum(not r['results_match'] for r in comparison)
                slowdowns=sum(r['investigate_slowdown'] for r in comparison)
                reporter.status.update(result_mismatches=mismatches,slowdown_flags=slowdowns,
                    comparison_acceptance='needs_review' if mismatches or slowdowns else 'read_checks_passed',
                    production_ready=False)
                await reporter.publish('stopping_private_database')
            finally:
                if conn is not None:
                    await conn.close()
                    conn=None
                if started:
                    await restore._stop_restored_postgres(pgdata)
                    if (pgdata/'postmaster.pid').exists():
                        raise RuntimeError('Private server did not stop')
        await reporter.finish('needs_review' if mismatches or slowdowns else 'succeeded','candidate_rehearsal_complete',
            next_step='Review comparisons; complete writer/cache/backup tests before any live migration')
    except BaseException as error:
        await reporter.finish('failed','candidate_rehearsal_failed',error_type=type(error).__name__,
                              failed_phase=reporter.status.get('phase'),production_ready=False)
        raise RuntimeError('Candidate rehearsal failed; inspect the local sanitized report') from None
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
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--retune-from',type=Path,
                        help='Reuse a stopped, validated candidate; diagnose outputs and add read indexes only')
    parser.add_argument('--legacy-source-root',type=Path,default=Path('/app'))
    asyncio.run(main(parser.parse_args()))
