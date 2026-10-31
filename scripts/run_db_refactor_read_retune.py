"""Rerun actual application reads on the untouched private candidate; no writers/backups."""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
import sys
import traceback
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def run(args):
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.redis_client import get_redis_pool,close_redis_pool
    from scripts.run_db_refactor_candidate import verified_workspace,verified_retune
    from scripts.run_db_refactor_application import source_fingerprint
    from scripts.db_refactor_candidate import guard
    from scripts.db_refactor_application_checks import measure_application_reads
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from scripts.fen_migration_progress import ProgressReporter

    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists() or Path('/backup-volume').is_mount():
        raise RuntimeError('Live database, socket and existing backups must not be mounted')
    baseline=json.loads((args.baseline/'manifest.json').read_text())
    workspace=verified_workspace(json.loads((args.baseline/'status.json').read_text()),baseline,restore.RESTORE_TEST_ROOT)
    prior=verified_retune(args.candidate,baseline,workspace)
    app=json.loads((args.application/'status.json').read_text())
    acceptance=json.loads((args.application/'acceptance.json').read_text())
    if (prior['status']['state']!='succeeded' or app.get('state') not in ('needs_review','succeeded')
        or app.get('phase')!='application_rehearsal_complete' or app.get('live_database_changed') is not False
        or app.get('backup_id')!=baseline['backup']['backup_id']
        or app.get('source_comparison')!=str(args.candidate)
        or acceptance.get('application_write_checks')!='passed'
        or acceptance.get('online_backup_worker_and_wal_restore')!='passed'
        or str(workspace) in (app.get('workspace'),app.get('restored_workspace'))):
        raise RuntimeError('Need the completed app/backup report and a separate unchanged read copy')
    if os.environ.get('BENCHMARK_RESOURCE_PROFILE')!=baseline['resource_profile']:
        raise RuntimeError('Resource profile must match')
    if args.output.exists():
        raise RuntimeError('Use a new report directory')
    args.output.mkdir(parents=True)
    reporter=ProgressReporter(args.output/'status.json',baseline['backup']['backup_id'],{})
    reporter.status.update(mode='isolated_application_reads',live_database_changed=False,
        production_ready=False,workspace=str(workspace),source_comparison=str(args.candidate),
        prior_application=str(args.application),focused=args.focused,ratings_only=args.ratings_only,
        analyze_plans=args.analyze_plans,queries_completed=0,queries_total=0,
        resource_profile=baseline['resource_profile'])
    await reporter.publish('checking_read_retune_workspace')
    root=Path(__file__).resolve().parents[1]
    fingerprints=source_fingerprint(root)
    atomic_write_json(args.output/'source-files.json',fingerprints)
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    redis=conn=monitor=None
    reserved=started=False
    job_id='db-refactor-reads-'+uuid.uuid4().hex[:12]
    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    def save(rows):
        reporter.status.update(queries_completed=len(rows),result_mismatches=sum(not r['matches'] for r in rows),
            slowdown_flags=sum(r['slowdown'] for r in rows),query_errors=sum(bool(r['errors']) for r in rows))
        atomic_write_json(args.output/'application-reads.json',{'comparison_version':2,
            'scope':'ratings' if args.ratings_only else ('focused' if args.focused else 'full'),'results':rows})
        reporter.flush()
    try:
        redis=await get_redis_pool()
        await ensure_backup_reservation(redis,job_id)
        reserved=True
        monitor=asyncio.create_task(heartbeat())
        with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
            if not acquired:
                raise RuntimeError('Shared rehearsal workspace is busy')
            if not (pgdata/'PG_VERSION').exists() or (pgdata/'postmaster.pid').exists():
                raise RuntimeError('Read candidate must be retained and stopped')
            try:
                started=True
                await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(workspace/'read-retune-postgres.log'),
                    '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
                conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,
                    database=restore.RESTORE_DATABASE,server_settings={'application_name':'read-retune-guard'})
                await wait_for_recovery_completion(conn)
                await guard(conn,pgdata)
                await conn.execute("SET default_transaction_read_only='on'")
                players=prior['manifest']['inputs']['players']
                methods=None
                if args.focused:
                    players=[p for p in players if p['cohort']!='large']
                    methods={'get_player_games_page','get_player_game_summary','get_player_performance_summary',
                        'get_player_game_averages','get_player_termination_stats','get_player_behavioural_activity',
                        'get_player_behavioural_ratings'}
                await measure_application_reads(socket,players,reporter.publish,save,repeats=args.repeats,
                    methods=methods,capture_plans=True,ratings_only=args.ratings_only,analyze_plans=args.analyze_plans)
                if fingerprints!=source_fingerprint(root):
                    raise RuntimeError('Source changed during the measured run')
            finally:
                if conn:
                    await conn.close(); conn=None
                if started:
                    await restore._stop_restored_postgres(pgdata)
                    if (pgdata/'postmaster.pid').exists():
                        raise RuntimeError('Private database did not stop')
                    started=False
        state='needs_review' if reporter.status['result_mismatches'] or reporter.status['slowdown_flags'] or reporter.status['query_errors'] else 'succeeded'
        atomic_write_json(args.output/'acceptance.json',{'read_checks':state,'production_ready':False,
            'comparison_exclusion':'Only top-level generated_at on the four timestamped response types; timezone syntax still validated.',
            'scope':'ratings: four modes and one calendar-year filter per cohort' if args.ratings_only
                else ('focused' if args.focused else '36 application calls, four player cohorts'),
            'backup_checks':'Not rerun; historical passed result remains in '+str(args.application),
            'live_cutover':'Not authorized or performed.'})
        await reporter.finish(state,'application_read_retune_complete',production_ready=False)
    except BaseException as error:
        atomic_write_json(args.output/'failure-location.json',{'type':type(error).__name__,
            'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
                      for f in traceback.extract_tb(error.__traceback__)]})
        await reporter.finish('failed','application_read_retune_failed',failed_phase=reporter.status.get('phase'),
                              error_type=type(error).__name__,production_ready=False)
        raise RuntimeError('Private read-only retune failed; see report') from None
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
    await asyncio.wait_for(run(args),timeout=2*60*60)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('baseline','candidate','application','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--repeats',type=int,choices=range(3,6),default=3)
    scope=parser.add_mutually_exclusive_group()
    scope.add_argument('--focused',action='store_true')
    scope.add_argument('--ratings-only',action='store_true')
    parser.add_argument('--analyze-plans',action='store_true',help='Capture actual SELECT execution plans outside the timed samples, in read-only sessions')
    asyncio.run(main(parser.parse_args()))
