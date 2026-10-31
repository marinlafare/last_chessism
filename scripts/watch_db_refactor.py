"""Read local baseline progress; no PostgreSQL or cloud calls."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


def render(report):
    elapsed = int(report.get('elapsed_seconds', 0))
    candidate = report.get('mode')=='isolated_candidate'
    writes = report.get('mode')=='isolated_write_validation'
    read_retune = report.get('mode')=='isolated_application_reads'
    preflight = report.get('mode')=='isolated_cutover_preflight'
    release = report.get('mode')=='isolated_canonical_release'
    http_backup = report.get('mode')=='isolated_http_backup'
    corpus = report.get('mode')=='isolated_corpus_finalization'
    sequence = report.get('mode')=='isolated_migration_sequence'
    live = report.get('mode')=='live_game_player'
    application = report.get('mode')=='isolated_application' or read_retune
    title = 'WRITE/BACKUP REHEARSAL' if writes else ('CANDIDATE REHEARSAL' if candidate else 'BASELINE')
    if application:
        title = 'APPLICATION / ONLINE BACKUP REHEARSAL'
    if read_retune:
        title = 'APPLICATION READ RETUNE'
    if preflight:
        title = 'CANONICAL SWITCH REHEARSAL' if report.get('rehearse_switch') else 'CUTOVER DEPENDENCY PREFLIGHT'
    if release:
        title = 'CANONICAL STARTUP / RESTART REHEARSAL'
    if http_backup:
        title = 'HTTP / QUEUE / BACKUP ACCEPTANCE'
    if corpus:
        title = 'CORPUS SUMMARY / ROLLBACK ACCEPTANCE'
    if sequence:
        title = 'WHOLE MIGRATION REHEARSAL'
    if live:
        title = 'GAME / PLAYER MIGRATION'
    lines = [f"ISOLATED DB {title} — {report['state'].upper()}",
             'Live database unchanged. Local rehearsal only; no cloud jobs.',
             f"Phase: {report.get('phase', 'starting')}",
             f"Elapsed: {elapsed//3600:02}:{elapsed//60%60:02}:{elapsed%60:02}",
             f"Queries: {report.get('queries_completed',0)} / {report.get('queries_total',0)}",
             f"Timeouts: {report.get('query_timeouts',0)}"]
    if live:
        lines[0]=f"LIVE DB {title} — {report['state'].upper()}"
        lines[1]='Approved maintenance operation. Keep application services stopped.'
    if sequence or live:
        lines = lines[:4]+[
            f"Current phase elapsed: {int(report.get('phase_elapsed_seconds',0))}s",
            f"Row checks passed: {report.get('row_checks',0)} | Summary steps: {report.get('steps_completed',0)} / 8",
            f"Combined cutover/summary rollback: {'verified' if report.get('rollback_verified') else 'pending'}"]
        if not live:
            lines.append(f"Source backup: {report.get('snapshot_at','unknown')} (not the current live DB)")
        else:
            lines.append(f"Rollback backup: {report.get('backup_id','unknown')}")
        if report.get('phase')=='copying_whole_migration_database':
            total=report.get('source_bytes',0);done=report.get('copy_bytes_done',0)
            lines.append(f"Copy: {done/1024**3:.1f} / {total/1024**3:.1f} GiB ({100*done/total if total else 0:.1f}%)")
        db=report.get('database') or {}
        activity=db.get('activity') or {}
        if activity.get('state')=='active':
            lines.append(f"PostgreSQL: {activity.get('operation','working')} — {int(activity.get('query_seconds') or 0)}s")
            if activity.get('wait_event'):
                lines.append(f"Wait: {activity.get('wait_event_type')} / {activity['wait_event']}")
        counters=db.get('counters') or {}
        if counters:
            lines.append(f"Index/statistics: {counters.get('phase','working')}")
            for unit in ('blocks','tuples'):
                total=counters.get(unit+'_total',0);done=counters.get(unit+'_done',0)
                if total:
                    lines.append(f"{unit.capitalize()}: {done:,} / {total:,} ({100*done/total:.1f}%)")
        if db.get('wal_bytes_since_start') is not None:
            lines.append(f"Private WAL generated: {db['wal_bytes_since_start']/1024**3:.2f} GiB (activity, not completion %)")
        if report.get('phase')=='whole_migration_complete':
            lines.append('Private PostgreSQL stopped. Full rehearsal passed; live migration still requires approval.')
        if report.get('phase')=='migration_committed_services_still_stopped':
            lines.append('Migration committed; original tables retained. Coordinated service deployment is next.')
    if corpus:
        lines=lines[:4]+[f"Summary steps: {report.get('steps_completed',0)} / {report.get('steps_total',8)}",
            f"Current step elapsed: {int(report.get('phase_elapsed_seconds',0))}s",
            f"Injected-failure rollback: {'verified' if report.get('rollback_verified') else 'pending'}",
            'Independent private copy; no live migration, API/worker deployment or cloud jobs.']
        if report.get('phase')=='copying_corpus_rehearsal_database':
            total=report.get('source_bytes',0);done=report.get('copy_bytes_done',0)
            lines.append(f"Copy: {done/1024**3:.1f} / {total/1024**3:.1f} GiB ({100*done/total if total else 0:.1f}%)")
        if report.get('phase')=='corpus_finalization_complete':
            lines.append('Private PostgreSQL stopped. Summary validation and restart passed; live cutover still gated.')
    if http_backup:
        lines=lines[:4]+[f"HTTP checks: {report.get('http_checks',0)} | Completed queue jobs: {report.get('completed_jobs',0)} / 3",
            f"Current phase elapsed: {int(report.get('phase_elapsed_seconds',0))}s",
            f"Worker phase: {report.get('worker_phase') or 'not started'}"]
        total=int(report.get('worker_total') or 0)
        done=int(report.get('worker_processed') or 0)
        if total:
            lines.append(f"Worker progress: {done:,} / {total:,} ({100*done/total:.1f}%)")
        if report.get('phase')=='copying_http_rehearsal_database':
            total=report.get('source_bytes',0);done=report.get('copy_bytes_done',0)
            lines.append(f"Copy: {done/1024**3:.1f} / {total/1024**3:.1f} GiB ({100*done/total if total else 0:.1f}%)")
        lines.append('Isolated API/Redis/worker, no external network. No live database or cloud work.')
        if report.get('phase')=='http_backup_acceptance_complete':
            lines.append('Private services stopped; test backup retained. Live migration still requires approval.')
    if release:
        lines=lines[:4]+[f"Checks passed: {report.get('checks_completed',0)}",
            f"Current step elapsed: {int(report.get('phase_elapsed_seconds',0))}s",
            'Independent private copy only. No API/worker deployment or cloud jobs.']
        if report.get('phase')=='copying_private_release_workspace':
            total=report.get('source_bytes',0)
            done=report.get('copy_bytes_done',0)
            lines.append(f"Copy: {done/1024**3:.1f} / {total/1024**3:.1f} GiB ({100*done/total if total else 0:.1f}%)")
        if report.get('phase')=='canonical_release_rehearsal_complete':
            lines.append('Private PostgreSQL stopped. Accepted source preserved; live-cutover gates remain.')
    if preflight:
        lines=lines[:4]+[
            f"Relations: {report.get('relations',0)} | Foreign keys: {report.get('foreign_keys',0)}",
            f"Triggers: {report.get('triggers',0)} | View dependencies: {report.get('dependent_views',0)}",
            f"Sequences: {report.get('sequences',0)} | Functions: {report.get('functions',0)}",
            ('Private transaction only; every switch is rolled back, including success.' if report.get('rehearse_switch')
             else 'Read-only metadata inventory. No table switch or migration performed.')]
        if report.get('phase') in ('cutover_inventory_complete','cutover_switch_rehearsal_complete'):
            lines.append('Private PostgreSQL stopped. Dependency review and switch rehearsal still required.')
        if report.get('switch_rolled_back'):
            lines[-1]=f"Private PostgreSQL stopped. Switch checks: {report.get('switch_checks',0)}. Rollback verified; no deployment."
    if application:
        lines[-1]=f"Read differences: {report.get('result_mismatches',0)} | Slowdown flags: {report.get('slowdown_flags',0)}"
        lines.append(f"Query errors: {report.get('query_errors',0)}")
        lines.append(f"Current step elapsed: {int(report.get('phase_elapsed_seconds',0))}s")
        if report.get('phase','').startswith('online_application_'):
            lines.append(f"Backup worker phase: {report.get('backup_phase','starting')}")
            total=int(report.get('backup_bytes_total') or 0)
            done=int(report.get('backup_bytes_done') or 0)
            if total:
                lines.append(f"Backup bytes: {done:,} / {total:,} ({100*done/total:.1f}%)")
        if report.get('phase')=='restoring_online_application_backup':
            lines.append(f"Restore progress: {report.get('phase_progress_percent',0)}%")
        if report.get('phase') in ('application_rehearsal_complete','application_read_retune_complete'):
            lines.append('Private PostgreSQL stopped. Review required before canonical/live cutover.')
        if read_retune:
            lines.append('Read checks only. No new backups, ingestion, schema changes or live migration.')
        lines.append('Existing backups and live services unchanged; backup lane reserved while running.')
    if writes:
        lines=lines[:4]+[f"Write checks: {report.get('checks_completed',0)} / {report.get('checks_total',7)}"]
        if report.get('phase')=='checking_candidate_writes':
            lines.append(f"Latest check: {report.get('current_check')}")
        if report.get('phase')=='checking_normalized_backup_contents':
            lines.append(f"Tables: {report.get('tables_completed',0)} / {report.get('tables_total',0)} — {report.get('current_table')}")
        if report.get('phase') in ('backing_up_private_candidate','restoring_private_backup'):
            lines.append(f"Phase progress: {report.get('phase_progress_percent',0)}%")
        lines.append('Backup: private offline pgBackRest repository; existing backups untouched.')
        if report.get('phase')=='write_backup_validation_complete':
            lines.append(f"Writes: {report.get('write_checks',0)} passed | import core: {report.get('cloud_import_checks',0)} passed | restored writes: {report.get('restored_write_checks',0)} passed")
            lines.append('Private PostgreSQL stopped. Application integration / live deployment still pending.')
    if report.get('state') == 'running':
        stamp = report.get('updated_at')
        if stamp:
            age = (datetime.now(timezone.utc)-datetime.fromisoformat(stamp)).total_seconds()
            if age > 30:
                lines.append(f'WARNING: heartbeat is {int(age)}s old; inspect the container.')
        if report.get('phase','').startswith('measuring_'):
            lines.append(f"Current: {report.get('current_query')}" if application else
                         f"Current: {report.get('current_query')} — sample "
                         f"{report.get('sample',0)} / {report.get('samples_per_query','?')}")
        elif report.get('phase')=='verifying_tuned_queries':
            lines.append(f"Checking exact output: {report.get('current_query')}")
        elif report.get('phase')=='indexing_candidate_reads':
            lines.append(f"Index: {report.get('current_index')}")
    if report.get('state') == 'failed':
        lines.append(f"Failed at: {report.get('failed_phase')} ({report.get('error_type')})")
    if report.get('phase') == 'baseline_complete':
        lines.append('Baseline finished; private PostgreSQL stopped. Candidate migration is still pending.')
    if candidate:
        if report.get('original_mismatches_explained') is not None:
            lines.append(f"Original type mismatches verified: {report['original_mismatches_explained']}")
        if report.get('added_player'):
            player=report['added_player']
            lines.append(f"Added player: {player['player_name']} ({player['games']:,} games)")
        if report.get('phase')=='candidate_rehearsal_complete':
            lines.append(f"Result mismatches: {report.get('result_mismatches',0)} | "
                         f"Slowdown flags: {report.get('slowdown_flags',0)}")
            lines.append('Private PostgreSQL stopped. NOT ready for production deployment.')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        print(render(json.loads(args.report.read_text())))
    else:
        print('Waiting for the baseline report.')
