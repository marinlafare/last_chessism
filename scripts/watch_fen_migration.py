"""Print the local live-migration report. No database or cloud connection."""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fen_migration_progress import STAGES, REFERENCE_STAGES


def duration(seconds):
    seconds = max(0, int(seconds or 0))
    return f'{seconds//3600:02}:{seconds//60%60:02}:{seconds%60:02}'


def render(report, deployment=None):
    lines = [f"LOCAL DATABASE MIGRATION — {report['state'].upper()}",
             f"Elapsed: {duration(report.get('elapsed_seconds'))}    Current stage: {duration(report.get('phase_elapsed_seconds'))}"]
    if report.get('mode', '').startswith('isolated'):
        lines.append('Mode: isolated rehearsal — live database unchanged')
    phase_labels = dict(STAGES + REFERENCE_STAGES)
    phase_labels.update({
        'restoring_verified_backup': 'Restore verified backup into isolated workspace',
        'starting_isolated_postgres': 'Start isolated PostgreSQL',
        'waiting_for_recovery': 'Wait for isolated PostgreSQL recovery to finish',
        'checking_restored_database': 'Validate restored database before migration',
        'testing_portable_backup_roundtrip': 'Test portable FEN backup roundtrip',
        'checking_restored_identity': 'Validate restored numeric identities',
        'rehearsal_complete': 'Rehearsal complete',
    })
    if report.get('phase'):
        lines.append('Current operation: ' + phase_labels.get(report['phase'], report['phase']))
    if report.get('failed_phase'):
        lines.append('Failed during: ' + phase_labels.get(report['failed_phase'], report['failed_phase']))
    if report.get('updated_at'):
        age = (datetime.now(timezone.utc)-datetime.fromisoformat(report['updated_at'])).total_seconds()
        lines.append(f'Last heartbeat: {int(max(0, age))}s ago' +
                     (' — STALE: inspect the migration container' if age > 30 and report['state']=='running' else ''))
    done = {p['phase']: p['seconds'] for p in report.get('phases', [])}
    stages = REFERENCE_STAGES if report.get('migration_kind') == 'references' else STAGES
    for number, (phase, label) in enumerate(stages, 1):
        mark = 'done' if phase in done else 'NOW ' if phase == report.get('phase') else '    '
        timing = f" ({duration(done[phase])})" if phase in done else ''
        lines.append(f'[{mark}] {number}/{len(stages)}  {label}{timing}')
    lines.append('')
    if report.get('rehearsal_stage_seconds') is not None:
        lines.append(f"This stage took {duration(report['rehearsal_stage_seconds'])} in rehearsal (reference, not an ETA).")
    database = report.get('database') or {}
    activity = database.get('activity') or {}
    if activity:
        wait = '/'.join(str(activity[k]) for k in ('wait_event_type','wait_event') if activity.get(k))
        lines.append(f"PostgreSQL: {activity['state']} | {activity['operation']} | statement {duration(activity['query_seconds'])}")
        lines.append(f"Wait: {wait or 'none reported'}")
    counters = database.get('counters')
    if counters:
        lines.append(f"Database substage: {counters['phase']}")
        for kind in ('blocks', 'tuples'):
            total, completed = counters.get(f'{kind}_total', 0), counters.get(f'{kind}_done', 0)
            if total:
                lines.append(f"  {kind}: {completed:,} / {total:,} ({completed/total*100:.1f}% of this substage)")
    elif report['state'] == 'running':
        lines.append('No exact row percentage is available for this statement.')
    if database.get('wal_bytes_since_start') is not None:
        lines.append(f"Database WAL generated: {database['wal_bytes_since_start']/1024**3:.2f} GiB (activity, NOT percent complete)")
    if report.get('monitor_error'):
        lines.append(f"Telemetry unavailable: {report['monitor_error']} — not a migration failure.")
    if report['state'] == 'succeeded':
        if report.get('mode', '').startswith('isolated'):
            lines.append('ISOLATED REHEARSAL SUCCEEDED. Live tables have not been changed.')
        elif (deployment and deployment.get('state') == 'succeeded'
                and report.get('started_at')
                and deployment.get('migration_started_at') == report['started_at']):
            lines.append('SCHEMA MIGRATION SUCCEEDED. Matching app deployed and verified; app reopened.')
            if not deployment.get('post_migration_backup_tested'):
                lines.append('Next: Backup database, then Test backup. Original table still retained.')
        else:
            lines.append('SCHEMA MIGRATION SUCCEEDED. App remains paused until deployment/verification.')
    elif report['state'] == 'failed':
        if report.get('mode', '').startswith('isolated'):
            lines.append('ISOLATED REHEARSAL FAILED. Live tables have not been changed.')
        else:
            lines.append('MIGRATION FAILED. Keep the app paused; inspect before restarting anything.')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, default=Path(__file__).resolve().parents[1] /
                        '.local-migrations/fen-identity/live/status.json')
    args = parser.parse_args()
    if args.report.exists():
        deployment_path = args.report.parent.parent / 'deployment.json'
        deployment = json.loads(deployment_path.read_text()) if deployment_path.exists() else None
        print(render(json.loads(args.report.read_text()), deployment))
    else:
        print('Live migration has not published its report yet.')
