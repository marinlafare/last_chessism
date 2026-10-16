"""Print local compact-schema preflight/rehearsal progress; no DB/cloud access."""
import argparse
import json
from pathlib import Path


def render(report):
    elapsed = int(report.get('elapsed_seconds', 0))
    live=report.get('mode')=='live_compact'
    title='LIVE DB COMPACT MIGRATION' if live else 'ISOLATED DB STRUCTURAL CHECKS'
    lines = [f"{title} — {report.get('state','unknown').upper()}",
        'Approved maintenance. Keep application services stopped.' if live else 'Live database unchanged. No cloud jobs.',
        f"Phase: {report.get('phase','starting')}",
        f"Elapsed: {elapsed//3600:02}:{elapsed//60%60:02}:{elapsed%60:02}",
        f"Current phase: {int(report.get('phase_elapsed_seconds',0))} seconds"]
    if report.get('mode') in ('isolated_compact_acceptance','isolated_compact_index_retune'):
        lines.append(f"Acceptance checks passed: {report.get('checks_completed',0)}")
        lines.append(f"Affected queries: {report.get('queries_completed',0)} / {report.get('queries_total',0)}")
        if report.get('current_check'):
            lines.append('Latest check: '+report['current_check'])
        if report.get('phase')=='measuring_affected_queries':
            lines.append('Query: '+report.get('current_query',''))
    elif report.get('checks_total'):
        lines.append(f"Full-data checks: {report.get('checks_completed',0)} / {report['checks_total']}")
    if report.get('detail') and (report.get('phase')=='restoring_snapshot' or report.get('state')!='running'):
        lines.append(str(report['detail']))
    if live:
        database=report.get('database') or {}
        activity=database.get('activity') or {}
        counters=database.get('counters') or {}
        if activity:
            lines.append(f"Database: {activity.get('operation','')} ({activity.get('query_seconds',0):.0f}s)")
        if counters:
            lines.append('Index/statistics: '+str(counters.get('phase','')))
            total=counters.get('blocks_total') or 0
            if total:lines.append(f"Blocks: {counters.get('blocks_done',0):,} / {total:,}")
        if database.get('wal_bytes_since_start') is not None:
            lines.append(f"Transaction WAL generated: {database['wal_bytes_since_start']/1024**3:.2f} GiB")
        if report.get('rollback_verified'):lines.append('Rollback verified; original v2 schema preserved.')
        if report.get('committed'):lines.append('Migration committed. App activation/backup verification still pending.')
    if report.get('state')=='succeeded' and not live:
        lines.append('Isolated checks passed; this is NOT approval or completion of the live migration.')
    if report.get('private_postgres_stopped'):
        lines.append('Private PostgreSQL stopped; restored copy retained for the next test.')
    return '\n'.join(lines)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('status',type=Path,nargs='?',default=Path('.local-migrations/db-refactor/compact-preflight-001/status.json'))
    args=parser.parse_args()
    print(render(json.loads(args.status.read_text())))
