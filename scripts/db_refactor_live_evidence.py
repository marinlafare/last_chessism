"""Offline release evidence and strict, recent rollback-backup validation."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def fingerprint(root):
    paths = [p for directory in ('chessism_api','scripts','stockfish-batch-worker/cloud_job')
             for p in (root/directory).rglob('*.py')]
    paths += [root/name for name in ('main.py','worker.py','docker-compose.yml','docker-compose.migration.yml',
        'requirements.txt','Dockerfile','postgres-backup/Dockerfile','stockfish-batch-worker/cloud_job/Dockerfile')]
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def evidence(report):
    names = ('status','acceptance','row-validation','migration-result','restart')
    data = {name:json.loads((report/(name+'.json')).read_text()) for name in names}
    status, acceptance = data['status'], data['acceptance']
    expected_rows = {'archive_game','archive_moves','game','game_analysis_summary',
        'game_fen_association','game_opening','game_player','game_player_engine_summary',
        'game_player_salience','games_partition','months','moves','player',
        'player_position_frequency','player_salience_pending_game','player_salience_summary'}
    if (status.get('mode')!='isolated_migration_sequence' or status.get('state')!='succeeded'
        or status.get('live_database_changed') is not False or status.get('source_unchanged') is not True
        or status.get('private_postgres_stopped') is not True or status.get('rollback_verified') is not True
        or acceptance.get('state')!='succeeded' or acceptance.get('atomic_cutover_and_summaries') is not True
        or acceptance.get('restart_validated') is not True or acceptance.get('fresh_shadow_build') is not True
        or set(data['row-validation'])!=expected_rows
        or any(row.get('mismatches')!=0 for row in data['row-validation'].values())
        or len(data['migration-result'].get('summaries',{}).get('checks',{}))!=8
        or not all(v is True for v in data['migration-result']['summaries']['checks'].values())
        or set(data['restart'].get('checks',[]))!={'api_and_queue_workers','cloud_controller_and_admin_tools',
            'legacy_profile_rejected','canonical_backup_contract'}):
        raise ValueError('Incomplete or failed whole-migration evidence')
    return {name:hashlib.sha256((report/(name+'.json')).read_bytes()).hexdigest() for name in names}


def prepare_plan(root, report):
    return {'version':1,'purpose':'prepared_live_game_player_migration','live_authorized':False,
            'rehearsal':str(report.resolve().relative_to(root.resolve())),'evidence':evidence(report),
            'source_files':fingerprint(root),'prepared_at':datetime.now(timezone.utc).isoformat()}


def verify_plan(plan, root):
    report = Path(plan.get('rehearsal',''))
    if report.is_absolute() or '..' in report.parts:
        raise ValueError('Rehearsal evidence must be inside the checked repository')
    if (plan.get('version')!=1 or plan.get('purpose')!='prepared_live_game_player_migration'
        or plan.get('live_authorized') is not False
        or plan.get('source_files')!=fingerprint(root)
        or plan.get('evidence')!=evidence(root/report)):
        raise ValueError('Prepared code or rehearsal evidence changed; review and prepare a new plan')


def validate_backup(backup, expected_id, system_identifier, info, *, now=None):
    now = now or datetime.now(timezone.utc)
    if (backup.get('backup_id')!=expected_id or backup.get('restore_tested') is not True
        or backup.get('restore_test_status')!='success' or backup.get('restore_test_error')):
        raise ValueError('Latest backup must match the requested, successfully restore-tested recovery point')
    completed = datetime.fromisoformat(backup['completed_at'])
    tested = datetime.fromisoformat(backup['restore_tested_at'])
    if (completed.tzinfo is None or tested.tzinfo is None or tested<completed
        or not 0 <= (now-completed).total_seconds() <= 24*3600 or tested>now):
        raise ValueError('Rollback backup must be at most 24 hours old, with a later successful restore test')
    matches = []
    for stanza in info:
        if stanza.get('status',{}).get('code')!=0:
            continue
        for saved in stanza.get('backup',[]):
            if saved.get('label')!=expected_id or saved.get('error'):
                continue
            database = saved.get('database',{})
            matches.extend(db for db in stanza.get('db',[])
                if db.get('id')==database.get('id') and db.get('repo-key')==database.get('repo-key')
                and str(db.get('system-id'))==system_identifier)
    if len(matches)!=1:
        raise ValueError('pgBackRest recovery point does not uniquely match the approved database cluster')
    return backup
