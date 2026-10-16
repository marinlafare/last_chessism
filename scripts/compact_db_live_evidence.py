"""Frozen source/evidence and compressed rollback-snapshot gates; no connections."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path


def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''):
            digest.update(chunk)
    return digest.hexdigest()


def sources(root):
    files=[p for folder in ('chessism_api','scripts','tests','stockfish-batch-worker/cloud_job')
           for p in (root/folder).rglob('*.py')]
    files += [root/name for name in ('main.py','worker.py','requirements.txt','docker-compose.yml',
              'docker-compose.compact.yml','Dockerfile','postgres-backup/Dockerfile',
              'stockfish-batch-worker/cloud_job/Dockerfile')]
    return {str(p.relative_to(root)):sha(p) for p in sorted(files)}


def acceptance(directory):
    status=json.loads((directory/'status.json').read_text())
    report=json.loads((directory/'acceptance.json').read_text())
    if (status.get('state')!='succeeded' or status.get('mode')!='isolated_compact_index_retune'
        or status.get('private_postgres_stopped') is not True or status.get('live_database_changed') is not False
        or len(report.get('checks',[]))!=20 or not all(c.get('passed') for c in report['checks'])
        or len(report.get('queries',[]))!=19 or any(q.get('error') or q.get('matches') is not True
            or q.get('slowdown') for q in report['queries']) or not report.get('ready_for_live_preflight')):
        raise ValueError('Complete successful compact acceptance required')
    return {name:sha(directory/(name+'.json')) for name in ('status','acceptance','candidate-contract','source-files')}


def prepare(root, directory, preflight):
    review=json.loads(preflight.read_text())
    if review.get('state')!='succeeded' or review.get('live_database_changed') is not False:
        raise ValueError('Successful read-only live preflight required')
    # Only the reviewed guard extraction and progress display changed since
    # acceptance. The SQL operations, application code and data proofs are reused.
    old=json.loads((directory/'source-files.json').read_text())
    allowed={'scripts/compact_db_preflight.py','scripts/compact_db_migration.py','scripts/watch_compact_db.py'}
    changed=[name for name,digest in old.items() if sha(root/name)!=digest]
    if set(changed)-allowed:
        raise ValueError('Application acceptance invalidated: '+', '.join(sorted(set(changed)-allowed)))
    return {'version':1,'purpose':'compact_live_v3','live_authorized':False,
        'prepared_at':datetime.now(timezone.utc).isoformat(),
        'acceptance':str(directory.resolve().relative_to(root.resolve())),
        'evidence':acceptance(directory),'source_files':sources(root),
        'reviewed_guard_changes':changed,'preflight':review}


def verify(plan,root):
    relative=Path(plan.get('acceptance',''))
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('Acceptance must be inside repository')
    if (plan.get('version')!=1 or plan.get('purpose')!='compact_live_v3'
        or plan.get('live_authorized') is not False or plan.get('source_files')!=sources(root)
        or plan.get('evidence')!=acceptance(root/relative)):
        raise ValueError('Prepared source or evidence changed; refusing live apply')


def validate_snapshot(backup,manifest,expected,*,now=None):
    now=now or datetime.now(timezone.utc)
    completed=datetime.fromisoformat(backup['completed_at'])
    tested=datetime.fromisoformat(backup['restore_tested_at'])
    if (backup.get('backup_id')!=expected['backup_id'] or backup.get('storage_mode')!='compressed_snapshot_v1'
        or backup.get('restore_tested') is not True or backup.get('restore_test_status')!='success'
        or backup.get('restore_test_error') or completed.tzinfo is None or tested.tzinfo is None
        or not 0<=(now-completed).total_seconds()<=24*3600 or not completed<=tested<=now
        or manifest.get('format')!='chessism-pg-dump-custom'
        or manifest.get('relational_storage',{}).get('version')!='relational_v2'
        or backup.get('archive_sha256')!=expected['archive_sha256']
        or manifest.get('archive_sha256')!=expected['archive_sha256']):
        raise ValueError('Need the exact recent restore-tested compressed v2 snapshot')
