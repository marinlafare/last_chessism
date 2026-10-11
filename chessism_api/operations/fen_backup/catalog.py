"""Small metadata only: listing never scans or decompresses the backup payload."""
from datetime import datetime, timezone
from uuid import uuid4

from ..backup_files import atomic_write_json
from . import archive

REBASE_FILE = '.fen-backup-restore-epoch.json'


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def restore_epoch(root):
    path = root / REBASE_FILE
    if path.is_symlink():
        raise ValueError('Restore epoch must not be a symbolic link')
    return archive.read_json(path)['epoch'] if path.exists() else None


def invalidate_after_restore(root):
    # Restoration preserves historical analyzed_at values, so the next backup
    # must start a NEW base rather than trusting a timestamp-based delta.
    atomic_write_json(root / REBASE_FILE, {'epoch': uuid4().hex})


def legacy(root, path):
    archive.safe_path(root, path.name)
    meta_path = path.with_name(path.name + '.meta.json')
    if meta_path.is_symlink():
        raise ValueError('Backup metadata must not be a symbolic link')
    meta = archive.read_json(meta_path) if meta_path.exists() else {}
    return {**meta, 'filename': path.name, 'bytes': path.stat().st_size,
            'format_version': int(meta.get('version') or 2), 'record_label': 'positions',
            'updated_at': meta.get('updated_at') or meta.get('created_at')}


def load(root, filename, *, check_files=True):
    path = archive.safe_path(root, filename)
    if archive.MANIFEST_NAME.fullmatch(filename):
        value = archive.validate_manifest(root, archive.read_json(path), check_files=check_files)
        if value.get('filename') != filename or not (
                timestamp(value['snapshot_cutoff']) <= timestamp(value['snapshot_at']) <= timestamp(value['updated_at'])):
            raise ValueError('Invalid FEN-backup identity or checkpoint')
        return value
    return legacy(root, path)


def listing(root):
    backups = [legacy(root, path) for path in root.glob('fen-analysis-*.jsonl.gz')
               if archive.LEGACY_NAME.fullmatch(path.name)]
    for path in root.glob('fen-analysis-*.manifest.json'):
        value = load(root, path.name, check_files=False)
        backups.append({**{key: item for key, item in value.items() if key != 'files'},
                        'parts': len(value['files'])})
    backups.sort(key=lambda row: (row.get('updated_at') or '', row['filename']), reverse=True)
    return backups


def source_files(root, backup):
    if backup.get('version') == 3:
        return backup['files']
    if not backup.get('sha256') or type(backup.get('records')) is not int:
        raise ValueError('Legacy backup needs its record count and checksum metadata')
    return [{'file': backup['filename'], 'records': backup['records'],
             'bytes': backup['bytes'], 'sha256': backup['sha256']}]
