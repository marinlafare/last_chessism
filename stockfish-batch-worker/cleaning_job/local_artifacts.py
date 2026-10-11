"""Dry-run-first cleanup of verified, imported local benchmark batch copies.

Never touches Google, PostgreSQL, inputs, backup files, or benchmark reports.
Only removes manifest-listed download/batches/NNNNNN.json files after checking
the saved import/cleanup proof and every result checksum. Safe to rerun.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    if path.is_symlink():
        raise ValueError(f'Symlink refused: {path}')
    return json.loads(path.read_bytes())


def plan(benchmark: Path):
    for directory in (benchmark, benchmark / 'download', benchmark / 'download/batches'):
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError(f'Expected a real benchmark directory: {directory}')
    receipt = read_json(benchmark / 'import-receipt.json')
    cleanup = read_json(benchmark / 'cleanup-result.json')
    finish = read_json(benchmark / 'finish-status.json')
    manifest = read_json(benchmark / 'download/manifest.json')
    if (finish.get('state') != 'complete' or manifest.get('status') != 'complete'
            or receipt.get('cleanup_ready') is not True
            or receipt.get('database_receipts_verified') is not True
            or receipt.get('remaining_claims') != 0 or cleanup.get('complete') is not True
            or any(cleanup.get(key) for key in ('remaining_compute', 'remaining_jobs',
                       'remaining_images', 'remaining_object_versions', 'image_blockers'))
            or receipt.get('imported') != manifest.get('position_count')
            or len(manifest['records']) != manifest['position_count']
            or digest(encode(manifest)) != receipt.get('manifest_sha256')):
        raise ValueError('Missing/mismatched successful import and cleanup evidence')
    groups, ids = {}, set()
    for record in manifest['records']:
        name = record['object']
        if not re.fullmatch(r'batches/[0-9]{6}\.json', name) or record['id'] in ids:
            raise ValueError('Unexpected batch name or duplicate FEN identity')
        ids.add(record['id'])
        groups.setdefault(name, {})[record['id']] = record['sha256']
    files = []
    for name, expected in sorted(groups.items()):
        path = benchmark / 'download' / name
        if path.is_symlink():
            raise ValueError(f'Symlink refused: {path}')
        if not path.exists():
            continue  # Previously removed after verification; retry is idempotent.
        raw = path.read_bytes()
        batch = json.loads(raw)
        records = batch['records']
        if batch.get('fingerprint') != manifest['fingerprint'] or len(records) != len(expected):
            raise ValueError(f'Batch identity changed: {path}')
        actual = {}
        for record in records:
            result_hash = digest(encode(record['engine_result']))
            if (record.get('fingerprint') != manifest['fingerprint']
                    or record['id'] in actual or record.get('result_sha256') != result_hash):
                raise ValueError(f'Result checksum/identity changed: {path}')
            actual[record['id']] = result_hash
        if actual != expected:
            raise ValueError(f'Batch does not match imported manifest: {path}')
        files.append({'path': str(path), 'bytes': len(raw), 'sha256': digest(raw)})
    return {'benchmark': str(benchmark), 'db_job_id': receipt['db_job_id'],
            'manifest_sha256': receipt['manifest_sha256'], 'files': files,
            'bytes': sum(item['bytes'] for item in files)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', required=True, help='One direct directory name under stockfish-batch-worker/out')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.benchmark):
        parser.error('Invalid benchmark directory name')
    out = Path(__file__).resolve().parents[1] / 'out'
    if out.is_symlink():
        parser.error('Refusing a symlinked output directory')
    report = plan(out / args.benchmark)
    if args.apply:
        # Recheck the whole plan immediately before the first destructive action.
        if plan(out / args.benchmark) != report:
            raise ValueError('Artifacts changed after inspection; nothing removed')
        for item in report['files']:
            path = Path(item['path'])
            if path.is_symlink() or digest(path.read_bytes()) != item['sha256']:
                raise ValueError(f'Artifact changed; cleanup stopped: {path}')
            path.unlink()
    print(json.dumps({'applied': args.apply, 'benchmark': args.benchmark,
                      'file_count': len(report['files']), 'bytes': report['bytes'],
                      'db_job_id': report['db_job_id'],
                      'manifest_sha256': report['manifest_sha256']}, indent=2))


if __name__ == '__main__':
    main()
