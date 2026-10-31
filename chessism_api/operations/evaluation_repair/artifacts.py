"""Checksummed, non-overwriting reports; no database or engine dependency."""
import hashlib
import json
import os
from pathlib import Path

VERSION = 'legacy_evaluation_shadow_v1'
MAX_FENS = 16
MAX_REFERENCES = 20
MAX_CONTINUATIONS = 16
MAX_BYTES = 2 * 1024 * 1024


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def seal(value):
    payload = {k: v for k, v in value.items() if k != 'sha256'}
    return {**payload, 'sha256': digest(payload)}


def verify(value, kind):
    if (not isinstance(value, dict) or value.get('version') != VERSION
            or value.get('kind') != kind or value.get('sha256') != seal(value)['sha256']):
        raise ValueError('Invalid or modified repair artifact')
    if kind == 'plan':
        items = value.get('items', [])
        if not 1 <= len(items) <= MAX_FENS:
            raise ValueError('Repair pilot must contain 1 to 16 FENs')
        ids, fens = set(), set()
        for item in items:
            old = item['before']
            if (old['fen_id'] in ids or old['fen'] in fens
                    or old['score'] is None or old['analysis_source'] is not None
                    or old['analyzed_at'] is not None
                    or old['tablebase_wdl'] is not None or old['tablebase_dtz'] is not None
                    or not 1 <= len(item['references']) <= MAX_REFERENCES
                    or len(item['continuations']) > MAX_CONTINUATIONS
                    or item['snapshot_sha256'] != snapshot_digest(item)):
                raise ValueError('Invalid, duplicate, or unsafe repair input')
            ids.add(old['fen_id']); fens.add(old['fen'])
    return value


def snapshot_digest(item):
    return digest({k: item[k] for k in ('before', 'row_revision', 'continuations', 'references', 'players')})


def read(path, kind):
    with Path(path).open('rb') as stream:
        content = stream.read(MAX_BYTES + 1)
    if len(content) > MAX_BYTES:
        raise ValueError('Repair artifact exceeds the size limit')
    return verify(json.loads(content), kind)


def write(path, payload):
    payload = seal(payload)
    content = (json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    if len(content) > MAX_BYTES:
        raise ValueError('Repair artifact exceeds the size limit')
    # Explicit caller-selected destination, never overwrite or follow a symlink.
    # Interrupted files fail checksum/JSON validation; leave them for inspection.
    fd = os.open(Path(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    return payload
