"""Private, UUID-owned temporary files. No caller-supplied filesystem paths."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid

from . import config


def job_dir(job_id):
    if uuid.UUID(job_id).hex != job_id:
        raise ValueError('Invalid export identity.')
    path = config.ROOT / job_id
    if path.is_symlink():
        raise ValueError('Export directory must not be a symlink.')
    return path


def ensure_space():
    config.ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    if shutil.disk_usage(config.ROOT).free < config.FREE_FLOOR:
        raise ValueError('Export storage is below its protected free-space reserve.')


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with open(temporary, 'xb') as output:
        os.chmod(temporary, 0o600)
        output.write(canonical_bytes(value)); output.flush(); os.fsync(output.fileno())
    os.replace(temporary, path)


def read_request(job_id):
    path = job_dir(job_id) / 'request.json'
    if path.is_symlink() or path.stat().st_size > 131072:
        raise ValueError('Invalid stored export request.')
    return json.loads(path.read_bytes())


def remove_files(job_id):
    path = job_dir(job_id)
    if path.exists():
        # Exact validated UUID directory only; never the root or a client path.
        shutil.rmtree(path)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()
