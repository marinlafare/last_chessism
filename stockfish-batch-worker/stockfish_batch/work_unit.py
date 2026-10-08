"""Skip a finished immutable unit on a replacement VM, never a partial unit."""
import json
import re

from .checkpoints import make_contract, parse_input
from .performance import validate_performance
from .storage import MAX_INPUT_BYTES, MAX_MANIFEST_BYTES, Storage
from .worker import binary_digest


def completed(config, *, storage=None, engine_sha=None):
    config.validate()
    if not config.compact_results or config.batch_size != 500 or config.max_positions > 50_000:
        raise ValueError('Completed-unit reuse requires bounded 500-result checkpoints')
    storage = storage or Storage()
    raw_manifest = storage.read(config.output + '/manifest.json', MAX_MANIFEST_BYTES)
    raw_report = storage.read(config.output + '/performance.json', 128 * 1024)
    if raw_manifest is None or raw_report is None:
        return False
    raw = storage.read(config.input, MAX_INPUT_BYTES)
    if raw is None:
        raise ValueError('Completed unit input is missing')
    rows = parse_input(raw, config.max_positions)
    expected = make_contract(raw, rows, config, engine_sha or binary_digest(config.engine))
    raw_contract = storage.read(config.output + '/contract.json')
    if raw_contract is None or json.loads(raw_contract) != expected:
        raise ValueError('Completed unit contract changed')
    manifest = json.loads(raw_manifest)
    records = manifest.get('records')
    if (manifest.get('status') != 'complete' or manifest.get('fingerprint') != expected['fingerprint']
            or manifest.get('position_count') != len(rows) or not isinstance(records, list)
            or len(records) != len(rows)):
        raise ValueError('Invalid completed unit manifest')
    for index, (row, record) in enumerate(zip(rows, records)):
        if (set(record) != {'id', 'object', 'sha256'} or record['id'] != row['id']
                or record['object'] != f'batches/{index // 500:06d}.json'
                or not re.fullmatch(r'[a-f0-9]{64}', record.get('sha256', ''))):
            raise ValueError('Completed unit identity changed')
    validate_performance(json.loads(raw_report), expected, config.workers)
    # The local importer independently verifies every file/hash before cleanup.
    return True
