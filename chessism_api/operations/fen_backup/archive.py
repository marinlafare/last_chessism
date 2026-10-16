"""Bounded compressed segments and an atomically published restore manifest."""
import gzip
import json
import math
import os
from pathlib import Path
import re
from uuid import uuid4

from ..backup_files import atomic_write_json, sha256

FORMAT = 'chessism-fen-analysis'
SET_FORMAT = 'chessism-fen-analysis-set'
STAMP = r'\d{8}T\d{6}_\d{6}Z'
LEGACY_NAME = re.compile(rf'^fen-analysis-{STAMP}\.jsonl\.gz$')
MANIFEST_NAME = re.compile(rf'^fen-analysis-{STAMP}\.manifest\.json$')
PART_NAME = re.compile(r'^fen-analysis-parts/[a-f0-9]{32}/part-\d{6}\.jsonl\.gz$')
MAX_RECORD_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
PART_RECORDS = 50_000


def durable_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def safe_path(root, name):
    if not (LEGACY_NAME.fullmatch(name) or MANIFEST_NAME.fullmatch(name) or PART_NAME.fullmatch(name)):
        raise ValueError('Invalid FEN-backup path')
    path = root / name
    for component in (path, *path.parents):
        if component == root:
            break
        if component.is_symlink():
            raise ValueError('FEN backups must not contain symbolic links')
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('FEN-backup path escapes its directory')
    return path


def read_json(path):
    with path.open('rb') as source:
        raw = source.read(MAX_MANIFEST_BYTES + 1)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError('FEN-backup metadata is too large')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('Invalid FEN-backup metadata')
    return value


def records(path):
    with gzip.open(path, 'rt', encoding='utf-8') as source:
        header = json.loads(source.readline(MAX_RECORD_BYTES + 1))
        if header.get('format') != FORMAT or header.get('version') not in (1, 2):
            raise ValueError('Unsupported FEN-analysis backup format')
        while line := source.readline(MAX_RECORD_BYTES + 1):
            if len(line.encode('utf-8')) > MAX_RECORD_BYTES:
                raise ValueError('Oversized FEN-backup record')
            if not line.strip():
                continue
            record = json.loads(line)
            validate_record(record)
            yield record


def validate_record(record):
    if not isinstance(record, dict) or record.get('type') != 'fen_analysis' or not isinstance(record.get('fen'), str) or not record['fen']:
        raise ValueError('Invalid FEN-analysis record')
    if type(record.get('score')) not in (int, float) or not math.isfinite(record['score']):
        raise ValueError('FEN-analysis record has no score')
    from datetime import datetime
    if record.get('analyzed_at') is not None:
        datetime.fromisoformat(record['analyzed_at'])
    for field in ('piece_count', 'wdl_win', 'wdl_draw', 'wdl_loss', 'tablebase_dtz'):
        if record.get(field) is not None and type(record[field]) not in (int, float):
            raise ValueError(f'Invalid {field} in FEN-backup record')
    tablebase_wdl = record.get('tablebase_wdl')
    if tablebase_wdl is not None and (type(tablebase_wdl) not in (int, float)
            or not math.isfinite(tablebase_wdl) or tablebase_wdl != int(tablebase_wdl)
            or not -2 <= tablebase_wdl <= 2):
        raise ValueError('Invalid numeric tablebase WDL')
    for field in ('next_moves', 'analysis_source'):
        if record.get(field) is not None and not isinstance(record[field], str):
            raise ValueError(f'Invalid {field} in FEN-backup record')
    continuations = record.get('continuations') or []
    ranks = set()
    for item in continuations:
        rank = item.get('rank')
        if type(rank) is not int or rank < 1 or rank in ranks or not isinstance(item.get('move'), str) or type(item.get('score')) not in (int, float) or not math.isfinite(item['score']):
            raise ValueError('Invalid or duplicate FEN continuation')
        ranks.add(rank)


def descriptor(root, path, count):
    return {'file': path.relative_to(root).as_posix(), 'records': count,
            'bytes': path.stat().st_size, 'sha256': sha256(path)}


def validate_manifest(root, value, *, check_files=True):
    if value.get('format') != SET_FORMAT or value.get('version') != 3:
        raise ValueError('Unsupported FEN-backup manifest')
    files = value.get('files')
    if not isinstance(files, list) or len(files) > 100_000:
        raise ValueError('Invalid FEN-backup file list')
    names = set()
    for entry in files:
        name = entry['file']
        if not (LEGACY_NAME.fullmatch(name) or PART_NAME.fullmatch(name)) or name in names:
            raise ValueError('Invalid or repeated FEN-backup part')
        names.add(name)
        path = safe_path(root, name)
        if type(entry['records']) is not int or entry['records'] < 0 or type(entry['bytes']) is not int or entry['bytes'] < 1:
            raise ValueError('Invalid FEN-backup part size')
        if not re.fullmatch(r'[a-f0-9]{64}', entry['sha256']):
            raise ValueError('Missing FEN-backup checksum')
        if check_files and (not path.is_file() or path.stat().st_size != entry['bytes']):
            raise ValueError(f'Missing or changed backup part: {name}')
    if value.get('records') != sum(e['records'] for e in files):
        raise ValueError('Backup record count differs from its parts')
    if value.get('bytes') != sum(e['bytes'] for e in files):
        raise ValueError('Backup byte count differs from its parts')
    return value


def publish(root, filename, value):
    path = safe_path(root, filename)
    if path.exists():
        raise ValueError('Refusing to overwrite a published backup')
    validate_manifest(root, value)
    atomic_write_json(path, value)


class SegmentWriter:
    """Only the current batch is in RAM. No published backup is ever overwritten.

    Unpublished parts from a failed/interrupted export are deliberately retained
    but never listed or restored; a retry starts at the last published checkpoint.
    """
    def __init__(self, root, part_records=PART_RECORDS):
        self.root = root
        if (root / 'fen-analysis-parts').is_symlink():
            raise ValueError('Backup parts directory must not be a symbolic link')
        self.directory = root / 'fen-analysis-parts' / uuid4().hex
        self.directory.mkdir(parents=True)
        durable_directory(self.directory.parent)
        durable_directory(root)
        self.part_records = part_records
        self.files = []
        self.output = None
        self.part_count = self.count = 0

    @property
    def bytes(self):
        current = self.path.stat().st_size if self.output else 0
        return sum(item['bytes'] for item in self.files) + current

    def append(self, batch):
        for record in batch:
            validate_record(record)
            if self.output is None:
                self.path = self.directory / f'part-{len(self.files) + 1:06d}.jsonl.gz'
                self.output = gzip.open(self.path, 'xt', encoding='utf-8', compresslevel=3)
                self.output.write(json.dumps({'type': 'metadata', 'format': FORMAT, 'version': 2}) + '\n')
                self.part_count = 0
            line = json.dumps(record, separators=(',', ':'), allow_nan=False) + '\n'
            if len(line.encode('utf-8')) > MAX_RECORD_BYTES:
                raise ValueError('Oversized FEN-backup record')
            self.output.write(line)
            self.part_count += 1
            self.count += 1
            if self.part_count == self.part_records:
                self.close_part()

    def close_part(self):
        if self.output is None:
            return
        self.output.close()
        self.output = None
        with self.path.open('rb') as source:
            os.fsync(source.fileno())
        durable_directory(self.directory)
        self.files.append(descriptor(self.root, self.path, self.part_count))

    def abort(self):
        if self.output:
            self.output.close()
            self.output = None
