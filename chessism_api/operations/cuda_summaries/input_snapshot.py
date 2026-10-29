"""Durable, run-scoped input matrices. No pickle, cross-run reuse, or live writes.

Small extraction checkpoints compose large GPU batches. A committed descriptor
only references an atomically installed, checksummed artifact. Source changes
are still rejected by canonical/player revision checks at publication.
"""
import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
import numpy as np
from ..research_algorithms.storage import WORK_ROOT, FREE_FLOOR
from . import shared_source

ROOT = WORK_ROOT.parent / 'cuda-inputs'
VERSION = 'cuda_inputs_v2'


def folder(run_id):
    path = ROOT / str(uuid.UUID(str(run_id)))
    if ROOT.is_symlink() or path.is_symlink():
        raise ValueError('CUDA snapshot directory must not be a symlink.')
    return path


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4*1024**2), b''):
            result.update(block)
    return result.hexdigest()


def save(run_id, data):
    config, items, games, owners, raw, eligible, unsupported, transitions = data
    if not items:
        raise ValueError('Cannot checkpoint an empty input selection.')
    path = folder(run_id); path.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(path).free < FREE_FLOOR + 3*(raw.nbytes+transitions.nbytes+1024**2):
        raise ValueError('CUDA input snapshot would cross the protected disk-space reserve.')
    meta = json.dumps({'version': VERSION, 'run_id': str(run_id), 'config': config, 'items': items,
        'games': games, 'owners': owners, 'eligible': eligible, 'unsupported': unsupported},
        default=lambda value: value.isoformat(), allow_nan=False).encode()
    name = f'{items[0]["game_link"]}-{items[-1]["game_link"]}.npz'
    fd, temporary = tempfile.mkstemp(prefix='.writing-', suffix='.npz', dir=path)
    try:
        with os.fdopen(fd, 'wb') as stream:
            np.savez(stream, raw=raw, transitions=transitions, metadata=np.frombuffer(meta,dtype=np.uint8))
            stream.flush(); os.fsync(stream.fileno())
        checksum = digest(Path(temporary))
        os.replace(temporary, path/name)
        descriptor = os.open(path, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    return {'name': name, 'sha256': checksum, 'bytes': (path/name).stat().st_size,
            'games': len(items), 'positions': len(raw), 'first': items[0]['game_link'], 'last': items[-1]['game_link']}


def read(run_id, descriptor):
    name = descriptor['name']
    if name != Path(name).name or not name.endswith('.npz'):
        raise ValueError('Invalid CUDA snapshot artifact path.')
    path = folder(run_id)/name
    if path.is_symlink() or path.stat().st_size != descriptor['bytes'] or digest(path) != descriptor['sha256']:
        raise ValueError('CUDA input checkpoint is missing or corrupt; nothing published.')
    with np.load(path, allow_pickle=False) as archive:
        meta = json.loads(archive['metadata'].tobytes())
        raw = archive['raw']; transitions = archive['transitions']
    if meta['version'] != VERSION or meta['run_id'] != str(run_id) or len(raw) != descriptor['positions']:
        raise ValueError('CUDA input checkpoint identity changed.')
    if raw.dtype != shared_source.RAW or transitions.dtype != shared_source.compute.DTYPE:
        raise ValueError('CUDA input checkpoint array contract changed.')
    if len(meta['items']) != descriptor['games'] or meta['items'][0]['game_link'] != descriptor['first'] or meta['items'][-1]['game_link'] != descriptor['last']:
        raise ValueError('CUDA input checkpoint item range changed.')
    for group in ('games','eligible','unsupported'):
        for game in meta[group]:
            if game.get('played_at'):
                game['played_at'] = datetime.fromisoformat(game['played_at'])
    return meta['config'],meta['items'],meta['games'],meta['owners'],raw,meta['eligible'],meta['unsupported'],transitions


def choose(chunks, budget):
    selected = []; positions = games = 0
    for chunk in chunks:
        if not budget.fits(positions+chunk['positions'], games+chunk['games']):
            if not selected:
                raise RuntimeError('One input checkpoint exceeds available CUDA memory; no CPU fallback.')
            break
        selected.append(chunk); positions += chunk['positions']; games += chunk['games']
    return selected


def load(run_id, chunks):
    # Local reads only. DB sessions are closed before any GPU allocation/work.
    from ..canonical_blunders.compute import DTYPE
    raw = np.empty(sum(c['positions'] for c in chunks), shared_source.RAW)
    config = None; items = []; games = []; owners = []; eligible = []; unsupported = []
    transition_parts = []; offset = 0
    for chunk in chunks:
        current, ci, cg, co, cr, ce, cu, ct = read(run_id, chunk)
        if config is not None and current != config:
            raise ValueError('CUDA checkpoint model mismatch.')
        config = current
        cr['game'] += len(games); ct['game'] += len(eligible)
        raw[offset:offset+len(cr)] = cr; offset += len(cr)
        items.extend(ci); games.extend(cg); owners.extend(co); eligible.extend(ce); unsupported.extend(cu)
        transition_parts.append(ct)
    transitions = np.concatenate(transition_parts) if transition_parts else np.empty(0,DTYPE)
    return config,items,games,owners,raw,eligible,unsupported,transitions
