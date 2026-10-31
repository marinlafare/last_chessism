"""Checksummed numeric checkpoints; never infer independently per read chunk."""
import json
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
import numpy as np
from . import input_snapshot as files
from .frequency_compute import VERSION, SHARDS


def write(run_id, raw, name, shard):
    path = files.folder(run_id)
    path.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(path).free < files.FREE_FLOOR + 3 * (raw.nbytes + 1048576):
        raise ValueError('Frequency checkpoints would cross the protected disk reserve.')
    fd, temporary = tempfile.mkstemp(prefix='.writing-', suffix='.npz', dir=path)
    try:
        with os.fdopen(fd, 'wb') as stream:
            meta = json.dumps({'version': VERSION, 'run_id': str(run_id), 'shard': shard}).encode()
            np.savez(stream, raw=raw, metadata=np.frombuffer(meta, dtype=np.uint8))
            stream.flush(); os.fsync(stream.fileno())
        checksum = files.digest(Path(temporary))
        os.replace(temporary, path / name)
        directory = os.open(path, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    return dict(name=name, sha256=checksum, bytes=(path / name).stat().st_size,
                rows=len(raw), columns=raw.shape[1], shard=shard)


def save(run_id, raw, cursor):
    # One file per read, not one file per read per shard. Sorted, mmap-able
    # segments let each large GPU batch read only its selected hash groups.
    path = files.folder(run_id); path.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(path).free < files.FREE_FLOOR + 3 * (raw.nbytes + 1048576):
        raise ValueError('Frequency inputs would cross the protected disk reserve.')
    bucket = (raw[:, 0] * 31 + raw[:, 2]) % SHARDS
    order = np.argsort(bucket, kind='stable')
    data = raw[order]
    bounds = np.searchsorted(bucket[order],np.arange(SHARDS+1))
    segments=[dict(start=int(bounds[i]),end=int(bounds[i+1]),
        sha256=hashlib.sha256(data[bounds[i]:bounds[i+1]]).hexdigest()) for i in range(SHARDS)]
    name=f'frequency-input-{cursor[0]}-{cursor[1]}.npy'
    fd,temporary=tempfile.mkstemp(prefix='.writing-',suffix='.npy',dir=path)
    try:
        with os.fdopen(fd,'wb') as stream:
            np.save(stream,data,allow_pickle=False);stream.flush();os.fsync(stream.fileno())
        checksum=files.digest(Path(temporary));os.replace(temporary,path/name)
        directory=os.open(path,os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)
    return [dict(name=name,sha256=checksum,bytes=(path/name).stat().st_size,rows=len(raw),columns=4,
                 version=VERSION,run_id=str(run_id),segments=segments)]


def read(run_id, item):
    name = item['name']
    if name != Path(name).name or not name.startswith('frequency-') or not name.endswith(('.npz','.npy')):
        raise ValueError('Invalid frequency checkpoint name.')
    path = files.folder(run_id) / name
    if path.is_symlink() or path.stat().st_size != item['bytes'] or files.digest(path) != item['sha256']:
        raise ValueError('Frequency checkpoint missing or corrupt.')
    if name.endswith('.npy'):
        raw=np.load(path,allow_pickle=False)
        if raw.dtype!=np.int64 or raw.shape!=(item['rows'],4):raise ValueError('Invalid frequency matrix.')
        return raw
    with np.load(path, allow_pickle=False) as archive:
        raw = archive['raw']; meta = json.loads(archive['metadata'].tobytes())
    if (meta != dict(version=VERSION, run_id=str(run_id), shard=item['shard'])
            or raw.dtype != np.int64 or raw.shape != (item['rows'], item['columns'])):
        raise ValueError('Frequency checkpoint contract changed.')
    return raw


def choose(chunks, after, budget):
    counts = [0] * SHARDS
    for chunk in chunks:
        for i,segment in enumerate(chunk['segments']):counts[i]+=segment['end']-segment['start']
    selected, rows = [], 0
    for shard in range(after, SHARDS):
        total = rows + counts[shard]
        if total * 256 > budget.device_bytes or total * 128 > budget.host_bytes:
            if not selected:
                raise RuntimeError('One frequency shard exceeds memory; retain checkpoints, no CPU fallback.')
            break
        selected.append(shard); rows = total
    return selected


def load(run_id, chunks, shards):
    total=sum(c['segments'][i]['end']-c['segments'][i]['start'] for c in chunks for i in shards)
    result = np.empty((total, 4), dtype=np.int64)
    offset = 0
    for chunk in chunks:
        name=chunk['name'];path=files.folder(run_id)/name
        if name!=Path(name).name or not name.startswith('frequency-input-') or not name.endswith('.npy') \
                or path.is_symlink() or path.stat().st_size!=chunk['bytes'] \
                or chunk['version']!=VERSION or chunk['run_id']!=str(run_id):
            raise ValueError('Invalid frequency input checkpoint.')
        raw=np.load(path,allow_pickle=False,mmap_mode='r')
        if raw.dtype!=np.int64 or raw.shape!=(chunk['rows'],4):raise ValueError('Invalid frequency matrix.')
        for i in shards:
            segment=chunk['segments'][i];part=raw[segment['start']:segment['end']]
            if hashlib.sha256(part).hexdigest()!=segment['sha256']:raise ValueError('Corrupt frequency segment.')
            result[offset:offset+len(part)]=part;offset+=len(part)
        del raw
    return result


def player_rows(run_id, outputs, player):
    # Filtering/transport of already computed integers; no CPU frequency math.
    for output in outputs:
        part = read(run_id, output)
        for row in part[part[:, 0] == player]:
            yield (int(row[0]), bool(row[1]), int(row[3]), int(row[2]), int(row[4]))
