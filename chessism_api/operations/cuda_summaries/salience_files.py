"""Run-scoped numeric checkpoints, partitioned by complete owner/FEN groups."""
import os
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from . import input_snapshot as files
from .salience_compute import VERSION, SHARDS


def save(run_id, raw, cursor):
    path = files.folder(run_id); path.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(path).free < files.FREE_FLOOR+3*(raw.nbytes+1048576):
        raise ValueError('Salience inputs would cross the protected disk-space reserve.')
    # Partitioning is transport, not frequency calculation. Keep every copy of
    # an owner/FEN together; never calculate frequencies independently per chunk.
    bucket = (raw[:,0]*31+raw[:,2]) % SHARDS
    result = []
    order = np.argsort(bucket,kind='stable')
    boundaries = np.r_[0,np.flatnonzero(np.diff(bucket[order]))+1,len(raw)]
    for start,end in zip(boundaries[:-1],boundaries[1:]):
        if start == end:continue
        shard = int(bucket[order[start]])
        selected = raw[order[start:end]]
        name = f'salience-{cursor}-{shard}.npz'
        fd, temporary = tempfile.mkstemp(prefix='.writing-',suffix='.npz',dir=path)
        try:
            with os.fdopen(fd,'wb') as stream:
                meta = json.dumps({'version':VERSION,'run_id':str(run_id),'shard':shard}).encode()
                np.savez(stream,raw=selected,metadata=np.frombuffer(meta,dtype=np.uint8))
                stream.flush();os.fsync(stream.fileno())
            checksum = files.digest(Path(temporary));os.replace(temporary,path/name)
        finally:
            if os.path.exists(temporary):os.unlink(temporary)
        result.append({'name':name,'sha256':checksum,'bytes':(path/name).stat().st_size,'rows':len(selected),'shard':shard})
    directory = os.open(path,os.O_RDONLY)
    try:os.fsync(directory)
    finally:os.close(directory)
    return result


def read(run_id, item):
    name = item['name']
    if name != Path(name).name or not name.startswith('salience-') or not name.endswith('.npz'):
        raise ValueError('Invalid salience checkpoint path.')
    path = files.folder(run_id)/name
    if path.is_symlink() or path.stat().st_size != item['bytes'] or files.digest(path) != item['sha256']:
        raise ValueError('Salience checkpoint missing or corrupt.')
    with np.load(path,allow_pickle=False) as archive:
        raw = archive['raw']; meta = json.loads(archive['metadata'].tobytes())
    if meta != {'version':VERSION,'run_id':str(run_id),'shard':item['shard']} or raw.dtype != np.int64 \
            or raw.shape != (item['rows'],4):
        raise ValueError('Salience checkpoint contract changed.')
    return raw


def choose(chunks, after, budget):
    counts = [0]*SHARDS
    for chunk in chunks:counts[chunk['shard']] += chunk['rows']
    selected = []; rows = 0
    for shard in range(after,SHARDS):
        n = rows+counts[shard]
        if n*320 > budget.device_bytes or n*256 > budget.host_bytes:
            if not selected:raise RuntimeError('A salience frequency group partition exceeds available memory; no CPU fallback.')
            break
        selected.append(shard);rows=n
    return selected


def load(run_id, chunks, shards):
    selected = [c for c in chunks if c['shard'] in shards]
    raw = np.empty((sum(c['rows'] for c in selected),4),dtype=np.int64)
    offset = 0
    for chunk in selected:
        data = read(run_id,chunk);raw[offset:offset+len(data)] = data;offset += len(data)
    return raw
