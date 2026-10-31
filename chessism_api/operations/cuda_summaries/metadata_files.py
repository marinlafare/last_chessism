"""Checksummed numerical metadata checkpoints; no pickle or cross-run reuse."""
import json
import os
import shutil
import tempfile
from pathlib import Path
import numpy as np
from . import input_snapshot as files,behavior_compute as behavior,ending_compute as endings

VERSION='metadata_inputs_v1'


def save(run_id,rows,profiles):
    raw,dictionaries=behavior.encode(rows,profiles);extra=endings.encode(rows)
    mask=np.asarray([[r['take_behavior'],r['take_endings']] for r in rows],dtype=np.bool_)
    path=files.folder(run_id);path.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(path).free < files.FREE_FLOOR+3*(raw.nbytes+extra.nbytes+1048576):
        raise ValueError('Metadata inputs would cross the protected disk-space reserve.')
    meta={'version':VERSION,'run_id':str(run_id),'dictionaries':dictionaries,'profiles':profiles}
    name=f'metadata-{rows[0]["cursor"]}-{rows[-1]["cursor"]}.npz'
    fd,temporary=tempfile.mkstemp(prefix='.writing-',suffix='.npz',dir=path)
    try:
        with os.fdopen(fd,'wb') as stream:
            np.savez(stream,raw=raw,extra=extra,mask=mask,
                metadata=np.frombuffer(json.dumps(meta,allow_nan=False).encode(),dtype=np.uint8))
            stream.flush();os.fsync(stream.fileno())
        checksum=files.digest(Path(temporary));os.replace(temporary,path/name)
        descriptor=os.open(path,os.O_RDONLY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)
    return {'name':name,'sha256':checksum,'bytes':(path/name).stat().st_size,
            'rows':len(raw),'last':rows[-1]['cursor']}


def read(run_id,descriptor):
    name=descriptor['name']
    if name!=Path(name).name or not name.startswith('metadata-') or not name.endswith('.npz'):
        raise ValueError('Invalid metadata checkpoint path.')
    path=files.folder(run_id)/name
    if path.is_symlink() or path.stat().st_size!=descriptor['bytes'] or files.digest(path)!=descriptor['sha256']:
        raise ValueError('Metadata checkpoint is missing or corrupt.')
    with np.load(path,allow_pickle=False) as archive:
        raw,extra,mask=(archive[k] for k in ('raw','extra','mask'));meta=json.loads(archive['metadata'].tobytes())
    if meta['version']!=VERSION or meta['run_id']!=str(run_id) or raw.dtype!=behavior.DTYPE or extra.dtype!=endings.DTYPE \
            or len(raw)!=descriptor['rows'] or len(extra)!=len(raw) or mask.shape!=(len(raw),2) or mask.dtype!=np.bool_:
        raise ValueError('Metadata checkpoint contract mismatch.')
    return raw,extra,mask,meta


def load(run_id,descriptors):
    pieces=[read(run_id,d) for d in descriptors]
    dictionaries={k:[] for k in ('mode','opening','opponent')};indices={k:{} for k in dictionaries}
    profiles=pieces[0][3]['profiles']
    for raw,_,_,meta in pieces:
        if meta['profiles']!=profiles:raise ValueError('Metadata owner snapshot changed.')
        for name,values in meta['dictionaries'].items():
            mapping=[]
            for value in values:
                if value not in indices[name]:indices[name][value]=len(dictionaries[name]);dictionaries[name].append(value)
                mapping.append(indices[name][value])
            raw[name]=np.asarray(mapping,dtype=np.int64)[raw[name]]
    return (np.concatenate([p[0] for p in pieces]),np.concatenate([p[1] for p in pieces]),
            np.concatenate([p[2] for p in pieces]),dictionaries,profiles)


def choose(chunks,budget):
    # Metadata sorts/group temporaries plus worst-case unique catalog outputs.
    chosen=[];rows=0
    for chunk in chunks:
        n=rows+chunk['rows']
        if n*1536>budget.device_bytes or n*4096>budget.host_bytes:
            if not chosen:raise RuntimeError('Metadata chunk exceeds available memory; no CPU fallback.')
            break
        chosen.append(chunk);rows=n
    return chosen
