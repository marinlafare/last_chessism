#!/usr/bin/env python3
"""Request, download and checksum a Chessism Parquet export (standard library).

The token is read from a file, never supplied on the command line or printed.
Load the verified output with pandas.read_parquet(..., dtype_backend='pyarrow').
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://chessism-api:8000/exports/v1')
    parser.add_argument('--token-file',required=True)
    parser.add_argument('--request',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--idempotency-key',default=None)
    parser.add_argument('--timeout',type=int,default=1800)
    parser.add_argument('--delete-after-download',action='store_true')
    args=parser.parse_args()
    token=Path(args.token_file).read_text().strip()
    base=args.base_url.rstrip('/')
    target=Path(args.output).absolute()
    if target.exists() or target.with_suffix(target.suffix+'.manifest.json').exists():
        raise SystemExit('Output already exists; choose another filename.')
    target.parent.mkdir(parents=True,exist_ok=True)
    key=args.idempotency_key or uuid.uuid4().hex
    print(f'Request key: {key} (reuse this key if the connection is interrupted)',flush=True)

    def request(path='',body=None,method=None,stream=False):
        headers={'Authorization':f'Bearer {token}','Accept':'application/json','Idempotency-Key':key}
        data=json.dumps(body).encode() if body is not None else None
        if data is not None:headers['Content-Type']='application/json'
        response=urlopen(Request(base+path,data=data,headers=headers,method=method),timeout=60)
        if stream:return response
        with response:
            value=response.read()
            return json.loads(value) if value else None

    specification=json.loads(Path(args.request).read_text())
    endpoint={'sql':'/sql','move_matrix':'/move-matrix'}.get(specification.get('mode'),'')
    job=request(endpoint,body=specification)
    identity=job['id'];print(f'Export ID: {identity}',flush=True)
    deadline=time.monotonic()+args.timeout
    previous=None
    while job['status']!='complete':
        progress=(job['status'],job['phase'],job['rows_written'])
        if progress!=previous:print(f'{progress[0]} / {progress[1]}: {progress[2]:,} rows',flush=True);previous=progress
        if job['status'] in ('failed','cancelled','expired','deleted'):
            raise SystemExit(job.get('error') or job['status'])
        if time.monotonic()>deadline:raise SystemExit(f'Client wait timed out; server export {identity} was not cancelled.')
        time.sleep(2);job=request('/'+identity)
    manifest=request(f'/{identity}/manifest')
    fd, temporary=tempfile.mkstemp(prefix='.chessism-export-',dir=target.parent)
    try:
        digest=hashlib.sha256();size=0
        with os.fdopen(fd,'wb') as output,request(f'/{identity}/file',stream=True) as response:
            while chunk:=response.read(1024*1024):
                output.write(chunk);digest.update(chunk);size+=len(chunk)
            output.flush();os.fsync(output.fileno())
        if digest.hexdigest()!=manifest['file']['sha256'] or size!=manifest['file']['size_bytes']:
            raise SystemExit('Download checksum/size mismatch; no final output was published.')
        # Hard-link publication fails rather than overwriting a concurrent file.
        os.link(temporary,target)
        with open(target.with_suffix(target.suffix+'.manifest.json'),'x') as output:
            json.dump(manifest,output,indent=2)
        print(f'Verified {manifest["row_count"]:,} rows, {size:,} bytes → {target}',flush=True)
        if args.delete_after_download:request('/'+identity,method='DELETE')
    finally:
        Path(temporary).unlink(missing_ok=True)


if __name__=='__main__':
    try:main()
    except HTTPError as error:
        raise SystemExit(f'Chessism HTTP {error.code}: {error.read(2000).decode(errors="replace")}') from None
