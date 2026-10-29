"""Internal cloud readers. No public port, Docker socket, DB login, or job control."""
import os
from pathlib import Path
import tempfile

from fastapi import FastAPI, HTTPException
import uvicorn

from cloud_job.run_compose import prepare_credentials
from .reader import BillingCache, BillingError
from .uploads import UploadCache

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
cache = BillingCache()
uploads = UploadCache()


@app.get('/health')
def health():
    return {'ok': True}  # Never queries Google for a health check.


@app.get('/billing')
def billing():
    try:
        return cache.get()
    except BillingError as exc:
        raise HTTPException(503, str(exc)) from None


@app.get('/upload-progress/{run_id}')
def upload_progress(run_id: str):
    try:
        return uploads.get(run_id)
    except ValueError:
        raise HTTPException(400, 'Invalid run ID') from None


def main():
    os.umask(0o077)
    source = Path('/host-gcloud')
    owner = source.stat()
    if not source.is_dir() or owner.st_uid == 0:
        raise RuntimeError('Mount a non-root user gcloud configuration')
    if os.geteuid() == 0:
        os.setgroups([])
        os.setgid(owner.st_gid)
        os.setuid(owner.st_uid)
    if os.geteuid() != owner.st_uid:
        raise RuntimeError('Billing reader must run as the gcloud configuration owner')
    with tempfile.TemporaryDirectory(prefix='chessism-billing-') as directory:
        prepare_credentials(source, Path(directory))
        uvicorn.run(app, host='0.0.0.0', port=8080, access_log=False)


if __name__ == '__main__':
    main()
