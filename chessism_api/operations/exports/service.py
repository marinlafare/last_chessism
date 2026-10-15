"""Durable job admission and owner-scoped lifecycle, separate from chess data."""
import asyncio
from datetime import datetime, timezone, timedelta
import hashlib
import uuid

from fastapi import HTTPException
from sqlalchemy import func, select, text

from chessism_api.database.engine import AsyncDBSession
from . import config, files
from .auth import authorize_spec
from .models import ExportJob

ADMISSION_LOCK = 760_140_001
WORKER_LOCK = 760_140_002


def now():
    return datetime.now(timezone.utc)


def request_document(spec):
    from .analytical_spec import MoveMatrixSpec, MATRIX_CONTRACT_KEY
    document = spec.model_dump(mode='json')
    if isinstance(spec, MoveMatrixSpec) and spec.contract_version >= 2:
        # Internal marker, not a caller option. Old jobs/keys must never acquire
        # different eligibility, selection, ordering or schema on worker upgrade.
        # The new selection also applies when completeness is disabled.
        document[MATRIX_CONTRACT_KEY] = spec.contract_version
    # Preserve older idempotency fingerprints for unchanged default designs.
    for option in ('exclude_null_columns', 'require_complete_scores'):
        if document.get(option) is False:
            document.pop(option)
    if isinstance(spec, MoveMatrixSpec) and spec.player_win == 'any':
        document.pop('player_win', None)  # omitted/any retain old fingerprints
    return document


def compatible_matrix_replay(previous, spec, document):
    """Recognize an old, equivalent key without rewriting its saved instructions."""
    from .analytical_spec import MoveMatrixSpec, StoredLegacyMoveMatrixSpec, MATRIX_CONTRACT_KEY, MATRIX_CONTRACT_VERSION
    if not isinstance(spec, MoveMatrixSpec) or isinstance(spec, StoredLegacyMoveMatrixSpec):
        return False
    try:
        stored = files.read_request(previous.id)
        if hashlib.sha256(files.canonical_bytes(stored)).hexdigest() != previous.request_sha256:
            return False
        if stored.get('mode') != 'move_matrix':
            return False
        version = stored.pop(MATRIX_CONTRACT_KEY, 1)
        if version != MATRIX_CONTRACT_VERSION:
            return False  # old ordering/eligibility/schema is not the new dataset
        # Validation rejects old column dropping and smaller result guards;
        # neither can be treated as equivalent to the new semantics.
        normalized = MoveMatrixSpec.model_validate(stored)
        return request_document(normalized) == document
    except (OSError, ValueError):
        return False


def payload(job):
    return {key: getattr(job, key) for key in (
        'id', 'name', 'source', 'status', 'phase', 'row_limit', 'rows_written', 'file_bytes',
        'file_sha256', 'limit_reached', 'cancel_requested', 'error', 'error_code', 'created_at', 'updated_at',
        'started_at', 'snapshot_at', 'completed_at', 'expires_at')}


async def enqueue(redis, job_id):
    await redis.enqueue_job('run_export', job_id, _job_id=f'export:{job_id}', _queue_name=config.QUEUE)


async def submit(client, spec, key, redis):
    authorize_spec(client, spec)
    document = request_document(spec)
    encoded = files.canonical_bytes(document)
    if len(encoded) > 131072:
        raise HTTPException(422, 'Export request exceeds 128 KiB.')
    fingerprint = hashlib.sha256(encoded).hexdigest()
    created = None
    try:
        async with AsyncDBSession() as session:
            async with session.begin():
                await session.execute(text("SET LOCAL lock_timeout = '3s'"))
                await session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': ADMISSION_LOCK})
                previous = await session.scalar(select(ExportJob).where(ExportJob.client_id == client.id,
                                                                        ExportJob.idempotency_key == key))
                if previous:
                    if previous.request_sha256 != fingerprint and not compatible_matrix_replay(previous, spec, document):
                        raise HTTPException(409, 'Idempotency key already belongs to a different request.')
                    job = previous
                else:
                    active = select(ExportJob).where(ExportJob.status.in_(('queued', 'running')))
                    total = await session.scalar(select(func.count()).select_from(active.subquery()))
                    own = await session.scalar(select(func.count()).select_from(active.where(ExportJob.client_id == client.id).subquery()))
                    if total >= 8 or own >= 2:
                        raise HTTPException(429, 'Export queue is full. Retry later.', headers={'Retry-After': '30'})
                    await asyncio.to_thread(files.ensure_space)
                    job = ExportJob(id=uuid.uuid4().hex, client_id=client.id, idempotency_key=key,
                        request_sha256=fingerprint, name=spec.name, source=spec.source, row_limit=spec.limit,
                        expires_at=now()+timedelta(seconds=config.RETENTION))
                    created = job.id
                    files.job_dir(job.id).mkdir(mode=0o700)
                    await asyncio.to_thread(files.write_json, files.job_dir(job.id)/'request.json', document)
                    session.add(job)
                    await session.flush()
        # A queue outage must not lose a committed request: maintenance requeues
        # durable queued rows. Retrying the same key returns this identity.
        if job.status == 'queued':
            try:
                await enqueue(redis, job.id)
            except Exception:
                pass
        return payload(job)
    except BaseException:
        if created:
            # Check committed state before removing its immutable request.
            async with AsyncDBSession() as check:
                if await check.get(ExportJob, created) is None:
                    await asyncio.to_thread(files.remove_files, created)
        raise


async def owned_job(client, job_id, *, ready=False):
    async with AsyncDBSession() as session:
        job = await session.get(ExportJob, job_id)
    if job is None or job.client_id != client.id:
        raise HTTPException(404, 'Export not found.')
    if job.status in ('expired', 'deleted') or (job.expires_at and job.expires_at <= now()):
        raise HTTPException(410, 'This export has expired or was deleted.')
    if ready and job.status != 'complete':
        raise HTTPException(409, 'Export is not complete.')
    return job


async def cancel(client, job_id):
    await owned_job(client, job_id)
    async with AsyncDBSession() as session:
        job = await session.get(ExportJob, job_id, with_for_update=True)
        if job.status in ('queued', 'running'):
            job.cancel_requested = True
            job.phase = 'cancelling'
        await session.commit()
        return payload(job)


async def delete(client, job_id):
    await owned_job(client, job_id)
    async with AsyncDBSession() as session:
        job = await session.get(ExportJob, job_id, with_for_update=True)
        if job.status not in config.TERMINAL:
            raise HTTPException(409, 'Cancel the running export and wait for it to stop before deleting it.')
        job.status = job.phase = 'deleted'
        await asyncio.to_thread(files.remove_files, job.id)
        await session.commit()
