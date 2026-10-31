"""Authenticated via the parent cloud router. Draft edits never start compute."""
from datetime import datetime, timezone
import json
from uuid import uuid4
from typing import Literal
from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, func, insert, select, text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (CloudGameDraft, CloudGameDraftEntry, CloudGameDraftGame,
    CloudAnalysisJob, CloudBatchSequence)
from chessism_api.operations.cloud_analysis import game_drafts as drafts
from chessism_api.operations.cloud_analysis import draft_cache
from chessism_api.operations.cloud_analysis.admission import require_available
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest, MAX_BATCH_TOTAL_FENS, MAX_CLOUD_RUN_FENS, MAX_CLOUD_RUN_CPUS, batch_cycle_count
from chessism_api.operations.analysis_settings import DEFAULT_ANALYSIS_NODES, CLOUD_STALL_TIMEOUT_SECONDS
from chessism_api.redis_client import get_redis_pool

router = APIRouter()


class Revision(BaseModel):
    model_config = ConfigDict(extra='forbid')
    revision: int = Field(ge=1, strict=True)


class AddSelection(Revision):
    plan_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    replace_entry_id: str | None = Field(None, pattern=r'^[a-f0-9]{32}$')


class SubmitDraft(Revision):
    backend: Literal['batch_spot', 'cloud_run'] = 'batch_spot'
    n_vms: int = Field(1, ge=1, le=10, strict=True)
    n_cpus: int = Field(4, ge=1, le=MAX_CLOUD_RUN_CPUS, strict=True)


@router.get('/current')
async def current_draft():
    async with AsyncDBSession() as session:
        draft = (await session.scalars(select(CloudGameDraft)
            .where(CloudGameDraft.submitted_job_id.is_(None))
            .order_by(CloudGameDraft.created_at.desc()).limit(1))).first()
        return await drafts.view(session, draft) if draft else None


@router.post('', status_code=201)
async def create_draft():
    async with AsyncDBSession() as session, session.begin():
        # One shared active draft across tabs; repeated clicks cannot make extras.
        await session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': drafts.DRAFT_LOCK})
        draft = (await session.scalars(select(CloudGameDraft)
            .where(CloudGameDraft.submitted_job_id.is_(None)).limit(1))).first()
        if draft is None:
            draft = CloudGameDraft(id=uuid4().hex)
            session.add(draft)
            await session.flush()
        return await drafts.view(session, draft)


@router.get('/{draft_id}')
async def get_draft(draft_id: str):
    async with AsyncDBSession() as session:
        draft = await session.get(CloudGameDraft, draft_id)
        if draft is None:
            raise HTTPException(404, 'Combined selection draft not found')
        return await drafts.view(session, draft)


@router.post('/{draft_id}/entries')
async def save_entry(draft_id: str, data: AddSelection, redis: ArqRedis = Depends(get_redis_pool)):
    raw = await redis.get(f'chessism:player_game_analysis_plan:{data.plan_id}')
    if not raw:
        raise HTTPException(409, 'Game preview expired; preview again before adding it')
    values, links = drafts.preview_entry(json.loads(raw))
    async with AsyncDBSession() as session, session.begin():
        draft = await drafts.locked_draft(session, draft_id, data.revision)
        cache = await draft_cache.ensure(session, draft)
        if data.replace_entry_id:
            entry = await session.get(CloudGameDraftEntry, data.replace_entry_id)
            if not entry or entry.draft_id != draft.id:
                raise HTTPException(404, 'Selection not found in this draft')
            await draft_cache.remove_entry(session, cache, entry.id)
            await session.execute(delete(CloudGameDraftGame).where(CloudGameDraftGame.entry_id == entry.id))
            for key, value in values.items():
                setattr(entry, key, value)
        else:
            count, last = (await session.execute(select(func.count(), func.max(CloudGameDraftEntry.ordinal))
                .where(CloudGameDraftEntry.draft_id == draft.id))).one()
            if count >= drafts.MAX_DRAFT_ENTRIES:
                raise HTTPException(409, f'At most {drafts.MAX_DRAFT_ENTRIES} selections per draft')
            entry = CloudGameDraftEntry(id=uuid4().hex, draft_id=draft.id, ordinal=(last or 0) + 1, **values)
            session.add(entry)
        await session.flush()
        for offset in range(0, len(links), 1000):
            await session.execute(insert(CloudGameDraftGame), [
                {'entry_id': entry.id, 'game_link': link, 'ordinal': offset + i}
                for i, link in enumerate(links[offset:offset + 1000])])
        await draft_cache.add_entry(session, cache, entry.id)
        drafts.changed(draft)
        await draft_cache.publish(session, draft, cache)
        return await drafts.view(session, draft)


@router.post('/{draft_id}/entries/{entry_id}/remove')
async def remove_entry(draft_id: str, entry_id: str, data: Revision):
    async with AsyncDBSession() as session, session.begin():
        draft = await drafts.locked_draft(session, draft_id, data.revision)
        entry = await session.get(CloudGameDraftEntry, entry_id)
        if not entry or entry.draft_id != draft_id:
            raise HTTPException(404, 'Selection not found in this draft')
        cache = await draft_cache.ensure(session, draft)
        await draft_cache.remove_entry(session, cache, entry.id)
        await session.delete(entry)
        drafts.changed(draft)
        await session.flush()
        await draft_cache.publish(session, draft, cache)
        return await drafts.view(session, draft)


@router.post('/{draft_id}/preview')
async def preview_draft(draft_id: str, data: Revision):
    async with AsyncDBSession() as session, session.begin():
        draft = await drafts.locked_draft(session, draft_id, data.revision)
        # Explicit refresh rebuilds current membership/eligibility. Normal Add,
        # Save and Remove already return their incremental group preview.
        cache = await draft_cache.ensure(session, draft, rebuild=True)
        await draft_cache.publish(session, draft, cache)
        return await drafts.view(session, draft)


@router.post('/{draft_id}/submit', status_code=202)
async def submit_draft(draft_id: str, data: SubmitDraft):
    from .cloud_analysis import controller_online
    async with AsyncDBSession() as session, session.begin():
        draft = await drafts.locked_draft(session, draft_id, editable=False)
        if draft.submitted_job_id:
            job = await session.get(CloudAnalysisJob, draft.submitted_job_id)
            backend = job.selection.get('backend', 'batch_spot')
            if backend != data.backend:
                raise HTTPException(409, 'This draft was already submitted to a different cloud backend; start another process')
            return {'id': job.id, 'status': job.status, 'target': job.target,
                    'backend': backend, 'already_submitted': True}
        if data.revision != draft.revision or draft.preview_revision != draft.revision:
            raise HTTPException(409, 'Preview the current combined selection before submitting')
        if not await controller_online(session):
            raise HTTPException(409, 'Cloud controller is offline; start it with Docker Compose')
        await require_available(session)
        counts = await drafts.count_union(session, draft.id)
        target = counts['eligible']
        is_run = data.backend == 'cloud_run'
        maximum = MAX_CLOUD_RUN_FENS if is_run else MAX_BATCH_TOTAL_FENS
        backend_label = 'Cloud Run' if is_run else 'Batch Spot'
        if not 0 < target <= maximum:
            raise HTTPException(409, f'{backend_label} requires 1–{maximum:,} available unscored FENs; adjust the selection and preview again')
        if target > draft.preview_fens:
            raise HTTPException(409, 'Available FEN count increased; preview again before authorizing this job')
        links = await drafts.game_links(session, draft.id)
        # Both existing execution paths accept the same deduplicated game set.
        # Cloud Run gets one bounded job, never a Batch sequence or Spot VMs.
        selection = CloudJobRequest(mode='games', plan_id=draft.id, backend=data.backend,
                                    n_vms=data.n_vms, n_cpus=data.n_cpus).model_dump()
        selection.update(game_links=links, player_name='', total_fens=target,
            nodes=DEFAULT_ANALYSIS_NODES, stall_timeout_seconds=CLOUD_STALL_TIMEOUT_SECONDS,
            execution_mode='cloud_run_v1' if is_run else 'batch_sequence_v2',
            recovery_policy='cloud-run-task-retry-v1' if is_run else 'google-preemption-v1')
        job = CloudAnalysisJob(id=uuid4().hex, status='queued', target=target, imported=0, selection=selection)
        session.add(job)
        await session.flush()
        if not is_run:
            session.add(CloudBatchSequence(job_id=job.id, requested_count=target,
                                          repeat_count=batch_cycle_count(target, data.n_vms)))
        draft.submitted_job_id = job.id
        draft.updated_at = datetime.now(timezone.utc)
        await draft_cache.discard(session, draft.id)
        return {'id': job.id, 'status': job.status, 'target': target,
                'backend': data.backend, 'already_submitted': False}
