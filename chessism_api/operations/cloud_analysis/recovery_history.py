"""Durable retry and manual-pause events; no cloud calls or automatic resumes."""
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select, func
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudRecoveryEvent


async def record(job_id, **event):
    async with AsyncDBSession() as session, session.begin():
        session.add(CloudRecoveryEvent(job_id=job_id, **event))


async def transition(session, job, following, *, message=None):
    now = datetime.now(timezone.utc)
    if following == 'paused' and job.status != 'paused':
        session.add(CloudRecoveryEvent(job_id=job.id, episode_id=uuid4().hex,
            kind='paused', operation='local workflow', message=message, created_at=now))
    elif job.status == 'paused' and following == 'queued':
        paused = await session.scalar(select(CloudRecoveryEvent).where(
            CloudRecoveryEvent.job_id == job.id, CloudRecoveryEvent.kind == 'paused')
            .order_by(CloudRecoveryEvent.id.desc()).limit(1))
        session.add(CloudRecoveryEvent(job_id=job.id,
            episode_id=paused.episode_id if paused else uuid4().hex,
            kind='resumed', operation='manual recovery', created_at=now,
            elapsed_seconds=max(0, (now - paused.created_at).total_seconds()) if paused else None,
            message='Resumed the existing workflow; no new job created.'))


async def summary(session, job_id, limit=200):
    events = (await session.scalars(select(CloudRecoveryEvent).where(CloudRecoveryEvent.job_id == job_id)
        .order_by(CloudRecoveryEvent.id.desc()).limit(limit))).all()
    counts = dict((await session.execute(select(CloudRecoveryEvent.kind, func.count()).where(
        CloudRecoveryEvent.job_id == job_id).group_by(CloudRecoveryEvent.kind))).all())
    paused = await session.scalar(select(func.coalesce(func.sum(CloudRecoveryEvent.elapsed_seconds), 0)).where(
        CloudRecoveryEvent.job_id == job_id, CloudRecoveryEvent.kind == 'resumed'))
    return {'events': [{c.name: getattr(e, c.name) for c in e.__table__.columns} for e in reversed(events)],
            'counts': counts, 'paused_seconds': float(paused), 'total': sum(counts.values()),
            'note': 'Recovery events are recorded from this deployment onward; older pauses cannot be reconstructed.'}
