"""Serialize cloud admission across API processes/tabs, not just UI buttons."""
from fastapi import HTTPException
from sqlalchemy import select, text

from chessism_api.database.models import CloudAnalysisJob

OPEN_STATES = ("queued", "running", "paused", "waiting", "failed")
ADMISSION_LOCK = 731946220


async def blocking_job(session, excluding=None):
    query = select(CloudAnalysisJob).where(CloudAnalysisJob.status.in_(OPEN_STATES))
    if excluding is not None:
        query = query.where(CloudAnalysisJob.id != excluding)
    return (await session.scalars(query.order_by(CloudAnalysisJob.created_at, CloudAnalysisJob.id).limit(1))).first()


async def require_available(session, own_job_id=None):
    # Released on transaction commit/rollback, including rejected requests.
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": ADMISSION_LOCK})
    other = await blocking_job(session, excluding=own_job_id)
    if other:
        raise HTTPException(409, f"Cloud job {other.id} is {other.status}. "
                            "Finish or recover it before starting another cloud job.")
