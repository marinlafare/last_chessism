"""Superuser-only explicit CUDA summary requests; GET is strictly read-only."""
import logging
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from chessism_api.auth import require_superuser
from chessism_api.database.engine import AsyncDBSession
from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.cuda_summaries import runs
from chessism_api.operations.canonical_blunders.scheduler import schedule

router = APIRouter(dependencies=[Depends(require_superuser)])
LOG = logging.getLogger(__name__)


@router.get('')
async def get_status():
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        return await runs.status(session)


async def wake(redis):
    try:
        await schedule({'redis': redis})
    except Exception:
        LOG.exception('CUDA summary request saved; periodic wakeup will retry.')


@router.post('/run', status_code=202)
async def create_run(account=Depends(require_superuser), redis=Depends(get_redis_pool)):
    try:
        async with AsyncDBSession() as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout='3s'"))
            result = await runs.request_run(session, account.id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    await wake(redis)
    return result


@router.post('/{run_id}/retry', status_code=202)
async def retry(run_id: uuid.UUID, redis=Depends(get_redis_pool)):
    try:
        async with AsyncDBSession() as session, session.begin():
            result = await runs.retry_run(session, run_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    await wake(redis)
    return result
