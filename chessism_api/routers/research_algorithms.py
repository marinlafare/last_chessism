"""Superuser algorithm configuration, preflight, execution and durable results."""

from copy import deepcopy
import logging
import uuid
from typing import Literal

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, AlgorithmDefinition, AlgorithmRun, MatrixDefinition
from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.matrix_constructor.storage import matrix_catalog_lock, MatrixCatalogBusy
from chessism_api.operations.matrix_constructor.definitions import estimate_definition
from chessism_api.operations.research_algorithms.config import ACTIVE, MAX_COLUMNS, MAX_ROWS, QUEUE, algorithm_catalog, normalize_algorithm, source_config
from chessism_api.operations.research_algorithms.repository import definition_payload, list_runs, now, run_payload, terminal
from chessism_api.operations.research_algorithms.storage import capacity

router = APIRouter()
QUEUE_LOCK = 731_946_225


class AlgorithmRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    matrix_definition_id: uuid.UUID
    operation: Literal["feature_relationships"] = "feature_relationships"
    columns: list[str] = Field(min_length=2, max_length=MAX_COLUMNS)
    missing: Literal["drop_rows", "column_mean"] = "drop_rows"
    scaling: Literal["none", "standardize"] = "none"
    rating_difference: bool = False
    x: str
    y: str
    max_rows: int = Field(100000, ge=1, le=MAX_ROWS)
    seed: int = Field(42, ge=0, le=2 ** 32 - 1)


async def request_config(request, session):
    matrix = await session.get(MatrixDefinition, str(request.matrix_definition_id))
    if matrix is None:
        raise HTTPException(404, "Matrix definition not found. Refresh the input list.")
    try:
        return normalize_algorithm(request.model_dump(mode="json"), matrix.config)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


@router.get("/catalog")
async def catalog():
    return algorithm_catalog()


@router.post("/preflight")
async def preflight(request: AlgorithmRequest):
    async with AsyncDBSession() as session:
        config = await request_config(request, session)
    # A bounded count is advisory, never an assertion of an exact total.
    from sqlalchemy.exc import DBAPIError
    try:
        estimate = await estimate_definition(source_config(config))
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) == "57014":
            raise HTTPException(504, "Counting took too long. Narrow the matrix scope; saving does not require a count.") from error
        raise
    return {"estimate": estimate, "resources": capacity(config), "backend": "numpy_cpu", "materialized": False}


@router.get("/definitions")
async def definitions(limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0)):
    async with AsyncDBSession() as session:
        rows = list((await session.scalars(select(AlgorithmDefinition).order_by(AlgorithmDefinition.created_at.desc(), AlgorithmDefinition.id).offset(offset).limit(limit + 1))))
    return {"definitions": [definition_payload(row) for row in rows[:limit]], "has_more": len(rows) > limit}


@router.post("/definitions", status_code=201)
async def save_definition(request: AlgorithmRequest, account: Account = Depends(get_current_account)):
    try:
        async with matrix_catalog_lock(wait=False) as session:
            config = await request_config(request, session)
            row = AlgorithmDefinition(id=str(uuid.uuid4()), name=request.name.strip() or "Feature relationships",
                                      matrix_definition_id=str(request.matrix_definition_id), config=config, created_by=account.id)
            session.add(row)
            await session.flush()
            return definition_payload(row)
    except MatrixCatalogBusy as error:
        raise HTTPException(409, str(error)) from error


@router.delete("/definitions/{definition_id}", status_code=204)
async def delete_definition(definition_id: uuid.UUID):
    try:
        async with matrix_catalog_lock(wait=False) as session:
            row = await session.get(AlgorithmDefinition, str(definition_id))
            if row is None:
                raise HTTPException(404, "Algorithm definition not found.")
            await session.delete(row)  # Existing runs retain their frozen instructions.
    except MatrixCatalogBusy as error:
        raise HTTPException(409, str(error)) from error


@router.post("/definitions/{definition_id}/runs", status_code=202)
async def start_run(definition_id: uuid.UUID, account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    run_id = str(uuid.uuid4())
    async with AsyncDBSession() as session, session.begin():
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": QUEUE_LOCK})
        if await session.scalar(select(func.count()).select_from(AlgorithmRun).where(AlgorithmRun.status.in_(ACTIVE))) >= 5:
            raise HTTPException(409, "The algorithm queue already has five active requests. Wait or cancel one.")
        definition = await session.get(AlgorithmDefinition, str(definition_id), with_for_update=True)
        if definition is None:
            raise HTTPException(404, "Algorithm definition not found.")
        if not capacity(definition.config)["safe_to_run"]:
            raise HTTPException(409, "Not enough local disk space above the protected reserve.")
        row = AlgorithmRun(id=run_id, definition_id=definition.id, name=definition.name, config=deepcopy(definition.config),
                           created_by=account.id, status="queued", progress={"phase": "queued", "detail": "Waiting for the dedicated algorithm worker."})
        session.add(row)
    try:
        job = await redis.enqueue_job("run_algorithm_job", run_id=run_id, _queue_name=QUEUE, _job_id=f"algorithm-{run_id}")
        if job is None:
            raise RuntimeError("Unable to register algorithm job.")
    except Exception as error:
        await terminal(run_id, "failed", "Queue submission failed. Please try again.")
        raise HTTPException(503, "Algorithm queue is unavailable; the request was marked failed.") from error
    return {"id": run_id, "status": "queued"}


@router.get("/runs")
async def runs(limit: int = Query(30, ge=1, le=100), offset: int = Query(0, ge=0)):
    return await list_runs(limit, offset)


@router.get("/runs/{run_id}")
async def run_details(run_id: uuid.UUID):
    async with AsyncDBSession() as session:
        row = await session.get(AlgorithmRun, str(run_id))
        if row is None:
            raise HTTPException(404, "Algorithm run not found.")
        return run_payload(row, result=True)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: uuid.UUID, redis: ArqRedis = Depends(get_redis_pool)):
    async with AsyncDBSession() as session, session.begin():
        row = await session.get(AlgorithmRun, str(run_id), with_for_update=True)
        if row is None:
            raise HTTPException(404, "Algorithm run not found.")
        if row.status not in ACTIVE:
            raise HTTPException(409, "This run has already finished.")
        queued = row.status == "queued"
        row.cancel_requested = True
        if queued:
            row.status, row.finished_at = "cancelled", now()
            row.progress = {"phase": "cancelled", "detail": "Cancelled before execution."}
    if queued:
        try:
            await redis.zrem(QUEUE, f"algorithm-{run_id}")
        except Exception:
            # Durable cancellation is authoritative; the worker skips this row
            # even if Redis is temporarily unavailable to remove the ticket.
            logging.getLogger(__name__).warning("Cancelled algorithm ticket remains in Redis: %s", run_id, exc_info=True)
    return {"id": str(run_id), "status": "cancelled" if queued else "cancelling",
            "message": "Cancellation is checked between bounded chunks; a source query may take up to 60 seconds to return."}
