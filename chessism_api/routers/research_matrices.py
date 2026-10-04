"""Superuser matrix catalog, estimates, and background artifact jobs."""

from __future__ import annotations

import asyncio
import json
import shutil
import uuid
from datetime import date
from typing import Literal

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, MatrixArtifact
from chessism_api.operations.matrix_constructor import (
    estimate_matrix,
    matrix_catalog,
    normalize_matrix_config,
)
from chessism_api.operations.matrix_constructor.queries import ARTIFACT_ROOT
from chessism_api.operations.research_resources import analysis_jobs_active
from chessism_api.redis_client import get_redis_pool


router = APIRouter()


class MatrixFilters(BaseModel):
    players: list[str] = Field(default_factory=list, max_length=100)
    modes: list[Literal["bullet", "blitz", "rapid"]] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    min_moves: int = Field(0, ge=0, le=10_000)
    analyzed_only: bool = False
    max_rows: int = Field(100_000, ge=1, le=5_000_000)


class MatrixRequest(BaseModel):
    name: str = Field("", max_length=100)
    row_type: Literal["game", "game_player", "move", "position", "game_position", "player_period"]
    feature_columns: list[str] = Field(min_length=1, max_length=48)
    label_columns: list[str] = Field(default_factory=list, max_length=16)
    filters: MatrixFilters = Field(default_factory=MatrixFilters)


def _request_config(request: MatrixRequest) -> dict:
    try:
        return normalize_matrix_config(request.model_dump(mode="json"))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


async def _progress(redis: ArqRedis, artifact: MatrixArtifact) -> dict | None:
    if not artifact.job_id:
        return None
    raw = await redis.get(f"chessism:job_progress:{artifact.job_id}")
    return _decode_progress(raw)


def _decode_progress(raw) -> dict | None:
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except (TypeError, json.JSONDecodeError):
        return None


def _artifact_payload(artifact: MatrixArtifact, progress: dict | None = None) -> dict:
    return {
        "id": artifact.id,
        "name": artifact.name,
        "row_type": artifact.row_type,
        "status": artifact.status,
        "job_id": artifact.job_id,
        "config": artifact.config,
        "estimate": artifact.estimate,
        "result": artifact.result,
        "artifact_path": artifact.artifact_path,
        "row_count": artifact.row_count,
        "feature_count": artifact.feature_count,
        "label_count": artifact.label_count,
        "size_bytes": artifact.size_bytes,
        "error": artifact.error,
        "created_at": artifact.created_at.isoformat() if artifact.created_at else None,
        "started_at": artifact.started_at.isoformat() if artifact.started_at else None,
        "finished_at": artifact.finished_at.isoformat() if artifact.finished_at else None,
        "progress": progress,
    }


@router.get("/catalog")
async def get_matrix_catalog() -> dict:
    return matrix_catalog()


@router.post("/estimate")
async def preview_matrix(request: MatrixRequest) -> dict:
    try:
        return await estimate_matrix(_request_config(request))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.get("")
async def list_matrix_artifacts(
    limit: int = Query(30, ge=1, le=100),
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    async with AsyncDBSession() as session:
        result = await session.execute(
            select(MatrixArtifact).order_by(MatrixArtifact.created_at.desc()).limit(limit)
        )
        artifacts = list(result.scalars())
    active = [item for item in artifacts if item.job_id and item.status in {"queued", "running"}]
    progress = await redis.mget([f"chessism:job_progress:{item.job_id}" for item in active]) if active else []
    by_id = {item.id: _decode_progress(raw) for item, raw in zip(active, progress)}
    payloads = [_artifact_payload(item, by_id.get(item.id)) for item in artifacts]
    return {"artifacts": payloads}


@router.get("/{artifact_id}")
async def get_matrix_artifact(
    artifact_id: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
    return _artifact_payload(artifact, await _progress(redis, artifact))


@router.post("", status_code=202)
async def create_matrix_artifact(
    request: MatrixRequest,
    account: Account = Depends(get_current_account),
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    config = _request_config(request)
    if await analysis_jobs_active(redis):
        raise HTTPException(
            status_code=409,
            detail="Wait for Stockfish analysis to become idle before constructing a matrix.",
        )
    estimate = await estimate_matrix(config)
    if estimate["selected_rows"] < 1:
        raise HTTPException(status_code=422, detail="The selected scope contains no rows.")
    if not estimate["storage"]["safe_to_build"]:
        raise HTTPException(
            status_code=409,
            detail="The matrix would cross the protected free-space floor.",
        )
    artifact_id = str(uuid.uuid4())
    job_id = f"matrix-constructor-{artifact_id}"
    async with AsyncDBSession() as session:
        session.add(MatrixArtifact(
            id=artifact_id,
            name=config["name"],
            row_type=config["row_type"],
            status="queued",
            job_id=job_id,
            created_by=account.id,
            config=config,
            estimate=estimate,
        ))
        await session.commit()
    try:
        job = await redis.enqueue_job(
            "run_matrix_construction_job",
            artifact_id=artifact_id,
            _queue_name="research_queue",
            _job_id=job_id,
        )
        if job is None:
            raise RuntimeError("Redis did not create the matrix job.")
    except Exception as error:
        async with AsyncDBSession() as session:
            artifact = await session.get(MatrixArtifact, artifact_id)
            if artifact is not None:
                artifact.status = "failed"
                artifact.error = str(error)
                await session.commit()
        raise HTTPException(status_code=503, detail="Could not queue the matrix job.") from error
    return {"artifact_id": artifact_id, "job_id": job_id, "status": "queued"}


@router.delete("/{artifact_id}", status_code=204)
async def delete_matrix_artifact(artifact_id: str) -> None:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
        if artifact.status in {"queued", "running"}:
            raise HTTPException(status_code=409, detail="An active matrix artifact cannot be deleted.")
        try:
            uuid.UUID(artifact.id)
        except ValueError as error:
            raise HTTPException(status_code=409, detail="Unsafe artifact identifier.") from error
        root = ARTIFACT_ROOT.resolve()
        targets = [(root / name).resolve() for name in (artifact.id, f".{artifact.id}.building")]
        if any(target.parent != root for target in targets):
            raise HTTPException(status_code=409, detail="Unsafe artifact path.")
        # The hidden directory may survive an OS/container crash. The same
        # inactive-artifact guard and exact UUID bounds apply to its cleanup.
        for target in targets:
            if target.exists():
                await asyncio.to_thread(shutil.rmtree, target)
        await session.delete(artifact)
        await session.commit()
