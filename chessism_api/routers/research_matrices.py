"""Superuser matrix definitions/live previews and preserved legacy snapshots."""

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
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, MatrixArtifact, MatrixDefinition
from chessism_api.operations.matrix_constructor import (
    matrix_catalog,
    normalize_matrix_config,
)
from chessism_api.operations.matrix_constructor.queries import ARTIFACT_ROOT
from chessism_api.operations.matrix_constructor.artifact_files import file_operation
from chessism_api.operations.matrix_constructor.preview import PreviewUnavailable, read_matrix_preview
from chessism_api.operations.matrix_constructor.definitions import definition_config, definition_payload, estimate_definition
from chessism_api.operations.matrix_constructor.live_preview import preview_definition
from chessism_api.operations.matrix_constructor.storage import (
    MatrixCatalogBusy, display_manifest_path, matrix_catalog_lock,
)
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
        "artifact_path": display_manifest_path(artifact.id) if artifact.status == "complete" else None,
        "storage_kind": "working_copy",
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
        return await estimate_definition(_request_config(request))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except DBAPIError as error:
        _query_error(error)


def _query_error(error):
    if getattr(error.orig, "sqlstate", None) == "57014":
        raise HTTPException(status_code=504, detail="Live data query took too long. Narrow the players or date range and retry.") from error
    raise error


@router.post("/preview")
async def preview_unsaved_definition(
    request: MatrixRequest, limit: int = Query(50, ge=1, le=100),
    role: Literal["all", "features", "labels"] = "all",
) -> dict:
    try:
        return await preview_definition(_request_config(request), limit=limit, role=role)
    except DBAPIError as error:
        _query_error(error)


@router.get("")
async def list_matrix_definitions(limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0)) -> dict:
    async with AsyncDBSession() as session:
        result = await session.execute(select(MatrixDefinition).order_by(
            MatrixDefinition.created_at.desc(), MatrixDefinition.id,
        ).limit(limit + 1).offset(offset))
        definitions = list(result.scalars())
        legacy_count = await session.scalar(select(func.count()).select_from(MatrixArtifact))
    return {"definitions": [definition_payload(item) for item in definitions[:limit]],
            "has_more": len(definitions) > limit, "legacy_snapshot_count": legacy_count}


@router.get("/snapshots")
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
@router.get("/snapshots/{artifact_id}")
async def get_matrix_artifact(
    artifact_id: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
    return _artifact_payload(artifact, await _progress(redis, artifact))


@router.post("", status_code=201)
async def save_matrix_definition(
    request: MatrixRequest,
    account: Account = Depends(get_current_account),
) -> dict:
    config = definition_config(_request_config(request))
    try:
        async with matrix_catalog_lock(wait=False) as session:
            definition = MatrixDefinition(id=str(uuid.uuid4()), name=config["name"], row_type=config["row_type"],
                                          created_by=account.id, config=config)
            session.add(definition)
            await session.flush()
            payload = definition_payload(definition)
        return payload
    except MatrixCatalogBusy as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/definitions/{definition_id}")
async def get_matrix_definition(definition_id: uuid.UUID) -> dict:
    async with AsyncDBSession() as session:
        definition = await session.get(MatrixDefinition, str(definition_id))
        if definition is None:
            raise HTTPException(status_code=404, detail="Matrix definition not found.")
        return definition_payload(definition)


@router.get("/definitions/{definition_id}/preview")
async def preview_saved_definition(
    definition_id: uuid.UUID, limit: int = Query(50, ge=1, le=100),
    role: Literal["all", "features", "labels"] = "all",
) -> dict:
    definition = await get_matrix_definition(definition_id)
    try:
        return await preview_definition(definition["config"], limit=limit, role=role)
    except DBAPIError as error:
        _query_error(error)


@router.delete("/definitions/{definition_id}", status_code=204)
async def delete_matrix_definition(definition_id: uuid.UUID) -> None:
    try:
        async with matrix_catalog_lock(wait=False) as session:
            definition = await session.get(MatrixDefinition, str(definition_id))
            if definition is None:
                raise HTTPException(status_code=404, detail="Matrix definition not found.")
            await session.delete(definition)
    except MatrixCatalogBusy as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/{artifact_id}/preview")
@router.get("/snapshots/{artifact_id}/preview")
async def preview_matrix_artifact(
    artifact_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    role: Literal["all", "features", "labels"] = "all",
    slice_index: int = Query(0, ge=0),
) -> dict:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, str(artifact_id))
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
        if artifact.status != "complete":
            raise HTTPException(status_code=409, detail="Wait for the matrix snapshot to complete.")
    try:
        return await file_operation(
            read_matrix_preview, ARTIFACT_ROOT, str(artifact_id),
            offset=offset, limit=limit, role=role, slice_index=slice_index,
        )
    except PreviewUnavailable as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.delete("/{artifact_id}", status_code=204)
@router.delete("/snapshots/{artifact_id}", status_code=204)
async def delete_matrix_artifact(artifact_id: str) -> None:
    try:
        async with matrix_catalog_lock(wait=False):
            await _delete_working_artifact(artifact_id)
    except MatrixCatalogBusy as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


async def _delete_working_artifact(artifact_id: str) -> None:
    """Delete only local working files. Backup directories are never pruned."""
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
