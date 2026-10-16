"""Compatibility reads/deletion for existing snapshot files, never new builds."""

import json
import shutil
import uuid

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import MatrixArtifact
from chessism_api.operations.matrix_constructor.artifact_files import file_operation
from chessism_api.operations.matrix_constructor.storage import (
    ARTIFACT_ROOT, MatrixCatalogBusy, display_manifest_path, matrix_catalog_lock,
)
from chessism_api.redis_client import get_redis_pool
from .schemas import PreviewRole

router = APIRouter()


async def _active_progress(artifacts: list[MatrixArtifact]) -> dict:
    active = [item for item in artifacts if item.job_id and item.status in {"queued", "running"}]
    if not active:
        return {}  # Completed snapshots have no dependency on Redis availability.
    redis = await get_redis_pool()
    progress = await redis.mget([f"chessism:job_progress:{item.job_id}" for item in active])
    return {item.id: _decode_progress(raw) for item, raw in zip(active, progress)}


def _decode_progress(raw) -> dict | None:
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
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


@router.get("/snapshots")
async def list_matrix_artifacts(
    limit: int = Query(30, ge=1, le=100),
) -> dict:
    async with AsyncDBSession() as session:
        result = await session.execute(
            select(MatrixArtifact).order_by(MatrixArtifact.created_at.desc()).limit(limit)
        )
        artifacts = list(result.scalars())
    by_id = await _active_progress(artifacts)
    payloads = [_artifact_payload(item, by_id.get(item.id)) for item in artifacts]
    return {"artifacts": payloads}


@router.get("/{artifact_id}")
@router.get("/snapshots/{artifact_id}")
async def get_matrix_artifact(artifact_id: str) -> dict:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
    progress = await _active_progress([artifact])
    return _artifact_payload(artifact, progress.get(artifact.id))


@router.get("/{artifact_id}/preview")
@router.get("/snapshots/{artifact_id}/preview")
async def preview_matrix_artifact(
    artifact_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    role: PreviewRole = "all",
    slice_index: int = Query(0, ge=0),
) -> dict:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, str(artifact_id))
        if artifact is None:
            raise HTTPException(status_code=404, detail="Matrix artifact not found.")
        if artifact.status != "complete":
            raise HTTPException(status_code=409, detail="Wait for the matrix snapshot to complete.")
    from chessism_api.operations.matrix_constructor.preview import PreviewUnavailable, read_matrix_preview

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
        targets = [root / name for name in (artifact.id, f".{artifact.id}.building")]
        if any(target.is_symlink() or target.resolve().parent != root for target in targets):
            raise HTTPException(status_code=409, detail="Unsafe artifact path.")
        # The hidden directory may survive an OS/container crash. The same
        # inactive-artifact guard and exact UUID bounds apply to its cleanup.
        for target in targets:
            if target.exists():
                await file_operation(shutil.rmtree, target)
        await session.delete(artifact)
        await session.commit()
