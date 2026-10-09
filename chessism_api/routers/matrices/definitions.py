"""Metadata-only definitions and bounded live previews (superuser router)."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, MatrixArtifact, MatrixDefinition
from chessism_api.operations.matrix_constructor.catalog import matrix_catalog
from chessism_api.operations.matrix_constructor.definitions import definition_config, definition_payload, estimate_definition
from chessism_api.operations.matrix_constructor.live_preview import preview_definition
from chessism_api.operations.matrix_constructor.storage import MatrixCatalogBusy, matrix_catalog_lock
from .schemas import MatrixRequest, PreviewRole, request_config

router = APIRouter()


@router.get("/catalog")
async def get_matrix_catalog() -> dict:
    return matrix_catalog()


@router.post("/estimate")
async def estimate_matrix_definition(request: MatrixRequest) -> dict:
    try:
        return await estimate_definition(request_config(request))
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
    role: PreviewRole = "all",
) -> dict:
    try:
        return await preview_definition(request_config(request), limit=limit, role=role)
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


@router.post("", status_code=201)
async def save_matrix_definition(
    request: MatrixRequest,
    account: Account = Depends(get_current_account),
) -> dict:
    config = definition_config(request_config(request))
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
    role: PreviewRole = "all",
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
