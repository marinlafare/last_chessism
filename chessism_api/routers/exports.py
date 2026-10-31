"""Bearer-token integration API; deliberately independent of admin sessions."""
import asyncio
import json
import logging
import uuid

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.routing import APIRoute
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from sqlalchemy.exc import SQLAlchemyError

from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.exports import files, service
from chessism_api.operations.exports.auth import authorize_spec, require_client
from chessism_api.operations.exports.catalog import public_catalog
from chessism_api.operations.exports.query import ExportSpec
from chessism_api.operations.exports.preview import preview_slot, structured_preview as preview_rows
from chessism_api.operations.exports.analytical_spec import SQLExportSpec, MoveMatrixSpec, AnalyticalPreviewSpec, AnalyticalPreviewResult
from chessism_api.operations.exports.analytical_reader import AnalyticalError
from chessism_api.operations.exports.analytical_preview import preview as analytical_preview_rows, SAMPLE_TIMEOUT_SECONDS


class BoundedRequestRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()
        async def bounded(request: Request):
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 131072:
                    raise HTTPException(413, 'Export request exceeds 128 KiB.')
            request._body = bytes(body)
            response = await handler(request)
            response.headers['Cache-Control'] = 'private, no-store'
            return response
        return bounded


router = APIRouter(route_class=BoundedRequestRoute)


@router.get('/catalog')
async def catalog(client=Depends(require_client)):
    return public_catalog(client.allowed_sources, client.max_rows)


@router.get('/analytical-catalog')
async def analytical_catalog(client=Depends(require_client)):
    from chessism_api.operations.exports.analytical import catalog
    return catalog(client)


async def submit_analytical(spec, client, redis, key):
    from chessism_api.operations.exports.analytical_reader import connection
    authorize_spec(client, spec)
    # Refuse admission when isolation has not been installed or has drifted.
    try:
        async with connection():
            pass
    except Exception:
        raise HTTPException(503, 'Analytical reader is unavailable or its privilege boundary failed validation.') from None
    try:
        return await service.submit(client, spec, key, redis)
    except SQLAlchemyError:
        raise HTTPException(503, 'Export admission is temporarily unavailable.') from None


@router.post('/sql', status_code=202)
async def create_sql(spec: SQLExportSpec, client=Depends(require_client), redis=Depends(get_redis_pool),
                     idempotency_key: str = Header(min_length=8, max_length=128, pattern=r'^[A-Za-z0-9._:-]+$')):
    return await submit_analytical(spec, client, redis, idempotency_key)


@router.post('/move-matrix', status_code=202)
async def create_matrix(spec: MoveMatrixSpec, client=Depends(require_client), redis=Depends(get_redis_pool),
                        idempotency_key: str = Header(min_length=8, max_length=128, pattern=r'^[A-Za-z0-9._:-]+$')):
    return await submit_analytical(spec, client, redis, idempotency_key)


@router.post('/analytical-preview', response_model=AnalyticalPreviewResult,
             summary='Preview the final rows of a SQL or move-matrix design')
async def analytical_preview(request: AnalyticalPreviewSpec, client=Depends(require_client), redis=Depends(get_redis_pool)):
    authorize_spec(client, request.design)
    async with preview_slot(redis, client.id, analytical=True):
        return await run_analytical_preview(request)


async def run_analytical_preview(request):
    try:
        deadline = SAMPLE_TIMEOUT_SECONDS if request.preview_mode == 'sample' else 15
        return await asyncio.wait_for(analytical_preview_rows(request), timeout=deadline)
    except AnalyticalError as error:
        status = (504 if error.code == 'PREVIEW_TIMEOUT' else
                  503 if error.code in ('ANALYTICS_UNAVAILABLE', 'UNSAFE_QUERY_ROLE', 'ANALYTICS_BUSY') else 422)
        raise HTTPException(status, {'code': error.code, 'message': str(error)}) from None
    except asyncio.TimeoutError:
        raise HTTPException(504, {'code': 'PREVIEW_TIMEOUT', 'message': 'Preview exceeded its deadline. Narrow the design or request an asynchronous export.'}) from None
    except (OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError):
        raise HTTPException(503, {'code': 'PREVIEW_UNAVAILABLE', 'message': 'The analytical reader is temporarily unavailable.'}) from None
    except Exception as error:
        # Do not expose query text, database details or private credential paths.
        logging.getLogger(__name__).error('Analytical preview failed (%s)', type(error).__name__)
        raise HTTPException(503, {'code': 'PREVIEW_UNAVAILABLE', 'message': 'The analytical preview is temporarily unavailable.'}) from None


@router.post('/preview')
async def preview(spec: ExportSpec, client=Depends(require_client), redis=Depends(get_redis_pool)):
    authorize_spec(client, spec)
    async with preview_slot(redis, client.id):
        try:
            return await asyncio.wait_for(preview_rows(spec), timeout=15)
        except (SQLAlchemyError, asyncio.TimeoutError):
            raise HTTPException(503, 'Preview exceeded its query limit or is unavailable. Narrow the request.') from None


@router.post('', status_code=202)
async def create(spec: ExportSpec, client=Depends(require_client), redis=Depends(get_redis_pool),
                 idempotency_key: str = Header(min_length=8, max_length=128, pattern=r'^[A-Za-z0-9._:-]+$')):
    try:
        return await service.submit(client, spec, idempotency_key, redis)
    except ValueError as error:
        raise HTTPException(422, str(error)) from None
    except SQLAlchemyError:
        raise HTTPException(503, 'Export admission is temporarily unavailable.') from None


@router.get('/{export_id}')
async def status(export_id: uuid.UUID, client=Depends(require_client)):
    return service.payload(await service.owned_job(client, export_id.hex))


@router.get('/{export_id}/manifest')
async def manifest(export_id: uuid.UUID, client=Depends(require_client)):
    await service.owned_job(client, export_id.hex, ready=True)
    try:
        return json.loads((files.job_dir(export_id.hex)/'manifest.json').read_bytes())
    except FileNotFoundError:
        raise HTTPException(410, 'Export files are no longer available.') from None


@router.get('/{export_id}/file')
async def download(export_id: uuid.UUID, client=Depends(require_client)):
    job = await service.owned_job(client, export_id.hex, ready=True)
    try:
        source = open(files.job_dir(job.id)/'data.parquet', 'rb')
    except FileNotFoundError:
        raise HTTPException(410, 'Export files are no longer available.') from None
    # Open before responding: an already-open Linux file remains readable if
    # expiry removes its directory during a slow download.
    async def chunks():
        try:
            while chunk := await asyncio.to_thread(source.read, 1024*1024):
                yield chunk
        finally:
            source.close()
    return StreamingResponse(chunks(), media_type='application/vnd.apache.parquet',
        headers={'Content-Length': str(job.file_bytes), 'Content-Disposition': 'attachment; filename="data.parquet"',
                 'ETag': f'"{job.file_sha256}"'}, background=BackgroundTask(source.close))


@router.post('/{export_id}/cancel', status_code=202)
async def cancel(export_id: uuid.UUID, client=Depends(require_client)):
    return await service.cancel(client, export_id.hex)


@router.delete('/{export_id}', status_code=204)
async def delete(export_id: uuid.UUID, client=Depends(require_client)):
    await service.delete(client, export_id.hex)
    return Response(status_code=204)
