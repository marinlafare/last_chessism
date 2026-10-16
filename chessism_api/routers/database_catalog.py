"""Superuser-only structure and bounded row previews; no SQL or write routes."""
import logging
import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.exc import SQLAlchemyError

from chessism_api.auth import require_superuser
from chessism_api.operations.database_catalog import CatalogTooLarge, read_database_catalog
from chessism_api.operations.database_preview import PreviewNotFound, read_database_preview

router = APIRouter(dependencies=[Depends(require_superuser)])


@router.get('/schema')
async def database_schema(response: Response):
    response.headers['Cache-Control'] = 'no-store'
    try:
        return await read_database_catalog()
    except CatalogTooLarge as error:
        raise HTTPException(503, str(error)) from error
    except SQLAlchemyError as error:
        # Do not return driver errors, SQL, connection information or credentials.
        logging.getLogger(__name__).warning('Database catalog unavailable: %s', type(error).__name__)
        raise HTTPException(503, 'Database structure is temporarily unavailable. Try again.') from error


@router.get('/preview')
async def database_preview(response: Response, table: str = Query(min_length=1, max_length=512)):
    response.headers['Cache-Control'] = 'no-store'
    try:
        return await read_database_preview(table)
    except PreviewNotFound as error:
        raise HTTPException(404, str(error), headers={'Cache-Control': 'no-store'}) from error
    except (SQLAlchemyError, asyncio.TimeoutError) as error:
        logging.getLogger(__name__).warning('Database preview unavailable: %s', type(error).__name__)
        raise HTTPException(503, 'This table preview is temporarily unavailable or took too long. Try again.',
                            headers={'Cache-Control': 'no-store'}) from error
