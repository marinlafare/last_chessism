"""Superuser visibility and explicit retry of the derived chess graph."""
from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.neo4j.sync_schema import status, available

router = APIRouter()


@router.get('')
async def graph_status():
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL statement_timeout='3s'"))
        return await status(session)


@router.post('/retry')
async def retry_graph():
    async with AsyncDBSession() as session, session.begin():
        if not await available(session):
            raise HTTPException(409, 'Graph synchronization is not installed')
        await session.execute(text('UPDATE graph_sync.event SET retry_at=NULL WHERE completed_at IS NULL AND error IS NOT NULL'))
        return await status(session)
