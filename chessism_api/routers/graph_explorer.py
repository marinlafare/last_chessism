"""Authenticated exploration only. No Cypher input, ingestion, or graph writes."""
import asyncio
import uuid
from collections import OrderedDict
from time import monotonic, perf_counter

from fastapi import APIRouter, HTTPException, Query, Request
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired

from chessism_api.database.neo4j.config import GraphConfig
from chessism_api.database.neo4j import explorer
from chessism_api.database.neo4j import opening_map
from chessism_api.database.neo4j import ply_map_reader
from chessism_api.database.postgresql.engine import AsyncDBSession
from sqlalchemy import text

router = APIRouter()
_gate = asyncio.Semaphore(2)
_cache = OrderedDict()
MAX_CACHE = 32
TTL = 30


async def query(request, operation, *args):
    identity = (operation.__name__, *args)
    started = perf_counter()
    try:
        # Includes queueing/connection time, not just Cypher execution time.
        async def bounded():
            async with _gate:
                saved = _cache.get(identity)
                if saved and saved[0] > monotonic():
                    _cache.move_to_end(identity)
                    return saved[1]
                config = GraphConfig.from_env()
                driver = getattr(request.app.state, 'graph_explorer_driver', None)
                if driver is None:
                    driver = AsyncGraphDatabase.driver(config.uri, auth=config.credentials(),
                        max_connection_pool_size=2, connection_timeout=3,
                        connection_acquisition_timeout=4, max_transaction_retry_time=0,
                        telemetry_disabled=True)
                    request.app.state.graph_explorer_driver = driver
                data = await explorer.read(driver, config.database, operation, *args)
                _cache[identity] = (monotonic() + (300 if operation is opening_map.snapshot else TTL), data)
                _cache.move_to_end(identity)
                while len(_cache) > MAX_CACHE:
                    _cache.popitem(last=False)
                maps = [key for key in _cache if key[0] == 'snapshot']
                for key in maps[:-3]:
                    _cache.pop(key, None)
                return data
        result = await asyncio.wait_for(bounded(), timeout=25 if operation is opening_map.snapshot else 10)
    except (asyncio.TimeoutError, Neo4jError, ServiceUnavailable, SessionExpired, ValueError, OSError):
        raise HTTPException(503, 'The graph explorer is unavailable or this query exceeded its time limit. Try again.') from None
    if result is None:
        raise HTTPException(404, 'This position is not in the imported graph.')
    return result, round((perf_counter() - started) * 1000, 1)


PLAYER = Query('', max_length=64, pattern=r'^[A-Za-z0-9_.-]*$')
COLOR = Query('all', pattern=r'^(all|white|black)$')
KEY = Query(..., max_length=40, pattern=r'^chess:(initial|[0-9]+)$')


def selected_ids(value):
    try:
        ids=list(dict.fromkeys(int(p) for p in value.split(',') if p))
        if len(ids)>8 or any(p<1 or p>9223372036854775807 for p in ids): raise ValueError()
        return ids
    except ValueError:
        raise HTTPException(422,'Choose at most eight imported players.') from None


@router.get('/ply-map')
async def ply_map(snapshot: uuid.UUID | None = None, first: int = Query(0,ge=0,le=31),
                  last: int = Query(6,ge=0,le=31), offset: int = Query(0,ge=0,le=200000),
                  players: str = Query('',max_length=170)):
    if first>last: raise HTTPException(422,'The first ply must not follow the last ply.')
    ids=selected_ids(players)
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            state,saved=await ply_map_reader.snapshot(session,snapshot)
            if saved is None: return dict(status=state,snapshot=None,nodes=[],connections=[])
            return dict(status=state,snapshot=saved,**await ply_map_reader.view(session,saved['id'],first,last,offset,ids))
    except ValueError as error:
        raise HTTPException(409,str(error)) from None


@router.get('/ply-map/players')
async def ply_players(snapshot: uuid.UUID, search: str = Query('',max_length=64)):
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        return {'rows':await ply_map_reader.players(session,snapshot,search)}


@router.get('/ply-map/detail')
async def ply_detail(snapshot: uuid.UUID, key: str = Query(...,min_length=1,max_length=256),
                     offset: int = Query(0,ge=0,le=200000), players: str = Query('',max_length=170)):
    ids=selected_ids(players)
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await ply_map_reader.snapshot(session,snapshot)
            data=await ply_map_reader.detail(session,snapshot,key,offset,ids)
        from chessism_api.database.neo4j.ply_map_source import expand
        data['rows']=await asyncio.wait_for(expand(data['rows']),timeout=12)
        return data
    except ValueError as error:
        raise HTTPException(409,str(error)) from None
    except (asyncio.TimeoutError,Neo4jError,ServiceUnavailable,SessionExpired,OSError):
        raise HTTPException(503,'Could not load these saved paths. The map snapshot is still available; try again.') from None


@router.get('/map')
async def opening_snapshot(request: Request,
                           a: str = Query('lafareto', min_length=1, max_length=64, pattern=r'^[A-Za-z0-9_.-]+$'),
                           b: str = Query('hikaru', min_length=1, max_length=64, pattern=r'^[A-Za-z0-9_.-]+$'),
                           refresh: bool = False):
    identity = ('snapshot', a.lower(), b.lower())
    saved = _cache.get(identity)
    # Explicit refresh can catch a completed import, but cannot hammer the graph.
    if refresh and saved and saved[0] - monotonic() < 285:
        _cache.pop(identity, None)
    data, ms = await query(request, opening_map.snapshot, a.lower(), b.lower())
    return {**data, 'query_ms': ms, 'cache_seconds': 300}


@router.get('/players')
async def players(request: Request, search: str = Query('', max_length=64)):
    data, ms = await query(request, explorer.players, search.strip())
    return {'rows': data, 'query_ms': ms}


@router.get('/positions')
async def positions(request: Request, player: str = PLAYER, color: str = COLOR,
                    offset: int = Query(0, ge=0, le=990)):
    data, ms = await query(request, explorer.ranking, player.lower(), color if player else 'all')
    # Reuse one bounded ranking for pagination, rather than rescan each page.
    return {**data, 'rows': data['rows'][offset:offset + 10], 'offset': offset,
            'has_more': offset + 10 < min(1000, len(data['rows'])),
            'limited': len(data['rows']) > 1000, 'query_ms': ms, 'cache_seconds': TTL}


@router.get('/position')
async def position(request: Request, key: str = KEY, player: str = PLAYER, color: str = COLOR,
                   incoming: int = Query(0, ge=0, le=996), outgoing: int = Query(0, ge=0, le=996),
                   other: str = PLAYER, max_ply: int = Query(0, ge=0, le=30)):
    data, ms = await query(request, explorer.neighborhood, key, player.lower(),
                           color if player else 'all', incoming, outgoing, other.lower(), max_ply)
    return {**data, 'query_ms': ms}


@router.get('/games')
async def games(request: Request, key: str = KEY, player: str = PLAYER, color: str = COLOR,
                move_key: str = Query('', max_length=64, pattern=r'^([a-f0-9]{64})?$'),
                offset: int = Query(0, ge=0, le=9990), other: str = PLAYER,
                max_ply: int = Query(0, ge=0, le=30)):
    data, ms = await query(request, explorer.games, key, player.lower(), color if player else 'all', move_key, offset,
                           other.lower(), max_ply)
    return {**data, 'query_ms': ms}
