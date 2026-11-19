"""Opponent/opening catalog policy over canonical game metadata."""
import asyncio
import hashlib
import json
import logging

from redis.exceptions import RedisError
from sqlalchemy.exc import DBAPIError

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.database.postgresql import player_catalog_reads as storage
from chessism_api.operations.player_hero_analytics import AnalyticsScope, VALID_MODES
from chessism_api.operations.player_timezone import resolve_player_timezone
from chessism_api.operations.player_catalog_snapshot import catalog_min_games, catalog_ratio_bin, catalog_snapshot_page

CACHE_SECONDS = 60
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)
RATING_MODES = ('bullet', 'blitz', 'rapid')


class GameCatalogBusy(RuntimeError):
    pass


async def cached_page(redis, key):
    try:
        raw = await redis.get(key)
        if raw:
            payload = json.loads(raw)
            if isinstance(payload, dict) and payload.get('version') == 1:
                return payload
    except (RedisError, ValueError, TypeError):
        LOG.warning('Player game catalog cache unavailable; reading game metadata.')
    return None


async def get_player_game_catalog(player_name, redis, *, kind, mode='all', color='all', page=1, limit=10, search='', sort_by='games', ratio_bin=None, session_factory=None):
    if kind not in ('opponents', 'openings'):
        raise ValueError('Choose opponents or openings.')
    if color not in ('all', 'white', 'black'):
        raise ValueError('Color must be all, white or black.')
    ratio_bin = catalog_ratio_bin(sort_by, ratio_bin)
    if not 1 <= page <= 100000 or not 1 <= limit <= 50:
        raise ValueError('Invalid catalog page or page size.')
    if not isinstance(search, str) or len(search) > 128:
        raise ValueError('Search must contain at most 128 characters.')
    player = str(player_name or '').strip().lower()
    mode = str(mode or 'all').strip().lower()
    if not player:
        raise ValueError('A player name is required.')
    if mode not in VALID_MODES:
        raise ValueError('Mode must be all, bullet, blitz or rapid.')
    search = search.strip().lower()
    try:
        async with read_transaction(session_factory or AsyncDBSession) as session:
            await storage.read_limits(session)
            profile = await storage.active_profile(session, player)
            if not profile:
                raise LookupError(f'Player {player!r} was not found.')
            scope = AnalyticsScope(player, mode, None, None, resolve_player_timezone(dict(profile)))
            saved = await catalog_snapshot_page(session, redis, scope, kind=kind, mode=mode,
                color=color, search=search, page=page, limit=limit, sort_by=sort_by, ratio_bin=ratio_bin)
            if saved is not None:
                return saved
            key = 'chessism:player-game-catalog:v11:' + hashlib.sha256(json.dumps(
                [profile['id'], player, kind, mode, color, page, limit, search, sort_by, ratio_bin]).encode()).hexdigest()
            cached = await cached_page(redis, key)
            if cached is not None:
                return cached
            try:
                await asyncio.wait_for(READ_SLOTS.acquire(), timeout=2)
            except asyncio.TimeoutError as error:
                raise GameCatalogBusy('Game summaries are busy. Please retry shortly.') from error
            try:
                cached = await cached_page(redis, key)
                if cached is not None:
                    return cached
                records = await storage.catalog_page(session, player_id=profile['id'], kind=kind, mode=mode,
                    color=color, min_games=catalog_min_games(kind,search), sort_by=sort_by, ratio_bin=ratio_bin,
                    search=search, limit=limit, offset=(page-1)*limit)
                latest = await storage.latest_opponent_ratings(session,
                    [row['entry_key'] for row in records if row['entry_key']], mode) if kind == 'opponents' else {}
            finally:
                READ_SLOTS.release()
        rows = []
        for record in records:
            if record['entry_key'] is None:
                continue
            row = {name:int(record[name]) for name in ('rank','games','wins','draws','losses')}
            row['key'] = record['entry_key']
            row['unknown'] = row['games']-row['wins']-row['draws']-row['losses']
            if kind == 'opponents':
                row['latest_ratings'] = latest.get(row['key'],dict.fromkeys(RATING_MODES if mode=='all' else [mode]))
            rows.append(row)
        groups = int(records[0]['groups'])
        payload = {'version':1, **scope.response_base(), 'kind':kind, 'color':color, 'search':search,
            'sort_by':sort_by, 'ratio_bin':ratio_bin, 'rows':rows, 'total_games':int(records[0]['total_games']),
            'max_age_seconds':CACHE_SECONDS, 'pagination':{'page':page, 'limit':limit, 'total_groups':groups,
                'total_pages':(groups+limit-1)//limit, 'has_more':page*limit<groups}}
        try:
            await redis.set(key,json.dumps(payload),ex=CACHE_SECONDS)
        except RedisError:
            LOG.warning('Could not cache player game catalog page.')
        return payload
    except DBAPIError as error:
        if getattr(error.orig,'sqlstate',None) in ('57014','55P03'):
            raise GameCatalogBusy('Game summary took too long. Please retry shortly.') from error
        raise
