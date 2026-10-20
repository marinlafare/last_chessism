"""Small, on-demand opponent/opening summaries; never scan FENs or moves."""
import asyncio
import hashlib
import json
import logging

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_reads import player_rows_sql
from chessism_api.operations.player_hero_analytics import _analytics_scope

CACHE_SECONDS = 60
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)
# Normalized storage contains the slug; legacy reads may contain a full URL.
# Use exactly this expression for both grouping and game-list membership.
OPENING_KEY_SQL = r"""COALESCE(NULLIF(REGEXP_REPLACE(BTRIM(COALESCE(gp.eco, '')),
    '^https?://(www\.)?chess\.com/openings/', '', 'i'), 'no_eco'), '')"""


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


async def get_player_game_catalog(player_name, redis, *, kind, mode='all', color='all', page=1, limit=10, search=''):
    if kind not in ('opponents', 'openings'):
        raise ValueError('Choose opponents or openings.')
    if color not in ('all', 'white', 'black'):
        raise ValueError('Color must be all, white or black.')
    if not 1 <= page <= 100000 or not 1 <= limit <= 50:
        raise ValueError('Invalid catalog page or page size.')
    if not isinstance(search, str) or len(search) > 128:
        raise ValueError('Search must contain at most 128 characters.')
    search = search.strip().lower()
    # Check deletion/existence even when a cached page is available.
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='8s'"))
        scope = await _analytics_scope(session, player_name, mode)
    key = 'chessism:player-game-catalog:v2:' + hashlib.sha256(json.dumps(
        [scope.player, kind, scope.mode, color, page, limit, search]).encode()).hexdigest()
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
        field = 'opponent_name' if kind == 'opponents' else 'eco'
        entry = "COALESCE(gp.opponent_name, '')" if kind == 'opponents' else OPENING_KEY_SQL
        search_label = ("COALESCE(NULLIF(REPLACE(entry_key, '-', ' '), ''), 'Unknown opening')"
                        if kind == 'openings' else "COALESCE(NULLIF(entry_key, ''), 'Unknown opponent')")
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='8s'"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            query = player_rows_sql(session, f"""
                WITH buckets AS MATERIALIZED (
                    SELECT {entry} AS entry_key, COUNT(*)::bigint AS games,
                        COUNT(*) FILTER (WHERE gp.result=1)::bigint AS wins,
                        COUNT(*) FILTER (WHERE gp.result=0.5)::bigint AS draws,
                        COUNT(*) FILTER (WHERE gp.result=0)::bigint AS losses
                    FROM game_player gp
                    WHERE gp.player_name=:player AND gp.mode IN ('bullet','blitz','rapid')
                        AND (:mode='all' OR gp.mode=:mode)
                        AND (:color='all' OR gp.color=:color)
                    GROUP BY {entry}
                ), ranked AS (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY games DESC, entry_key) AS rank
                    FROM buckets
                ), matching AS MATERIALIZED (
                    SELECT * FROM ranked
                    WHERE :search='' OR STRPOS(LOWER(entry_key), :search)>0
                        OR STRPOS(LOWER({search_label}), :search)>0
                ), totals AS (
                    SELECT COUNT(*)::bigint AS groups, COALESCE(SUM(games),0)::bigint AS total_games
                    FROM matching
                )
                SELECT page.*, totals.groups, totals.total_games
                FROM totals LEFT JOIN LATERAL (
                    SELECT * FROM matching ORDER BY rank LIMIT :limit OFFSET :offset
                ) page ON TRUE ORDER BY page.rank
            """, ('mode', 'color', 'result', field))
            records = (await session.execute(query, {
                'player': scope.player, 'mode': scope.mode, 'color': color,
                'limit': limit, 'offset': (page - 1) * limit, 'search': search,
            })).mappings().all()
        rows = []
        for record in records:
            if record['entry_key'] is None:
                continue
            row = {name: int(record[name]) for name in ('rank', 'games', 'wins', 'draws', 'losses')}
            row['key'] = record['entry_key']
            row['unknown'] = row['games'] - row['wins'] - row['draws'] - row['losses']
            rows.append(row)
        groups = int(records[0]['groups'])
        payload = {
            'version': 1, **scope.response_base(), 'kind': kind, 'color': color, 'search': search,
            'rows': rows, 'total_games': int(records[0]['total_games']),
            'max_age_seconds': CACHE_SECONDS,
            'pagination': {'page': page, 'limit': limit, 'total_groups': groups,
                           'total_pages': (groups + limit - 1) // limit,
                           'has_more': page * limit < groups},
        }
        try:
            await redis.set(key, json.dumps(payload), ex=CACHE_SECONDS)
        except RedisError:
            LOG.warning('Could not cache player game catalog page.')
        return payload
    except DBAPIError as error:
        if getattr(error.orig, 'sqlstate', None) in ('57014', '55P03'):
            raise GameCatalogBusy('Game summary took too long. Please retry shortly.') from error
        raise
    finally:
        READ_SLOTS.release()
