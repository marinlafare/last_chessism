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
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.player_hero_analytics import _analytics_scope
from chessism_api.operations.player_catalog_snapshot import CATALOG_MIN_GAMES, catalog_ratio_bin, catalog_snapshot_page

CACHE_SECONDS = 60
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)
# Normalized storage contains the slug; legacy reads may contain a full URL.
# Use exactly this expression for both grouping and game-list membership.
OPENING_KEY_SQL = r"""COALESCE(NULLIF(REGEXP_REPLACE(BTRIM(COALESCE(gp.eco, '')),
    '^https?://(www\.)?chess\.com/openings/', '', 'i'), 'no_eco'), '')"""
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


async def _latest_opponent_ratings(session, opponents, mode):
    """Enrich only the visible page, in one read-only batch, not one HTTP call per row."""
    if not opponents:
        return {}
    modes = list(RATING_MODES) if mode == 'all' else [mode]
    layout = storage_for(session)
    if layout is None:
        query = text("""
            SELECT p.player_name, m.mode, latest.rating
            FROM player p CROSS JOIN unnest(CAST(:modes AS text[])) AS m(mode)
            LEFT JOIN LATERAL (
                SELECT gp.rating FROM game_player gp
                WHERE gp.player_name=p.player_name AND gp.mode=m.mode
                    AND gp.played_at IS NOT NULL
                ORDER BY gp.played_at DESC NULLS LAST, gp.link DESC, gp.color ASC
                LIMIT 1
            ) latest ON TRUE
            WHERE p.player_name=ANY(CAST(:opponents AS text[])) AND p.deleted_at IS NULL
        """)
    else:
        # Each side can seek the existing (player_id, mode_id, played_at, link)
        # game indexes. Compare at most two candidates per opponent/mode.
        # Preserve recorded ownership, including both sides of self-play.
        branches = []
        for side, is_white in (('white', 'true'), ('black', 'false')):
            branches.append(f"""(
                SELECT g.{side}_elo AS rating, g.played_at, g.link, {is_white} AS is_white
                FROM {layout.table('game')} g
                WHERE g.{side}_player_id=p.id AND g.mode_id=m.id
                    AND g.played_at IS NOT NULL
                    AND EXISTS (
                        SELECT 1 FROM {layout.table('game_player')} gp
                        WHERE gp.game_link=g.link AND gp.player_id=p.id AND gp.is_white={is_white}
                    )
                ORDER BY g.played_at DESC NULLS LAST, g.link DESC LIMIT 1
            )""")
        query = text(f"""
            WITH opponents AS MATERIALIZED (
                SELECT id, player_name FROM {layout.table('player')}
                WHERE player_name=ANY(CAST(:opponents AS text[])) AND deleted_at IS NULL
            ), modes AS MATERIALIZED (
                SELECT id, name FROM {layout.table('game_mode')}
                WHERE name=ANY(CAST(:modes AS text[]))
            )
            SELECT p.player_name, m.name AS mode, latest.rating
            FROM opponents p CROSS JOIN modes m
            LEFT JOIN LATERAL (
                SELECT candidates.rating FROM ({' UNION ALL '.join(branches)}) candidates
                ORDER BY candidates.played_at DESC NULLS LAST, candidates.link DESC, candidates.is_white ASC
                LIMIT 1
            ) latest ON TRUE
        """)
    records = (await session.execute(query, {'opponents': opponents, 'modes': modes})).mappings().all()
    ratings = {opponent: dict.fromkeys(modes) for opponent in opponents}
    for record in records:
        rating = record['rating']
        ratings[record['player_name']][record['mode']] = int(rating) if rating is not None and rating > 0 else None
    return ratings


async def get_player_game_catalog(player_name, redis, *, kind, mode='all', color='all', page=1, limit=10, search='', sort_by='games', ratio_bin=None):
    if kind not in ('opponents', 'openings'):
        raise ValueError('Choose opponents or openings.')
    if color not in ('all', 'white', 'black'):
        raise ValueError('Color must be all, white or black.')
    ratio_bin = catalog_ratio_bin(sort_by, ratio_bin)
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
        saved = await catalog_snapshot_page(session, redis, scope, kind=kind, mode=scope.mode,
                                            color=color, search=search, page=page, limit=limit, sort_by=sort_by, ratio_bin=ratio_bin)
        if saved is not None:
            return saved
    key = 'chessism:player-game-catalog:v9:' + hashlib.sha256(json.dumps(
        [scope.player, kind, scope.mode, color, page, limit, search, sort_by, ratio_bin]).encode()).hexdigest()
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
        # sort_by is a validated column name. Bin bounds stay bound parameters.
        bin_filter = ('' if ratio_bin is None else
                      f'WHERE {sort_by} * 10 >= :ratio_bin * games '
                      f'AND (:ratio_bin = 9 OR {sort_by} * 10 < (:ratio_bin + 1) * games)')
        order_by = ('games DESC, entry_key' if sort_by == 'games' else
                    f'{sort_by}::numeric / NULLIF(games, 0) DESC NULLS LAST, games DESC, entry_key')
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
                    HAVING COUNT(*) >= :min_games
                ), ranked AS (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_by}) AS rank
                    FROM buckets {bin_filter}
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
                'min_games': CATALOG_MIN_GAMES,
                'ratio_bin': ratio_bin,
                'limit': limit, 'offset': (page - 1) * limit, 'search': search,
            })).mappings().all()
            latest_ratings = await _latest_opponent_ratings(
                session, [record['entry_key'] for record in records if record['entry_key']], scope.mode,
            ) if kind == 'opponents' else {}
        rows = []
        for record in records:
            if record['entry_key'] is None:
                continue
            row = {name: int(record[name]) for name in ('rank', 'games', 'wins', 'draws', 'losses')}
            row['key'] = record['entry_key']
            row['unknown'] = row['games'] - row['wins'] - row['draws'] - row['losses']
            if kind == 'opponents':
                row['latest_ratings'] = latest_ratings.get(row['key'], dict.fromkeys(
                    RATING_MODES if scope.mode == 'all' else [scope.mode],
                ))
            rows.append(row)
        groups = int(records[0]['groups'])
        payload = {
            'version': 1, **scope.response_base(), 'kind': kind, 'color': color, 'search': search, 'sort_by': sort_by, 'ratio_bin': ratio_bin,
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
