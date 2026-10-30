"""Small histograms of saved game accuracy/blunders; no calculations on a read.

The histogram and game explorer share the same eligibility and bin boundaries.
Read only normalized player/game identities, coverage and engine summaries.
"""
import asyncio
import base64
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.player_chart_games import decode_cursor
from chessism_api.operations.player_hero_analytics import _analytics_scope

MODES = ('bullet', 'blitz', 'rapid')
CACHE_SECONDS = 60
BIN_WIDTH = 5
BIN_COUNT = 20
BLUNDER_BIN_COUNT = 7
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)


class AccuracyDistributionBusy(RuntimeError):
    pass


def accuracy_bins():
    return [{'id': index, 'lower': index * BIN_WIDTH, 'upper': (index + 1) * BIN_WIDTH,
             'upper_inclusive': index == BIN_COUNT - 1} for index in range(BIN_COUNT)]


def accuracy_bin_filter(bin_id):
    if type(bin_id) is not int or not 0 <= bin_id < BIN_COUNT:
        raise ValueError('accuracy_bin must be an integer between 0 and 19.')
    upper_operator = '<=' if bin_id == BIN_COUNT - 1 else '<'
    return f'accuracy >= :accuracy_lower AND accuracy {upper_operator} :accuracy_upper', {
        'accuracy_lower': bin_id * BIN_WIDTH, 'accuracy_upper': (bin_id + 1) * BIN_WIDTH,
    }


def blunder_bins():
    return [{'id': index, 'lower': index, 'upper': index if index < 6 else None,
             'label': str(index) if index < 6 else '6+'} for index in range(BLUNDER_BIN_COUNT)]


def blunder_bin_filter(bin_id):
    if type(bin_id) is not int or not 0 <= bin_id < BLUNDER_BIN_COUNT:
        raise ValueError('blunder_bin must be an integer between 0 and 6 (6 means 6+).')
    operator = '>=' if bin_id == 6 else '='
    return f'blunder_count {operator} :blunder_count', {'blunder_count': bin_id}


def summary_rows_sql(session, metric):
    """One saved result per recorded game/player side, with no FEN reads."""
    if metric not in ('accuracy', 'blunders'):
        raise ValueError('Unsupported saved-summary metric.')
    # Never treat a missing summary or a player with no measured moves as zero.
    eligibility = ('engine.blunder_count >= 0 AND engine.analyzed_player_moves > 0'
                   if metric == 'blunders' else 'engine.game_efficiency BETWEEN 0 AND 100')
    accuracy = 'CASE WHEN engine.game_efficiency BETWEEN 0 AND 100 THEN engine.game_efficiency END'
    layout = storage_for(session)
    if layout:
        from .canonical_blunders.readers import engine_table
        return f"""SELECT gp.game_link, CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS player_color,
            g.played_at, m.name AS mode, {accuracy} AS accuracy, engine.blunder_count
            FROM {layout.table('game_player')} gp
            JOIN {engine_table(layout)} engine ON engine.game_player_id=gp.id
            JOIN {layout.table('game')} g ON g.link=gp.game_link
            JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
            JOIN {layout.table('game_analysis_coverage')} coverage ON coverage.link=gp.game_link
            WHERE gp.player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player)
              AND m.name=ANY(CAST(:modes AS text[]))
              AND coverage.is_fully_analyzed AND coverage.total_positions>0
              AND {eligibility}"""
    # Legacy layouts have no canonical CUDA results. They must never serve old
    # CPU counts, including through explorer/filter paths.
    if metric == 'blunders': eligibility = 'false'
    return f"""SELECT gp.link AS game_link, gp.color AS player_color, gp.played_at, gp.mode,
        {accuracy} AS accuracy, NULL::integer AS blunder_count
        FROM game_player gp
        JOIN game_player_engine_summary engine ON engine.game_link=gp.link AND engine.player_color=gp.color
        JOIN game_analysis_summary coverage ON coverage.link=gp.link
        WHERE gp.player_name=:player AND gp.mode=ANY(CAST(:modes AS text[]))
          AND coverage.is_fully_analyzed AND coverage.total_positions>0
          AND {eligibility}"""


@asynccontextmanager
async def read_slot():
    try:
        await asyncio.wait_for(READ_SLOTS.acquire(), timeout=2)
    except asyncio.TimeoutError as error:
        raise AccuracyDistributionBusy('Saved game summaries are busy. Please retry shortly.') from error
    try:
        yield
    except DBAPIError as error:
        if getattr(error.orig, 'sqlstate', None) in ('57014', '55P03'):
            raise AccuracyDistributionBusy('Reading saved game summaries took too long. Please retry shortly.') from error
        raise
    finally:
        READ_SLOTS.release()


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='8s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))


async def cached_distribution(redis, key, metric, bin_count):
    try:
        raw = await redis.get(key)
        if raw:
            payload = json.loads(raw)
            if (payload.get('version') == 1 and payload.get('metric', 'accuracy') == metric
                    and isinstance(payload.get('by_mode'), dict)
                    and all(len(payload['by_mode'].get(mode, {}).get('counts', [])) == bin_count for mode in MODES)):
                return {**payload, 'cache_hit': True}
    except (RedisError, ValueError, TypeError, AttributeError):
        LOG.warning('Game histogram cache unavailable; reading saved summaries.')
    return None


async def get_accuracy_distribution(player_name, redis):
    return await get_summary_distribution(player_name, redis, metric='accuracy')


async def get_blunder_distribution(player_name, redis):
    from .canonical_blunders import readers
    player=str(player_name or '').strip().lower()
    if not player or len(player)>128:
        raise ValueError('A player name of 1–128 characters is required.')
    async with read_slot(), AsyncDBSession() as session:
        await prepare(session)
        return await readers.saved(session,player)


async def get_summary_distribution(player_name, redis, *, metric):
    if metric not in ('accuracy', 'blunders'):
        raise ValueError('Unsupported saved-summary metric.')
    if metric == 'blunders':
        return await get_blunder_distribution(player_name,redis)
    bins = blunder_bins() if metric == 'blunders' else accuracy_bins()
    bin_sql = 'LEAST(6, blunder_count)' if metric == 'blunders' else 'LEAST(19, FLOOR(accuracy / 5)::int)'
    player = str(player_name or '').strip().lower()
    if not player or len(player) > 128:
        raise ValueError('A player name of 1–128 characters is required.')
    async with read_slot():
        # Validate current ownership/existence even when a cached histogram exists.
        async with AsyncDBSession() as session:
            await prepare(session)
            exists = await session.scalar(text('''SELECT player_name FROM player
                WHERE player_name=:player AND deleted_at IS NULL'''), {'player': player})
        if not exists:
            raise LookupError(f'Player {player!r} was not found.')
        key = f'chessism:{metric}-distribution:v1:' + hashlib.sha256(player.encode()).hexdigest()
        cached = await cached_distribution(redis, key, metric, len(bins))
        if cached is not None:
            return cached
        async with AsyncDBSession() as session:
            await prepare(session)
            rows = (await session.execute(text(f"""WITH eligible AS ({summary_rows_sql(session, metric)})
                SELECT mode, {bin_sql} AS bin_id, COUNT(*)::bigint AS games
                FROM eligible GROUP BY mode, bin_id ORDER BY mode, bin_id"""),
                {'player': player, 'modes': list(MODES)})).mappings().all()
        by_mode = {mode: {'counts': [0] * len(bins), 'games_count': 0} for mode in MODES}
        for row in rows:
            item = by_mode[row['mode']]
            item['counts'][int(row['bin_id'])] = int(row['games'])
            item['games_count'] += int(row['games'])
        payload = {
            'version': 1, 'player_name': player, 'metric': metric, 'bins': bins,
            **({'bin_width': BIN_WIDTH} if metric == 'accuracy' else {}),
            'by_mode': by_mode, 'generated_at': datetime.now(timezone.utc).isoformat(),
            'source': f'saved_game_{metric}', 'fully_analyzed_only': True,
            'count_unit': 'game_player_side', 'cache_hit': False, 'max_age_seconds': CACHE_SECONDS,
        }
        try:
            await redis.set(key, json.dumps(payload), ex=CACHE_SECONDS)
        except RedisError:
            LOG.warning('Could not cache the saved game histogram.')
        return payload


async def explore_summary_games(player_name, *, metric, modes, bin_id, timezone_name, limit, cursor):
    if metric not in ('accuracy', 'blunders'):
        raise ValueError('Unsupported saved-summary metric.')
    clause, bounds = blunder_bin_filter(bin_id) if metric == 'blunders' else accuracy_bin_filter(bin_id)
    if not modes or set(modes) - set(MODES) or not 1 <= limit <= 100:
        raise ValueError('Choose valid game modes and a limit between 1 and 100.')
    async with read_slot(), AsyncDBSession() as session:
        await prepare(session)
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        from .canonical_blunders.readers import current
        canonical=await current(session)
        fingerprint = hashlib.sha256(json.dumps([scope.player, metric, sorted(set(modes)), bin_id,
            canonical['active_model'] if canonical else None]).encode()).hexdigest()
        cursor_time, cursor_id, cursor_color = decode_cursor(cursor, fingerprint)
        layout = storage_for(session)
        game_table = layout.table('game') if layout else 'game'
        opponent_join = (f"""LEFT JOIN {layout.table('player')} opponent ON opponent.id=
            CASE WHEN selected.player_color='white' THEN g.black_player_id ELSE g.white_player_id END"""
            if layout else '')
        opponent_name = 'opponent.player_name' if layout else "CASE WHEN selected.player_color='white' THEN g.black ELSE g.white END"
        from .player_blunder_page import blunder_rows_sql
        eligible = blunder_rows_sql(session, clause) if metric == 'blunders' else None
        query = f"""WITH eligible AS ({eligible or summary_rows_sql(session, metric)}), selected AS MATERIALIZED (
            SELECT *, COALESCE(played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00') AS cursor_time
            FROM eligible WHERE {clause}
              AND (CAST(:cursor_time AS timestamptz) IS NULL OR
                   (COALESCE(played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00'), game_link, player_color) <
                   (CAST(:cursor_time AS timestamptz), CAST(:cursor_id AS bigint), CAST(:cursor_color AS text)))
            ORDER BY cursor_time DESC, game_link DESC, player_color DESC LIMIT :limit
        )
        SELECT selected.*, {opponent_name} AS opponent_name,
            CASE WHEN selected.player_color='white' THEN g.white_elo ELSE g.black_elo END AS player_rating,
            CASE WHEN selected.player_color='white' THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
            CASE WHEN selected.player_color='white' THEN g.white_result ELSE g.black_result END AS result
        FROM selected JOIN {game_table} g ON g.link=selected.game_link {opponent_join}
        ORDER BY selected.cursor_time DESC, selected.game_link DESC, selected.player_color DESC"""
        rows = [] if metric == 'blunders' and (not canonical or not canonical['active_model']) else (await session.execute(text(query), {
            'player': scope.player, 'modes': list(modes), **bounds,
            'model': canonical['active_model'] if canonical else None,
            'cursor_time': cursor_time, 'cursor_id': cursor_id, 'cursor_color': cursor_color, 'limit': limit + 1,
        })).mappings().all()
    selected, has_more = rows[:limit], len(rows) > limit
    games = []
    for row in selected:
        played_at = row['played_at']
        local_time = scope.timezone.local_datetime(played_at) if played_at else None
        games.append({
            'game_id': int(row['game_link']), 'player_color': row['player_color'], 'mode': row['mode'],
            'played_at_utc': played_at.astimezone(timezone.utc).isoformat() if played_at else None,
            'played_at_local': local_time.isoformat() if local_time else None,
            'opponent_name': row['opponent_name'] or 'Unknown', 'player_rating': row['player_rating'],
            'opponent_rating': row['opponent_rating'],
            'accuracy': round(float(row['accuracy']), 2) if row['accuracy'] is not None else None,
            'blunder_count': row['blunder_count'],
            'result': {1: 'win', 0.5: 'draw', 0: 'loss'}.get(row['result'], 'unknown'),
        })
    next_cursor = None
    if has_more and selected:
        last = selected[-1]
        data = [fingerprint, last['cursor_time'].isoformat(), int(last['game_link']), last['player_color']]
        next_cursor = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=')
    bin_metadata = ({'blunder_bin': bin_id, 'blunders_from': bin_id, 'blunders_to': bin_id if bin_id < 6 else None}
                    if metric == 'blunders' else {'accuracy_bin': bin_id, 'accuracy_from': bounds['accuracy_lower'],
                        'accuracy_to': bounds['accuracy_upper'], 'upper_inclusive': bin_id == BIN_COUNT - 1})
    return {
        **scope.response_base(), 'selection': {'scope': metric, 'modes': list(modes),
            'analyzed_only': True, **bin_metadata},
        'summary': None, 'games': games,
        'pagination': {'limit': limit, 'returned_games': len(games), 'has_more': has_more, 'next_cursor': next_cursor},
    }
