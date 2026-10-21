"""Recorded resignation/timeout outcomes versus the exact final board evaluation.

One indexed move/position lookup per relevant game, never a scan of all plies
or a fallback to an earlier score. No engine work or new persistent tables.
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
THRESHOLD_CP = 200
ENDINGS = {
    'lost_on_time': 'Lost on time',
    'won_on_time': 'Won on time',
    'lost_by_resignation': 'Lost by resignation',
    'won_by_resignation': 'Won by resignation',
}
SCORE_BANDS = {
    'ahead': 'Ahead',
    'balanced': 'Balanced',
    'behind': 'Behind',
}
# Accept selection names from the previous four-card UI at the fixed boundary.
LEGACY_SELECTIONS = {
    'resigned_while_winning': ('lost_by_resignation', 'ahead'),
    'timed_out_while_winning': ('lost_on_time', 'ahead'),
    'won_on_time_while_losing': ('won_on_time', 'behind'),
    'won_by_resignation_while_losing': ('won_by_resignation', 'behind'),
}
COUNTS = ('eligible_games', 'assessable_games', 'ahead_games', 'balanced_games', 'behind_games', 'missing_final_position',
          'missing_final_score', 'mate_games', 'tablebase_games')
CACHE_SECONDS = 60
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)


class UnexpectedEndingsBusy(RuntimeError):
    pass


@asynccontextmanager
async def read_slot():
    try:
        await asyncio.wait_for(READ_SLOTS.acquire(), timeout=2)
    except asyncio.TimeoutError as error:
        raise UnexpectedEndingsBusy('Final-position statistics are busy. Please retry shortly.') from error
    try:
        yield
    except DBAPIError as error:
        if getattr(error.orig, 'sqlstate', None) in ('57014', '55P03'):
            raise UnexpectedEndingsBusy('Reading final positions took too long. Please retry shortly.') from error
        raise
    finally:
        READ_SLOTS.release()


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='15s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text('SET LOCAL jit=off'))


def validate_threshold(threshold_cp):
    if type(threshold_cp) is not int or threshold_cp != THRESHOLD_CP:
        raise ValueError('Unexpected endings uses a fixed threshold of 200 centipawns.')


def ending_rows_sql(session):
    """The last stored move must agree with the game's recorded move count.

    An absent/ambiguous association, incomplete move record or missing score is
    missing coverage, not a balanced position. Mate/tablebase remain separate.
    """
    layout = storage_for(session)
    table = layout.table if layout else lambda name: name
    if layout:
        identities = f"""SELECT gp.game_link, gp.is_white, g.played_at, m.name AS mode,
            g.n_moves, g.white_result, g.black_result, g.white_str_result, g.black_str_result
            FROM {table('game_player')} gp JOIN {table('game')} g ON g.link=gp.game_link
            JOIN {table('game_mode')} m ON m.id=g.mode_id
            WHERE gp.player_id=(SELECT id FROM {table('player')} WHERE player_name=:player)
              AND m.name=ANY(CAST(:modes AS text[]))"""
    else:
        identities = """SELECT gp.link AS game_link, gp.color='white' AS is_white,
            g.played_at, gp.mode, g.n_moves, g.white_result, g.black_result,
            g.white_str_result, g.black_str_result
            FROM game_player gp JOIN game g ON g.link=gp.link
            WHERE gp.player_name=:player AND gp.mode=ANY(CAST(:modes AS text[]))"""
    side = 'a.is_white' if layout and layout.compact else "(a.move_color='white')"
    return f"""WITH identities AS ({identities}), outcomes AS (
        SELECT *, CASE WHEN is_white THEN white_result ELSE black_result END AS result,
            CASE WHEN is_white THEN black_result ELSE white_result END AS opponent_result,
            LOWER(BTRIM(CASE WHEN is_white THEN white_str_result ELSE black_str_result END)) AS own_reason,
            LOWER(BTRIM(CASE WHEN is_white THEN black_str_result ELSE white_str_result END)) AS opponent_reason
        FROM identities
    ), categories AS (
        SELECT *, CASE
            WHEN result=0 AND opponent_result=1 AND own_reason IN ('resigned','resignation') THEN 'lost_by_resignation'
            WHEN result=0 AND opponent_result=1 AND own_reason='timeout' THEN 'lost_on_time'
            WHEN result=1 AND opponent_result=0 AND opponent_reason='timeout' THEN 'won_on_time'
            WHEN result=1 AND opponent_result=0 AND opponent_reason IN ('resigned','resignation') THEN 'won_by_resignation'
            END AS ending
        FROM outcomes
    ), candidates AS MATERIALIZED (
        SELECT * FROM categories WHERE ending IS NOT NULL
          AND (CAST(:ending AS text) IS NULL OR ending=:ending)
    ), evaluated AS (
        SELECT c.game_link, CASE WHEN c.is_white THEN 'white' ELSE 'black' END AS player_color,
            c.played_at, c.mode, c.result, c.ending,
            CASE WHEN c.is_white THEN f.score ELSE -f.score END AS player_cp,
            CASE WHEN f.fen_id IS NULL THEN 'missing_final_position'
                WHEN f.analysis_source='tablebase' OR f.tablebase_wdl IS NOT NULL THEN 'tablebase'
                WHEN f.score IS NULL THEN 'missing_final_score'
                WHEN ABS(f.score)>=9000 THEN 'mate' ELSE 'cp' END AS score_kind
        FROM candidates c
        LEFT JOIN LATERAL (
            SELECT m.n_move, CASE
                WHEN LOWER(BTRIM(m.white_move)) NOT IN ('','--','none','null') THEN
                    CASE WHEN LOWER(BTRIM(m.black_move)) NOT IN ('','--','none','null') THEN FALSE
                         WHEN BTRIM(m.black_move)='--' THEN TRUE END
                END AS is_white
            FROM {table('moves')} m WHERE m.link=c.game_link ORDER BY m.n_move DESC LIMIT 1
        ) last_move ON last_move.n_move=c.n_moves AND c.n_moves>0
        LEFT JOIN LATERAL (
            SELECT MAX(a.fen_id) AS fen_id FROM {table('game_fen_association')} a
            WHERE a.game_link=c.game_link AND a.n_move=last_move.n_move AND {side}=last_move.is_white
            HAVING COUNT(*)=1
        ) final_position ON TRUE
        LEFT JOIN {table('fen')} f ON f.fen_id=final_position.fen_id
    ), classified AS (
        SELECT *, CASE WHEN score_kind='cp' THEN
            CASE WHEN player_cp>:threshold_cp THEN 'ahead'
                 WHEN player_cp < -:threshold_cp THEN 'behind'
                 ELSE 'balanced' END END AS score_band
        FROM evaluated
    )"""


async def get_unexpected_endings(player_name, redis, *, threshold_cp=200):
    validate_threshold(threshold_cp)
    async with read_slot(), AsyncDBSession() as session:
        await prepare(session)
        scope = await _analytics_scope(session, player_name)
        key = 'chessism:unexpected-endings:v2:' + hashlib.sha256(
            json.dumps([scope.player, threshold_cp]).encode()).hexdigest()
        try:
            raw = await redis.get(key)
            if raw:
                payload = json.loads(raw)
                if payload.get('version') == 2 and isinstance(payload.get('by_mode'), dict):
                    return {**payload, 'cache_hit': True}
        except (RedisError, ValueError, TypeError, AttributeError):
            LOG.warning('Unexpected-endings cache unavailable; reading final positions.')
        query = ending_rows_sql(session) + """
            SELECT mode, ending, COUNT(*)::bigint AS eligible_games,
                COUNT(*) FILTER (WHERE score_kind='cp')::bigint AS assessable_games,
                COUNT(*) FILTER (WHERE score_band='ahead')::bigint AS ahead_games,
                COUNT(*) FILTER (WHERE score_band='balanced')::bigint AS balanced_games,
                COUNT(*) FILTER (WHERE score_band='behind')::bigint AS behind_games,
                COUNT(*) FILTER (WHERE score_kind='missing_final_position')::bigint AS missing_final_position,
                COUNT(*) FILTER (WHERE score_kind='missing_final_score')::bigint AS missing_final_score,
                COUNT(*) FILTER (WHERE score_kind='mate')::bigint AS mate_games,
                COUNT(*) FILTER (WHERE score_kind='tablebase')::bigint AS tablebase_games
            FROM classified GROUP BY mode, ending"""
        rows = (await session.execute(text(query), {'player': scope.player, 'modes': list(MODES),
            'threshold_cp': threshold_cp, 'ending': None})).mappings().all()
    by_mode = {mode: {ending: dict.fromkeys(COUNTS, 0) for ending in ENDINGS} for mode in MODES}
    for row in rows:
        by_mode[row['mode']][row['ending']] = {key: int(row[key]) for key in COUNTS}
    payload = {
        'version': 2, 'player_name': scope.player, 'threshold_cp': threshold_cp,
        'score_perspective': 'player', 'score_source': 'final_recorded_position',
        'percentage_denominator': 'assessable_games_with_this_result_and_ending',
        'special_scores': 'mate_and_tablebase_excluded', 'fully_analyzed_only': False,
        'endings': [{'id': key, 'label': label} for key, label in ENDINGS.items()],
        'score_bands': [
            {'id': 'ahead', 'label': 'Ahead', 'lower': THRESHOLD_CP, 'upper': None, 'lower_inclusive': False},
            {'id': 'balanced', 'label': 'Balanced', 'lower': -THRESHOLD_CP, 'upper': THRESHOLD_CP,
             'lower_inclusive': True, 'upper_inclusive': True},
            {'id': 'behind', 'label': 'Behind', 'lower': None, 'upper': -THRESHOLD_CP, 'upper_inclusive': False},
        ],
        'by_mode': by_mode, 'generated_at': datetime.now(timezone.utc).isoformat(),
        'cache_hit': False, 'max_age_seconds': CACHE_SECONDS,
    }
    try:
        await redis.set(key, json.dumps(payload), ex=CACHE_SECONDS)
    except RedisError:
        LOG.warning('Could not cache unexpected endings.')
    return payload


async def explore_unexpected_endings(player_name, *, modes, ending, threshold_cp,
                                     timezone_name, limit, cursor, score_band=None):
    validate_threshold(threshold_cp)
    if ending in LEGACY_SELECTIONS:
        ending, legacy_band = LEGACY_SELECTIONS[ending]
        if score_band not in (None, legacy_band):
            raise ValueError('The score band conflicts with the selected ending.')
        score_band = legacy_band
    if ending not in ENDINGS or not modes or set(modes) - set(MODES) or not 1 <= limit <= 100:
        raise ValueError('Choose a supported ending, game modes and limit between 1 and 100.')
    if score_band not in SCORE_BANDS:
        raise ValueError('Choose ahead, balanced or behind for score_band.')
    async with read_slot(), AsyncDBSession() as session:
        await prepare(session)
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        fingerprint = hashlib.sha256(json.dumps([
            scope.player, 'unexpected_endings:v2', sorted(set(modes)), ending, score_band, threshold_cp]).encode()).hexdigest()
        cursor_time, cursor_id, cursor_color = decode_cursor(cursor, fingerprint)
        layout = storage_for(session)
        game = layout.table('game') if layout else 'game'
        opponent_join = (f"""LEFT JOIN {layout.table('player')} opponent ON opponent.id=
            CASE WHEN selected.player_color='white' THEN g.black_player_id ELSE g.white_player_id END"""
            if layout else '')
        opponent_name = 'opponent.player_name' if layout else "CASE WHEN selected.player_color='white' THEN g.black ELSE g.white END"
        query = ending_rows_sql(session) + f""", selected AS MATERIALIZED (
            SELECT *, COALESCE(played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00') AS cursor_time
            FROM classified WHERE score_band=:score_band AND (CAST(:cursor_time AS timestamptz) IS NULL OR
                (COALESCE(played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00'),game_link,player_color) <
                (CAST(:cursor_time AS timestamptz),CAST(:cursor_id AS bigint),CAST(:cursor_color AS text)))
            ORDER BY cursor_time DESC, game_link DESC, player_color DESC LIMIT :limit
        ) SELECT selected.*, {opponent_name} AS opponent_name,
            CASE WHEN selected.player_color='white' THEN g.white_elo ELSE g.black_elo END AS player_rating,
            CASE WHEN selected.player_color='white' THEN g.black_elo ELSE g.white_elo END AS opponent_rating
        FROM selected JOIN {game} g ON g.link=selected.game_link {opponent_join}
        ORDER BY selected.cursor_time DESC, selected.game_link DESC, selected.player_color DESC"""
        rows = (await session.execute(text(query), {
            'player': scope.player, 'modes': list(modes), 'ending': ending, 'threshold_cp': threshold_cp,
            'score_band': score_band,
            'cursor_time': cursor_time, 'cursor_id': cursor_id, 'cursor_color': cursor_color, 'limit': limit+1,
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
            'opponent_rating': row['opponent_rating'], 'result': 'win' if row['result'] == 1 else 'loss',
            'final_player_cp': int(row['player_cp']),
        })
    next_cursor = None
    if has_more and selected:
        last = selected[-1]
        data = [fingerprint, last['cursor_time'].isoformat(), int(last['game_link']), last['player_color']]
        next_cursor = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=')
    return {
        **scope.response_base(), 'selection': {'scope': 'unexpected_endings', 'modes': list(modes),
            'ending': ending, 'score_band': score_band, 'threshold_cp': threshold_cp,
            'label': f'{ENDINGS[ending]} · {SCORE_BANDS[score_band]}',
            'analyzed_only': False, 'score_source': 'final_recorded_position'},
        'summary': None, 'games': games,
        'pagination': {'limit': limit, 'returned_games': len(games), 'has_more': has_more, 'next_cursor': next_cursor},
    }
