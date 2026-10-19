"""Paginated game lists for rating and outcome bars, without engine/FEN scans."""
import base64
import binascii
import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_reads import player_rows_sql
from chessism_api.operations.player_hero_analytics import _analytics_scope

RESULTS = {'win': 1, 'draw': 0.5, 'loss': 0}


def selection_filter(scope_kind, rating_from, result):
    if scope_kind == 'rating':
        if type(rating_from) is not int or not 0 <= rating_from <= 2_147_483_638 or rating_from % 10:
            raise ValueError('rating_from must be the lower edge of a 10-point rating bin.')
        if result is not None:
            raise ValueError('A rating-bin selection cannot also specify a result.')
        return ('gp.rating > 0 AND gp.rating >= :rating_from AND gp.rating < :rating_to',
                {'rating_from': rating_from, 'rating_to': rating_from + 10}, f'Rating {rating_from}–{rating_from + 9}')
    if scope_kind == 'result':
        if result not in RESULTS or rating_from is not None:
            raise ValueError('Choose win, draw or loss without a rating-bin filter.')
        # The existing Results chart counts games with a recorded start date.
        return ('gp.result = :game_result AND gp.played_at IS NOT NULL',
                {'game_result': RESULTS[result]}, {'win': 'Wins', 'draw': 'Draws', 'loss': 'Losses'}[result])
    raise ValueError('Unsupported chart selection.')


def decode_cursor(cursor, fingerprint):
    if not cursor:
        return None, None, None
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor + '=' * (-len(cursor) % 4)))
        if not isinstance(data, list) or len(data) != 4 or data[0] != fingerprint:
            raise ValueError()
        played_at = datetime.fromisoformat(data[1])
        if played_at.tzinfo is None or type(data[2]) is not int or not 0 < data[2] < 2 ** 63 or data[3] not in ('white', 'black'):
            raise ValueError()
        return played_at, data[2], data[3]
    except (ValueError, TypeError, binascii.Error, OverflowError) as error:
        raise ValueError('Invalid pagination cursor for this chart selection.') from error


async def explore_chart_games(player_name, *, scope_kind, modes, rating_from, result,
                              timezone_name, limit, cursor):
    clause, selection_params, label = selection_filter(scope_kind, rating_from, result)
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        fingerprint = hashlib.sha256(json.dumps([scope.player, scope_kind, sorted(modes), rating_from, result]).encode()).hexdigest()
        cursor_time, cursor_id, cursor_color = decode_cursor(cursor, fingerprint)
        query = player_rows_sql(session, f'''
            WITH selected AS MATERIALIZED (
                SELECT gp.link AS game_link, gp.color AS player_color, gp.played_at,
                    gp.mode, gp.rating, gp.opponent_rating, gp.result,
                    COALESCE(gp.played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00') AS cursor_time
                FROM game_player gp
                WHERE gp.player_name=:player AND gp.mode = ANY(CAST(:modes AS text[]))
                  AND ({clause})
                  AND (CAST(:cursor_time AS timestamptz) IS NULL OR
                       (COALESCE(gp.played_at, TIMESTAMPTZ '0001-01-01 00:00:00+00'), gp.link, gp.color) <
                       (CAST(:cursor_time AS timestamptz), CAST(:cursor_id AS bigint), CAST(:cursor_color AS text)))
                ORDER BY cursor_time DESC, gp.link DESC, gp.color DESC
                LIMIT :limit
            )
            SELECT selected.*, CASE WHEN selected.player_color='white' THEN g.black ELSE g.white END AS opponent_name
            FROM selected LEFT JOIN game g ON g.link=selected.game_link
            ORDER BY selected.cursor_time DESC, selected.game_link DESC, selected.player_color DESC
        ''', ('link', 'color', 'played_at', 'mode', 'rating', 'opponent_rating', 'result'))
        rows = (await session.execute(query, {
            'player': scope.player, 'modes': modes, **selection_params,
            'cursor_time': cursor_time, 'cursor_id': cursor_id, 'cursor_color': cursor_color,
            'limit': limit + 1,
        })).mappings().all()
    has_more = len(rows) > limit
    selected = rows[:limit]
    games = []
    for row in selected:
        played_at = row['played_at']
        local_time = scope.timezone.local_datetime(played_at) if played_at else None
        games.append({
            'game_id': int(row['game_link']), 'player_color': row['player_color'],
            'opponent_name': row['opponent_name'] or 'Unknown', 'mode': row['mode'],
            'player_rating': row['rating'], 'opponent_rating': row['opponent_rating'],
            'played_at_utc': played_at.astimezone(timezone.utc).isoformat() if played_at else None,
            'played_at_local': local_time.isoformat() if local_time else None,
            'result': next((name for name, value in RESULTS.items() if row['result'] == value), 'unknown'),
        })
    next_cursor = None
    if has_more and selected:
        last = selected[-1]
        data = [fingerprint, last['cursor_time'].isoformat(), int(last['game_link']), last['player_color']]
        next_cursor = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=')
    return {
        **scope.response_base(),
        'selection': {'scope': scope_kind, 'modes': modes, 'rating_from': rating_from,
                      'result': result, 'label': label, 'analyzed_only': False},
        'summary': None, 'games': games,
        'pagination': {'limit': limit, 'returned_games': len(games), 'has_more': has_more, 'next_cursor': next_cursor},
    }
