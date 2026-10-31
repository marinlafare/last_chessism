"""Indexed, date-descending game pages for one player's selected FEN and color."""
import base64
import hashlib
import json
from datetime import timezone
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from .player_chart_games import decode_cursor, RESULTS
from .player_hero_analytics import _analytics_scope

RARE_LIMIT = 10000


def query(layout, color, rare, mode_count):
    t = layout.table
    side, other = ('white', 'black') if color == 'white' else ('black', 'white')
    flag = 'true' if color == 'white' else 'false'
    arrival = f'''WITH arrivals AS MATERIALIZED (
        SELECT DISTINCT game_link FROM {t('game_fen_association')} WHERE fen_id=:fen)''' if rare else 'WITH'
    membership = f'''EXISTS (SELECT 1 FROM {t('game_player')} gp
        WHERE gp.game_link=g.link AND gp.is_white={flag} AND gp.player_id=:owner)'''
    occurrence = 'g.link IN (SELECT game_link FROM arrivals)' if rare else f'''EXISTS (
        SELECT 1 FROM {t('game_fen_association')} a
        WHERE a.game_link=g.link AND a.fen_id=:fen OFFSET 0)'''
    # One ordered branch per mode uses the existing player/mode/recent indexes.
    # Rare FENs instead begin with the bounded-size global occurrence set.
    branches = []
    for i in range(mode_count):
        branches.append(f'''(SELECT g.link, g.played_at, g.mode_id,
            g.{side}_elo AS rating,g.{other}_elo AS opponent_rating,
            g.{side}_result AS result,g.{other}_player_id AS opponent_id
            FROM {t('game')} g
            WHERE g.{side}_player_id=:owner AND g.mode_id=:mode_{i}
              AND {membership} AND {occurrence}
              AND (CAST(:after_id AS bigint) IS NULL OR
                (CAST(:after_time AS timestamptz) IS NOT NULL AND
                  ((g.played_at,g.link)<(CAST(:after_time AS timestamptz),CAST(:after_id AS bigint)) OR g.played_at IS NULL))
                OR (CAST(:after_time AS timestamptz) IS NULL AND g.played_at IS NULL AND g.link<:after_id))
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC LIMIT :limit)''')
    return text(f'''{arrival}{',' if rare else ''} selected AS MATERIALIZED (
        SELECT * FROM ({' UNION ALL '.join(branches)}) candidates
        ORDER BY played_at DESC NULLS LAST,link DESC LIMIT :limit)
        SELECT s.*,p.player_name AS opponent_name,m.name AS mode FROM selected s
        JOIN {t('player')} p ON p.id=s.opponent_id JOIN {t('game_mode')} m ON m.id=s.mode_id
        ORDER BY s.played_at DESC NULLS LAST,s.link DESC''')


async def explore_position_games(player_name, *, fen_id, color, modes, timezone_name=None, limit=15, cursor=None):
    if type(fen_id) is not int or not 0 < fen_id < 2**63 or color not in ('white', 'black'):
        raise ValueError('Choose a valid FEN ID and the player color.')
    if not modes or not set(modes) <= {'bullet','blitz','rapid'} or not 1 <= limit <= 30:
        raise ValueError('Invalid position game filters.')
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='4s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        await session.execute(text("SET LOCAL work_mem='8MB'"))
        layout = storage_for(session)
        if layout is None:
            raise ValueError('Position game pages require normalized storage.')
        t = layout.table
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        owner = await session.scalar(text(f'SELECT id FROM {t("player")} WHERE player_name=:name'), {'name': scope.player})
        count = await session.scalar(text(f'SELECT n_games FROM {t("fen")} WHERE fen_id=:id'), {'id': fen_id})
        if count is None:
            raise LookupError('Position not found.')
        mode_ids = (await session.execute(text(f'SELECT id FROM {t("game_mode")} WHERE name=ANY(CAST(:modes AS text[])) ORDER BY id'), {'modes': modes})).scalars().all()
        if not mode_ids:
            raise ValueError('Game modes are unavailable.')
        fingerprint = hashlib.sha256(json.dumps(['position-v1',scope.player,fen_id,color,sorted(modes)]).encode()).hexdigest()
        after_time, after_id, _ = decode_cursor(cursor, fingerprint)
        if after_time is not None and after_time.year == 1:
            after_time = None
        params = {'owner': owner, 'fen': fen_id, 'limit': limit+1,
                  'after_id': after_id, 'after_time': after_time,
                  **{f'mode_{i}': mode for i, mode in enumerate(mode_ids)}}
        rows = (await session.execute(query(layout,color,count<=RARE_LIMIT,len(mode_ids)),params)).mappings().all()
    selected = rows[:limit]
    games = []
    for r in selected:
        played = r['played_at']
        games.append({'game_id': int(r['link']), 'player_color': color, 'mode': r['mode'],
            'player_rating': r['rating'], 'opponent_rating': r['opponent_rating'], 'opponent_name': r['opponent_name'],
            'played_at_utc': played.astimezone(timezone.utc).isoformat() if played else None,
            'played_at_local': scope.timezone.local_datetime(played).isoformat() if played else None,
            'result': next((key for key,value in RESULTS.items() if value==r['result']), 'unknown')})
    next_cursor = None
    if len(rows)>limit:
        last = selected[-1]
        boundary = last['played_at'].isoformat() if last['played_at'] else '0001-01-01T00:00:00+00:00'
        next_cursor = base64.urlsafe_b64encode(json.dumps([fingerprint,boundary,int(last['link']),color]).encode()).decode().rstrip('=')
    return {**scope.response_base(), 'summary': None, 'games': games,
        'selection': {'scope':'position','fen_id':str(fen_id),'color':color,'modes':modes,'analyzed_only':False},
        'pagination': {'limit':limit,'returned_games':len(games),'has_more':bool(next_cursor),'next_cursor':next_cursor}}
