"""Indexed, date-descending game pages for one player's selected FEN and color."""
import base64
import hashlib
import json
from datetime import timezone
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import player_position_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from .player_chart_games import decode_cursor, RESULTS
from .player_hero_analytics import _analytics_scope

RARE_LIMIT = 10000


async def explore_position_games(player_name, *, fen_id, color, modes, timezone_name=None, limit=15, cursor=None, session_factory=None):
    if type(fen_id) is not int or not 0 < fen_id < 2**63 or color not in ('white', 'black'):
        raise ValueError('Choose a valid FEN ID and the player color.')
    if not modes or not set(modes) <= {'bullet','blitz','rapid'} or not 1 <= limit <= 30:
        raise ValueError('Invalid position game filters.')
    async with read_transaction(session_factory or AsyncDBSession) as session:
        await reads.read_limits(session, '4s')
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        owner = await reads.owner_id(session, scope.player)
        count = await reads.fen_frequency(session, fen_id)
        if count is None:
            raise LookupError('Position not found.')
        mode_ids = await reads.mode_ids(session, modes)
        if not mode_ids:
            raise ValueError('Game modes are unavailable.')
        fingerprint = hashlib.sha256(json.dumps(['position-v2',owner,fen_id,color,sorted(set(modes))]).encode()).hexdigest()
        after_time, after_id, _ = decode_cursor(cursor, fingerprint)
        params = {'owner': owner, 'fen': fen_id, 'limit': limit+1,
                  'after_id': after_id, 'after_time': after_time,
                  **{f'mode_{i}': mode for i, mode in enumerate(mode_ids)}}
        rows = await reads.position_games(session,color,count<=RARE_LIMIT,len(mode_ids),params)
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
        boundary = last['played_at'].isoformat()
        next_cursor = base64.urlsafe_b64encode(json.dumps([fingerprint,boundary,int(last['link']),color]).encode()).decode().rstrip('=')
    return {**scope.response_base(), 'summary': None, 'games': games,
        'selection': {'scope':'position','fen_id':str(fen_id),'color':color,'modes':modes,'analyzed_only':False},
        'pagination': {'limit':limit,'returned_games':len(games),'has_more':bool(next_cursor),'next_cursor':next_cursor}}
