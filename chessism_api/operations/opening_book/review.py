"""Attach opening reference data without changing evaluation classifications."""
import chess
from sqlalchemy import text
from .catalog import position_key
from .store import TABLE, lookup


async def annotate_moves(session, moves):
    for move in moves:
        move['book'] = None
    if not moves or not await session.scalar(text('SELECT to_regclass(:table) IS NOT NULL'), {'table': TABLE}):
        return
    keyed = []
    for move in moves:
        if not move.get('uci') or not move.get('fen_before'):
            continue
        try:
            key = (position_key(chess.Board(move['fen_before'])), move['uci'])
        except ValueError:
            continue
        keyed.append((move, key))
    keys = list(dict.fromkeys(key for _, key in keyed))
    matches = {}
    for start in range(0, len(keys), 512):
        matches.update(await lookup(session, keys[start:start+512]))
    for move, key in keyed:
        if key in matches:
            row = matches[key]
            move['book'] = {'source': 'lichess', 'source_revision': row['source_revision'],
                            'openings': row['opening_labels']}
