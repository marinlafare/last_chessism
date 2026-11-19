"""Bounded matching examples, not a player-wide frequency calculation.

Seek narrow game/occurrence identities; inspect at most 512 positions per
request and fetch board text only for ten examples. Sparse bins return a
continuation even when this batch has no match. No global ranking or counts.
"""
import base64
import hashlib
import json

from chessism_api.database.postgresql import player_position_reads as reads

GAME_BATCH = 32
POSITION_BATCH = 512
PAGE_SIZE = 10


def scope_key(player, modes, bin_id, color=None):
    return hashlib.sha256(json.dumps([player, list(modes), bin_id, color]).encode()).hexdigest()


def encode_cursor(scope, game, white, move, ply):
    return base64.urlsafe_b64encode(json.dumps(
        [5, scope, int(game), bool(white), move, ply]).encode()).decode().rstrip('=')


def decode_cursor(cursor, scope):
    if not cursor:
        return None, None, None, None
    try:
        if len(cursor) > 512:
            raise ValueError()
        version, expected, game, white, move, ply = json.loads(base64.b64decode(
            cursor + '=' * (-len(cursor) % 4), altchars=b'-_', validate=True))
        if (version != 5 or expected != scope or type(game) is not int or not 0 < game < 2**63
                or type(white) is not bool
                or (move is None and ply is not None)
                or (move is not None and (type(move) is not int or not 0 <= move <= 2**31-1 or type(ply) is not bool))):
            raise ValueError()
        return game, white, move, ply
    except (ValueError, TypeError, UnicodeError):
        raise ValueError('Invalid position-list cursor. Reopen the list.') from None


def matches_interval(score, interval):
    lower, upper = interval['lower'], interval['upper']
    return not (
        lower is not None and (score < lower or score == lower and not interval['lower_inclusive'])
        or upper is not None and (score > upper or score == upper and not interval['upper_inclusive']))


async def read_position_page(session, player, modes, interval, cursor=None, *, color=None):
    if color not in (None, 'white', 'black'):
        raise ValueError('Choose White, Black, or both player colors.')
    scope = scope_key(player, modes, interval['id'], color)
    after_game, after_white, after_move, after_ply = decode_cursor(cursor, scope)
    player_id = await reads.owner_id(session, player)
    if player_id is None:
        raise LookupError(f'Player {player!r} was not found.')
    params = dict(player=player, player_id=player_id, modes=list(modes), color=color, after_game=after_game,
                  after_white=after_white, after_move=after_move, after_ply=after_ply,
                  game_limit=GAME_BATCH + 1, scan_limit=POSITION_BATCH + 1)
    games = await reads.example_games(session, params)
    candidates = games[:GAME_BATCH]
    records = []
    if candidates:
        records = await reads.example_occurrences(session, {
            **params, 'games': [r['game_link'] for r in candidates],
            'sides': [r['is_white'] for r in candidates]})
    rows, seen, continuation = [], set(), None
    scanned = records[:POSITION_BATCH]
    for index, record in enumerate(scanned):
        score = record['score']
        if score is None or record['analysis_source'] == 'tablebase' or abs(score) >= 9000:
            continue
        score = float(score) * (1 if record['is_white'] else -1)
        identity = (record['fen_id'], record['is_white'])
        if identity in seen or not matches_interval(score, interval):
            continue
        seen.add(identity)
        rows.append({'fen_id': str(record['fen_id']), 'score_min': score, 'score_max': score,
                     'player_color': 'white' if record['is_white'] else 'black'})
        if len(rows) == PAGE_SIZE:
            if index + 1 < len(records) or len(games) > GAME_BATCH:
                continuation = (record['game_link'], record['is_white'], record['n_move'], record['ply_is_white'])
            break
    else:
        if len(records) > POSITION_BATCH:
            last = scanned[-1]
            continuation = (last['game_link'], last['is_white'], last['n_move'], last['ply_is_white'])
        elif len(games) > GAME_BATCH:
            last = candidates[-1]
            continuation = (last['game_link'], last['is_white'], None, None)
    if rows:
        boards = await reads.boards(session, [int(row['fen_id']) for row in rows])
        for row in rows:
            row['fen'] = boards[int(row['fen_id'])]
    return {'rows': rows, 'next_cursor': encode_cursor(scope, *continuation) if continuation else None,
            'snapshot': scope, 'bin_id': interval['id'], 'modes': list(modes), 'color': color,
            'score_perspective': 'player',
            'sort': 'matching_examples', 'scanned_positions': len(scanned)}
