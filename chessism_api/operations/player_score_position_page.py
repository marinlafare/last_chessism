"""Bounded matching examples, not a player-wide frequency calculation.

Seek narrow game/occurrence identities; inspect at most 512 positions per
request and fetch board text only for ten examples. Sparse bins return a
continuation even when this batch has no match. No global ranking or counts.
"""
import base64
import hashlib
import json

from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for

GAME_BATCH = 32
POSITION_BATCH = 512
PAGE_SIZE = 10


def scope_key(player, modes, bin_id):
    return hashlib.sha256(json.dumps([player, list(modes), bin_id]).encode()).hexdigest()


def encode_cursor(scope, game, white, occurrence):
    return base64.urlsafe_b64encode(json.dumps(
        [3, scope, int(game), bool(white), int(occurrence)]).encode()).decode().rstrip('=')


def decode_cursor(cursor, scope):
    if not cursor:
        return None, None, None
    try:
        if len(cursor) > 512:
            raise ValueError()
        version, expected, game, white, occurrence = json.loads(base64.b64decode(
            cursor + '=' * (-len(cursor) % 4), altchars=b'-_', validate=True))
        if (version != 3 or expected != scope or type(game) is not int or not 0 < game < 2**63
                or type(white) is not bool or type(occurrence) is not int or not 0 <= occurrence < 2**63):
            raise ValueError()
        return game, white, occurrence
    except (ValueError, TypeError, UnicodeError):
        raise ValueError('Invalid position-list cursor. Reopen the list.') from None


def matches_interval(score, interval):
    lower, upper = interval['lower'], interval['upper']
    return not (
        lower is not None and (score < lower or score == lower and not interval['lower_inclusive'])
        or upper is not None and (score > upper or score == upper and not interval['upper_inclusive']))


def candidate_games_sql(session):
    layout = storage_for(session)
    if layout:
        t = layout.table
        source = f"""SELECT gp.game_link,gp.is_white FROM {t('game_player')} gp
            JOIN {t('game')} g ON g.link=gp.game_link
            JOIN {t('game_mode')} m ON m.id=g.mode_id
            WHERE gp.player_id=:player_id AND m.name=ANY(CAST(:modes AS text[]))"""
    else:
        source = """SELECT link AS game_link,color='white' AS is_white FROM game_player
            WHERE player_name=:player AND mode=ANY(CAST(:modes AS text[]))"""
    return text(f"""SELECT * FROM ({source}) owned
        WHERE CAST(:after_game AS bigint) IS NULL
            OR (game_link,is_white)<(:after_game,CAST(:after_white AS boolean))
            OR (game_link=:after_game AND is_white=:after_white AND :after_occurrence>0)
        ORDER BY game_link DESC,is_white DESC LIMIT :game_limit""")


def occurrences_sql(session):
    layout = storage_for(session)
    t = layout.table if layout else lambda name: name
    return text(f"""WITH source AS MATERIALIZED (
        SELECT g.game_link,g.is_white,a.id AS occurrence_id,a.fen_id
        FROM unnest(CAST(:games AS bigint[]),CAST(:sides AS boolean[])) g(game_link,is_white)
        CROSS JOIN LATERAL (
            SELECT id,fen_id FROM {t('game_fen_association')}
            WHERE game_link=g.game_link AND (
                CAST(:after_game AS bigint) IS NULL OR game_link<>:after_game
                OR g.is_white<>:after_white OR id<:after_occurrence)
            ORDER BY id DESC LIMIT :scan_limit
        ) a
        ORDER BY g.game_link DESC,g.is_white DESC,a.id DESC LIMIT :scan_limit
    ) SELECT s.*,f.score,f.analysis_source FROM source s
      JOIN {t('fen')} f ON f.fen_id=s.fen_id
      ORDER BY s.game_link DESC,s.is_white DESC,s.occurrence_id DESC""")


async def read_position_page(session, player, modes, interval, cursor=None):
    scope = scope_key(player, modes, interval['id'])
    after_game, after_white, after_occurrence = decode_cursor(cursor, scope)
    layout = storage_for(session)
    t = layout.table if layout else lambda name: name
    player_id = await session.scalar(text(f"""SELECT {'id' if layout else 'player_name'}
        FROM {t('player')} WHERE player_name=:player AND joined>0 AND deleted_at IS NULL"""), {'player': player})
    if player_id is None:
        raise LookupError(f'Player {player!r} was not found.')
    params = dict(player=player, player_id=player_id, modes=list(modes), after_game=after_game,
                  after_white=after_white, after_occurrence=after_occurrence,
                  game_limit=GAME_BATCH + 1, scan_limit=POSITION_BATCH + 1)
    games = (await session.execute(candidate_games_sql(session), params)).mappings().all()
    candidates = games[:GAME_BATCH]
    records = []
    if candidates:
        records = (await session.execute(occurrences_sql(session), {
            **params, 'games': [r['game_link'] for r in candidates],
            'sides': [r['is_white'] for r in candidates]})).mappings().all()
    rows, seen, continuation = [], set(), None
    scanned = records[:POSITION_BATCH]
    for index, record in enumerate(scanned):
        score = record['score']
        if score is None or record['analysis_source'] == 'tablebase' or abs(score) >= 9000:
            continue
        score = float(score) * (1 if record['is_white'] else -1)
        if record['fen_id'] in seen or not matches_interval(score, interval):
            continue
        seen.add(record['fen_id'])
        rows.append({'fen_id': str(record['fen_id']), 'score_min': score, 'score_max': score})
        if len(rows) == PAGE_SIZE:
            if index + 1 < len(records) or len(games) > GAME_BATCH:
                continuation = (record['game_link'], record['is_white'], record['occurrence_id'])
            break
    else:
        if len(records) > POSITION_BATCH:
            last = scanned[-1]
            continuation = (last['game_link'], last['is_white'], last['occurrence_id'])
        elif len(games) > GAME_BATCH:
            last = candidates[-1]
            continuation = (last['game_link'], last['is_white'], 0)
    if rows:
        boards = dict((await session.execute(text(f'SELECT fen_id,fen FROM {t("fen")} WHERE fen_id=ANY(CAST(:ids AS bigint[]))'),
            {'ids': [int(row['fen_id']) for row in rows]})).all())
        for row in rows:
            row['fen'] = boards[int(row['fen_id'])]
    return {'rows': rows, 'next_cursor': encode_cursor(scope, *continuation) if continuation else None,
            'snapshot': scope, 'bin_id': interval['id'], 'modes': list(modes),
            'sort': 'matching_examples', 'scanned_positions': len(scanned)}
