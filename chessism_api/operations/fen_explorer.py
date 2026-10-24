"""Bounded, sampled FEN browsing and on-demand distinct-game discovery."""
import json
import random
import re
import uuid
import chess
from sqlalchemy import text
from chessism_api.database.compact_storage import compact_for
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.common_positions import move_label, verified_example_line

TTL = 900
SAMPLE_LIMIT = 100


def scope_filter(scope):
    if scope == 'unique': return 'n_games = 1', {}
    if scope == 'repeated': return 'n_games > 1', {}
    # Match the scored/unscored predicate used by the appearance histogram.
    if scope == 'scored': return 'score IS NOT NULL', {}
    if scope == 'unscored': return 'score IS NULL', {}
    if scope in {str(i) for i in range(2, 11)}: return 'n_games = :appearances', {'appearances': int(scope)}
    raise ValueError('Choose unique, repeated, scored, unscored, or 2–10 appearances.')


def validate_fen(fen):
    if not isinstance(fen, str) or len(fen) > 120 or len(fen.split()) != 4:
        raise ValueError('A four-field position FEN is required.')
    fen = ' '.join(fen.split())
    try: chess.Board(fen + ' 0 1')
    except ValueError as error: raise ValueError('Invalid FEN.') from error
    return fen


async def sample_positions(scope):
    condition, params = scope_filter(scope)
    found = {}
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL statement_timeout = '4s'"))
        # Bounded block samples, not a full-table random sort. Exploratory, not
        # statistically uniform. Sparse groups may return fewer examples.
        for percent in (.002, .02, .2):
            rows = (await session.execute(text(f"""
                SELECT fen, n_games AS appearances FROM fen
                TABLESAMPLE SYSTEM (:percent) REPEATABLE (:seed)
                WHERE {condition} LIMIT 120
            """), {**params, 'percent': percent, 'seed': random.SystemRandom().randrange(2**31)})).mappings().all()
            found.update({row['fen']: dict(row) for row in rows})
            if len(found) >= SAMPLE_LIMIT: break
    rows = list(found.values())
    random.SystemRandom().shuffle(rows)
    return rows[:SAMPLE_LIMIT]


async def describe_examples(rows):
    output = []
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL statement_timeout = '4s'"))
        compact = compact_for(session) is not None
        # Compact moves are unique on (link, n_move) and no longer have an id.
        # Keep deterministic duplicate selection for the legacy layout only.
        move_order = '' if compact else 'ORDER BY id'
        example_sql = """
            SELECT n_move,white_move,black_move FROM moves
            WHERE link=:link AND n_move<=:n_move ORDER BY n_move LIMIT 100
        """ if compact else """
            SELECT DISTINCT ON (n_move) n_move,white_move,black_move FROM moves
            WHERE link=:link AND n_move<=:n_move ORDER BY n_move,id LIMIT 100
        """
        for row in rows:
            arrival = (await session.execute(text(f"""
                SELECT a.game_link, a.n_move, a.move_color,
                    CASE WHEN a.move_color = 'white' THEN m.white_move ELSE m.black_move END AS san
                FROM (SELECT game_link,n_move,move_color FROM game_fen_association
                    WHERE fen_id=(SELECT fen_id FROM fen WHERE fen=:fen) LIMIT 1) a
                LEFT JOIN LATERAL (SELECT white_move,black_move FROM moves
                    WHERE link=a.game_link AND n_move=a.n_move {move_order} LIMIT 1) m ON TRUE
            """), {'fen': row['fen']})).mappings().first()
            item = {**row, 'label': 'Position', 'example_line': None}
            if arrival:
                item['label'] = move_label(arrival['n_move'], arrival['move_color'], arrival['san'])
                if arrival['n_move'] <= 100:
                    moves = (await session.execute(text(example_sql),
                        {'link': arrival['game_link'], 'n_move': arrival['n_move']})).mappings().all()
                    item['example_line'] = verified_example_line(moves,arrival['n_move'],arrival['move_color'],row['fen'])
            output.append(item)
    return output


async def explore_positions(redis, account_id, scope, token=None, page=0, *, include_details=True):
    scope_filter(scope)
    if not 0 <= page < 10: raise ValueError('Sample page must be between 0 and 9.')
    if token and not re.fullmatch(r'[0-9a-f]{32}', token): raise ValueError('Invalid sample token.')
    if not token:
        if page: raise ValueError('Start a sample at page zero.')
        token = uuid.uuid4().hex
        data = {'scope': scope, 'rows': await sample_positions(scope)}
        await redis.set(f'fen-explorer:{account_id}:{token}',json.dumps(data),ex=TTL)
    else:
        raw = await redis.get(f'fen-explorer:{account_id}:{token}')
        if not raw: raise LookupError('This sample expired. Shuffle to load new examples.')
        data = json.loads(raw)
        if data['scope'] != scope: raise ValueError('Sample does not match this slice.')
    rows = data['rows'][page*10:page*10+10]
    # Thumbnails only need the FEN. Avoid per-position game/move lookups, while
    # preserving the existing detailed response for callers that still use it.
    if include_details:
        key = f'fen-explorer:{account_id}:{token}:page:{page}'
        cached = await redis.get(key)
        rows = json.loads(cached) if cached else await describe_examples(rows)
        if not cached: await redis.set(key,json.dumps(rows),ex=TTL)
    return {'token': token, 'scope': scope, 'page': page, 'rows': rows,
            'sample_size': len(data['rows']), 'has_next': (page+1)*10 < len(data['rows']),
            'sampled': True}


async def position_games(fen, after=0):
    fen = validate_fen(fen)
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        ids = (await session.execute(text("""
            SELECT DISTINCT game_link FROM game_fen_association
            WHERE fen_id=(SELECT fen_id FROM fen WHERE fen=:fen)
              AND game_link>:after ORDER BY game_link LIMIT 11
        """), {'fen':fen,'after':after})).scalars().all()
        if not ids: return {'rows': [], 'next_after': None}
        games = (await session.execute(text("""
            SELECT link,white,black,white_elo,black_elo,year,month,day,white_str_result,time_control
            FROM game WHERE link=ANY(:ids) ORDER BY link
        """), {'ids':ids[:10]})).mappings().all()
        occurrences = (await session.execute(text("""
            SELECT game_link,n_move,move_color FROM game_fen_association
            WHERE fen_id=(SELECT fen_id FROM fen WHERE fen=:fen) AND game_link=ANY(:ids)
            ORDER BY game_link,n_move,move_color LIMIT 2001
        """), {'fen':fen,'ids':ids[:10]})).mappings().all()
    return {'rows':[{**dict(game), 'occurrences':[{'n_move':o['n_move'],'move_color':o['move_color']}
            for o in occurrences[:2000] if o['game_link']==game['link']]} for game in games],
            'next_after':ids[9] if len(ids)>10 else None, 'occurrences_truncated':len(occurrences)>2000}
