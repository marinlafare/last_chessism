"""Bounded, sampled FEN browsing and on-demand distinct-game discovery."""
import json
import random
import re
import uuid
import chess
from sqlalchemy import text
from chessism_api.database.postgresql import fen_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.operations.common_positions import move_label, verified_example_line

TTL = 900
SAMPLE_LIMIT = 100


def validate_scope(scope):
    if scope not in {'unique','repeated','scored','unscored',*(str(i) for i in range(2,11))}:
        raise ValueError('Choose unique, repeated, scored, unscored, or 2–10 appearances.')


def validate_fen(fen):
    if not isinstance(fen, str) or len(fen) > 120 or len(fen.split()) != 4:
        raise ValueError('A four-field position FEN is required.')
    fen = ' '.join(fen.split())
    try: chess.Board(fen + ' 0 1')
    except ValueError as error: raise ValueError('Invalid FEN.') from error
    return fen


async def sample_positions(scope, *, factory=None):
    validate_scope(scope)
    found = {}
    async with read_transaction(factory or AsyncDBSession) as session:
        await session.execute(text("SET LOCAL statement_timeout = '4s'"))
        # Bounded block samples; sparse groups may return fewer examples.
        for percent in (.002, .02, .2):
            rows = await reads.sample(session, scope, percent, random.SystemRandom().randrange(2**31))
            found.update({row['fen']: row for row in rows})
            if len(found) >= SAMPLE_LIMIT: break
    rows = list(found.values())
    random.SystemRandom().shuffle(rows)
    return rows[:SAMPLE_LIMIT]


async def describe_examples(rows, *, factory=None):
    output = []
    async with read_transaction(factory or AsyncDBSession) as session:
        await session.execute(text("SET LOCAL statement_timeout = '4s'"))
        for row in rows:
            arrival = await reads.arrival(session, row['fen'])
            item = {**row, 'label': 'Position', 'example_line': None}
            if arrival:
                item['label'] = move_label(arrival['n_move'], arrival['move_color'], arrival['san'])
                if arrival['n_move'] <= 100:
                    moves = await reads.example_moves(session, arrival['game_link'], arrival['n_move'])
                    item['example_line'] = verified_example_line(moves,arrival['n_move'],arrival['move_color'],row['fen'])
            output.append(item)
    return output


async def explore_positions(redis, account_id, scope, token=None, page=0, *, include_details=True):
    validate_scope(scope)
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


async def position_games(fen, after=0, *, factory=None):
    fen = validate_fen(fen)
    async with read_transaction(factory or AsyncDBSession) as session:
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        ids = await reads.game_ids(session, fen, after)
        if not ids: return {'rows': [], 'next_after': None}
        games = await reads.games(session, ids[:10])
        occurrences = await reads.occurrences(session, fen, ids[:10])
    return {'rows':[{**dict(game), 'occurrences':[{'n_move':o['n_move'],'move_color':o['move_color']}
            for o in occurrences[:2000] if o['game_link']==game['link']]} for game in games],
            'next_after':ids[9] if len(ids)>10 else None, 'occurrences_truncated':len(occurrences)>2000}
