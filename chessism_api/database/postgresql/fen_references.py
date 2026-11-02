"""Resolve portable position keys at database boundaries, in bounded queries."""
from sqlalchemy import select

from .models import Fen


async def resolve_fen_ids(session, fens):
    keys = list(dict.fromkeys(fens))
    resolved = {}
    for start in range(0, len(keys), 1000):
        rows = await session.execute(select(Fen.fen, Fen.fen_id).where(Fen.fen.in_(keys[start:start + 1000])))
        resolved.update(rows.all())
    return resolved


async def association_rows(session, rows):
    """Accept portable ingestion payloads; never persist their repeated text.

    The caller must bound rows and own the transaction. Missing positions are
    errors, not silently discarded associations. Numeric-only callers are
    checked by the database foreign key.
    """
    identifiers = await resolve_fen_ids(session, [r['fen_fen'] for r in rows if 'fen_fen' in r])
    output = []
    for row in rows:
        item = dict(row)
        if 'fen_fen' in item:
            identifier = identifiers.get(item.pop('fen_fen'))
            if identifier is None:
                raise ValueError('Cannot link game: a FEN identifier is missing')
            if item.get('fen_id') is not None and item['fen_id'] != identifier:
                raise ValueError('Conflicting FEN text and numeric identifier')
            item['fen_id'] = identifier
        if item.get('fen_id') is None:
            raise ValueError('Cannot link game without a FEN identifier')
        output.append(item)
    return output
