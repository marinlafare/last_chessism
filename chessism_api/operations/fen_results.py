"""Backup continuation helper retained until the backup adapter is converted."""
from sqlalchemy import delete, insert

from chessism_api.database.postgresql.models import FenContinuation
from chessism_api.database.postgresql.fen_references import resolve_fen_ids
from chessism_api.database.postgresql.compact_storage import compact_for, compact_table, integer_result


async def replace_fen_continuations(session, fens, continuations, *, fen_ids=None):
    keys = set(fens)
    if not keys:
        if continuations:
            raise ValueError('Continuations require matching FEN results')
        return
    identifiers = fen_ids if fen_ids is not None else await resolve_fen_ids(session, keys)
    if any(identifiers.get(fen) is None for fen in keys):
        raise ValueError('Cannot save continuations: a FEN identifier is missing')
    prepared = []
    layout = compact_for(session)
    for row in continuations:
        if layout:
            row = integer_result(row)
        if row['fen_fen'] not in keys:
            raise ValueError('Continuation belongs to a different result set')
        prepared.append({'fen_id': identifiers[row['fen_fen']], 'rank': row['rank'],
                         'move': row['move'], 'score': row['score']})
    await session.execute(delete(FenContinuation).where(
        FenContinuation.fen_id.in_([identifiers[fen] for fen in keys])
    ))
    if prepared:
        target = compact_table(FenContinuation, layout.core) if layout else FenContinuation.__table__
        await session.execute(insert(target), prepared)
