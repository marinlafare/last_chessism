"""One persistence path for local engines and downloaded cloud results.

The caller owns the transaction, including any import receipts or leases.
"""
from sqlalchemy import delete, insert, select, text

from chessism_api.evaluations import integer, validate_batch

from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Fen, FenContinuation
from chessism_api.database.fen_references import resolve_fen_ids
from chessism_api.database.compact_storage import compact_for, compact_table, integer_result


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


async def increment_new_fen_counts(session, rows):
    """Only for locked, verified unscored -> scored transitions; caller commits."""
    if not rows:
        return
    result = await session.execute(text('''UPDATE database_summary SET
        analyzed_fens=analyzed_fens+:count,
        unscored_fens=GREATEST(unscored_fens-:count,0),
        scored_fens=scored_fens+:nonzero,
        nonzero_scored_fens=nonzero_scored_fens+:nonzero,
        refreshed_at=CURRENT_TIMESTAMP WHERE id=1'''),
        {'count': len(rows), 'nonzero': sum(row['score'] != 0 for row in rows)})
    if result.rowcount != 1:
        raise ValueError('Database totals are missing; evaluation transaction must be retried after initialization')


async def stage_fen_results(session, formatted, continuations, *, interface=None, fen_ids=None, count_new=False):
    keys = validate_batch(formatted, source='stockfish')
    ranks = set()
    for row in continuations:
        if row.get('fen_fen') not in keys:
            raise ValueError('Continuation belongs to a different result set')
        integer(row.get('rank'), 'continuation rank', 1, 32767)
        integer(row.get('score'), 'continuation score', -2147483648, 2147483647)
        key = row['fen_fen'], row['rank']
        if key in ranks or not isinstance(row.get('move'), str) or not row['move']:
            raise ValueError('Invalid or duplicate continuation')
        ranks.add(key)
    if count_new and keys:
        previous = (await session.execute(select(Fen.fen, Fen.score).where(Fen.fen.in_(keys))
                    .order_by(Fen.fen).with_for_update())).all()
        if len(previous) != len(keys) or any(row.score is not None for row in previous):
            raise ValueError('FENs disappeared or are already scored; refusing to overwrite or double-count')
    await (interface or DBInterface(Fen)).update_fen_analysis_data(session, formatted)
    await replace_fen_continuations(session, [row['fen'] for row in formatted], continuations, fen_ids=fen_ids)
    if count_new:
        await increment_new_fen_counts(session, formatted)
