"""Canonical evaluation publication inside the caller's existing transaction.

Selection/claims and receipts belong to the application operation. This module
neither commits nor closes its session, and never chooses a storage profile.
"""
from chessism_api.evaluations import integer, validate_batch

from .transactions import driver_connection
from .summary_writes import increment_scored_fens

BATCH_SIZE = 1000


async def increment_new_fen_counts(session, rows):
    """Only for locked, verified unscored -> scored transitions; caller commits."""
    if rows:
        await increment_scored_fens(await driver_connection(session), rows)


async def stage_fen_results(session, formatted, continuations, *, count_new=False):
    """Stage complete Stockfish results, alternatives and optional new-score totals.

    Live analysis requires count_new=True and refuses already-scored positions.
    Reviewed replacement publication owns its own aggregate adjustment and uses
    count_new=False. Both paths validate the entire batch before database writes.
    """
    keys = validate_batch(formatted, source='stockfish')
    for row in formatted:
        if row.get('piece_count') is not None:
            integer(row['piece_count'], 'piece_count', 2, 32)
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
    if not keys:
        return

    connection = await driver_connection(session)
    previous = await connection.fetch('''SELECT fen,fen_id,score FROM public.fen
        WHERE fen=ANY($1::text[]) ORDER BY fen FOR UPDATE''', list(keys))
    if len(previous) != len(keys) or (count_new and any(row['score'] is not None for row in previous)):
        raise ValueError('FENs disappeared or are already scored; refusing to overwrite or double-count')
    identifiers = {row['fen']: row['fen_id'] for row in previous}

    # Typed arrays also handle all-NULL PV/WDL columns. One statement per bounded
    # batch lets the durable summary trigger deduplicate affected games once.
    fields = ('fen', 'piece_count', 'score', 'next_moves', 'wdl_win', 'wdl_draw', 'wdl_loss')
    for start in range(0, len(formatted), BATCH_SIZE):
        batch = formatted[start:start + BATCH_SIZE]
        columns = [[(int(row[name]) if name not in ('fen', 'next_moves') else row[name])
                    if row.get(name) is not None else None for row in batch] for name in fields]
        changed = await connection.fetch('''UPDATE public.fen f SET
            piece_count=COALESCE(f.piece_count,i.piece_count), score=i.score,
            next_moves=i.next_moves,wdl_win=i.wdl_win,wdl_draw=i.wdl_draw,wdl_loss=i.wdl_loss,
            analysis_source='stockfish',tablebase_wdl=NULL,tablebase_dtz=NULL,analyzed_at=now()
            FROM unnest($1::text[],$2::smallint[],$3::integer[],$4::text[],
                        $5::smallint[],$6::smallint[],$7::smallint[])
                i(fen,piece_count,score,next_moves,wdl_win,wdl_draw,wdl_loss)
            WHERE f.fen=i.fen RETURNING f.fen_id''', *columns)
        if len(changed) != len(batch):
            raise ValueError('Not every evaluation matched a stored FEN; refusing a partial batch')

    await connection.execute('DELETE FROM public.fen_continuation WHERE fen_id=ANY($1::bigint[])',
                             list(identifiers.values()))
    for start in range(0, len(continuations), BATCH_SIZE):
        await connection.copy_records_to_table('fen_continuation', schema_name='public',
            columns=('fen_id', 'rank', 'move', 'score'),
            records=[(identifiers[row['fen_fen']], int(row['rank']), row['move'], int(row['score']))
                     for row in continuations[start:start + BATCH_SIZE]])
    if count_new:
        await increment_scored_fens(connection, formatted)
