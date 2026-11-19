"""Portable FEN export/restore on canonical keys with explicit transactions."""
from datetime import datetime

from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import driver_connection, read_transaction, write_transaction
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from chessism_api.evaluations import integer
from .archive import validate_record

FIELDS = ('fen', 'score', 'next_moves', 'wdl_win', 'wdl_draw', 'wdl_loss',
          'piece_count', 'analysis_source', 'tablebase_wdl', 'tablebase_dtz', 'analyzed_at')
PAGE_SIZE = 1000


async def export_fence():
    """Fence before the snapshot; replay late commits from their transaction time."""
    async with read_transaction(AsyncDBSession) as session:
        if await session.scalar(text("SELECT current_setting('max_prepared_transactions')::int")):
            raise RuntimeError('FEN-backup checkpoints require prepared transactions to be disabled')
        result = (await session.execute(text('''
            SELECT LEAST(transaction_timestamp(), COALESCE(min(xact_start), transaction_timestamp())) AS boundary,
                   count(*) FILTER (WHERE state IS NULL) AS hidden
            FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid()
              AND backend_type='client backend'
        '''))).mappings().one()
        if result['hidden']:
            raise RuntimeError('Backup cannot inspect all database transactions; checkpoint refused')
        return result['boundary']


async def export_batches(session, lower=None):
    if not session.in_transaction():
        raise ValueError('Export requires a caller-owned read snapshot')
    # Without a timestamp, exporting again is safer than silently missing a change.
    condition = 'AND (analyzed_at IS NULL OR analyzed_at>=:lower)' if lower is not None else ''
    order = 'analyzed_at NULLS FIRST,fen_id' if lower is not None else 'fen'
    stream = await session.stream(text(f'''SELECT fen_id AS _fen_id,{','.join(FIELDS)}
        FROM public.fen WHERE score IS NOT NULL {condition} ORDER BY {order}'''),
        {'lower': lower} if lower is not None else {}, execution_options={'yield_per': PAGE_SIZE})
    try:
        async for rows in stream.mappings().partitions(PAGE_SIZE):
            batch, by_id = [], {}
            for row in rows:
                record = dict(row)
                identifier = record.pop('_fen_id')
                record.update(type='fen_analysis', continuations=[])
                if record['analyzed_at']:
                    record['analyzed_at'] = record['analyzed_at'].isoformat()
                batch.append(record); by_id[identifier] = record
            continuations = await session.execute(text('''SELECT fen_id,rank,move,score
                FROM public.fen_continuation WHERE fen_id=ANY(CAST(:ids AS bigint[]))
                ORDER BY fen_id,rank'''), {'ids': list(by_id)})
            for identifier, rank, move, score in continuations:
                by_id[identifier]['continuations'].append({'rank': rank, 'move': move, 'score': score})
            yield batch
    finally:
        await finish_analysis_cleanup(stream.close())


def restore_values(row):
    """Validate portable bytes without attributing or rounding historical results."""
    validate_record({'type': 'fen_analysis', **row})
    values = {field: row.get(field) for field in FIELDS}
    if values['piece_count'] is None:
        values['piece_count'] = sum(c.isalpha() for c in row['fen'].split(' ', 1)[0])
    bounds = {'score': (-2147483648, 2147483647), 'piece_count': (2, 32),
              'wdl_win': (0, 1000), 'wdl_draw': (0, 1000), 'wdl_loss': (0, 1000),
              'tablebase_wdl': (-2, 2), 'tablebase_dtz': (-2147483648, 2147483647)}
    for field, (low, high) in bounds.items():
        if values[field] is not None:
            integer(values[field], field, low, high); values[field] = int(values[field])
    wdl = [values[field] for field in ('wdl_win', 'wdl_draw', 'wdl_loss')]
    if any(v is not None for v in wdl) and (None in wdl or sum(wdl) != 1000):
        raise ValueError('Saved WDL must be complete and sum to 1000')
    stamp = values['analyzed_at']
    if isinstance(stamp, str):
        stamp = datetime.fromisoformat(stamp)
    if stamp is not None and (not isinstance(stamp, datetime) or stamp.utcoffset() is None):
        raise ValueError('Saved analysis timestamp must include its timezone')
    values['analyzed_at'] = stamp
    source = values['analysis_source']
    if source not in (None, 'stockfish', 'tablebase'):
        raise ValueError('Unsupported saved evaluation source')
    if source is not None and stamp is None:
        raise ValueError('Attributed results require their saved analysis timestamp')
    if source == 'tablebase':
        if values['tablebase_wdl'] is None:
            raise ValueError('Saved tablebase result requires WDL')
    elif any(values[field] is not None for field in ('tablebase_wdl', 'tablebase_dtz')):
        raise ValueError('Tablebase metadata requires tablebase attribution')
    continuations = []
    for item in row.get('continuations') or []:
        integer(item['rank'], 'continuation rank', 1, 32767)
        integer(item['score'], 'continuation score', -2147483648, 2147483647)
        if not item['move']:
            raise ValueError('Continuation move must not be empty')
        continuations.append((int(item['rank']), item['move'], int(item['score'])))
    return values, continuations


async def restore_batch(records):
    # Newest duplicate version wins; absent FENs are counted, never manufactured.
    latest = {row['fen']: restore_values(row) for row in records}
    async with write_transaction(AsyncDBSession) as session:
        connection = await driver_connection(session)
        existing = dict(await connection.fetch('''SELECT fen,fen_id FROM public.fen
            WHERE fen=ANY($1::text[]) ORDER BY fen FOR UPDATE''', list(latest)))
        rows = [latest[fen][0] for fen in sorted(existing)]
        if rows:
            await connection.execute('''UPDATE public.fen f SET score=r.score,next_moves=r.next_moves,
                wdl_win=r.wdl_win,wdl_draw=r.wdl_draw,wdl_loss=r.wdl_loss,piece_count=r.piece_count,
                analysis_source=r.analysis_source,tablebase_wdl=r.tablebase_wdl,
                tablebase_dtz=r.tablebase_dtz,analyzed_at=r.analyzed_at
                FROM unnest($1::text[],$2::int[],$3::text[],$4::smallint[],$5::smallint[],
                    $6::smallint[],$7::smallint[],$8::text[],$9::smallint[],$10::int[],$11::timestamptz[])
                    r(fen,score,next_moves,wdl_win,wdl_draw,wdl_loss,piece_count,analysis_source,
                      tablebase_wdl,tablebase_dtz,analyzed_at) WHERE f.fen=r.fen''',
                *[[row[field] for row in rows] for field in FIELDS])
            await connection.execute('DELETE FROM public.fen_continuation WHERE fen_id=ANY($1::bigint[])', list(existing.values()))
            moves = [(identifier, *item) for fen, identifier in existing.items() for item in latest[fen][1]]
            for start in range(0, len(moves), PAGE_SIZE):
                await connection.copy_records_to_table('fen_continuation', schema_name='public',
                    columns=['fen_id', 'rank', 'move', 'score'], records=moves[start:start+PAGE_SIZE])
    return sum(row['fen'] in existing for row in records), sum(row['fen'] not in existing for row in records)
