"""Consistent export, bounded continuation reads, and conservative checkpoints."""
from datetime import datetime

from sqlalchemy import bindparam, delete, insert, select, text, update
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation

FIELDS = ('fen', 'score', 'next_moves', 'wdl_win', 'wdl_draw', 'wdl_loss',
          'piece_count', 'analysis_source', 'tablebase_wdl', 'tablebase_dtz', 'analyzed_at')
PAGE_SIZE = 1000


async def export_fence():
    """Must run BEFORE opening the export's repeatable-read snapshot.

    Analysis uses transaction_timestamp(), not commit time. Replaying from the
    oldest transaction that existed at this fence catches late commits, even if
    they become visible only after the export snapshot. Include equality so two
    commits sharing the boundary timestamp cannot be lost. No finite lookback
    interval or transaction-ID ordering is assumed.
    """
    async with AsyncDBSession() as session:
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
    query = select(*(getattr(Fen, field).label(field) for field in FIELDS)).where(Fen.score.is_not(None))
    if lower is not None:
        query = query.where(Fen.analyzed_at >= lower).order_by(Fen.analyzed_at, Fen.fen)
    else:
        query = query.order_by(Fen.fen)
    stream = await session.stream(query.execution_options(yield_per=PAGE_SIZE))
    try:
        async for rows in stream.mappings().partitions(PAGE_SIZE):
            batch = []
            for row in rows:
                record = dict(row)
                record.update(type='fen_analysis', continuations=[])
                if record['analyzed_at']:
                    record['analyzed_at'] = record['analyzed_at'].isoformat()
                batch.append(record)
            by_fen = {row['fen']: row for row in batch}
            continuations = await session.execute(select(FenContinuation.fen_fen,
                FenContinuation.rank, FenContinuation.move, FenContinuation.score)
                .where(FenContinuation.fen_fen.in_(by_fen)).order_by(FenContinuation.fen_fen, FenContinuation.rank))
            for fen, rank, move, score in continuations:
                by_fen[fen]['continuations'].append({'rank': rank, 'move': move, 'score': score})
            yield batch
    finally:
        await stream.close()


async def restore_batch(records):
    # Duplicate versions in a batch are legitimate; newest file/record wins.
    latest = {row['fen']: row for row in records}
    async with AsyncDBSession() as session:
        existing = set((await session.scalars(select(Fen.fen).where(Fen.fen.in_(latest)))).all())
        prepared, continuations = [], []
        for fen, row in latest.items():
            if fen not in existing:
                continue
            values = {f'p_{field}': row.get(field) for field in FIELDS}
            if isinstance(values['p_analyzed_at'], str):
                values['p_analyzed_at'] = datetime.fromisoformat(values['p_analyzed_at'])
            if values['p_piece_count'] is None:
                values['p_piece_count'] = sum(c.isalpha() for c in fen.split(' ', 1)[0])
            values['p_analysis_source'] = values['p_analysis_source'] or 'stockfish_backup'
            prepared.append(values)
            continuations.extend({'fen_fen': fen, 'rank': item['rank'], 'move': item['move'],
                                  'score': item['score']} for item in row.get('continuations') or [])
        if prepared:
            await session.execute(update(Fen.__table__).where(Fen.fen == bindparam('p_fen'))
                .values(**{field: bindparam(f'p_{field}') for field in FIELDS if field != 'fen'}),
                prepared, execution_options={'synchronize_session': False})
            await session.execute(delete(FenContinuation).where(FenContinuation.fen_fen.in_(existing)))
            if continuations:
                await session.execute(insert(FenContinuation), continuations)
        await session.commit()
    return sum(row['fen'] in existing for row in records), sum(row['fen'] not in existing for row in records)
