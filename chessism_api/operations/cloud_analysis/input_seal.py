"""Seal bounded input arrays into the existing checksum-verified wire format.

No full-run Python list, JSON roundtrip, or second chess validation. The final
reference/status change and removal of staging rows commit atomically.
"""
import asyncio
import hashlib
import json
from sqlalchemy import delete, insert, literal, select, cast, Text, text, BigInteger
from sqlalchemy.dialects.postgresql import ARRAY
from chessism_api.database.postgresql import cloud_columns as columns
from chessism_api.database.postgresql.cloud_codec import encode_document
from chessism_api.database.postgresql.models import CloudInputChunk, CloudInputPreparation, CloudJobCancellation


def canonical(rows):
    return b','.join(json.dumps({'id': ident, 'fen': fen}, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()
        for row in rows for ident, fen in zip(row.ids, row.fens))


async def seal(session, run):
    digest, count, after, aligned = hashlib.sha256(b'['), 0, -1, True
    while True:
        rows = (await session.execute(select(CloudInputChunk.ordinal, CloudInputChunk.ids, CloudInputChunk.fens)
            .where(CloudInputChunk.run_id == run.id, CloudInputChunk.ordinal > after)
            .order_by(CloudInputChunk.ordinal).limit(100))).all()
        if not rows:
            break
        for row in rows:
            if row.ordinal != count:
                raise ValueError('Input checkpoint has a gap or overlap')
            aligned = aligned and row.ordinal % 500 == 0
            count += len(row.ids)
        if after >= 0:
            digest.update(b',')
        digest.update(await asyncio.to_thread(canonical, rows))
        after = rows[-1].ordinal
        if await session.get(CloudJobCancellation, run.job_id):
            return False
    digest.update(b']')
    if count != run.position_count:
        raise ValueError('Input checkpoint/reservation counts disagree')
    ident = run.id + ':positions'
    root, groups = encode_document(ident, 'positions', [])
    root.update(sha256=digest.hexdigest(), table_names=['position', 'positions'] if count else ['positions'])
    await session.execute(delete(columns.ROOTS).where(columns.ROOTS.c.id == ident))
    await session.execute(insert(columns.ROOTS).values(root))
    await session.execute(insert(columns.TABLES['positions']), groups['positions'])
    chunk = CloudInputChunk.__table__
    # Importers address packs by their index, not their position offset. Most
    # checkpoints are already aligned; an interrupted/short page may need
    # repacking before it can use the same bounded importer as legacy inputs.
    packs = select((chunk.c.ordinal // 500).label('ordinal'), chunk.c.ids, chunk.c.fens).where(
        chunk.c.run_id == run.id).subquery()
    if not aligned:
        packs = text('''SELECT (c.ordinal + item.n - 1) / 500 AS ordinal,
            array_agg(item.id ORDER BY c.ordinal + item.n) AS ids,
            array_agg(item.fen ORDER BY c.ordinal + item.n) AS fens
            FROM cloud_input_chunk c
            CROSS JOIN LATERAL unnest(c.ids, c.fens) WITH ORDINALITY AS item(id, fen, n)
            WHERE c.run_id = :run_id GROUP BY (c.ordinal + item.n - 1) / 500
        ''').bindparams(run_id=run.id).columns(ordinal=BigInteger, ids=ARRAY(Text), fens=ARRAY(Text)).subquery()
    start = cast(packs.c.ordinal * 500, Text)
    names = ['document_id', 'path', 'parent_path', 'collection', 'item_key', 'ordinal',
             'present_fields', 'null_fields', 'integer_fields', columns.column_name('id'), columns.column_name('fen')]
    await session.execute(insert(columns.TABLES['position']).from_select(names, select(
        literal(ident), literal('$/items/') + start, literal('$'), literal('items'), start,
        packs.c.ordinal, literal(['id', 'fen'], type_=columns.TABLES['position'].c.present_fields.type),
        literal([], type_=columns.TABLES['position'].c.null_fields.type),
        literal([], type_=columns.TABLES['position'].c.integer_fields.type), packs.c.ids, packs.c.fens
    )))
    run._positions_ref = ident
    await session.execute(delete(CloudInputPreparation).where(CloudInputPreparation.run_id == run.id))
    return True
