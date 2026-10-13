"""Preserve ID high-water marks when copying tables; never rewind a source."""
from scripts.db_refactor_candidate import qi


async def preserve_sequence(conn, source_schema, target_schema, table, column='id'):
    if source_schema == target_schema:
        raise ValueError('A sequence copy requires different source and target schemas')
    source_table = f'{qi(source_schema)}.{qi(table)}'
    target_table = f'{qi(target_schema)}.{qi(table)}'
    source = await conn.fetchval('SELECT pg_get_serial_sequence($1,$2)', source_table, column)
    target = await conn.fetchval('SELECT pg_get_serial_sequence($1,$2)', target_table, column)
    if not source or not target or source == target:
        raise RuntimeError('Independent owned source and target sequences required')
    states = []
    for sequence in (source, target):
        settings = await conn.fetchrow('SELECT seqincrement,seqcycle,seqmax FROM pg_sequence WHERE seqrelid=$1::regclass', sequence)
        if settings['seqincrement'] != 1 or settings['seqcycle']:
            raise RuntimeError('Nonstandard sequence needs explicit migration review')
        sql = await conn.fetchval("SELECT format('SELECT last_value,is_called FROM %s',$1::regclass)", sequence)
        states.append(dict(await conn.fetchrow(sql)))
    maximum = await conn.fetchval(f'SELECT max({qi(column)}) FROM {target_table}')
    # Preserve even consumed-but-deleted IDs, sequence cache allocations, and
    # an already-higher target. Also preserve never-called empty sequences.
    last = max(*(state['last_value'] for state in states), maximum or 1)
    called = maximum == last or any(state['is_called'] and state['last_value'] == last for state in states)
    if last >= settings['seqmax']:
        raise RuntimeError('Sequence exhausted; widening requires a separate review')
    await conn.fetchval('SELECT setval($1::regclass,$2,$3)', target, last, called)
    return {'table': table, 'target_schema': target_schema, 'last_value': last, 'is_called': called}
