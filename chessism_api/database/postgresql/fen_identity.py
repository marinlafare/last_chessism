"""Cheap schema checks shared by startup and backup restoration validation."""
from sqlalchemy import text

SHAPE_SQL = """
SELECT c.relname AS table_name, a.attname AS column_name,
       a.attnotnull AS not_null, a.attidentity::text AS identity_kind,
       pg_catalog.format_type(a.atttypid,a.atttypmod) AS data_type
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
WHERE n.nspname=current_schema() AND c.relname IN ('fen','fen_continuation')
  AND a.attnum>0 AND NOT a.attisdropped
"""


def schema_version(rows):
    columns = {(r['table_name'], r['column_name']): r for r in rows}
    if not columns:
        return 'empty'
    if ('fen', 'fen_id') not in columns and ('fen_continuation', 'fen_fen') in columns:
        return 'text_v1'
    parent, child = columns.get(('fen', 'fen_id')), columns.get(('fen_continuation', 'fen_id'))
    if (parent and child and ('fen_continuation', 'fen_fen') not in columns
            and parent['data_type'] == child['data_type'] == 'bigint'
            and parent['not_null'] and child['not_null'] and parent['identity_kind'] in ('a', 'd')):
        return 'numeric_v2'
    return 'incomplete'


async def require_numeric_fen_schema(connection):
    shape = schema_version((await connection.execute(text(SHAPE_SQL))).mappings().all())
    if shape not in ('empty', 'numeric_v2'):
        raise RuntimeError('FEN numeric-ID migration is required before starting this application version. '
                           'Use the explicit migration procedure; startup never migrates existing analysis data.')


async def validate_identity(connection, *, allow_legacy=False):
    """asyncpg connection; validates restored identity without modifying data."""
    shape = schema_version(await connection.fetch(SHAPE_SQL))
    if shape == 'text_v1' and allow_legacy:
        return {'version': shape}
    if shape != 'numeric_v2':
        raise RuntimeError('Incomplete or missing FEN numeric-ID schema')
    valid = await connection.fetchval("""
      SELECT count(*)=3 FROM pg_constraint
      WHERE convalidated AND (
        (conrelid='fen'::regclass AND conname='uq_fen_fen_id' AND contype='u') OR
        (conrelid='fen_continuation'::regclass AND conname='uq_fen_continuation_fen_id_rank' AND contype='u') OR
        (conrelid='fen_continuation'::regclass AND conname='fk_fen_continuation_fen_id'
         AND confrelid='fen'::regclass AND contype='f'))
    """)
    if not valid:
        raise RuntimeError('Missing or unvalidated FEN identity constraints')
    result = {'version': shape}
    for table, column in (('fen', 'fen_id'), ('fen_continuation', 'id')):
        sequence = await connection.fetchval('SELECT pg_get_serial_sequence($1,$2)', table, column)
        if not sequence:
            raise RuntimeError(f'Missing {table} identifier sequence')
        # Sequence comes from pg_catalog; relation/column pairs are fixed above.
        sequence_sql = await connection.fetchval("SELECT format('SELECT last_value,is_called FROM %s', $1::regclass)", sequence)
        state = await connection.fetchrow(sequence_sql)
        maximum = await connection.fetchval(f'SELECT max({column}) FROM {table}') or 0
        if int(state['last_value']) + (0 if state['is_called'] else -1) < maximum:
            raise RuntimeError(f'{table} identifier sequence is behind stored identifiers')
        result[table] = {'maximum_id': maximum, 'sequence_last_value': state['last_value']}
    return result
