"""Version and integrity checks for game and player numeric FEN references."""
from sqlalchemy import text

TABLES = ('game_fen_association', 'player_position_frequency')
SHAPE_SQL = """
SELECT c.relname AS table_name, a.attname AS column_name,
       a.attnotnull AS not_null, pg_catalog.format_type(a.atttypid,a.atttypmod) AS data_type
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid
WHERE n.nspname=current_schema()
  AND c.relname IN ('game_fen_association','player_position_frequency')
  AND a.attnum>0 AND NOT a.attisdropped
"""


def schema_version(rows):
    columns = {(r['table_name'], r['column_name']): r for r in rows}
    if not columns:
        return 'empty'
    if all((t, 'fen_fen') in columns and (t, 'fen_id') not in columns for t in TABLES):
        return 'text_v1'
    if all((t, 'fen_id') in columns and (t, 'fen_fen') not in columns
           and columns[t, 'fen_id']['not_null']
           and columns[t, 'fen_id']['data_type'] == 'bigint' for t in TABLES):
        return 'numeric_v1'
    return 'incomplete'


async def require_numeric_reference_schema(connection):
    shape = schema_version((await connection.execute(text(SHAPE_SQL))).mappings().all())
    if shape not in ('empty', 'numeric_v1'):
        raise RuntimeError('Game/player FEN reference migration is required. Use the explicit '
                           'rehearsed migration; application startup never rewrites these tables.')


async def validate_references(connection, *, allow_legacy=False):
    version = schema_version(await connection.fetch(SHAPE_SQL))
    if version == 'text_v1' and allow_legacy:
        return {'version': version}
    if version != 'numeric_v1':
        raise RuntimeError('Incomplete or missing game/player numeric FEN references')
    constraints = await connection.fetch("""
        SELECT c.relname AS table_name, k.contype::text AS kind, k.convalidated AS validated,
               ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(id,n)
                     JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=x.id ORDER BY x.n) AS columns,
               f.relname AS target,
               ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(id,n)
                     JOIN pg_attribute a ON a.attrelid=k.confrelid AND a.attnum=x.id ORDER BY x.n) AS target_columns,
               k.confdeltype::text AS delete_action
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_class f ON f.oid=k.confrelid
        WHERE n.nspname=current_schema()
          AND c.relname IN ('game_fen_association','player_position_frequency')
    """)
    for table, kind, columns, target, target_columns, delete_action in (
        (TABLES[0], 'p', ['id'], None, [], ' '),
        (TABLES[0], 'u', ['game_link','fen_id','n_move','move_color'], None, [], ' '),
        (TABLES[0], 'f', ['fen_id'], 'fen', ['fen_id'], 'a'),
        (TABLES[0], 'f', ['game_link'], 'game', ['link'], 'a'),
        (TABLES[1], 'p', ['player_name','player_color','fen_id'], None, [], ' '),
        (TABLES[1], 'f', ['fen_id'], 'fen', ['fen_id'], 'c'),
        (TABLES[1], 'f', ['player_name'], 'player', ['player_name'], 'c'),
    ):
        if not any(r['validated'] and r['table_name'] == table and r['kind'] == kind
                   and r['columns'] == columns and r['target'] == target
                   and r['target_columns'] == target_columns
                   and (kind != 'f' or r['delete_action'] == delete_action) for r in constraints):
            raise RuntimeError(f'Missing or unvalidated {table} identity constraint')
    checks = await connection.fetchval("""
        SELECT count(*) FROM pg_constraint WHERE conrelid='player_position_frequency'::regclass
        AND convalidated AND contype='c' AND conname IN ('player_position_frequency_color',
          'player_position_frequency_games_positive','player_position_frequency_occurrences_valid')
    """)
    if checks != 3:
        raise RuntimeError('Missing player position frequency validation constraints')
    sequence = await connection.fetchval("SELECT pg_get_serial_sequence('game_fen_association','id')")
    if not sequence:
        raise RuntimeError('Missing game association sequence')
    query = await connection.fetchval("SELECT format('SELECT last_value,is_called FROM %s', $1::regclass)", sequence)
    state = await connection.fetchrow(query)
    maximum = await connection.fetchval('SELECT max(id) FROM game_fen_association') or 0
    if int(state['last_value']) + (0 if state['is_called'] else -1) < maximum:
        raise RuntimeError('Game association sequence is behind stored identifiers')
    return {'version': version, 'game_fen_association': {
        'maximum_id': maximum, 'sequence_last_value': state['last_value']}}
