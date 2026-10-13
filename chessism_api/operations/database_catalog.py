"""Read-only schema inspection. Never select application rows or evaluate defaults."""
from datetime import datetime, timezone

from sqlalchemy import bindparam, text

from chessism_api.database.engine import AsyncDBSession

MAX_TABLES = 2000
MAX_COLUMNS = 50000

RELATIONS_SQL = text("""
SELECT c.oid::bigint AS oid, n.nspname AS schema, c.relname AS name,
       pg_catalog.format('%I.%I', n.nspname, c.relname) AS id,
       c.relkind::text AS kind, c.relispartition AS is_partition,
       CASE WHEN c.relkind IN ('r', 'm') AND c.reltuples >= 0
            THEN c.reltuples::bigint ELSE NULL END AS estimated_rows
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm')
  AND n.nspname <> 'information_schema'
  AND left(n.nspname, 3) <> 'pg_'
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
  AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
ORDER BY n.nspname, c.relname
LIMIT :limit
""")

COLUMNS_SQL = text("""
SELECT a.attrelid::bigint AS table_oid, a.attname AS name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
       NOT a.attnotnull AS nullable,
       EXISTS (SELECT 1 FROM pg_catalog.pg_constraint pk
               WHERE pk.conrelid = a.attrelid AND pk.contype = 'p'
                 AND a.attnum = ANY(pk.conkey)) AS primary_key
FROM pg_catalog.pg_attribute a
WHERE a.attrelid IN :oids AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum
LIMIT :limit
""").bindparams(bindparam('oids', expanding=True))

FOREIGN_KEYS_SQL = text("""
SELECT fk.oid::bigint AS oid, fk.conname AS name,
       fk.conrelid::bigint AS source_oid, fk.confrelid::bigint AS target_oid,
       source.attname AS source_column, target.attname AS target_column
FROM pg_catalog.pg_constraint fk
CROSS JOIN LATERAL unnest(fk.conkey, fk.confkey)
  WITH ORDINALITY AS pair(source_num, target_num, position)
JOIN pg_catalog.pg_attribute source
  ON source.attrelid = fk.conrelid AND source.attnum = pair.source_num
JOIN pg_catalog.pg_attribute target
  ON target.attrelid = fk.confrelid AND target.attnum = pair.target_num
WHERE fk.contype = 'f' AND fk.conrelid IN :oids
ORDER BY fk.oid, pair.position
LIMIT :limit
""").bindparams(bindparam('oids', expanding=True))


class CatalogTooLarge(ValueError):
    pass


def catalog_payload(relations, columns, foreign_keys):
    """An explicit response allowlist; no defaults, comments, SQL or row values."""
    kinds = {'r': 'table', 'p': 'partitioned table', 'v': 'view', 'm': 'materialized view'}
    tables = {
        row['oid']: {key: row[key] for key in ('id', 'schema', 'name', 'is_partition', 'estimated_rows')}
        | {'kind': kinds[row['kind']], 'columns': []}
        for row in relations
    }
    column_index = {}
    for row in columns:
        if row['table_oid'] not in tables:
            continue
        column = {key: row[key] for key in ('name', 'data_type', 'nullable', 'primary_key')}
        column['foreign_key'] = False
        tables[row['table_oid']]['columns'].append(column)
        column_index[(row['table_oid'], row['name'])] = column
    relationships = {}
    for row in foreign_keys:
        column = column_index.get((row['source_oid'], row['source_column']))
        if column is not None:
            column['foreign_key'] = True
        if row['source_oid'] not in tables or row['target_oid'] not in tables:
            continue
        relationship = relationships.setdefault(row['oid'], {
            'id': str(row['oid']), 'name': row['name'],
            'source_table': tables[row['source_oid']]['id'],
            'target_table': tables[row['target_oid']]['id'],
            'source_columns': [], 'target_columns': [],
        })
        relationship['source_columns'].append(row['source_column'])
        relationship['target_columns'].append(row['target_column'])
    return {
        'tables': list(tables.values()), 'relationships': list(relationships.values()),
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }


async def read_database_catalog():
    async with AsyncDBSession() as session:
        async with session.begin():
            # Catalog inspection cannot modify data, and cannot wait indefinitely
            # behind concurrent schema maintenance. Pool settings reset on exit.
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            await session.execute(text("SET LOCAL lock_timeout = '1s'"))
            relations = (await session.execute(RELATIONS_SQL, {'limit': MAX_TABLES + 1})).mappings().all()
            if len(relations) > MAX_TABLES:
                raise CatalogTooLarge('Too many tables for this workspace.')
            if not relations:
                return catalog_payload([], [], [])
            params = {'oids': [row['oid'] for row in relations], 'limit': MAX_COLUMNS + 1}
            columns = (await session.execute(COLUMNS_SQL, params)).mappings().all()
            foreign_keys = (await session.execute(FOREIGN_KEYS_SQL, params)).mappings().all()
            if len(columns) > MAX_COLUMNS or len(foreign_keys) > MAX_COLUMNS:
                raise CatalogTooLarge('Too many columns for this workspace.')
            return catalog_payload(relations, columns, foreign_keys)
