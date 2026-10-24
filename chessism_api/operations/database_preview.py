"""Bounded, read-only DataFrame previews, not a SQL console or data export."""
import asyncio
import math
import re

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from chessism_api.database.engine import AsyncDBSession

ROW_LIMIT = 50
CANDIDATE_LIMIT = 1000
COLUMN_LIMIT = 128
CELL_LIMIT = 512

RELATION_SQL = text("""
SELECT c.oid::bigint AS oid, n.nspname AS schema, c.relname AS name,
       c.relkind::text AS kind, c.reltuples, c.relpages
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE pg_catalog.format('%I.%I', n.nspname, c.relname) = :table_id
  AND c.relkind IN ('r', 'p', 'v', 'm')
  AND n.nspname <> 'information_schema' AND left(n.nspname, 3) <> 'pg_'
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
  AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
""")
COLUMNS_SQL = text("""
SELECT a.attname AS name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = :oid AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
""")


class PreviewNotFound(ValueError):
    pass


def protected_column(name):
    normalized = re.sub(r'[^a-z0-9]', '', name.lower())
    return any(word in normalized for word in (
        'password', 'passwd', 'secret', 'token', 'credential', 'privatekey',
        'apikey', 'authorization', 'cookie', 'connectionstring', 'databaseurl',
    ))


def sampling_percent(relation):
    # Aim for at least 400 candidate rows and 16 pages, including wide tables.
    # SYSTEM is random block sampling, not a uniform statistical sample.
    rows, pages = float(relation['reltuples']), int(relation['relpages'])
    if not math.isfinite(rows) or rows <= 0 or pages <= 0:
        return 100.0
    return min(100.0, max(40000.0 / rows, 1600.0 / pages))


def preview_statement(relation, columns, sampled):
    quote = postgresql.dialect().identifier_preparer.quote_identifier
    qualified = f"{quote(relation['schema'])}.{quote(relation['name'])}"
    projection = []
    for index, column in enumerate(columns):
        # Sensitive columns are replaced in SQL: their values never leave PostgreSQL.
        if column['redacted']:
            expression = "'[redacted]'::text"
        elif column['data_type'] == 'bytea':
            expression = f"CASE WHEN {quote(column['name'])} IS NULL THEN NULL ELSE '[binary]'::text END"
        else:
            expression = f"left({quote(column['name'])}::text, :cell_limit + 1)"
        projection.append(f'{expression} AS c{index}')
    projection_sql = ', '.join(projection) or 'NULL::text AS empty_row'
    sample_sql = ' TABLESAMPLE SYSTEM (:percentage)' if sampled else ''
    # MATERIALIZED + LIMIT bound both the candidate pool and random sort. Never
    # ORDER BY random() over the entire application table (tens of millions).
    return text(f"""
WITH candidates AS MATERIALIZED (
    SELECT {projection_sql} FROM {qualified}{sample_sql} LIMIT :candidate_limit
)
SELECT * FROM candidates ORDER BY random() LIMIT :row_limit
""")


async def read_database_preview(table_id):
    # Bound the whole request as well as individual statements/lock waits.
    return await asyncio.wait_for(_read_database_preview(table_id), timeout=6)


async def _read_database_preview(table_id):
    async with AsyncDBSession() as session:
        async with session.begin():
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout = '4s'"))
            await session.execute(text("SET LOCAL lock_timeout = '1s'"))
            relation = (await session.execute(RELATION_SQL, {'table_id': table_id})).mappings().one_or_none()
            if relation is None:
                raise PreviewNotFound('Table is unavailable. Refresh the database structure.')
            all_columns = (await session.execute(COLUMNS_SQL, {'oid': relation['oid']})).mappings().all()
            columns = [dict(column, redacted=protected_column(column['name'])) for column in all_columns[:COLUMN_LIMIT]]
            sampled = relation['kind'] in ('r', 'm')
            percentage = sampling_percent(relation) if sampled else 100.0
            statement = preview_statement(relation, columns, sampled)
            params = {'cell_limit': CELL_LIMIT, 'candidate_limit': CANDIDATE_LIMIT, 'row_limit': ROW_LIMIT}
            # Retry a sparse sample, then fall back to a bounded pool. This
            # also returns every row when a table has fewer than fifty.
            attempts = list(dict.fromkeys([percentage, min(100.0, percentage * 8), 100.0]))
            for percentage in attempts:
                rows = (await session.execute(statement, dict(params, percentage=percentage))).all()
                if len(rows) >= ROW_LIMIT:
                    break
            truncated_cells = 0
            values = []
            for row in rows:
                cells = []
                for cell in row[:len(columns)]:
                    if cell is not None and len(cell) > CELL_LIMIT:
                        truncated_cells += 1
                        cell = cell[:CELL_LIMIT] + '…'
                    cells.append(cell)
                values.append(cells)
            return {
                'table': table_id, 'columns': columns, 'rows': values,
                'row_count': len(values), 'row_limit': ROW_LIMIT,
                'sample_method': 'random_blocks' if sampled and percentage < 100 else 'bounded_candidates',
                'candidate_limit': CANDIDATE_LIMIT,
                'omitted_columns': max(0, len(all_columns) - COLUMN_LIMIT),
                'cell_limit': CELL_LIMIT, 'truncated_cells': truncated_cells,
            }
