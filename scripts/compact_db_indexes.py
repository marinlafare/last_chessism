"""Reviewed impact-index DDL shared by fresh migration and isolated repair.

The query remains integer arithmetic. Never mask a missing/wrong index with
IF NOT EXISTS, or rewrite the application query to float to hide the mismatch.
"""
import re

INDEX_NAME = 'ix_fen_scored_impact_desc'
CREATE_IMPACT_INDEX = '''CREATE INDEX ix_fen_scored_impact_desc
    ON public.fen ((n_games * abs(score)) DESC, n_games DESC)
    WHERE score IS NOT NULL'''


def normalized(expression):
    return re.sub(r'[\s()]', '', expression or '')


def validate_index(row, *, integer=False):
    expressions = {'n_games*absscore'} if integer else {
        'n_games*absscore', 'n_games::doubleprecision*absscore',
        'n_games::doubleprecision*absscore::doubleprecision'}
    if (not row or row['table_name'] != 'fen' or row['table_schema'] != 'public'
            or row['unique'] or row['primary'] or not row['valid'] or not row['ready']
            or row['keys'] != 2 or not row['descending']
            or normalized(row['expression']) not in expressions
            or normalized(row['predicate']) != 'scoreISNOTNULL'
            or row['second_key'] != 'n_games'
            or (integer and row['value_type'] != 'bigint')):
        raise RuntimeError('Unexpected impact index; refusing an unreviewed definition')


async def read_index(conn):
    row = await conn.fetchrow('''SELECT t.relname AS table_name,n.nspname AS table_schema,
        i.indisunique AS unique,i.indisprimary AS primary,i.indisvalid AS valid,i.indisready AS ready,
        i.indnkeyatts AS keys, (i.indoption[0]&1)=1 AND (i.indoption[1]&1)=1 AS descending,
        pg_get_expr(i.indexprs,i.indrelid) AS expression,pg_get_expr(i.indpred,i.indrelid) AS predicate,
        pg_get_indexdef(i.indexrelid,2,true) AS second_key,
        a.atttypid::regtype::text AS value_type,pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
        JOIN pg_attribute a ON a.attrelid=i.indexrelid AND a.attnum=1
        WHERE i.indexrelid=to_regclass('public.ix_fen_scored_impact_desc')''')
    return dict(row) if row else None


async def drop_reviewed_impact_index(conn):
    row = await read_index(conn)
    if row is not None:
        validate_index(row)
        await conn.execute('DROP INDEX public.ix_fen_scored_impact_desc')
    return row


async def create_integer_impact_index(conn):
    await conn.execute(CREATE_IMPACT_INDEX)
    row = await read_index(conn)
    validate_index(row, integer=True)
    return row


def uses_impact_index(plan):
    return isinstance(plan, dict) and (
        plan.get('Index Name') == INDEX_NAME
        or any(uses_impact_index(child) for child in plan.get('Plans', [])))
