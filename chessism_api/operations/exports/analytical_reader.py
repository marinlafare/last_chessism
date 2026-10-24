"""Separate unprivileged connection; caller SQL never reaches the metadata engine."""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path

import asyncpg

from .analytical_admin import READER, DEFAULT_CREDENTIAL
from .analytical_spec import MAX_ANALYTICAL_COLUMNS
from .sql_policy import SCHEMA


class AnalyticalError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


async def check_plan(conn, sql, parameters):
    # A conservative admission heuristic, NOT a hard CPU/RAM guarantee.
    # EXPLAIN does not execute the query; values stay bound parameters.
    plan = json.loads(await conn.fetchval('EXPLAIN (FORMAT JSON) '+sql, *parameters))[0]['Plan']
    maximum = float(os.environ.get('CHESSISM_EXPORT_MAX_PLAN_COST', '250000'))
    if plan['Total Cost'] > maximum:
        raise AnalyticalError('QUERY_PLAN_LIMIT', 'Estimated query cost exceeds the primary-database analytical budget. Narrow the selection; larger analysis requires an approved reporting database.')
    return plan['Total Cost']


@asynccontextmanager
async def connection():
    path = Path(os.environ.get('CHESSISM_EXPORT_READER_FILE', DEFAULT_CREDENTIAL))
    if not path.exists():
        raise AnalyticalError('ANALYTICS_UNAVAILABLE', 'The isolated analytical reader is not configured.')
    credential = json.loads(path.read_text())
    if credential.get('user') != READER:
        raise AnalyticalError('UNSAFE_QUERY_ROLE', 'Analytical queries require the dedicated reader role.')
    conn = await asyncpg.connect(**credential, timeout=10, command_timeout=60,
        server_settings={'search_path': f'pg_catalog,{SCHEMA}', 'default_transaction_read_only':'on',
                         'statement_timeout':'60000', 'lock_timeout':'2000', 'application_name':'chessism-analytical-export'})
    try:
        unsafe = await conn.fetchval('''SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication OR rolbypassrls
            OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
            OR has_database_privilege(current_database(),'CREATE,TEMP')
            OR EXISTS(SELECT 1 FROM pg_namespace n WHERE has_schema_privilege(n.oid,'CREATE'))
            OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname!='information_schema' AND c.relkind IN ('r','p','v','m','f')
                AND (has_table_privilege(c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
                     OR (has_table_privilege(c.oid,'SELECT') AND n.nspname!=$1)))
            FROM pg_roles r WHERE rolname=current_user''', SCHEMA)
        if unsafe:
            raise AnalyticalError('UNSAFE_QUERY_ROLE', 'Reader privileges no longer match the approved read-only boundary.')
        async with conn.transaction(isolation='repeatable_read', readonly=True):
            await conn.execute("SET LOCAL TIME ZONE 'UTC'")
            yield conn
    finally:
        await conn.close(timeout=5)


def output_schema(attributes):
    import pyarrow as pa
    types = {16:pa.bool_(),20:pa.int64(),21:pa.int16(),23:pa.int32(),25:pa.string(),1043:pa.string(),
             700:pa.float32(),701:pa.float64(),1082:pa.date32(),1114:pa.timestamp('us'),1184:pa.timestamp('us',tz='UTC')}
    if not 1 <= len(attributes) <= MAX_ANALYTICAL_COLUMNS:
        raise AnalyticalError('OUTPUT_COLUMNS_EXCEEDED', 'Final output must contain 1–128 columns.')
    names = [attribute.name for attribute in attributes]
    if len(set(names)) != len(names):
        raise AnalyticalError('DUPLICATE_COLUMNS', 'Give every output column a unique alias.')
    fields = []
    for attribute in attributes:
        if attribute.type.oid not in types:
            raise AnalyticalError('UNSUPPORTED_RESULT_TYPE', f'Column {attribute.name[:64]} has unsupported type {attribute.type.name}; explicitly cast to a supported scalar type (for example double precision for AVG).')
        fields.append(pa.field(attribute.name, types[attribute.type.oid], nullable=True))
    return pa.schema(fields)
