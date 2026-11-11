"""Canonical PostgreSQL foundation, deliberately disconnected from runtime startup."""
from hashlib import sha256
import json
from pathlib import Path

VERSION = 3
CONTRACT = 'chessism_canonical_foundation'
SQL_FILES = ('core.sql', 'support.sql', 'projections.sql')
INIT_LOCK = 6975310201


class SchemaMismatch(RuntimeError):
    """The target is not the exact schema supported by this foundation."""


def schema_sql():
    return '\n'.join(Path(__file__).with_name(name).read_text() for name in SQL_FILES)


def schema_digest():
    return sha256(schema_sql().encode()).hexdigest()


async def catalog_digest(connection):
    """Ignore data/sequence positions; detect changed columns, keys, indexes or code."""
    queries = (
        """SELECT c.relname, c.relkind::text, c.relrowsecurity, c.relforcerowsecurity
           FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname='public' ORDER BY c.relname""",
        """SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod) AS type,
           a.attnotnull, a.attidentity::text, a.attgenerated::text,
           pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS default_value
           FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
           WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped
           ORDER BY c.relname,a.attnum""",
        """SELECT c.relname,k.conname,k.convalidated,pg_catalog.pg_get_constraintdef(k.oid) AS definition
           FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_class c ON c.oid=k.conrelid
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname='public' ORDER BY c.relname,k.conname""",
        "SELECT indexname,indexdef FROM pg_catalog.pg_indexes WHERE schemaname='public' ORDER BY indexname",
        "SELECT viewname,definition FROM pg_catalog.pg_views WHERE schemaname='public' ORDER BY viewname",
        """SELECT c.relname,t.tgname,t.tgenabled::text,pg_catalog.pg_get_triggerdef(t.oid) AS definition
           FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname='public' AND NOT t.tgisinternal ORDER BY c.relname,t.tgname""",
        """SELECT p.proname,pg_catalog.pg_get_functiondef(p.oid) AS definition
           FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
           WHERE n.nspname='public' ORDER BY p.proname,p.oid""",
        """SELECT t.typname,e.enumlabel FROM pg_catalog.pg_type t
           JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
           JOIN pg_catalog.pg_enum e ON e.enumtypid=t.oid WHERE n.nspname='public'
           ORDER BY t.typname,e.enumsortorder""",
    )
    rows = [[dict(row) for row in await connection.fetch(query)] for query in queries]
    return sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


async def validate(connection):
    """Read-only validation. Never initializes, repairs, migrates, commits or closes."""
    if not await connection.fetchval("SELECT pg_catalog.to_regclass('public.canonical_schema_version')"):
        raise SchemaMismatch('Canonical schema has not been initialized')
    rows = await connection.fetch('SELECT * FROM public.canonical_schema_version')
    if len(rows) != 1:
        raise SchemaMismatch('Expected exactly one canonical schema version')
    row = rows[0]
    if (row['contract'], row['version'], row['ddl_sha256']) != (CONTRACT, VERSION, schema_digest()):
        raise SchemaMismatch('Incompatible canonical schema version or DDL artifact')
    if row['catalog_sha256'] != await catalog_digest(connection):
        raise SchemaMismatch('Canonical schema catalog drift; automatic repair refused')
    return VERSION


async def initialize(connection):
    """Own one atomic initialization transaction on an explicitly supplied connection.

    Return True when created, False for an exact repeat. Never drop data or migrate
    an existing database. The caller owns connection closure, not this function.
    """
    if connection.is_in_transaction():
        raise ValueError('Initializer requires a connection without an active transaction')
    async with connection.transaction():
        await connection.execute("SET LOCAL search_path TO public, pg_catalog")
        await connection.execute("SET LOCAL lock_timeout TO '5s'")
        await connection.execute("SET LOCAL statement_timeout TO '30s'")
        await connection.execute('SELECT pg_catalog.pg_advisory_xact_lock($1)', INIT_LOCK)
        if await connection.fetchval("SELECT pg_catalog.to_regclass('public.canonical_schema_version')"):
            await validate(connection)
            return False
        occupied = await connection.fetchval("""SELECT EXISTS(
            SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
                AND n.nspname NOT LIKE 'pg_temp%'
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
            WHERE n.nspname='public' AND t.typtype IN ('e','d')
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='public')""")
        if occupied:
            raise SchemaMismatch('Initializer requires an empty database; existing objects preserved')
        await connection.execute(schema_sql())
        await connection.execute('''CREATE TABLE public.canonical_schema_version (
            singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
            contract text NOT NULL, version integer NOT NULL,
            ddl_sha256 text NOT NULL, catalog_sha256 text NOT NULL,
            initialized_at timestamptz NOT NULL DEFAULT now())''')
        await connection.execute('''INSERT INTO public.canonical_schema_version
            (contract,version,ddl_sha256,catalog_sha256) VALUES($1,$2,$3,$4)''',
            CONTRACT, VERSION, schema_digest(), await catalog_digest(connection))
    return True
