"""Explicit administrator installation; never called by an HTTP request."""
import json
import os
from pathlib import Path
import secrets

from sqlalchemy import text
from sqlalchemy.engine import make_url

from .catalog import SOURCES
from .sql_policy import SCHEMA

READER = 'chessism_export_reader'
OWNER = 'chessism_export_view_owner'
DEFAULT_CREDENTIAL = '/research-data/integrations/export-reader.json'


def view_select(source, tables):
    ordered, done = [], set()
    def include(name):
        if name not in source.joins or name in done: return
        template, dependencies = source.joins[name]
        for dependency in dependencies: include(dependency)
        done.add(name); ordered.append(template.format(**tables))
    for name in source.joins: include(name)
    fields = ', '.join(f'{column.expression} AS "{name}"' for name, column in source.columns.items())
    return f'SELECT {fields} FROM {source.base.format(**tables)} {" ".join(ordered)} WHERE {source.predicate}'


async def install(engine, tables, output=DEFAULT_CREDENTIAL):
    """Add views/roles and two metadata columns; no copies or chess-data writes."""
    url = make_url(os.environ['DATABASE_URL'])
    path = Path(output)
    if path.exists():
        credential = json.loads(path.read_text())
        if credential.get('user') != READER or credential.get('database') != url.database:
            raise ValueError('Existing reader credential does not match this installation.')
        password = credential['password']
    else:
        password = secrets.token_urlsafe(48)
        credential = dict(host=url.host, port=url.port or 5432, database=url.database,
                          user=READER, password=password)
    if any(char not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for char in password):
        raise ValueError('Invalid reader credential encoding.')
    database = '"' + url.database.replace('"', '""') + '"'
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SET LOCAL lock_timeout='5s'"))
            await conn.execute(text('SELECT pg_advisory_xact_lock(760140003)'))
            # PUBLIC TEMP would give every new reader a write capability.
            # Refuse a shared multi-user DB rather than silently altering others.
            other_logins = await conn.scalar(text("SELECT count(*) FROM pg_roles WHERE rolcanlogin AND NOT rolsuper AND rolname != :reader"), {'reader': READER})
            if other_logins and await conn.scalar(text("SELECT has_database_privilege('public',current_database(),'TEMP')")):
                raise ValueError('Other non-admin database users exist: review PUBLIC TEMP privileges before installing analytical exports.')
            await conn.execute(text(f'REVOKE TEMPORARY ON DATABASE {database} FROM PUBLIC'))
            for role in (OWNER, READER):
                if not await conn.scalar(text('SELECT 1 FROM pg_roles WHERE rolname=:name'), {'name': role}):
                    await conn.execute(text(f'CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'))
            members = await conn.scalar(text('SELECT count(*) FROM pg_auth_members WHERE member IN (SELECT oid FROM pg_roles WHERE rolname IN (:a,:b))'), {'a': OWNER, 'b': READER})
            if members: raise ValueError('Export roles must not inherit or be members of any other role.')
            await conn.execute(text(f'ALTER ROLE {READER} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2 PASSWORD \'{password}\''))
            await conn.execute(text(f'ALTER ROLE {OWNER} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'))
            await conn.execute(text(f'GRANT CONNECT ON DATABASE {database} TO {READER}'))
            for setting, value in {'default_transaction_read_only':'on', 'statement_timeout':'60s',
                    'lock_timeout':'2s', 'idle_in_transaction_session_timeout':'30s',
                    'work_mem':'16MB', 'temp_file_limit':'256MB', 'max_parallel_workers_per_gather':'0',
                    'jit':'off', 'search_path':f'pg_catalog,{SCHEMA}'}.items():
                await conn.execute(text(f"ALTER ROLE {READER} SET {setting} = '{value}'"))
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA} AUTHORIZATION {OWNER}'))
            await conn.execute(text(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC'))
            await conn.execute(text(f'GRANT USAGE ON SCHEMA {SCHEMA} TO {READER}'))
            for schema in {table.split('.')[0] for table in tables.values()}:
                await conn.execute(text(f'GRANT USAGE ON SCHEMA {schema} TO {OWNER}'))
            for table in tables.values():
                await conn.execute(text(f'GRANT SELECT ON {table} TO {OWNER}'))
            for name, source in SOURCES.items():
                await conn.execute(text(f'CREATE OR REPLACE VIEW {SCHEMA}.{name} WITH (security_barrier=true) AS {view_select(source,tables)}'))
                await conn.execute(text(f'ALTER VIEW {SCHEMA}.{name} OWNER TO {OWNER}'))
                await conn.execute(text(f'REVOKE ALL ON {SCHEMA}.{name} FROM PUBLIC'))
                await conn.execute(text(f'GRANT SELECT ON {SCHEMA}.{name} TO {READER}'))
            await conn.execute(text('ALTER TABLE public.export_client ADD COLUMN IF NOT EXISTS allow_analytics boolean NOT NULL DEFAULT false'))
            await conn.execute(text('ALTER TABLE public.export_job ADD COLUMN IF NOT EXISTS error_code varchar(64)'))
    except Exception:
        # SQLAlchemy exceptions may include the administrative PASSWORD SQL.
        raise RuntimeError('Analytical installation failed; no changes from its transaction were committed. Check role/schema prerequisites without logging credentials.') from None
    if not path.exists():
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'w') as destination:
            json.dump(credential, destination); destination.flush(); os.fsync(destination.fileno())

