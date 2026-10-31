"""Whole migration orchestration, restricted to disposable private copies.

Never accepts a live URL. Original public tables are retained at cutover, and
summary finalization shares the cutover transaction. No deployment/cleanup.
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.db_refactor_candidate import C, A, R, V, guard, qi, standard, columns
from scripts.db_refactor_workload import digest

SHADOWS = (C, A, R, V)


async def outside_catalog(conn):
    """Fingerprint catalog objects outside the four disposable shadow schemas.

Used around transactional DROP CASCADE: an unexpected external dependency
must roll the entire reset back. Ignore physical/statistics changes.
    """
    schemas = [r['oid'] for r in await conn.fetch("""SELECT oid FROM pg_namespace
        WHERE nspname<>ALL($1::text[]) AND nspname NOT LIKE 'pg_%'
        AND nspname<>'information_schema' ORDER BY oid""", list(SHADOWS))]
    relations = [r['oid'] for r in await conn.fetch(
        'SELECT oid FROM pg_class WHERE relnamespace=ANY($1::oid[]) ORDER BY oid', schemas)]
    queries = (
        ("SELECT oid,relname,relnamespace,relkind,relowner,relacl,reloptions,relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid=ANY($1::oid[]) ORDER BY oid", relations),
        ("SELECT attrelid,attname,attnum,atttypid,atttypmod,attnotnull,attidentity,attgenerated,attisdropped FROM pg_attribute WHERE attrelid=ANY($1::oid[]) ORDER BY attrelid,attnum", relations),
        ("SELECT oid,adrelid,adnum,pg_get_expr(adbin,adrelid) FROM pg_attrdef WHERE adrelid=ANY($1::oid[]) ORDER BY oid", relations),
        ("SELECT oid,conrelid,confrelid,conname,convalidated,pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=ANY($1::oid[]) ORDER BY oid", schemas),
        ("SELECT oid,ev_class,pg_get_ruledef(oid) FROM pg_rewrite WHERE ev_class=ANY($1::oid[]) ORDER BY oid", relations),
        ("SELECT oid,tgrelid,tgenabled,pg_get_triggerdef(oid) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid=ANY($1::oid[]) ORDER BY oid", relations),
        ("SELECT oid,proname,prosrc,probin,proargtypes::text,prorettype,proconfig,proacl FROM pg_proc WHERE pronamespace=ANY($1::oid[]) ORDER BY oid", schemas),
        ("SELECT oid,polrelid,polname,polcmd,polroles,polqual::text,polwithcheck::text FROM pg_policy WHERE polrelid=ANY($1::oid[]) ORDER BY oid", relations),
        ("SELECT oid,typname,typtype,typrelid,typbasetype,typnotnull,typdefault FROM pg_type WHERE typnamespace=ANY($1::oid[]) ORDER BY oid", schemas),
    )
    return digest([[dict(r) for r in await conn.fetch(sql, args)] for sql, args in queries])


async def reset_cloned_shadows(conn, pgdata):
    """Discard old experimental tables ONLY inside a newly owned test clone."""
    await guard(conn, pgdata)
    if (not pgdata.parent.name.startswith('db-refactor-baseline-sequence-')
        or not (pgdata.parent/'migration-sequence-owner.json').is_file()):
        raise RuntimeError('Shadow reset requires a newly owned migration-sequence clone')
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_namespace
        WHERE nspname=ANY($1::text[]))""", ['chessism_legacy','chessism_archive','chessism_reads']):
        raise RuntimeError('Expected legacy source, not an already canonical copy')
    if await conn.fetchval("SELECT to_regclass('public.relational_storage_release') IS NOT NULL"):
        raise RuntimeError('Source already has a release marker')
    async with conn.transaction():
        before = await outside_catalog(conn)
        for schema in SHADOWS:
            await conn.execute(f'DROP SCHEMA IF EXISTS {qi(schema)} CASCADE')
        if await outside_catalog(conn) != before:
            raise RuntimeError('Shadow reset touched an external dependency; rolled back')
    return {'reset_schemas': list(SHADOWS), 'outside_catalog_unchanged': True}


async def validate_application_copies(conn, pgdata, publish):
    """Check omitted moves/months and complete active/archive game partition."""
    await guard(conn, pgdata)
    await publish('validating_active_moves_and_months')
    result = {}
    for table, source in (
        ('moves', f'SELECT m.* FROM public.moves m JOIN {R}.game g ON g.link=m.link'),
        ('months', 'SELECT * FROM public.months'),
    ):
        fields = await columns(conn, table)
        # IDs are unique on both sides. Full outer join verifies all values,
        # absent rows and extra rows without sorting the entire move corpus.
        left = ','.join('old.'+qi(c) for c in fields)
        right = ','.join('new.'+qi(c) for c in fields)
        mismatch = await conn.fetchval(f'''SELECT EXISTS(SELECT 1 FROM ({source}) old
            FULL JOIN {C}.{qi(table)} new USING(id)
            WHERE old.id IS NULL OR new.id IS NULL OR ROW({left}) IS DISTINCT FROM ROW({right}))''')
        if mismatch:
            raise RuntimeError(f'Application copy differs: {table}')
        result[table] = {'mismatches': 0}
    if await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM public.game g WHERE {standard()} IS NULL)'):
        raise RuntimeError('Unknown standard-game classification would omit source games')
    result['games_partition'] = {'mismatches': 0}
    return result


async def finish_private_sequence(socket, pgdata, publish, *, database='chessism_db', inject=False):
    """One transaction switches tables, refreshes summaries, then approves.

Failure in any refresh restores the old namespace, triggers, FKs and caches.
The separate shadow-build transaction remains available for diagnosis/retry.
    """
    from scripts.db_refactor_cutover import inspect_cutover
    from scripts.db_refactor_release_rehearsal import private_url
    from scripts.db_refactor_switch_rehearsal import promote_private_candidate, catalog_identity
    from scripts.db_refactor_corpus import finalize
    engine = create_async_engine(private_url(socket, database), isolation_level='REPEATABLE READ',
        connect_args={'server_settings': {'statement_timeout':'7200000'}})
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.execute(text('SET TRANSACTION READ ONLY'))
                driver = (await connection.get_raw_connection()).driver_connection
                inventory = await inspect_cutover(driver, pgdata)
                before = await catalog_identity(driver)
            try:
                async with connection.begin():
                    await publish('switching_private_tables_and_finalizing_atomically')
                    checks = await promote_private_candidate(connection, inventory, pgdata)
                    await connection.execute(text('''CREATE TABLE public.relational_storage_release (
                        version text PRIMARY KEY,application_ready boolean NOT NULL DEFAULT false,
                        rehearsal_only boolean NOT NULL DEFAULT true,created_at timestamptz NOT NULL DEFAULT now())'''))
                    await connection.execute(text("INSERT INTO public.relational_storage_release(version) VALUES('relational_v2')"))
                    summaries = await finalize(connection, pgdata, publish, inject=inject)
                    await connection.execute(text("""UPDATE public.relational_storage_release
                        SET application_ready=true WHERE version='relational_v2' AND rehearsal_only"""))
                return {'checks':checks, 'summaries':summaries, 'committed_to_private_copy':True}
            except BaseException:
                # The exception has exited/rolled back the cutover transaction.
                async with connection.begin():
                    if await catalog_identity(driver) != before:
                        raise RuntimeError('Failed cutover did not restore its catalog')
                raise
    finally:
        await engine.dispose()
