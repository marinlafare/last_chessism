"""Approved live transaction, also exercised against disposable PostgreSQL.

No service management, cloud calls, shadow reuse or automatic table deletion.
All table copies and the switch commit together; errors roll back together.
"""
from sqlalchemy import text

from scripts.db_refactor_candidate import C,A,R,V,qi,standard,prepare_reference,build_candidate
from scripts.db_refactor_live_target import identity,authorize,require_no_clients,require_shared_memory
from scripts.db_refactor_cutover import collect_inventory
from scripts.db_refactor_switch_rehearsal import LEGACY,ARCHIVE,READS,promote_private_candidate,catalog_identity


async def preflight(conn, expected, observer_pid):
    current = await identity(conn)
    if set(expected) != {'database','pgdata','port','system_identifier'} or any(
        current[key] != value for key,value in expected.items()
    ):
        raise RuntimeError('Database identity does not match the approved target')
    shared_memory_bytes = await require_shared_memory(conn)
    await require_no_clients(conn,(current['pid'],observer_pid))
    if await conn.fetchval('SELECT pg_is_in_recovery()'):
        raise RuntimeError('Migration requires the primary database')
    if await conn.fetchval('SELECT EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE database=current_database())'):
        raise RuntimeError('Prepared transactions must be resolved before migration')
    if await conn.fetchval('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=ANY($1::text[]))',[C,A,R,V,LEGACY,ARCHIVE,READS]):
        raise RuntimeError('Migration namespaces already exist; no automatic deletion or retry')
    if await conn.fetchval("SELECT to_regclass('public.relational_storage_release') IS NOT NULL"):
        raise RuntimeError('Release marker already exists; inspect before retrying')
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM public.cloud_analysis_job
        WHERE status NOT IN ('complete','cancelled','failed'))"""):
        raise RuntimeError('Cloud analysis is still active; finish it before maintenance')
    if await conn.fetchval('SELECT EXISTS(SELECT 1 FROM public.game WHERE fens_processing)'):
        raise RuntimeError('FEN processing claims remain; drain/check local work first')
    if await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM public.game g WHERE {standard()} IS NULL)'):
        raise RuntimeError('Unknown game classification requires review')
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('p','f','m'))"):
        raise RuntimeError('Partitioned, foreign or materialized tables require a separate lock plan')
    tables = await conn.fetch("""SELECT relname,oid::bigint FROM pg_class
        WHERE relnamespace='public'::regnamespace AND relkind='r' ORDER BY relname""")
    return {'identity':current,'tables':[dict(row) for row in tables],
            'postgres_shared_memory_bytes':shared_memory_bytes}


async def migrate(connection, expected, observer_pid, publish, checkpoint, *, inject=False):
    """The caller supplies a fresh SQLAlchemy connection, never a live DSN here."""
    from scripts.db_refactor_application_checks import prepare_application_copy
    from scripts.db_refactor_tuning import apply_tuning
    from scripts.db_refactor_migration_sequence import validate_application_copies
    from scripts.db_refactor_corpus import finalize
    from chessism_api.database.relational_storage import describe_contract
    from scripts.db_refactor_switch_rehearsal import LAYOUT

    async with connection.begin():
        await connection.execute(text('SET TRANSACTION READ ONLY'))
        driver = (await connection.get_raw_connection()).driver_connection
        before = await preflight(driver,expected,observer_pid)
        original = await catalog_identity(driver)
    committed = False
    try:
        async with connection.begin():
            await connection.execute(text("SET LOCAL lock_timeout='5s'"))
            await connection.execute(text("SET LOCAL statement_timeout='2h'"))
            # Acquire all source locks BEFORE the first SELECT establishes the
            # repeatable-read snapshot. No writer can slip between table copies.
            tables = ','.join('public.'+qi(row['relname']) for row in before['tables'])
            await connection.execute(text(f'LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE'))
            target = await authorize(driver,expected,observer_pid)
            current = await preflight(driver,expected,observer_pid)
            if current != before:
                raise RuntimeError('Source catalog changed while acquiring maintenance locks')
            if await driver.fetchval('SHOW transaction_isolation') != 'repeatable read':
                raise RuntimeError('Live migration requires REPEATABLE READ')

            async def stage(phase, **details):
                await target.verify(driver)
                await checkpoint()
                await publish(phase, **details)

            await stage('source_tables_locked',postgres_shared_memory_bytes=before['postgres_shared_memory_bytes'])
            reference = await prepare_reference(driver,target)
            rows = await build_candidate(driver,target,stage)
            await apply_tuning(driver,target,stage)
            await prepare_application_copy(driver,target,stage)
            rows.update(await validate_application_copies(driver,target,stage))
            await stage('switching_live_tables',row_checks=len(rows))
            inventory = await collect_inventory(driver,target)
            checks = await promote_private_candidate(connection,inventory,target)
            await connection.execute(text('''CREATE TABLE public.relational_storage_release (
                version text PRIMARY KEY,application_ready boolean NOT NULL DEFAULT false,
                rehearsal_only boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now())'''))
            await connection.execute(text("INSERT INTO public.relational_storage_release(version) VALUES('relational_v2')"))
            summaries = await finalize(connection,target,stage,inject=inject)
            contract = await describe_contract(connection,LAYOUT)
            await stage('ready_to_commit_live_migration')
            await connection.execute(text("""UPDATE public.relational_storage_release SET application_ready=true
                WHERE version='relational_v2' AND NOT rehearsal_only"""))
        committed = True
        return {'committed':True,'reference':reference,'rows':rows,'checks':checks,
                'summaries':summaries,'contract':contract,'legacy_tables_retained':True}
    except BaseException:
        if not committed and not connection.invalidated:
            async with connection.begin():
                if await catalog_identity(driver) != original:
                    raise RuntimeError('Migration outcome uncertain; keep services stopped and inspect database')
            await publish('live_transaction_rolled_back',rollback_verified=True)
        raise
