"""Approved compact migration transaction; also exercised on a tiny private DB.

No service management or backup cleanup. The column changes and release marker
commit together, only after both exact source/target schema contracts validate.
"""
from sqlalchemy import text

from chessism_api.database.relational_storage import RelationalStorage, describe_contract
from chessism_api.database.backup_contract import contract_differences
from scripts.compact_db_migration import migrate_authorized
from scripts.db_refactor_live_target import identity, authorize, require_no_clients, require_shared_memory

V2 = RelationalStorage('public', 'chessism_archive', 'chessism_reads')
V3 = RelationalStorage('public', 'chessism_archive', 'chessism_reads', 'relational_v3')


async def contract(connection, layout):
    # pg_get_* deparsing depends on search_path; use the same application path
    # as the verified backup and accepted candidate, not the DDL's public path.
    await connection.execute(text("SELECT set_config('search_path',:path,true)"), {'path':layout.search_path})
    return await describe_contract(connection, layout)


async def preflight(connection, expected, observer_pid, source_contract):
    driver = (await connection.get_raw_connection()).driver_connection
    current = await identity(driver)
    if set(expected) != {'database','pgdata','port','system_identifier'} or any(
            current[key] != value for key,value in expected.items()):
        raise RuntimeError('Database identity does not match the approved compact target')
    await require_shared_memory(driver)
    await require_no_clients(driver, (current['pid'],observer_pid))
    if await driver.fetchval('SELECT pg_is_in_recovery()'):
        raise RuntimeError('Primary database required')
    if await driver.fetchval('SELECT EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE database=current_database())'):
        raise RuntimeError('Prepared transactions must be resolved first')
    release = [dict(r) for r in await driver.fetch(
        'SELECT version,application_ready,rehearsal_only FROM public.relational_storage_release')]
    if release != [{'version':'relational_v2','application_ready':True,'rehearsal_only':False}]:
        raise RuntimeError('Expected the approved v2 release; no automatic retries after commit')
    if await driver.fetchval("SELECT EXISTS(SELECT 1 FROM public.cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached'))"):
        raise RuntimeError('Cloud work must finish before maintenance, including recovery of failed jobs')
    if await driver.fetchval('SELECT EXISTS(SELECT 1 FROM public.game WHERE fens_processing)'):
        raise RuntimeError('FEN-processing claims remain')
    actual = await contract(connection,V2)
    if contract_differences(source_contract,actual):
        raise RuntimeError('Live schema no longer matches the verified source snapshot')
    return actual


async def migrate(connection, expected, observer_pid, source_contract, candidate_contract,
                  publish, checkpoint, *, inject_failure=False):
    async with connection.begin():
        await connection.execute(text('SET TRANSACTION READ ONLY'))
        original = await preflight(connection,expected,observer_pid,source_contract)
    driver = (await connection.get_raw_connection()).driver_connection
    committed = False
    try:
        async with connection.begin():
            await connection.execute(text("SET LOCAL lock_timeout='5s';"))
            await connection.execute(text("SET LOCAL statement_timeout='2h'"))
            target = await authorize(driver,expected,observer_pid)
            await connection.execute(text('LOCK TABLE public.relational_storage_release IN ACCESS EXCLUSIVE MODE'))
            await preflight(connection,expected,observer_pid,source_contract)

            async def stage(phase, **details):
                await target.verify(driver)
                await checkpoint()
                await publish(phase,**details)

            await stage('live_source_verified')
            proofs = await migrate_authorized(driver,target,stage)
            await stage('validating_live_compact_contract')
            actual = await contract(connection,V3)
            differences = contract_differences(candidate_contract,actual)
            if differences:
                raise RuntimeError('Migrated schema differs from accepted candidate: '+', '.join(differences))
            # ALTER TYPE is lossless after full prechecks; still verify preserved
            # large-table cardinalities before approving the release.
            for table,key in (('fen','fen_integer_values'),('fen_continuation','continuation_integer_values'),
                              ('game_fen_association','association_sides')):
                await stage('verifying_live_rows_'+table)
                count = await driver.fetchval('SELECT count(*) FROM public.'+table)
                if count != proofs[key]['rows']:
                    raise RuntimeError('Changed row count: '+table)
            for table in ('fen','fen_continuation','moves','game_fen_association','months','player_stats',
                          'main_character_mode_summary','home_titled_player_summary'):
                await stage('analyzing_live_'+table)
                await driver.execute('ANALYZE public.'+table)
            await driver.execute("UPDATE public.relational_storage_release SET version='relational_v3',created_at=now() WHERE version='relational_v2'")
            if inject_failure:
                raise RuntimeError('Injected compact live failure before commit')
            await stage('ready_to_commit_compact_migration')
        committed = True
        return {'committed':True,'proofs':proofs,'contract':actual,'profile':'relational_v3'}
    except BaseException:
        if not committed and not connection.invalidated:
            async with connection.begin():
                if contract_differences(original,await contract(connection,V2)):
                    raise RuntimeError('Rollback outcome uncertain; keep services stopped')
                release = await driver.fetchval("SELECT version FROM public.relational_storage_release")
                if release != 'relational_v2':
                    raise RuntimeError('Release rollback uncertain; keep services stopped')
            await publish('compact_live_transaction_rolled_back',rollback_verified=True)
        raise
