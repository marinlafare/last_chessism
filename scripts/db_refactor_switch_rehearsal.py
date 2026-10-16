"""Exercise canonical namespace promotion then ALWAYS ROLLBACK, private DB only.

No release marker, commit option, new data copy, sequence reset or live URL.
The production migration runner and service activation remain separate work.
"""
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.relational_storage import RelationalStorage,require_storage_contract
from chessism_api.database.home_summary_schema import install_relational_home_triggers
from scripts.db_refactor_candidate import C,A,V,guard,qi
from scripts.db_refactor_cutover import TABLE_MAP,NEW_TABLES,inspect_cutover
from scripts.db_refactor_workload import digest

LEGACY='chessism_legacy'
ARCHIVE='chessism_archive'
READS='chessism_reads'
LAYOUT=RelationalStorage('public',ARCHIVE,READS)
EXTERNAL_PLAYERS={'main_character_mode_summary','player_stats'}


class InjectedSwitchFailure(RuntimeError):
    pass


def validate_inventory(report):
    relations={(row['schema'],row['name']):row for row in report['relations']}
    for source,target in TABLE_MAP.items():
        for schema,name in (('public',source),(C,target)):
            row=relations.get((schema,name),{})
            if row.get('kind')!='r' or row.get('row_security') or row.get('force_row_security') or row.get('acl'):
                raise RuntimeError(f'Review table kind/security before switch: {schema}.{name}')
    for name in NEW_TABLES:
        if (C,name) not in relations or ('public',name) in relations:
            raise RuntimeError('Unexpected new dimension placement')
    for fk in report['foreign_keys']:
        if not fk['validated']:
            raise RuntimeError('Unvalidated foreign key needs review')
        if fk['action']=='retarget_active_foreign_key':
            if (fk['source_schema']!='public' or fk['source_table'] not in EXTERNAL_PLAYERS
                or fk['target_table']!='player' or fk['source_columns']!=['player_name']
                or fk['target_columns']!=['player_name']):
                raise RuntimeError('Unreviewed external foreign key')
    expected={f'home_summary_v1_{table}_{suffix}' for table in ('player','game','fen','game_fen_association')
              for suffix in ('rows','values')}
    if {r['name'] for r in report['triggers']}!=expected or len(report['triggers'])!=8:
        raise RuntimeError('Unexpected source triggers need review')
    if any(r['schema']!='public' or r['function_schema']!='public'
           or r['function']!='invalidate_home_summary_v1' or r['enabled']!='O' for r in report['triggers']):
        raise RuntimeError('Unexpected trigger function or enablement')
    if any(r['schema'] not in (V,'refactor_reference_v1') for r in report['dependent_views']):
        raise RuntimeError('Additional dependent views need explicit handling')
    if report['policies']:
        raise RuntimeError('Row security policies need explicit migration')
    if [(r['schema'],r['name'],r['arguments']) for r in report['functions']]!=[('public','invalidate_home_summary_v1','')]:
        raise RuntimeError('Additional stored functions need review for embedded SQL')


def retarget_sql(fk):
    """Reconstruct catalog options; never substitute text in arbitrary SQL."""
    actions={'a':'NO ACTION','r':'RESTRICT','c':'CASCADE','n':'SET NULL','d':'SET DEFAULT'}
    matches={'s':'SIMPLE','f':'FULL','p':'PARTIAL'}
    columns=','.join(map(qi,fk['source_columns']))
    target_columns=','.join(map(qi,fk['target_columns']))
    deferred=('DEFERRABLE INITIALLY '+('DEFERRED' if fk['deferred'] else 'IMMEDIATE')) if fk['deferrable'] else 'NOT DEFERRABLE'
    return (f'ALTER TABLE {qi(fk["source_schema"])}.{qi(fk["source_table"])} ADD CONSTRAINT {qi(fk["name"])} '
            f'FOREIGN KEY ({columns}) REFERENCES public.{qi(TABLE_MAP[fk["target_table"]])} ({target_columns}) '
            f'MATCH {matches[fk["match_type"]]} ON UPDATE {actions[fk["update_action"]]} '
            f'ON DELETE {actions[fk["delete_action"]]} {deferred}')


async def catalog_identity(driver):
    """Verify rollback by object identity/definitions, not just table names."""
    relations=await driver.fetch("""SELECT n.nspname,c.relname,c.oid::bigint,c.relkind::text
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=ANY($1::text[]) ORDER BY n.nspname,c.relname""",
        ['public',C,A,V,LEGACY,ARCHIVE,READS])
    constraints=await driver.fetch("""SELECT k.oid::bigint,k.conrelid::bigint,k.confrelid::bigint,
        k.conname,k.convalidated,pg_get_constraintdef(k.oid) AS definition
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=ANY($1::text[]) ORDER BY k.oid""",['public',C,A,LEGACY,ARCHIVE])
    triggers=await driver.fetch("""SELECT t.oid::bigint,t.tgrelid::bigint,t.tgname,pg_get_triggerdef(t.oid) AS definition
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE NOT t.tgisinternal AND n.nspname=ANY($1::text[]) ORDER BY t.oid""",['public',C,A,LEGACY,ARCHIVE])
    sequences=await driver.fetch("""SELECT schemaname,sequencename,last_value FROM pg_sequences
        WHERE schemaname=ANY($1::text[]) ORDER BY schemaname,sequencename""",['public',C,A,LEGACY,ARCHIVE])
    return digest([[dict(r) for r in rows] for rows in (relations,constraints,triggers,sequences)])


async def promote_private_candidate(connection,inventory,pgdata,*,inject=False):
    """Shared switch; caller owns transaction and a private path or live permit."""
    if not connection.in_transaction():
        raise RuntimeError('Promotion requires a caller-owned transaction')
    # SQLAlchemy begins the driver's transaction lazily on the first SQL call.
    await connection.execute(text('SELECT 1'))
    driver=(await connection.get_raw_connection()).driver_connection
    await guard(driver,pgdata)
    if not driver.is_in_transaction():
        raise RuntimeError('Promotion requires a caller-owned transaction')
    validate_inventory(inventory)
    await connection.execute(text("SET LOCAL lock_timeout='5s'"))
    await driver.execute(f'CREATE SCHEMA {LEGACY}')
    for trigger in inventory['triggers']:
        await driver.execute(f'DROP TRIGGER {qi(trigger["name"])} ON public.{qi(trigger["table"])}')
    external=[r for r in inventory['foreign_keys'] if r['action']=='retarget_active_foreign_key']
    for fk in inventory['foreign_keys']:
        if fk['action'] in ('retarget_active_foreign_key','detach_snapshot_from_active_parent'):
            await driver.execute(f'ALTER TABLE {qi(fk["source_schema"])}.{qi(fk["source_table"])} DROP CONSTRAINT {qi(fk["name"])}')
    for source in TABLE_MAP:
        await driver.execute(f'ALTER TABLE public.{qi(source)} SET SCHEMA {LEGACY}')
    if inject:
        raise InjectedSwitchFailure('Failure after legacy tables move, before promotion')
    for target in (*TABLE_MAP.values(),*NEW_TABLES):
        await driver.execute(f'ALTER TABLE {C}.{qi(target)} SET SCHEMA public')
    await driver.execute(f'ALTER SCHEMA {A} RENAME TO {ARCHIVE}')
    await driver.execute(f'ALTER SCHEMA {V} RENAME TO {READS}')
    for fk in external:
        await driver.execute(retarget_sql(fk))
    await require_storage_contract(connection,LAYOUT)
    await install_relational_home_triggers(connection,LAYOUT)
    # No approval marker: normalized startup must remain blocked.
    if await driver.fetchval("SELECT to_regclass('public.relational_storage_release') IS NOT NULL"):
        raise RuntimeError('Release marker already exists')
    stale=await driver.fetchval("""SELECT count(*) FROM pg_constraint k
        JOIN pg_class child ON child.oid=k.conrelid JOIN pg_namespace cn ON cn.oid=child.relnamespace
        JOIN pg_class parent ON parent.oid=k.confrelid JOIN pg_namespace pn ON pn.oid=parent.relnamespace
        WHERE k.contype='f' AND cn.nspname<>$1 AND pn.nspname=$1""",LEGACY)
    if stale:
        raise RuntimeError('An active FK still references retained data')
    stale_views=await driver.fetchval("""SELECT count(*) FROM pg_depend d JOIN pg_rewrite r
        ON d.classid='pg_rewrite'::regclass AND r.oid=d.objid
        JOIN pg_class v ON v.oid=r.ev_class JOIN pg_namespace vn ON vn.oid=v.relnamespace
        JOIN pg_class s ON d.refclassid='pg_class'::regclass AND s.oid=d.refobjid
        JOIN pg_namespace sn ON sn.oid=s.relnamespace WHERE vn.nspname=$1 AND sn.nspname=$2""",READS,LEGACY)
    if stale_views:
        raise RuntimeError('A canonical read view still references retained data')
    triggers=await driver.fetchval("SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname LIKE 'home_summary_v2_%'")
    if triggers!=8:
        raise RuntimeError('Expected eight numeric summary triggers')
    await connection.execute(text(f'SET LOCAL search_path={LAYOUT.search_path}'))
    await connection.execute(text('SELECT link,white,black,mode FROM game ORDER BY link LIMIT 1'))
    return ['canonical_storage_contract','external_foreign_keys','read_view_dependencies',
                             'eight_numeric_triggers','legacy_shaped_read','no_release_marker']



async def exercise_switch(socket,pgdata,publish,*,database='chessism_db'):
    engine=create_async_engine(URL.create('postgresql+asyncpg',username='chessism_user',database=database,
        port=55432,query={'host':str(socket)}),connect_args={'server_settings':{'statement_timeout':'120000'}})
    try:
        async with engine.connect() as connection:
            driver=(await connection.get_raw_connection()).driver_connection
            async with connection.begin():
                await connection.execute(text('SET TRANSACTION READ ONLY'))
                inventory=await inspect_cutover(driver,pgdata)
                validate_inventory(inventory)
                original=await catalog_identity(driver)
                if await driver.fetchval('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=ANY($1::text[]))',
                                         [LEGACY,ARCHIVE,READS]):
                    raise RuntimeError('Canonical/retained namespace already exists')
                if await driver.fetchval("SELECT to_regclass('public.relational_storage_release') IS NOT NULL"):
                    raise RuntimeError('A release marker must not exist in this rehearsal')
            report={'live_database_changed':False,'production_ready':False,'committed':False,'checks':[]}
            for inject in (True,False):
                await publish('rehearsing_injected_switch_rollback' if inject else 'rehearsing_complete_switch_rollback')
                tx=await connection.begin()
                try:
                    report['checks'].extend(await promote_private_candidate(connection,inventory,pgdata,inject=inject))
                except InjectedSwitchFailure:
                    if not inject: raise
                    report['checks'].append('injected_failure_reached')
                finally:
                    # This is the only transaction exit, including success.
                    await tx.rollback()
                async with connection.begin():
                    await connection.execute(text('SET TRANSACTION READ ONLY'))
                    assert await catalog_identity(driver)==original,'Catalog/sequence identity changed after rollback'
                report['checks'].append('injected_rollback_exact' if inject else 'complete_rollback_exact')
            return report
    finally:
        await engine.dispose()
