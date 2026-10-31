"""Read-only dependency inventory for the expanded game/player cutover.

This is NOT a migration runner. In particular, no table/constraint/trigger is
changed and no release marker is installed by this module.
"""
from scripts.db_refactor_candidate import C, A, V, guard


TABLE_MAP = {
    'player': 'player', 'game': 'game', 'game_player': 'game_player',
    'moves': 'moves', 'months': 'months',
    'game_fen_association': 'game_fen_association',
    'player_position_frequency': 'player_position_frequency',
    'game_player_salience': 'game_player_salience',
    'game_player_engine_summary': 'game_player_engine_summary',
    'player_salience_summary': 'player_salience_summary',
    'player_salience_pending_game': 'player_salience_pending_game',
    'game_opening': 'game_opening',
    'game_analysis_summary': 'game_analysis_coverage',
}
NEW_TABLES = ('game_mode', 'opening_sequence')


def foreign_key_action(row):
    source_is_old = row['source_schema'] == 'public' and row['source_table'] in TABLE_MAP
    target_is_old = row['target_schema'] == 'public' and row['target_table'] in TABLE_MAP
    if source_is_old and target_is_old:
        return 'retain_with_legacy_snapshot'
    if source_is_old:
        return 'detach_snapshot_from_active_parent'
    if target_is_old:
        return 'retarget_active_foreign_key'
    return 'retain_normalized_reference'


async def inspect_cutover(conn, pgdata):
    await guard(conn, pgdata)
    # Prevent this diagnostic helper from silently doing work in a writer txn.
    if await conn.fetchval('SHOW transaction_read_only') != 'on':
        raise RuntimeError('Cutover inventory requires a read-only transaction')
    return await collect_inventory(conn, pgdata)


async def collect_inventory(conn, pgdata):
    """Catalog reads also used inside the approved atomic live transaction."""
    await guard(conn, pgdata)
    schemas = ['public', C, A, V]
    tables = list(TABLE_MAP) + list(TABLE_MAP.values()) + list(NEW_TABLES) + ['fen']
    report = {'version': 1, 'mode': 'dependency_inventory_only',
              'live_database_changed': False, 'production_ready': False,
              'table_mapping': TABLE_MAP, 'new_tables': list(NEW_TABLES)}
    queries = {
        'relations': """SELECT n.nspname AS schema,c.relname AS name,c.relkind::text AS kind,
            pg_get_userbyid(c.relowner) AS owner,c.relrowsecurity AS row_security,
            c.relforcerowsecurity AS force_row_security,c.relacl::text AS acl,
            c.reltuples::bigint AS estimated_rows,pg_total_relation_size(c.oid) AS bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=ANY($1::text[]) AND c.relname=ANY($2::text[])
            AND c.relkind IN ('r','p','v','m') ORDER BY n.nspname,c.relname""",
        'columns': """SELECT table_schema AS schema,table_name AS table,column_name AS name,
            data_type,udt_name,is_nullable,is_identity,column_default
            FROM information_schema.columns
            WHERE table_schema=ANY($1::text[]) AND table_name=ANY($2::text[])
            ORDER BY table_schema,table_name,ordinal_position""",
        'foreign_keys': """SELECT sn.nspname AS source_schema,s.relname AS source_table,
            tn.nspname AS target_schema,t.relname AS target_table,k.conname AS name,
            k.convalidated AS validated,k.condeferrable AS deferrable,k.condeferred AS deferred,
            k.confdeltype::text AS delete_action,k.confupdtype::text AS update_action,k.confmatchtype::text AS match_type,
            pg_get_constraintdef(k.oid) AS definition,
            ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(id,n)
                JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=x.id ORDER BY x.n) AS source_columns,
            ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(id,n)
                JOIN pg_attribute a ON a.attrelid=k.confrelid AND a.attnum=x.id ORDER BY x.n) AS target_columns
            FROM pg_constraint k
            JOIN pg_class s ON s.oid=k.conrelid JOIN pg_namespace sn ON sn.oid=s.relnamespace
            JOIN pg_class t ON t.oid=k.confrelid JOIN pg_namespace tn ON tn.oid=t.relnamespace
            WHERE k.contype='f' AND (
                (sn.nspname=ANY($1::text[]) AND s.relname=ANY($2::text[])) OR
                (tn.nspname=ANY($1::text[]) AND t.relname=ANY($2::text[])))
            ORDER BY sn.nspname,s.relname,k.conname""",
        'triggers': """SELECT n.nspname AS schema,c.relname AS table,t.tgname AS name,
            t.tgenabled::text AS enabled,pn.nspname AS function_schema,p.proname AS function,
            pg_get_triggerdef(t.oid) AS definition
            FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid
            JOIN pg_namespace pn ON pn.oid=p.pronamespace
            WHERE NOT t.tgisinternal AND n.nspname=ANY($1::text[]) AND c.relname=ANY($2::text[])
            ORDER BY n.nspname,c.relname,t.tgname""",
        'dependent_views': """SELECT DISTINCT vn.nspname AS schema,v.relname AS name,
            sn.nspname AS source_schema,s.relname AS source_table,pg_get_viewdef(v.oid,true) AS definition
            FROM pg_depend d JOIN pg_rewrite r ON d.classid='pg_rewrite'::regclass AND r.oid=d.objid
            JOIN pg_class v ON v.oid=r.ev_class JOIN pg_namespace vn ON vn.oid=v.relnamespace
            JOIN pg_class s ON d.refclassid='pg_class'::regclass AND s.oid=d.refobjid
            JOIN pg_namespace sn ON sn.oid=s.relnamespace
            WHERE v.oid<>s.oid AND v.relkind IN ('v','m')
                AND sn.nspname=ANY($1::text[]) AND s.relname=ANY($2::text[])
            ORDER BY vn.nspname,v.relname,sn.nspname,s.relname""",
        'sequences': """SELECT n.nspname AS table_schema,t.relname AS table,a.attname AS column,
            ns.nspname AS sequence_schema,s.relname AS sequence,d.deptype::text AS dependency,
            q.seqstart AS start,q.seqincrement AS increment,q.seqmin AS minimum,q.seqmax AS maximum
            FROM pg_depend d JOIN pg_class s ON d.classid='pg_class'::regclass AND s.oid=d.objid AND s.relkind='S'
            JOIN pg_sequence q ON q.seqrelid=s.oid JOIN pg_namespace ns ON ns.oid=s.relnamespace
            JOIN pg_class t ON d.refclassid='pg_class'::regclass AND t.oid=d.refobjid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
            WHERE d.deptype IN ('a','i') AND n.nspname=ANY($1::text[]) AND t.relname=ANY($2::text[])
            ORDER BY n.nspname,t.relname,a.attname""",
    }
    for label, query in queries.items():
        report[label] = [dict(row) for row in await conn.fetch(query, schemas, tables)]
    # PostgreSQL cannot dependency-track dynamic PL/pgSQL text. Preserve every
    # user function in these schemas for explicit review; never declare a
    # dependency list complete solely because pg_depend found no references.
    report['functions'] = [dict(row) for row in await conn.fetch("""
        SELECT n.nspname AS schema,p.proname AS name,
            pg_get_function_identity_arguments(p.oid) AS arguments,
            l.lanname AS language,pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_language l ON l.oid=p.prolang
        WHERE n.nspname=ANY($1::text[]) AND p.prokind IN ('f','p')
        ORDER BY n.nspname,p.proname,p.oid""", schemas)]
    report['policies'] = [dict(row) for row in await conn.fetch("""
        SELECT schemaname AS schema,tablename AS table,policyname,roles,cmd,qual,with_check
        FROM pg_policies WHERE schemaname=ANY($1::text[]) AND tablename=ANY($2::text[])
        ORDER BY schemaname,tablename,policyname""", schemas, tables)]
    for row in report['foreign_keys']:
        row['action'] = foreign_key_action(row)
    report['limitations'] = [
        'This catalogs dependencies on the restored snapshot, not current live state.',
        'Foreign-key orphan scans, sequence high-water checks and all switch/rollback tests are still required.',
        'Functions using dynamic SQL and application entry points require explicit source review.',
        'No DROP, ALTER, release approval, service restart or data migration was executed.',
    ]
    return report
