"""Offline, atomic migration of game associations and player frequencies.

The caller owns maintenance coordination and verified backup selection. Never
called by application startup. Both old tables remain until backup acceptance.
"""
from .fen_identity import validate_identity
from .fen_identity_migration import MIGRATION_LOCK
from .fen_reference_identity import SHAPE_SQL, TABLES, schema_version, validate_references

LEGACY = {table: table + '_legacy_text' for table in TABLES}
NEW = {table: table + '_numeric_v1' for table in TABLES}
HOME_TRIGGERS = {
    'home_summary_v1_game_fen_association_rows',
    'home_summary_v1_game_fen_association_values',
}


async def sizes(connection):
    return [dict(r) for r in await connection.fetch("""
        SELECT c.relname AS name,pg_table_size(c.oid) AS table_bytes,
            pg_indexes_size(c.oid) AS index_bytes,pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=current_schema() AND c.relname=ANY($1::text[])
    """, list(TABLES) + list(LEGACY.values()))]


async def compare(connection, table, original, replacement):
    if table not in TABLES or original not in (table, LEGACY[table]) or replacement not in (table, NEW[table]):
        raise ValueError('Unexpected comparison relations')
    old_count = await connection.fetchval(f'SELECT count(*) FROM {original}')
    new_count = await connection.fetchval(f'SELECT count(*) FROM {replacement}')
    if table == TABLES[0]:
        join = 'new.id=old.id'
        values = 'ROW(new.game_link,new.fen_id,new.n_move,new.move_color) IS DISTINCT FROM ROW(old.game_link,f.fen_id,old.n_move,old.move_color)'
        missing = 'new.id IS NULL'
    else:
        join = 'new.player_name=old.player_name AND new.player_color=old.player_color AND new.fen_id=f.fen_id'
        values = 'ROW(new.games_with_position,new.total_occurrences) IS DISTINCT FROM ROW(old.games_with_position,old.total_occurrences)'
        missing = 'new.fen_id IS NULL'
    mismatches = await connection.fetchval(f"""
        SELECT count(*) FROM {original} old LEFT JOIN fen f ON f.fen=old.fen_fen
        LEFT JOIN {replacement} new ON {join}
        WHERE f.fen_id IS NULL OR {missing} OR {values}
    """)
    if old_count != new_count or mismatches:
        raise RuntimeError(f'{table} verification failed: old={old_count}, new={new_count}, mismatches={mismatches}')
    return {'original_rows': old_count, 'numeric_rows': new_count, 'mismatches': mismatches}


async def preflight(connection):
    await validate_identity(connection)
    expected_columns = {
        TABLES[0]: {'id','game_link','fen_fen','n_move','move_color'},
        TABLES[1]: {'player_name','player_color','fen_fen','games_with_position','total_occurrences'},
    }
    supported_constraints = {
        TABLES[0]: {'game_fen_association_pkey','_game_fen_move_color_uc',
                    'game_fen_association_game_link_fkey','game_fen_association_fen_fen_fkey'},
        TABLES[1]: {'player_position_frequency_pkey','player_position_frequency_player_name_fkey',
                    'player_position_frequency_fen_fen_fkey','player_position_frequency_color',
                    'player_position_frequency_games_positive','player_position_frequency_occurrences_valid'},
    }
    supported_indexes = {
        TABLES[0]: {'game_fen_association_pkey','_game_fen_move_color_uc',
                    'ix_game_fen_association_fen_fen','ix_game_fen_association_game_link',
                    'ix_game_fen_association_game_order'},
        TABLES[1]: {'player_position_frequency_pkey'},
    }
    trigger_definitions = []
    for table in TABLES:
        if await connection.fetchval('SELECT to_regclass($1) IS NOT NULL OR to_regclass($2) IS NOT NULL', LEGACY[table], NEW[table]):
            raise RuntimeError('A migration relation already exists; inspect before retrying')
        columns = {r['column_name'] for r in await connection.fetch(SHAPE_SQL) if r['table_name'] == table}
        if columns != expected_columns[table]:
            raise RuntimeError('Unexpected reference columns; migration needs review')
        constraints = {r['conname'] for r in await connection.fetch(
            'SELECT conname FROM pg_constraint WHERE conrelid=$1::regclass', table)}
        indexes = {r['relname'] for r in await connection.fetch(
            'SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=$1::regclass', table)}
        if constraints - supported_constraints[table] or indexes - supported_indexes[table]:
            raise RuntimeError('Additional constraints/indexes need a reviewed migration strategy')
        dependencies = await connection.fetchval("""
            SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE confrelid=$1::regclass)
              OR EXISTS(SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
                        WHERE d.refobjid=$1::regclass AND d.classid='pg_rewrite'::regclass)
              OR EXISTS(SELECT 1 FROM pg_class WHERE oid=$1::regclass
                        AND (relacl IS NOT NULL OR relrowsecurity OR relkind<>'r'))
        """, table)
        if dependencies:
            raise RuntimeError(f'{table} dependencies need a reviewed migration strategy')
        triggers = await connection.fetch("""
            SELECT t.tgname,pg_get_triggerdef(t.oid) AS definition,p.proname,
                   t.tgtype,t.tgenabled::text AS enabled,
                   ARRAY(SELECT a.attname::text FROM unnest(t.tgattr::smallint[]) x(id)
                         JOIN pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=x.id) AS columns
            FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
            WHERE t.tgrelid=$1::regclass AND NOT t.tgisinternal
        """, table)
        if triggers and (table != TABLES[0] or {r['tgname'] for r in triggers} != HOME_TRIGGERS):
            raise RuntimeError('Unexpected user triggers on reference table')
        for trigger in triggers:
            is_update = trigger['tgname'].endswith('_values')
            if (trigger['proname'] != 'invalidate_home_summary_v1' or trigger['enabled'] != 'O'
                    or trigger['tgtype'] != (16 if is_update else 44)
                    or set(trigger['columns']) != ({'game_link','fen_fen'} if is_update else set())):
                raise RuntimeError('Homepage trigger differs from its supported definition')
            trigger_definitions.append(trigger['definition'])
    return trigger_definitions


async def migrate(connection, publish):
    if not await connection.fetchval('SELECT pg_try_advisory_lock($1)', MIGRATION_LOCK):
        raise RuntimeError('Database schema maintenance is already active')
    try:
        await connection.execute("SET lock_timeout='5s'")
        await connection.execute("SET statement_timeout='2h'")
        await connection.execute("SET work_mem='64MB'")
        await connection.execute("SET maintenance_work_mem='256MB'")
        await connection.execute('SET max_parallel_workers_per_gather=2')
        version = schema_version(await connection.fetch(SHAPE_SQL))
        if version == 'numeric_v1':
            return {'already_migrated': True, 'identity': await validate_references(connection)}
        if version != 'text_v1':
            raise RuntimeError('Expected both original text-reference tables')
        before = await sizes(connection)
        async with connection.transaction():
            # Parents cannot change during copying; old associations cannot be
            # read/written by an accidentally resumed application mid-cutover.
            await connection.execute('LOCK TABLE fen, game, player IN SHARE MODE')
            await connection.execute('LOCK TABLE game_fen_association, player_position_frequency IN ACCESS EXCLUSIVE MODE')
            triggers = await preflight(connection)
            old_sequence = await connection.fetchval("SELECT pg_get_serial_sequence('game_fen_association','id')")
            if not old_sequence:
                raise RuntimeError('Original association ID sequence is missing')
            sequence_query = await connection.fetchval("SELECT format('SELECT last_value,is_called FROM %s',$1::regclass)", old_sequence)
            old_sequence_state = await connection.fetchrow(sequence_query)
            await publish('copying_game_associations', sizes_before=before)
            await connection.execute("""
                CREATE TABLE game_fen_association_numeric_v1 (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY NOT NULL,
                    game_link BIGINT NOT NULL,fen_id BIGINT NOT NULL,
                    n_move INTEGER NOT NULL,move_color VARCHAR(5) NOT NULL);
                INSERT INTO game_fen_association_numeric_v1 (id,game_link,fen_id,n_move,move_color)
                SELECT a.id,a.game_link,f.fen_id,a.n_move,a.move_color
                FROM game_fen_association a JOIN fen f ON f.fen=a.fen_fen;
            """)
            await publish('copying_player_frequencies')
            await connection.execute("""
                CREATE TABLE player_position_frequency_numeric_v1 (
                    player_name VARCHAR NOT NULL,player_color VARCHAR(5) NOT NULL,fen_id BIGINT NOT NULL,
                    games_with_position BIGINT NOT NULL,total_occurrences BIGINT NOT NULL,
                    CONSTRAINT player_position_frequency_color CHECK (player_color IN ('white','black')),
                    CONSTRAINT player_position_frequency_games_positive CHECK (games_with_position>1),
                    CONSTRAINT player_position_frequency_occurrences_valid CHECK (total_occurrences>=games_with_position));
                INSERT INTO player_position_frequency_numeric_v1
                SELECT p.player_name,p.player_color,f.fen_id,p.games_with_position,p.total_occurrences
                FROM player_position_frequency p JOIN fen f ON f.fen=p.fen_fen;
            """)
            await publish('indexing_numeric_references')
            await connection.execute("""
                ALTER TABLE game_fen_association_numeric_v1 ADD PRIMARY KEY (id);
                ALTER TABLE game_fen_association_numeric_v1 ADD CONSTRAINT game_fen_numeric_move_uc UNIQUE(game_link,fen_id,n_move,move_color);
                CREATE INDEX game_fen_numeric_fen_idx ON game_fen_association_numeric_v1(fen_id);
                CREATE INDEX game_fen_numeric_game_idx ON game_fen_association_numeric_v1(game_link);
                CREATE INDEX game_fen_numeric_order_idx ON game_fen_association_numeric_v1(game_link,n_move,move_color);
                ALTER TABLE player_position_frequency_numeric_v1 ADD PRIMARY KEY(player_name,player_color,fen_id);
                ALTER TABLE game_fen_association_numeric_v1 ADD CONSTRAINT fk_game_fen_association_fen_id FOREIGN KEY(fen_id) REFERENCES fen(fen_id) NOT VALID;
                ALTER TABLE game_fen_association_numeric_v1 ADD CONSTRAINT game_fen_association_game_link_fkey FOREIGN KEY(game_link) REFERENCES game(link) NOT VALID;
                ALTER TABLE player_position_frequency_numeric_v1 ADD CONSTRAINT fk_player_position_frequency_fen_id FOREIGN KEY(fen_id) REFERENCES fen(fen_id) ON DELETE CASCADE NOT VALID;
                ALTER TABLE player_position_frequency_numeric_v1 ADD CONSTRAINT player_position_frequency_player_name_fkey FOREIGN KEY(player_name) REFERENCES player(player_name) ON DELETE CASCADE NOT VALID;
            """)
            await publish('validating_foreign_keys')
            for table, constraint in (
                (NEW[TABLES[0]],'fk_game_fen_association_fen_id'),
                (NEW[TABLES[0]],'game_fen_association_game_link_fkey'),
                (NEW[TABLES[1]],'fk_player_position_frequency_fen_id'),
                (NEW[TABLES[1]],'player_position_frequency_player_name_fkey'),
            ):
                await connection.execute(f'ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}')
            await publish('comparing_all_reference_rows')
            comparison = {table: await compare(connection, table, table, NEW[table]) for table in TABLES}
            maximum = await connection.fetchval('SELECT max(id) FROM game_fen_association_numeric_v1') or 0
            used = max(maximum, old_sequence_state['last_value'] if old_sequence_state['is_called'] else old_sequence_state['last_value'] - 1)
            await connection.execute("SELECT setval(pg_get_serial_sequence('game_fen_association_numeric_v1','id'),$1,$2)", max(1,used), used>0)
            await publish('swapping_reference_tables', comparison=comparison)
            for table in TABLES:
                # Retained snapshots must neither block later game deletions nor
                # change through cascading deletes after application resumes.
                fks = await connection.fetch("SELECT conname FROM pg_constraint WHERE conrelid=$1::regclass AND contype='f'", table)
                for fk in fks:
                    quoted = '"' + fk['conname'].replace('"','""') + '"'
                    await connection.execute(f'ALTER TABLE {table} DROP CONSTRAINT {quoted}')
                await connection.execute(f'ALTER TABLE {table} RENAME TO {LEGACY[table]}')
                await connection.execute(f'ALTER TABLE {NEW[table]} RENAME TO {table}')
            # Rename retained indexes to free the stable names expected by
            # metadata/startup. Keep the old table and its owned sequence.
            index_renames = (
                ('game_fen_association_pkey','game_fen_association_legacy_pkey'),
                ('_game_fen_move_color_uc','game_fen_legacy_move_uc'),
                ('ix_game_fen_association_game_link','game_fen_legacy_game_idx'),
                ('ix_game_fen_association_game_order','game_fen_legacy_order_idx'),
                ('player_position_frequency_pkey','player_position_frequency_legacy_pkey'),
                ('game_fen_association_numeric_v1_pkey','game_fen_association_pkey'),
                ('game_fen_numeric_move_uc','_game_fen_move_color_uc'),
                ('game_fen_numeric_fen_idx','ix_game_fen_association_fen_id'),
                ('game_fen_numeric_game_idx','ix_game_fen_association_game_link'),
                ('game_fen_numeric_order_idx','ix_game_fen_association_game_order'),
                ('player_position_frequency_numeric_v1_pkey','player_position_frequency_pkey'),
            )
            for old,new in index_renames:
                # Some minimal fixtures lack optional original indexes.
                if await connection.fetchval('SELECT to_regclass($1) IS NOT NULL',old):
                    await connection.execute(f'ALTER INDEX {old} RENAME TO {new}')
            for definition in triggers:
                name = next(name for name in HOME_TRIGGERS if name in definition)
                await connection.execute(f'DROP TRIGGER {name} ON {LEGACY[TABLES[0]]}')
                await connection.execute(definition.replace('fen_fen','fen_id'))
            await publish('checking_numeric_reference_identity')
            identity = await validate_references(connection)
            await publish('analyzing_reference_tables')
            for table in TABLES:
                await connection.execute(f'ANALYZE {table}')
            after = await sizes(connection)
        return {'comparison':comparison,'identity':identity,'sizes_before':before,'sizes_after':after,
                'legacy_tables':list(LEGACY.values()),'homepage_triggers_preserved':len(triggers)}
    finally:
        await connection.execute('SELECT pg_advisory_unlock($1)', MIGRATION_LOCK)
