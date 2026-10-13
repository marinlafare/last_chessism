"""Shared transactional DDL for private rehearsal and guarded live maintenance.

All changes are one transaction and never CASCADE through unknown dependencies.
The caller must hold the isolated workspace lock and stop other private clients.
"""
from scripts.compact_db_preflight import require_private, require_target, prove_data
from chessism_api.database.relational_storage import identifier
from chessism_api.database.compact_storage import PLAYER_TABLES
from scripts.compact_db_indexes import drop_reviewed_impact_index, create_integer_impact_index


async def key_constraints(conn, table):
    return await conn.fetch('''SELECT k.conname,k.contype::text AS kind,
        ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(id,n)
              JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=x.id ORDER BY x.n) AS columns
        FROM pg_constraint k WHERE k.conrelid=$1::regclass''', table)


async def numeric_player_table(conn, table):
    target = 'public.' + identifier(table)
    fields = [r['attname'] for r in await conn.fetch('''SELECT attname FROM pg_attribute
        WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum''', target)]
    await conn.execute(f'ALTER TABLE {target} ADD COLUMN player_id bigint')
    await conn.execute(f'''UPDATE {target} s SET player_id=p.id FROM public.player p
        WHERE p.player_name=s.player_name''')
    await conn.execute(f'ALTER TABLE {target} ALTER COLUMN player_id SET NOT NULL')
    # Preserve every other constraint; replace just ownership keys/FKs.
    for key in await key_constraints(conn, target):
        if 'player_name' in key['columns']:
            await conn.execute(f'ALTER TABLE {target} DROP CONSTRAINT {identifier(key["conname"])}')
    await conn.execute(f'ALTER TABLE {target} DROP COLUMN player_name')
    if table == 'months':
        await conn.execute(f'ALTER TABLE {target} ADD UNIQUE(player_id,year,month)')
    else:
        keys = 'mode,player_id' if table == 'main_character_mode_summary' else 'player_id'
        await conn.execute(f'ALTER TABLE {target} ADD PRIMARY KEY({keys})')
    action = ' ON DELETE CASCADE' if table in ('player_stats','main_character_mode_summary') else ''
    await conn.execute(f'ALTER TABLE {target} ADD FOREIGN KEY(player_id) REFERENCES public.player(id){action}')
    projection = ','.join('p.player_name' if field=='player_name' else 's.'+identifier(field) for field in fields)
    await conn.execute(f'''CREATE VIEW chessism_reads.{identifier(table)} AS SELECT {projection}
        FROM {target} s JOIN public.player p ON p.id=s.player_id''')


async def migrate_private(conn, pgdata, publish, *, inject_failure=False):
    await require_private(conn, pgdata)
    return await migrate_authorized(conn, pgdata, publish, inject_failure=inject_failure)


async def migrate_authorized(conn, pgdata, publish, *, inject_failure=False):
    # A filesystem path can ONLY select the private copy. A live target must
    # bind this connection/cluster, own the maintenance lock and exclude clients.
    from scripts.db_refactor_live_target import LiveMigrationTarget
    await require_target(conn, pgdata)
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
                           "AND table_name='game_fen_association' AND column_name='is_white')"):
        raise RuntimeError('Candidate already migrated; do not rerun the migration')
    # Reprove in the same locked transaction: a prior report is not a substitute
    # for the actual data being changed, even in a private copy.
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout='2h'; SET LOCAL lock_timeout='5s'; "
                           "SET LOCAL maintenance_work_mem='256MB'; SET LOCAL search_path=public")
        await conn.execute('LOCK TABLE public.fen,public.fen_continuation,public.game_fen_association,public.moves,'
                           'chessism_archive.moves,public.months,public.player_stats,public.main_character_mode_summary,'
                           'public.home_titled_player_summary,public.player IN ACCESS EXCLUSIVE MODE')
        if isinstance(pgdata, LiveMigrationTarget):
            await pgdata.verify(conn)
        else:
            others = await conn.fetchval('''SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
                AND pid<>pg_backend_pid() AND backend_type='client backend' ''')
            if others:
                raise RuntimeError('Other private clients must stop before candidate DDL')
        proofs = await prove_data(conn, pgdata, publish)
        await conn.execute("SET LOCAL statement_timeout='2h'")
        for schema in ('public','chessism_archive'):
            await publish('compacting_' + schema + '_moves')
            table = schema + '.moves'
            if schema == 'public':
                await conn.execute('DROP VIEW chessism_reads.moves')
            for key in await key_constraints(conn,table):
                if key['kind'] in ('p','u') and key['columns'] in (['id'],['link','n_move']):
                    await conn.execute(f'ALTER TABLE {table} DROP CONSTRAINT {identifier(key["conname"])}')
            await conn.execute(f'ALTER TABLE {table} DROP COLUMN id, ADD PRIMARY KEY(link,n_move)')
            if schema == 'public':
                await conn.execute('CREATE VIEW chessism_reads.moves AS SELECT * FROM public.moves')

        await publish('compacting_game_associations')
        await conn.execute('DROP VIEW chessism_reads.game_fen_association')
        table = 'public.game_fen_association'
        for key in await key_constraints(conn,table):
            if 'move_color' in key['columns']:
                await conn.execute(f'ALTER TABLE {table} DROP CONSTRAINT {identifier(key["conname"])}')
        # Indexes referencing the old side expression are rebuilt by PostgreSQL
        # on a type change. Remove only the reviewed ordering index; keep fen_id.
        indexes = await conn.fetch('''SELECT i.indexrelid::regclass::text AS name,
            ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY x(id,n)
                JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=x.id ORDER BY x.n) AS columns
            FROM pg_index i WHERE i.indrelid='public.game_fen_association'::regclass AND NOT i.indisunique''')
        for index in indexes:
            if index['columns'] == ['game_link','n_move','move_color']:
                # regclass string comes from trusted catalog, quote each component.
                name = index['name'].split('.')[-1]
                await conn.execute(f'DROP INDEX public.{identifier(name)}')
        await conn.execute(f'''ALTER TABLE {table} ALTER COLUMN move_color TYPE boolean USING (move_color='white');
            ALTER TABLE {table} RENAME COLUMN move_color TO is_white;
            ALTER TABLE {table} ADD CONSTRAINT compact_association_move UNIQUE(game_link,n_move,is_white);
            CREATE VIEW chessism_reads.game_fen_association AS
                SELECT id,game_link,fen_id,n_move,CASE WHEN is_white THEN 'white'::varchar(5) ELSE 'black'::varchar(5) END AS move_color
                FROM {table};''')

        await publish('converting_numeric_player_references')
        for table in PLAYER_TABLES:
            await numeric_player_table(conn, table)

        await publish('converting_exact_engine_numbers')
        # An ALTER TYPE rebuild preserves the old index's float casts, which
        # no longer match integer query arithmetic. Build the correct index
        # once AFTER conversion instead of rebuilding the obsolete expression.
        await drop_reviewed_impact_index(conn)
        await conn.execute('''DROP INDEX IF EXISTS public.ix_fen_backup_changed;
            ALTER TABLE public.fen
            ALTER COLUMN score TYPE integer USING score::integer,
            ALTER COLUMN wdl_win TYPE smallint USING wdl_win::smallint,
            ALTER COLUMN wdl_draw TYPE smallint USING wdl_draw::smallint,
            ALTER COLUMN wdl_loss TYPE smallint USING wdl_loss::smallint,
            ADD CONSTRAINT compact_wdl_range CHECK (
                (wdl_win IS NULL OR wdl_win BETWEEN 0 AND 1000) AND
                (wdl_draw IS NULL OR wdl_draw BETWEEN 0 AND 1000) AND
                (wdl_loss IS NULL OR wdl_loss BETWEEN 0 AND 1000));
            ALTER TABLE public.fen_continuation ALTER COLUMN score TYPE integer USING score::integer;''')
        await publish('rebuilding_numeric_fen_backup_index')
        await conn.execute('CREATE INDEX ix_fen_backup_changed ON public.fen(analyzed_at,fen_id) WHERE score IS NOT NULL')
        await publish('rebuilding_integer_fen_impact_index')
        await create_integer_impact_index(conn)
        if inject_failure:
            raise RuntimeError('Injected compact migration rollback test')
        # Explicitly NOT ready: passing structural migration is not passing
        # application, full-value comparison, performance and backup acceptance.
        await publish('private_candidate_schema_ready')
    return proofs
