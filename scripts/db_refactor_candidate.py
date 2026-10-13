"""Experimental normalized tables in a PRIVATE restored database only.

Never called by app startup. This is not a live migration: public tables are
retained and never altered. Read adapters let us compare legacy query contracts
before implementing production readers/writers and the eventual cutover.
"""
from pathlib import Path

C = 'refactor_candidate_v1'
A = 'refactor_archive_v1'
R = 'refactor_reference_v1'
V = 'refactor_read_v1'
START = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -'
SCORE_COLUMNS = {'score_sum','abs_score_sum','avg_score','avg_abs_score','max_abs_score'}


def standard(alias='g'):
    if alias not in ('g', 'game_row'):
        raise ValueError('Unexpected SQL alias')
    return (f"({alias}.rules='chess' AND ({alias}.initial_setup IS NULL OR "
            f"regexp_replace(btrim({alias}.initial_setup), '\\s+', ' ', 'g') "
            f"IN ('','{START}','{START} 0 1')))" )


def qi(name):
    return '"' + name.replace('"','""') + '"'


async def columns(conn, table):
    return [row['column_name'] for row in await conn.fetch('''
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position
    ''', table)]


async def guard(conn, pgdata):
    # Live access requires an explicit, connection-bound permit from the live
    # runner. Ordinary path arguments retain the strict private-only boundary.
    from scripts.db_refactor_live_target import LiveMigrationTarget
    if isinstance(pgdata, LiveMigrationTarget):
        await pgdata.verify(conn)
        return
    pgdata = Path(pgdata)
    if (pgdata.name != 'pgdata' or not pgdata.parent.name.startswith('db-refactor-baseline-')
            or pgdata.parent.parent != Path('/restore-test')):
        raise RuntimeError('Candidate requires an explicitly retained private baseline workspace')
    state = await conn.fetchrow('''
        SELECT current_setting('data_directory') AS directory,
               current_setting('port') AS port, inet_server_addr()::text AS address
    ''')
    if (not state or Path(state['directory']).resolve()!=pgdata.resolve()
            or state['port']!='55432' or state['address'] is not None):
        raise RuntimeError('Refusing candidate DDL outside the private restored database')


async def prepare_reference(conn, pgdata):
    await guard(conn, pgdata)
    for schema in (C,A,R,V):
        if await conn.fetchval('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)',schema):
            raise RuntimeError('Candidate namespace already exists; inspect before another attempt')
    async with conn.transaction():
        await conn.execute(f'CREATE SCHEMA {R}')
        await conn.execute(f'CREATE VIEW {R}.game AS SELECT g.* FROM public.game g WHERE {standard()}')
        scoped = {'game_player':'link','moves':'link','game_fen_association':'game_link',
                  'game_opening':'link','game_analysis_summary':'link',
                  'game_player_salience':'game_link','game_player_engine_summary':'game_link',
                  'player_salience_pending_game':'game_link'}
        for table,key in scoped.items():
            await conn.execute(f'''CREATE VIEW {R}.{table} AS SELECT s.* FROM public.{table} s
                JOIN {R}.game g ON g.link=s.{key}''')
    return {'standard_games':await conn.fetchval(f'SELECT count(*) FROM {R}.game'),
            'archived_games':await conn.fetchval(f'SELECT count(*) FROM public.game g WHERE NOT {standard()}')}


async def copy_with_reference(conn, table, key, target_key, mapping, *, excluded=()):
    """Clone unmodified value columns, substituting a numeric ownership key."""
    kept = [col for col in await columns(conn,table) if col not in set(excluded)|{key}]
    await conn.execute(f'CREATE TABLE {C}.{table} (LIKE public.{table} INCLUDING STORAGE INCLUDING CONSTRAINTS)')
    for col in set(excluded)|{key}:
        await conn.execute(f'ALTER TABLE {C}.{table} DROP COLUMN {qi(col)}')
    await conn.execute(f'ALTER TABLE {C}.{table} ADD COLUMN {target_key} bigint NOT NULL')
    fields = ','.join(qi(col) for col in kept)
    source = ','.join('s.'+qi(col) for col in kept)
    await conn.execute(f'''INSERT INTO {C}.{table} ({target_key},{fields})
        SELECT owner.id,{source} FROM public.{table} s {mapping}''')
    return kept


async def build_candidate(conn, pgdata, publish):
    await guard(conn,pgdata)
    if not await conn.fetchval('SELECT to_regnamespace($1) IS NOT NULL',R):
        raise RuntimeError('Measure the scoped reference before building a candidate')
    # FEN-based caches can be preserved only if the archived games contributed
    # no positions. Otherwise an explicit recalculation design is required.
    if await conn.fetchval(f'''SELECT EXISTS(SELECT 1 FROM public.game g
        JOIN public.game_fen_association a ON a.game_link=g.link WHERE NOT {standard()})'''):
        raise RuntimeError('Archived games have FENs: cache recalculation needs review')
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM public.game
        WHERE eco <> 'no_eco' AND eco NOT LIKE 'https://www.chess.com/openings/%')"""):
        raise RuntimeError('Unexpected ECO values need a lossless encoding decision')
    async with conn.transaction():
        for schema in (C,A,V):
            await conn.execute(f'CREATE SCHEMA {schema}')
        await conn.execute("SET LOCAL statement_timeout='2h'")
        await conn.execute("SET LOCAL lock_timeout='5s'")
        await conn.execute("SET LOCAL maintenance_work_mem='256MB'")
        await publish('archiving_variant_games')
        # Typed source records; no JSON archive and no source deletion. Omit
        # sequence defaults so archive inserts cannot consume public sequences.
        for table in ('game','moves'):
            await conn.execute(f'CREATE TABLE {A}.{table} (LIKE public.{table} INCLUDING STORAGE)')
        await conn.execute(f'''INSERT INTO {A}.game SELECT g.* FROM public.game g WHERE NOT {standard()};
            ALTER TABLE {A}.game ADD PRIMARY KEY(link);
            INSERT INTO {A}.moves SELECT m.* FROM public.moves m JOIN {A}.game g ON g.link=m.link;
            ALTER TABLE {A}.moves ADD PRIMARY KEY(id);
            ALTER TABLE {A}.moves ADD UNIQUE(link,n_move);
            ALTER TABLE {A}.moves ADD FOREIGN KEY(link) REFERENCES {A}.game(link);''')

        await publish('building_numeric_players_and_modes')
        player_cols = ','.join(qi(c) for c in await columns(conn,'player'))
        await conn.execute(f'''CREATE TABLE {C}.player (LIKE public.player INCLUDING STORAGE);
            ALTER TABLE {C}.player ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY;
            INSERT INTO {C}.player ({player_cols}) SELECT {player_cols} FROM public.player ORDER BY player_name COLLATE "C";
            ALTER TABLE {C}.player ADD PRIMARY KEY(id);
            ALTER TABLE {C}.player ADD UNIQUE(player_name);
            CREATE TABLE {C}.game_mode (id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                       name varchar(16) NOT NULL UNIQUE);
            INSERT INTO {C}.game_mode(name) SELECT DISTINCT mode FROM public.game WHERE mode IS NOT NULL ORDER BY mode;
            CREATE TABLE {C}.game (LIKE public.game INCLUDING STORAGE);
            ALTER TABLE {C}.game DROP COLUMN white,DROP COLUMN black,DROP COLUMN mode,
                DROP COLUMN rules,DROP COLUMN initial_setup,
                ADD COLUMN white_player_id bigint NOT NULL,ADD COLUMN black_player_id bigint NOT NULL,
                ADD COLUMN mode_id smallint;''')
        game_cols = [c for c in await columns(conn,'game') if c not in {'white','black','mode','rules','initial_setup'}]
        game_values = ["CASE WHEN g.eco='no_eco' THEN g.eco ELSE substr(g.eco,32) END" if c=='eco'
                       else 'g.'+qi(c) for c in game_cols]
        await conn.execute(f'''INSERT INTO {C}.game ({','.join(map(qi,game_cols))},white_player_id,black_player_id,mode_id)
            SELECT {','.join(game_values)},w.id,b.id,m.id FROM {R}.game g
            JOIN {C}.player w ON w.player_name=g.white JOIN {C}.player b ON b.player_name=g.black
            LEFT JOIN {C}.game_mode m ON m.name=g.mode;
            ALTER TABLE {C}.game ADD PRIMARY KEY(link);
            ALTER TABLE {C}.game ADD FOREIGN KEY(white_player_id) REFERENCES {C}.player(id);
            ALTER TABLE {C}.game ADD FOREIGN KEY(black_player_id) REFERENCES {C}.player(id);
            ALTER TABLE {C}.game ADD FOREIGN KEY(mode_id) REFERENCES {C}.game_mode(id);
            CREATE INDEX ON {C}.game(mode_id,avg_elo);
            CREATE INDEX ON {C}.game(played_at);
            CREATE INDEX ON {C}.game(white_player_id,played_at);
            CREATE INDEX ON {C}.game(black_player_id,played_at);''')

        await publish('building_compact_game_players')
        await conn.execute(f'''CREATE TABLE {C}.game_player (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            game_link bigint NOT NULL REFERENCES {C}.game(link) ON DELETE CASCADE,
            player_id bigint NOT NULL REFERENCES {C}.player(id) ON DELETE CASCADE,is_white boolean NOT NULL,
            UNIQUE(game_link,is_white));
            INSERT INTO {C}.game_player(game_link,player_id,is_white)
            SELECT gp.link,p.id,gp.color='white' FROM public.game_player gp
            JOIN {C}.game g ON g.link=gp.link JOIN {C}.player p ON p.player_name=gp.player_name
            ORDER BY gp.link,gp.color;
            CREATE INDEX ON {C}.game_player(player_id,game_link DESC);''')

        await publish('copying_numeric_fen_associations')
        await conn.execute(f'''CREATE TABLE {C}.game_fen_association (
            id integer GENERATED BY DEFAULT AS IDENTITY,
            game_link bigint NOT NULL,fen_id bigint NOT NULL,n_move integer NOT NULL,
            move_color varchar(5) NOT NULL CHECK(move_color IN ('white','black')));
            INSERT INTO {C}.game_fen_association(id,game_link,fen_id,n_move,move_color)
            SELECT a.id,a.game_link,f.fen_id,a.n_move,a.move_color
            FROM public.game_fen_association a JOIN public.fen f ON f.fen=a.fen_fen
            JOIN {C}.game g ON g.link=a.game_link;''')
        await publish('indexing_numeric_fen_associations')
        await conn.execute(f'''ALTER TABLE {C}.game_fen_association ADD PRIMARY KEY(id);
            ALTER TABLE {C}.game_fen_association ADD UNIQUE(game_link,fen_id,n_move,move_color);
            CREATE INDEX ON {C}.game_fen_association(fen_id);
            CREATE INDEX ON {C}.game_fen_association(game_link,n_move,move_color);
            ALTER TABLE {C}.game_fen_association ADD FOREIGN KEY(game_link) REFERENCES {C}.game(link);
            ALTER TABLE {C}.game_fen_association ADD FOREIGN KEY(fen_id) REFERENCES public.fen(fen_id);''')
        sequence = await conn.fetchval(f"SELECT pg_get_serial_sequence('{C}.game_fen_association','id')")
        source_sequence = await conn.fetchval("SELECT pg_get_serial_sequence('public.game_fen_association','id')")
        # regclass -> format(%s) safely qualifies the catalog-derived sequence.
        source_query = await conn.fetchval("SELECT format('SELECT last_value,is_called FROM %s',$1::regclass)",source_sequence)
        source_state = await conn.fetchrow(source_query)
        maximum = await conn.fetchval(f'SELECT max(id) FROM {C}.game_fen_association')
        last = max(maximum or 1,source_state['last_value'])
        await conn.fetchval('SELECT setval($1::regclass,$2,$3)',sequence,last,
                            bool(maximum) or source_state['is_called'])

        await publish('copying_player_frequency_and_salience')
        await conn.execute(f'''CREATE TABLE {C}.player_position_frequency (
            player_id bigint NOT NULL REFERENCES {C}.player(id),is_white boolean NOT NULL,
            fen_id bigint NOT NULL REFERENCES public.fen(fen_id) ON DELETE CASCADE,
            games_with_position bigint NOT NULL CHECK(games_with_position>1),
            total_occurrences bigint NOT NULL CHECK(total_occurrences>=games_with_position),
            PRIMARY KEY(player_id,is_white,fen_id));
            INSERT INTO {C}.player_position_frequency
            SELECT p.id,s.player_color='white',f.fen_id,s.games_with_position,s.total_occurrences
            FROM public.player_position_frequency s JOIN {C}.player p ON p.player_name=s.player_name
            JOIN public.fen f ON f.fen=s.fen_fen;''')
        for table in ('game_player_salience','game_player_engine_summary'):
            excluded = {'player_color','player_name'}
            if table=='game_player_salience':
                excluded.add('repeated_position_count')
            await copy_with_reference(conn,table,'game_link','game_player_id',
                f"JOIN {C}.game_player owner ON owner.game_link=s.game_link AND owner.is_white=(s.player_color='white')",
                excluded=excluded)
            await conn.execute(f'''ALTER TABLE {C}.{table} ADD PRIMARY KEY(game_player_id);
                ALTER TABLE {C}.{table} ADD FOREIGN KEY(game_player_id) REFERENCES {C}.game_player(id) ON DELETE CASCADE;''')
        await copy_with_reference(conn,'player_salience_summary','player_name','player_id',
                                  f'JOIN {C}.player owner ON owner.player_name=s.player_name')
        await conn.execute(f'''ALTER TABLE {C}.player_salience_summary ADD PRIMARY KEY(player_id);
            ALTER TABLE {C}.player_salience_summary ADD FOREIGN KEY(player_id) REFERENCES {C}.player(id) ON DELETE CASCADE;
            ALTER TABLE {C}.player_salience_summary ADD CHECK(status IN ('stale','queued','running','ready','failed'));
            CREATE INDEX ON {C}.player_salience_summary(status,player_id);
            CREATE TABLE {C}.player_salience_pending_game (
                game_player_id bigint PRIMARY KEY REFERENCES {C}.game_player(id) ON DELETE CASCADE);
            INSERT INTO {C}.player_salience_pending_game SELECT gp.id
                FROM public.player_salience_pending_game s JOIN {C}.game_player gp
                ON gp.game_link=s.game_link AND gp.is_white=(s.player_color='white');''')

        await publish('building_opening_dictionary_and_coverage')
        await conn.execute(f'''CREATE TABLE {C}.opening_sequence (
            id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,opening text NOT NULL UNIQUE);
            INSERT INTO {C}.opening_sequence(opening)
                SELECT DISTINCT opening FROM {R}.game_opening ORDER BY opening;
            CREATE TABLE {C}.game_opening (
                link bigint NOT NULL REFERENCES {C}.game(link) ON DELETE CASCADE,
                n_moves integer NOT NULL,opening_id integer NOT NULL REFERENCES {C}.opening_sequence(id),
                PRIMARY KEY(link,n_moves));
            INSERT INTO {C}.game_opening SELECT s.link,s.n_moves,d.id FROM {R}.game_opening s
                JOIN {C}.opening_sequence d ON d.opening=s.opening;
            CREATE INDEX ON {C}.game_opening(opening_id,link);
            CREATE INDEX ON {C}.game_opening(n_moves,link);
            CREATE TABLE {C}.game_analysis_coverage (LIKE public.game_analysis_summary INCLUDING STORAGE);''')
        for col in SCORE_COLUMNS:
            await conn.execute(f'ALTER TABLE {C}.game_analysis_coverage DROP COLUMN {col}')
        coverage_cols = [col for col in await columns(conn,'game_analysis_summary') if col not in SCORE_COLUMNS]
        field_list = ','.join(map(qi,coverage_cols))
        await conn.execute(f'''INSERT INTO {C}.game_analysis_coverage ({field_list})
            SELECT {field_list} FROM {R}.game_analysis_summary;
            ALTER TABLE {C}.game_analysis_coverage ADD PRIMARY KEY(link);
            ALTER TABLE {C}.game_analysis_coverage ADD FOREIGN KEY(link) REFERENCES {C}.game(link) ON DELETE CASCADE;
            CREATE INDEX ON {C}.game_analysis_coverage(unscored_positions DESC,total_positions DESC,link DESC)
                WHERE NOT is_fully_analyzed AND total_positions>0;
            CREATE INDEX ON {C}.game_analysis_coverage(total_positions DESC,link DESC)
                WHERE is_fully_analyzed AND total_positions>0;''')
        await publish('building_read_adapters')
        await create_read_views(conn,game_cols,coverage_cols)
        await publish('validating_candidate_rows')
        comparisons = await validate_rows(conn)
        await publish('analyzing_candidate_indexes', comparisons=comparisons)
        for table in ('player','game_mode','game','game_player','game_fen_association',
                      'player_position_frequency','game_player_salience','game_player_engine_summary',
                      'player_salience_summary','player_salience_pending_game','opening_sequence',
                      'game_opening','game_analysis_coverage'):
            await conn.execute(f'ANALYZE {C}.{table}')
    return comparisons


async def create_read_views(conn, game_cols, coverage_cols):
    fields = ["CASE WHEN g.eco='no_eco' THEN g.eco ELSE 'https://www.chess.com/openings/'||g.eco END AS eco"
              if col=='eco' else 'g.'+qi(col) for col in game_cols]
    await conn.execute(f'''CREATE VIEW {V}.game AS SELECT {','.join(fields)},
        w.player_name AS white,b.player_name AS black,m.name AS mode,
        'chess'::varchar(32) AS rules,NULL::varchar(128) AS initial_setup
        FROM {C}.game g JOIN {C}.player w ON w.id=g.white_player_id
        JOIN {C}.player b ON b.id=g.black_player_id LEFT JOIN {C}.game_mode m ON m.id=g.mode_id;
        CREATE VIEW {V}.game_player AS SELECT g.link,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END::varchar(5) AS color,
            p.player_name,CASE WHEN gp.is_white THEN g.black ELSE g.white END AS opponent_name,
            CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
            CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
            CASE WHEN gp.is_white THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
            g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed,g.avg_elo
            FROM {C}.game_player gp JOIN {C}.player p ON p.id=gp.player_id
            JOIN {V}.game g ON g.link=gp.game_link;
        CREATE VIEW {V}.game_fen_association AS SELECT * FROM {C}.game_fen_association;
        CREATE VIEW {V}.moves AS SELECT m.* FROM public.moves m JOIN {C}.game g ON g.link=m.link;
        CREATE VIEW {V}.player_position_frequency AS SELECT p.player_name,
            CASE WHEN s.is_white THEN 'white' ELSE 'black' END::varchar(5) AS player_color,
            s.fen_id,s.games_with_position,s.total_occurrences
            FROM {C}.player_position_frequency s JOIN {C}.player p ON p.id=s.player_id;
        CREATE VIEW {V}.game_opening AS SELECT o.link,o.n_moves,d.opening,
            g.mode,g.avg_elo,g.played_at FROM {C}.game_opening o
            JOIN {C}.opening_sequence d ON d.id=o.opening_id JOIN {V}.game g ON g.link=o.link;
        CREATE VIEW {V}.game_analysis_summary AS SELECT * FROM {C}.game_analysis_coverage;''')
    for table in ('game_player_salience','game_player_engine_summary'):
        values = [col for col in await columns(conn,table)
                  if col not in {'game_link','player_color','player_name','repeated_position_count'}]
        values_sql = ','.join('s.'+qi(col) for col in values)
        repeated = (',s.position_occurrence_count-s.unique_position_count AS repeated_position_count'
                    if table=='game_player_salience' else '')
        await conn.execute(f'''CREATE VIEW {V}.{table} AS SELECT gp.game_link,p.player_name,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END::varchar(5) AS player_color,
            {values_sql}{repeated} FROM {C}.{table} s
            JOIN {C}.game_player gp ON gp.id=s.game_player_id JOIN {C}.player p ON p.id=gp.player_id''')
    summary_cols = [col for col in await columns(conn,'player_salience_summary') if col!='player_name']
    await conn.execute(f'''CREATE VIEW {V}.player_salience_summary AS SELECT p.player_name,
        {','.join('s.'+qi(c) for c in summary_cols)} FROM {C}.player_salience_summary s
        JOIN {C}.player p ON p.id=s.player_id;
        CREATE VIEW {V}.player_salience_pending_game AS SELECT p.player_name,gp.game_link,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END::varchar(5) AS player_color
            FROM {C}.player_salience_pending_game s JOIN {C}.game_player gp ON gp.id=s.game_player_id
            JOIN {C}.player p ON p.id=gp.player_id;''')


async def validate_rows(conn):
    report = {}

    async def compare(name,left,right):
        # EXCEPT ALL in both directions verifies every value and multiplicity,
        # not just a count or a probabilistic checksum.
        mismatches = await conn.fetchval(f'''SELECT count(*) FROM (
            ({left} EXCEPT ALL {right}) UNION ALL ({right} EXCEPT ALL {left})
        ) AS difference''')
        if mismatches:
            raise RuntimeError(f'Candidate row comparison failed: {name}')
        report[name] = {'mismatches':0}

    for table in ('game','game_player','game_opening','game_analysis_summary',
                  'game_player_salience','game_player_engine_summary',
                  'player_salience_pending_game','player_salience_summary'):
        fields = await columns(conn,table)
        if table=='game':
            fields = [col for col in fields if col not in {'rules','initial_setup'}]
        if table=='game_analysis_summary':
            fields = [col for col in fields if col not in SCORE_COLUMNS]
        field_sql = ','.join(map(qi,fields))
        source = 'public' if table=='player_salience_summary' else R
        await compare(table, f'SELECT {field_sql} FROM {source}.{table}',
                      f'SELECT {field_sql} FROM {V}.{table}')
    player_cols = ','.join(map(qi,await columns(conn,'player')))
    await compare('player',f'SELECT {player_cols} FROM public.player',f'SELECT {player_cols} FROM {C}.player')
    await compare('archive_game',f'SELECT g.* FROM public.game g WHERE NOT {standard()}',f'SELECT * FROM {A}.game')
    await compare('archive_moves',f'SELECT m.* FROM public.moves m JOIN {A}.game g ON g.link=m.link',f'SELECT * FROM {A}.moves')
    # A PK join avoids sorting the entire ~58M-row association twice.
    bad = await conn.fetchval(f'''SELECT count(*) FROM public.game_fen_association old
        JOIN {C}.game g ON g.link=old.game_link LEFT JOIN public.fen f ON f.fen=old.fen_fen
        LEFT JOIN {C}.game_fen_association new ON new.id=old.id
        WHERE new.id IS NULL OR ROW(new.game_link,new.fen_id,new.n_move,new.move_color)
          IS DISTINCT FROM ROW(old.game_link,f.fen_id,old.n_move,old.move_color)''')
    old_count = await conn.fetchval(f'SELECT count(*) FROM {R}.game_fen_association')
    new_count = await conn.fetchval(f'SELECT count(*) FROM {C}.game_fen_association')
    if bad or old_count!=new_count:
        raise RuntimeError('Candidate FEN association comparison failed')
    report['game_fen_association'] = {'rows':new_count,'mismatches':0}
    await compare('player_position_frequency','''SELECT player_name,player_color,fen_fen,
        games_with_position,total_occurrences FROM public.player_position_frequency''',
        f'''SELECT p.player_name,p.player_color,f.fen,p.games_with_position,p.total_occurrences
            FROM {V}.player_position_frequency p JOIN public.fen f ON f.fen_id=p.fen_id''')
    return report
