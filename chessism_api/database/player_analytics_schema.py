"""Explicit, additive migration for versioned player metadata summaries.

Never invoked by API startup. Source triggers enqueue dates, not calculations;
Stockfish/FEN updates do not invalidate these game-metadata summaries.
"""
from sqlalchemy import text

from .relational_storage import identifier


def analytics_table(layout, name):
    return f'{identifier(layout.core if layout else "public")}.{identifier("player_analytics_" + name)}'


async def install_player_analytics(connection, layout=None):
    schema = identifier(layout.core if layout else 'public')
    table = lambda name: analytics_table(layout, name)
    source = lambda name: layout.table(name) if layout else f'public.{name}'
    await connection.execute(text("SET LOCAL lock_timeout='1s'"))
    definitions = {
        'state': '''player_name text PRIMARY KEY, revision bigint NOT NULL DEFAULT 1,
            full_rebuild boolean NOT NULL DEFAULT true, published_revision bigint NOT NULL DEFAULT 0,
            attempted_at timestamptz, error text''',
        'dirty': '''player_name text NOT NULL, day date NOT NULL,
            PRIMARY KEY(player_name,day)''',
        'partition': '''player_name text NOT NULL, day date NOT NULL, payload jsonb NOT NULL,
            PRIMARY KEY(player_name,day)''',
        'snapshot': '''player_name text NOT NULL, section text NOT NULL, revision bigint NOT NULL,
            generated_at timestamptz NOT NULL DEFAULT clock_timestamp(), payload jsonb NOT NULL,
            PRIMARY KEY(player_name,section)''',
    }
    for name, columns in definitions.items():
        await connection.execute(text(f'CREATE TABLE IF NOT EXISTS {table(name)} ({columns})'))

    # Only tracked profiles need snapshots. Promotion queues a full rebuild;
    # retained state also lets deletion/demotion remove a published snapshot.
    await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {schema}.queue_player_analytics(
        owner_name text, changed_at timestamptz, rebuild boolean DEFAULT false)
        RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF owner_name IS NULL THEN RETURN; END IF;
        INSERT INTO {table('state')} AS current(player_name,full_rebuild)
            VALUES(owner_name,true)
            ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                full_rebuild=current.full_rebuild OR rebuild, error=NULL;
        INSERT INTO {table('dirty')}(player_name,day)
            VALUES(owner_name,COALESCE((changed_at AT TIME ZONE 'UTC')::date,DATE '-infinity'))
            ON CONFLICT DO NOTHING;
        END $$'''))

    # Transition-table triggers batch the dirty keys: one call per distinct
    # player/date in a statement, never one call per move or FEN.
    for relation in ('game_player', 'game', 'player'):
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            alias_names = (['old_rows', 'new_rows'] if event == 'UPDATE'
                           else ['new_rows'] if event == 'INSERT' else ['old_rows'])
            pieces = []
            for alias in alias_names:
                if relation == 'player':
                    changed = ''
                    if event == 'UPDATE':
                        changed = f'''WHERE NOT EXISTS (SELECT 1 FROM {"new_rows" if alias == "old_rows" else "old_rows"} other
                            WHERE other.player_name=r.player_name) OR EXISTS (SELECT 1 FROM old_rows o JOIN new_rows n
                            ON o.player_name=n.player_name WHERE r.player_name=o.player_name
                            AND (o.timezone,o.timezone_source,o.country,o.location,o.joined,o.deleted_at)
                                IS DISTINCT FROM
                                (n.timezone,n.timezone_source,n.country,n.location,n.joined,n.deleted_at))'''
                    pieces.append(f'SELECT r.player_name, NULL::timestamptz AS played_at FROM {alias} r {changed}')
                elif relation == 'game_player':
                    if layout:
                        pieces.append(f'''SELECT p.player_name,g.played_at FROM {alias} r
                            JOIN {source('player')} p ON p.id=r.player_id
                            LEFT JOIN {source('game')} g ON g.link=r.game_link''')
                    else:
                        pieces.append(f'SELECT r.player_name,r.played_at FROM {alias} r')
                else:
                    # A cascading game deletion may remove ownership before an
                    # AFTER trigger can read it. The old game retains both IDs.
                    changed = ''
                    if event == 'UPDATE':
                        fields = (['white_player_id','black_player_id','mode_id'] if layout else ['white','black','mode'])
                        fields += ['played_at','white_elo','black_elo','white_result','black_result','time_elapsed','eco']
                        old = ','.join('o.' + field for field in fields)
                        new = ','.join('n.' + field for field in fields)
                        changed = f'''WHERE EXISTS(SELECT 1 FROM old_rows o JOIN new_rows n USING(link)
                            WHERE r.link=o.link AND ROW({old}) IS DISTINCT FROM ROW({new}))'''
                    if layout:
                        pieces.append(f'''SELECT p.player_name,r.played_at FROM {alias} r
                            JOIN {source('player')} p ON p.id IN(r.white_player_id,r.black_player_id) {changed}''')
                    else:
                        pieces.append(f'''SELECT owner.player_name,r.played_at FROM {alias} r
                            CROSS JOIN LATERAL (VALUES(r.white),(r.black)) owner(player_name) {changed}''')
            function = f'{schema}.player_analytics_{relation}_{event.lower()}'
            await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
                FOR item IN SELECT DISTINCT changes.player_name,date_trunc('day',played_at,'UTC') AS played_at FROM ({' UNION '.join(pieces)}) changes
                    WHERE EXISTS(SELECT 1 FROM {source('player')} p WHERE p.player_name=changes.player_name
                        AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM {table('state')} s WHERE s.player_name=changes.player_name)
                    ORDER BY changes.player_name,played_at LOOP
                    PERFORM {schema}.queue_player_analytics(item.player_name,item.played_at,{str(relation == 'player').lower()});
                END LOOP; RETURN NULL; END $$'''))
            trigger = f'player_analytics_{relation}_{event.lower()}'
            await connection.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON {source(relation)}'))
            referencing = ' '.join(('OLD TABLE AS old_rows' if alias == 'old_rows' else 'NEW TABLE AS new_rows')
                                   for alias in alias_names)
            await connection.execute(text(f'''CREATE TRIGGER {trigger} AFTER {event} ON {source(relation)}
                REFERENCING {referencing} FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
        function = f'{schema}.player_analytics_truncate'
        await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
            LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
            UPDATE {table('state')} SET revision=revision+1,full_rebuild=true,error=NULL;
            RETURN NULL; END $$'''))
        trigger = f'player_analytics_{relation}_truncate'
        await connection.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON {source(relation)}'))
        await connection.execute(text(f'''CREATE TRIGGER {trigger} AFTER TRUNCATE ON {source(relation)}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
    await connection.execute(text(f'''INSERT INTO {table('state')}(player_name)
        SELECT player_name FROM {source('player')} WHERE joined > 0 AND deleted_at IS NULL
        ON CONFLICT DO NOTHING'''))
