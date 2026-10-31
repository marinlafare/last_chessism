"""Explicit additive migration. Queue changed game days; never calculate in triggers.

Coverage is refreshed by the existing local/cloud/import pipelines for every
game owning an analyzed FEN, including partial games and rescoring. Listening
there avoids adding work to each FEN write or restarting those pipelines.
"""
from sqlalchemy import text
from .relational_storage import identifier


def summary_table(layout, name):
    return f'{identifier(layout.core if layout else "public")}.{identifier("player_stockfish_" + name)}'


async def install_stockfish_summaries(connection, layout=None):
    schema = identifier(layout.core if layout else 'public')
    table = lambda name: summary_table(layout, name)
    source = lambda name: layout.table(name) if layout else f'public.{name}'
    await connection.execute(text("SET LOCAL lock_timeout='1s'"))
    await connection.execute(text("SET LOCAL statement_timeout='30s'"))
    for name, fields in {
        'state': '''player_name text PRIMARY KEY, revision bigint NOT NULL DEFAULT 1,
            published_revision bigint NOT NULL DEFAULT 0, full_rebuild boolean NOT NULL DEFAULT true,
            attempted_at timestamptz, error text''',
        'dirty': 'player_name text NOT NULL, day date NOT NULL, PRIMARY KEY(player_name,day)',
        'partition': 'player_name text NOT NULL, day date NOT NULL, payload jsonb NOT NULL, PRIMARY KEY(player_name,day)',
        'snapshot': '''player_name text PRIMARY KEY, revision bigint NOT NULL, payload jsonb NOT NULL,
            generated_at timestamptz NOT NULL DEFAULT clock_timestamp()''',
    }.items():
        await connection.execute(text(f'CREATE TABLE IF NOT EXISTS {table(name)} ({fields})'))
    await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {schema}.queue_player_stockfish(
        owner_name text, changed_at timestamptz, rebuild boolean DEFAULT false)
        RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF owner_name IS NULL THEN RETURN; END IF;
        INSERT INTO {table('state')} AS current(player_name) VALUES(owner_name)
            ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                full_rebuild=current.full_rebuild OR rebuild,error=NULL;
        INSERT INTO {table('dirty')}(player_name,day)
            VALUES(owner_name,COALESCE((changed_at AT TIME ZONE 'UTC')::date,DATE '-infinity'))
            ON CONFLICT DO NOTHING;
        END $$'''))
    await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {schema}.stockfish_summary_truncate()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        UPDATE {table('state')} SET revision=revision+1,full_rebuild=true,error=NULL;
        RETURN NULL; END $$'''))
    coverage = 'game_analysis_coverage' if layout else 'game_analysis_summary'
    for relation in ('player', 'game', 'game_player', 'game_fen_association', 'moves', coverage):
        await connection.execute(text(f'DROP TRIGGER IF EXISTS stockfish_summary_truncate ON {source(relation)}'))
        await connection.execute(text(f'''CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON {source(relation)}
            FOR EACH STATEMENT EXECUTE FUNCTION {schema}.stockfish_summary_truncate()'''))
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            aliases = ['old_rows', 'new_rows'] if event == 'UPDATE' else ['new_rows' if event == 'INSERT' else 'old_rows']
            pieces = []
            for alias in aliases:
                if relation == 'player':
                    changed = '' if event != 'UPDATE' else '''WHERE EXISTS(SELECT 1 FROM old_rows o
                        FULL JOIN new_rows n USING(player_name) WHERE r.player_name=COALESCE(o.player_name,n.player_name)
                        AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name))'''
                    pieces.append(f'SELECT r.player_name,NULL::timestamptz AS played_at FROM {alias} r {changed}')
                elif relation == 'game':
                    # Old game identities also survive cascading ownership deletion.
                    if layout:
                        pieces.append(f'''SELECT p.player_name,r.played_at FROM {alias} r
                            JOIN {source('player')} p ON p.id IN(r.white_player_id,r.black_player_id)''')
                    else:
                        pieces.append(f'''SELECT owner.player_name,r.played_at FROM {alias} r
                            CROSS JOIN LATERAL (VALUES(r.white),(r.black)) owner(player_name)''')
                elif relation == 'game_player':
                    if layout:
                        pieces.append(f'''SELECT p.player_name,g.played_at FROM {alias} r
                            JOIN {source('player')} p ON p.id=r.player_id
                            LEFT JOIN {source('game')} g ON g.link=r.game_link''')
                    else:
                        pieces.append(f'SELECT r.player_name,r.played_at FROM {alias} r')
                else:
                    link = 'game_link' if relation == 'game_fen_association' else 'link'
                    if layout:
                        pieces.append(f'''SELECT p.player_name,g.played_at FROM (SELECT DISTINCT {link} AS link FROM {alias}) r
                            JOIN {source('game_player')} gp ON gp.game_link=r.link
                            JOIN {source('player')} p ON p.id=gp.player_id
                            JOIN {source('game')} g ON g.link=r.link''')
                    else:
                        pieces.append(f'''SELECT gp.player_name,gp.played_at FROM (SELECT DISTINCT {link} AS link FROM {alias}) r
                            JOIN {source('game_player')} gp ON gp.link=r.link''')
            function = f'{schema}.stockfish_summary_{relation}_{event.lower()}'
            await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM ({' UNION '.join(pieces)}) changed WHERE
                    EXISTS(SELECT 1 FROM {source('player')} p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM {table('state')} s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO {table('state')} AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR {str(relation=='player').lower()},error=NULL
                    RETURNING player_name
                )
                INSERT INTO {table('dirty')}(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$'''))
            trigger = f'stockfish_summary_{relation}_{event.lower()}'
            await connection.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON {source(relation)}'))
            refs = ' '.join('OLD TABLE AS old_rows' if a == 'old_rows' else 'NEW TABLE AS new_rows' for a in aliases)
            await connection.execute(text(f'''CREATE TRIGGER {trigger} AFTER {event} ON {source(relation)}
                REFERENCING {refs} FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
    await connection.execute(text(f'''INSERT INTO {table('state')}(player_name)
        SELECT player_name FROM {source('player')} WHERE joined>0 AND deleted_at IS NULL ON CONFLICT DO NOTHING'''))
