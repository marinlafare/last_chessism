"""Explicit additive installation; no source-game updates or GPU work in triggers."""
from sqlalchemy import text
from chessism_api.database.relational_storage import identifier


def table(layout, name):
    return f'{identifier(layout.core if layout else "public")}.{identifier("canonical_blunder_"+name)}'


async def installed(session, layout):
    return bool(await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': table(layout, 'control')}))


async def install(connection, layout):
    if not layout:
        raise ValueError('Canonical publication requires the normalized storage contract.')
    t = lambda name: table(layout, name)
    core = identifier(layout.core)
    await connection.execute(text("SET LOCAL lock_timeout='1s'"))
    await connection.execute(text("SET LOCAL statement_timeout='30s'"))
    definitions = {
        'model': '''id text PRIMARY KEY,source_run text NOT NULL,config jsonb NOT NULL,
            evidence jsonb NOT NULL,approved_by text NOT NULL,approved_at timestamptz NOT NULL DEFAULT now(),
            approval text NOT NULL CHECK(approval='provisional_user_approved')''',
        'control': '''id boolean PRIMARY KEY DEFAULT true CHECK(id),target_model text,active_model text,
            previous_model text,status text NOT NULL DEFAULT 'idle',progress jsonb NOT NULL DEFAULT '{}',
            error text,updated_at timestamptz NOT NULL DEFAULT now()''',
        'dirty': 'game_link bigint PRIMARY KEY,revision bigserial NOT NULL',
        'game': '''model_id text NOT NULL,game_link bigint NOT NULL,revision bigint NOT NULL,
            mode text,played_at timestamptz,white_owner text,black_owner text,
            status text NOT NULL,white_count int,black_count int,white_assessed int,black_assessed int,
            flags jsonb NOT NULL,source_sha256 text NOT NULL,
            PRIMARY KEY(model_id,game_link),CHECK(white_count>=0 AND black_count>=0)''',
        'snapshot': '''model_id text NOT NULL,player_name text NOT NULL,payload jsonb NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(model_id,player_name)''',
    }
    for name, fields in definitions.items():
        await connection.execute(text(f'CREATE TABLE IF NOT EXISTS {t(name)} ({fields})'))
    for owner in ('white_owner', 'black_owner'):
        await connection.execute(text(f'CREATE INDEX IF NOT EXISTS canonical_blunder_game_{owner} ON {t("game")}(model_id,{owner})'))
    await connection.execute(text(f'INSERT INTO {t("control")}(id) VALUES(true) ON CONFLICT DO NOTHING'))
    await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {core}.protect_canonical_blunder_model()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Canonical model records are immutable'; END $$'''))
    await connection.execute(text(f'DROP TRIGGER IF EXISTS canonical_model_immutable ON {t("model")}'))
    await connection.execute(text(f'''CREATE TRIGGER canonical_model_immutable BEFORE UPDATE OR DELETE ON {t('model')}
        FOR EACH ROW EXECUTE FUNCTION {core}.protect_canonical_blunder_model()'''))
    coverage = 'game_analysis_coverage'
    for relation in ('game', 'game_player', 'player', 'game_fen_association', 'moves', coverage):
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            aliases = ('old_rows', 'new_rows') if event == 'UPDATE' else ('new_rows' if event == 'INSERT' else 'old_rows',)
            pieces = []
            for alias in aliases:
                if relation == 'player':
                    changed = '' if event != 'UPDATE' else '''WHERE EXISTS(SELECT 1 FROM old_rows o FULL JOIN new_rows n USING(id)
                        WHERE r.id=COALESCE(o.id,n.id) AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name))'''
                    pieces.append(f'''SELECT g.link FROM {alias} r JOIN {layout.table('game')} g
                        ON r.id IN(g.white_player_id,g.black_player_id) {changed}''')
                else:
                    key = 'game_link' if relation in ('game_player', 'game_fen_association') else 'link'
                    pieces.append(f'SELECT {key} AS link FROM {alias}')
            function = f'{core}.canonical_blunder_{relation}_{event.lower()}'
            await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO {t('dirty')}(game_link) SELECT DISTINCT link FROM ({' UNION '.join(pieces)}) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM {t('control')} WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$'''))
            name = f'canonical_blunder_{relation}_{event.lower()}'
            await connection.execute(text(f'DROP TRIGGER IF EXISTS {name} ON {layout.table(relation)}'))
            refs = ' '.join('OLD TABLE AS old_rows' if a == 'old_rows' else 'NEW TABLE AS new_rows' for a in aliases)
            await connection.execute(text(f'''CREATE TRIGGER {name} AFTER {event} ON {layout.table(relation)}
                REFERENCING {refs} FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
        function = f'{core}.canonical_blunder_truncate'
        await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
            LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
            INSERT INTO {t('dirty')}(game_link)
                SELECT game_link FROM {t('game')} WHERE EXISTS(SELECT 1 FROM {t('control')} WHERE target_model IS NOT NULL)
                UNION SELECT link FROM {layout.table('game')} WHERE EXISTS(SELECT 1 FROM {t('control')} WHERE target_model IS NOT NULL)
                ORDER BY 1 ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
            RETURN NULL; END $$'''))
        await connection.execute(text(f'DROP TRIGGER IF EXISTS canonical_blunder_truncate ON {layout.table(relation)}'))
        await connection.execute(text(f'''CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON {layout.table(relation)}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
    # A read-only overlay preserves the compatibility view's exact columns and
    # types. Legacy CPU values remain intact for rollback and old experiments.
    columns = (await connection.execute(text('''SELECT column_name FROM information_schema.columns
        WHERE table_schema=:schema AND table_name='game_player_engine_summary' ORDER BY ordinal_position'''), {'schema': layout.core})).scalars().all()
    projected = [f'''CASE WHEN c.active_model IS NULL THEN s.blunder_count
        ELSE CASE WHEN gp.is_white THEN b.white_count ELSE b.black_count END END::int AS blunder_count'''
        if col == 'blunder_count' else 's.'+identifier(col) for col in columns]
    await connection.execute(text(f'''CREATE OR REPLACE VIEW {t('engine')} AS SELECT {','.join(projected)}
        FROM {layout.table('game_player_engine_summary')} s JOIN {layout.table('game_player')} gp ON gp.id=s.game_player_id
        CROSS JOIN {t('control')} c LEFT JOIN {t('game')} b ON b.model_id=c.active_model AND b.game_link=gp.game_link'''))
    compat = f'{identifier(layout.reads)}.{identifier("game_player_engine_summary")}'
    definition = await connection.scalar(text('SELECT pg_get_viewdef(to_regclass(:name),true)'), {'name': compat})
    if definition:
        # Replace only its native source, not the view itself or any writer.
        candidates = (f'{layout.core}.game_player_engine_summary', layout.table('game_player_engine_summary'))
        changed = definition
        for name in candidates: changed = changed.replace(name, t('engine'))
        if changed == definition and 'canonical_blunder_engine' not in definition:
            raise ValueError('Unrecognized engine compatibility view; installation rolled back.')
        await connection.execute(text(f'CREATE OR REPLACE VIEW {compat} AS {changed}'))
