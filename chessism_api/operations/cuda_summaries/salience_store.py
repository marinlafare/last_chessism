"""Manual-only salience policy, input revisions and atomic publication staging."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from .schema import CONTROL, RUN
from .salience_compute import VERSION
from .salience_daily import VERSION as DAILY_VERSION

STATE = 'public.cuda_salience_state'
SCOPE = 'public.cuda_salience_scope'
PART = 'public.cuda_salience_part'
FREQUENCY = 'public.cuda_salience_frequency'
GAME = 'public.cuda_salience_game_stage'
DAILY = 'public.cuda_salience_daily'


async def enabled(session):
    if not storage_for(session) or not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'), {'t':STATE}):
        return False
    return bool(await session.scalar(text(f'SELECT manual AND salience FROM {CONTROL} WHERE id')))


async def install(session, layout):
    await session.execute(text(f'ALTER TABLE {CONTROL} ADD COLUMN IF NOT EXISTS salience boolean NOT NULL DEFAULT false'))
    # Older/minimal normalized schemas can use the other CUDA metrics without
    # installing salience. Activation remains unavailable until its tables exist.
    if not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'), {'t':layout.table('player_salience_summary')}):
        return
    for table, fields in (
        (STATE, '''player_id bigint PRIMARY KEY, revision bigint NOT NULL DEFAULT 0,
            published_revision bigint NOT NULL DEFAULT -1, version text, generated_at timestamptz'''),
        (SCOPE, f'''run_id uuid REFERENCES {RUN}(id),player_id bigint,player_name text NOT NULL,
            revision bigint NOT NULL,score_revision bigint, status text NOT NULL DEFAULT 'pending',
            PRIMARY KEY(run_id,player_id)'''),
        (PART, f'''run_id uuid REFERENCES {RUN}(id),batch int,game_player_id bigint,
            numerator float8 NOT NULL,positions int NOT NULL,unique_positions int NOT NULL,
            PRIMARY KEY(run_id,game_player_id,batch)'''),
        (FREQUENCY, f'''run_id uuid REFERENCES {RUN}(id),owner bigint,fen_id bigint,
            games bigint NOT NULL,occurrences bigint NOT NULL,PRIMARY KEY(run_id,owner,fen_id)'''),
        (GAME, f'''run_id uuid REFERENCES {RUN}(id),game_player_id bigint,
            numerator float8 NOT NULL,positions int NOT NULL,unique_positions int NOT NULL,
            salience float8 NOT NULL CHECK(salience>0 AND salience<=1),PRIMARY KEY(run_id,game_player_id)'''),
        (DAILY, '''player_id bigint,day date,mode_mask smallint,payload jsonb NOT NULL,
            PRIMARY KEY(player_id,day,mode_mask)'''),
    ):
        await session.execute(text(f'CREATE TABLE IF NOT EXISTS {table}({fields})'))
    await session.execute(text(f'ALTER TABLE {STATE} ADD COLUMN IF NOT EXISTS daily_version text'))
    await session.execute(text(f"ALTER TABLE {STATE} ADD COLUMN IF NOT EXISTS timezone jsonb NOT NULL DEFAULT '{{}}'"))
    await session.execute(text(f'''INSERT INTO {STATE}(player_id) SELECT id FROM {layout.table('player')}
        WHERE joined>0 AND deleted_at IS NULL ON CONFLICT DO NOTHING'''))
    # Ingestion already invalidates this row transactionally. Observe repeated
    # stale->stale notifications too; a boolean stale flag alone loses races.
    await session.execute(text(f'''CREATE OR REPLACE FUNCTION public.invalidate_cuda_salience()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF NEW.status='stale' THEN
            INSERT INTO {STATE}(player_id,revision) VALUES(NEW.player_id,1)
            ON CONFLICT(player_id) DO UPDATE SET revision=cuda_salience_state.revision+1;
        END IF; RETURN NEW; END $$'''))
    summary = layout.table('player_salience_summary')
    await session.execute(text(f'DROP TRIGGER IF EXISTS invalidate_cuda_salience ON {summary}'))
    await session.execute(text(f'''CREATE TRIGGER invalidate_cuda_salience AFTER INSERT OR UPDATE OF status
        ON {summary} FOR EACH ROW EXECUTE FUNCTION public.invalidate_cuda_salience()'''))


def needed(layout):
    return f'''SELECT p.id AS player_id,p.player_name,COALESCE(s.revision,0) AS revision,
        c.revision AS score_revision FROM {layout.table('player')} p
        LEFT JOIN {STATE} s ON s.player_id=p.id
        LEFT JOIN {layout.table('player_salience_summary')} old ON old.player_id=p.id
        LEFT JOIN {summary_table(layout,'state')} c USING(player_name)
        WHERE p.joined>0 AND p.deleted_at IS NULL AND (s.version IS DISTINCT FROM '{VERSION}'
            OR s.daily_version IS DISTINCT FROM '{DAILY_VERSION}'
            OR s.revision<>s.published_revision OR old.status IS DISTINCT FROM 'ready')'''


async def capture(session, run_id):
    if not await enabled(session):
        return 0
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_id,player_name,revision,score_revision)
        SELECT :id,player_id,player_name,revision,score_revision FROM ({needed(storage_for(session))}) n'''), {'id':run_id})
    return await session.scalar(text(f'SELECT count(*) FROM {SCOPE} WHERE run_id=:id'), {'id':run_id})


async def activate(session):
    from .runs import ACTIVE
    layout = storage_for(session)
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {RUN} WHERE status IN {ACTIVE})')):
        raise ValueError('Wait for the active CUDA run before enabling salience.')
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {layout.table('player_salience_summary')} WHERE status IN ('queued','running'))")):
        raise ValueError('Wait for the old salience worker to become idle before enabling CUDA salience.')
    await session.execute(text(f'UPDATE {CONTROL} SET salience=true WHERE id AND manual AND scores AND accuracy'))
    if not await enabled(session):
        raise ValueError('Manual scores and accuracy must be enabled before CUDA salience.')


async def published_version(session, player):
    if not storage_for(session) or not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'), {'t':STATE}):
        return None
    return await session.scalar(text(f'''SELECT s.version FROM {STATE} s
        JOIN {storage_for(session).table('player')} p ON p.id=s.player_id WHERE p.player_name=:player'''), {'player':player})
