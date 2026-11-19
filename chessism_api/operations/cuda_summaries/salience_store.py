"""Manual-only salience policy, input revisions and atomic publication staging."""
from sqlalchemy import text
from .schema import CONTROL, RUN
from .salience_compute import VERSION
from .salience_daily import VERSION as DAILY_VERSION

STATE = 'public.cuda_salience_state'
SCOPE = 'public.cuda_salience_scope'
PART = 'public.cuda_salience_part'
FREQUENCY = 'public.cuda_salience_frequency'
GAME = 'public.cuda_salience_game_stage'
DAILY = 'public.cuda_salience_daily'
INPUT = 'public.cuda_salience_input'


async def enabled(session):
    return bool(await session.scalar(text(f'SELECT manual AND salience FROM {CONTROL} WHERE id')))




def needed():
    return f'''SELECT p.id AS player_id,p.player_name,COALESCE(s.revision,0) AS revision,
        c.revision AS score_revision FROM public.player p
        LEFT JOIN {STATE} s ON s.player_id=p.id
        LEFT JOIN public.player_salience_summary old ON old.player_id=p.id
        LEFT JOIN public.player_stockfish_state c USING(player_name)
        WHERE p.joined>0 AND p.deleted_at IS NULL AND (s.version IS DISTINCT FROM '{VERSION}'
            OR s.daily_version IS DISTINCT FROM '{DAILY_VERSION}'
            OR s.revision<>s.published_revision OR old.status IS DISTINCT FROM 'ready')'''


async def capture(session, run_id):
    if not await enabled(session):
        return 0
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_id,player_name,revision,score_revision)
        SELECT :id,player_id,player_name,revision,score_revision FROM ({needed()}) n'''), {'id':run_id})
    await session.execute(text(f'''INSERT INTO {INPUT}(run_id,input_index,game_link,is_white)
        SELECT :id,row_number() OVER(ORDER BY gp.game_link,gp.is_white),gp.game_link,gp.is_white
        FROM {SCOPE} s JOIN public.game_player gp ON gp.player_id=s.player_id
        JOIN public.game g ON g.link=gp.game_link
        WHERE s.run_id=:id AND g.is_standard AND g.n_moves>0 AND g.fens_done'''), {'id':run_id})
    return await session.scalar(text(f'SELECT count(*) FROM {SCOPE} WHERE run_id=:id'), {'id':run_id})


async def activate(session):
    from .runs import ACTIVE
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {RUN} WHERE status IN {ACTIVE})')):
        raise ValueError('Wait for the active CUDA run before enabling salience.')
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM public.player_salience_summary WHERE status IN ('queued','running'))")):
        raise ValueError('Wait for the old salience worker to become idle before enabling CUDA salience.')
    await session.execute(text(f'UPDATE {CONTROL} SET salience=true WHERE id AND manual AND scores AND accuracy'))
    if not await enabled(session):
        raise ValueError('Manual scores and accuracy must be enabled before CUDA salience.')


async def published_version(session, player):
    return await session.scalar(text(f'''SELECT s.version FROM {STATE} s
        JOIN public.player p ON p.id=s.player_id WHERE p.player_name=:player'''), {'player':player})
