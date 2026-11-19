"""Bounded player inventory and revision checks for development training."""
from sqlalchemy import text

MAX_PLAYERS = 100


async def prepare(session):
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL jit=off'))
    if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
        raise ValueError('Active cloud workflow: wait before reading cohort inputs.')


async def profiles(session):
    rows = [dict(r) for r in (await session.execute(text('''SELECT p.player_name,s.revision,s.published_revision
        FROM public.player p LEFT JOIN public.player_stockfish_state s USING(player_name)
        WHERE p.joined>0 AND p.deleted_at IS NULL ORDER BY p.player_name LIMIT :limit'''),
        {'limit': MAX_PLAYERS+1})).mappings()]
    if not 1 <= len(rows) <= MAX_PLAYERS:
        raise ValueError('Empty or oversized cohort; no player truncation is allowed.')
    if any(r['revision'] is None or r['revision'] != r['published_revision'] for r in rows):
        raise ValueError('Finish existing player summary refreshes before the development training.')
    return rows


async def current_revision(session, player):
    return await session.scalar(text('''SELECT s.revision FROM public.player_stockfish_state s
        JOIN public.player p USING(player_name) WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL'''), {'player': player})
