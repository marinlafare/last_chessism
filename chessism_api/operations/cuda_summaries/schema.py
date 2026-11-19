"""Canonical manual-run controls. Only the empty initializer owns their schema."""
from sqlalchemy import text

CONTROL = 'public.cuda_summary_control'
PENDING = 'public.cuda_summary_pending'
RUN = 'public.cuda_summary_run'
ITEM = 'public.cuda_summary_item'


async def enabled(session):
    return bool(await session.scalar(text(f'SELECT manual FROM {CONTROL} WHERE id')))




async def activate(session):
    """Call only after upgraded producers/consumers are deployed and idle."""
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    for relation, predicate in (
        ('cloud_analysis_job', "status NOT IN ('complete','cancelled','limit_reached','failed')"),
        ('algorithm_run', "status IN ('queued','running','cancelling')"),
        ('ingestion_pipeline_run', "status IN ('queued','running')"),
    ):
        if await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': relation}):
            if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
                raise ValueError(f'Wait for active {relation} work before activating manual summaries.')
    await session.execute(text(f'UPDATE {CONTROL} SET manual=true WHERE id'))
    await session.execute(text(f'''INSERT INTO {PENDING}(metric,game_link,revision)
        SELECT m.metric,d.game_link,d.revision FROM public.canonical_blunder_dirty d
        CROSS JOIN unnest(ARRAY['blunders','accuracy','score_distribution']) m(metric)
        ON CONFLICT(metric,game_link) DO UPDATE SET revision=EXCLUDED.revision'''))
