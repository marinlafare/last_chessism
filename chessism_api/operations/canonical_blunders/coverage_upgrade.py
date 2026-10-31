"""Atomic read-policy upgrade using saved results; never re-fit or run CUDA."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..fen_summary_recovery import install as install_recovery
from . import schema, assessment, publication


async def upgrade(session):
    layout = storage_for(session)
    if not layout or not await schema.installed(session, layout):
        raise ValueError('Install canonical summaries before upgrading coverage.')
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    control = (await session.execute(text(f'SELECT * FROM {schema.table(layout,"control")} WHERE id FOR UPDATE'))).mappings().one()
    if control['active_model'] != control['target_model']:
        raise ValueError('Wait for the active model rebuild before upgrading coverage.')
    if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
        raise ValueError('Wait for the active cloud job before upgrading coverage.')
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {schema.table(layout,"dirty")})')):
        raise ValueError('Wait for pending canonical refreshes before upgrading coverage.')
    await install_recovery(session, layout)
    await assessment.install_view(session, layout)
    await schema.install_engine_view(session, layout)
    players = (await session.execute(text(f'SELECT player_name FROM {layout.table("player")} WHERE joined>0 AND deleted_at IS NULL ORDER BY player_name'))).scalars().all()
    if control['active_model']:
        for player in players:
            await publication.summaries(session, control['active_model'], [player])
    return {'assessment_policy': assessment.POLICY, 'players': len(players),
            'model_id': control['active_model'], 'inference_changed': False}
