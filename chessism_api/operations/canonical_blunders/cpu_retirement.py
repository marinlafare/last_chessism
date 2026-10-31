"""Remove the compatibility view's CPU fallback without changing saved results."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..evaluation_repair.plan import require_idle
from . import schema


async def upgrade(session):
    layout=storage_for(session)
    if not layout or not await schema.installed(session,layout):
        raise ValueError('Canonical CUDA schema must be installed first')
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    await require_idle(session,read_only=False)
    control=(await session.execute(text(f'SELECT * FROM {schema.table(layout,"control")} WHERE id FOR UPDATE'))).mappings().one()
    if not control['active_model'] or control['active_model']!=control['target_model']:
        raise ValueError('A complete active CUDA model is required')
    for relation in (schema.table(layout,'dirty'),layout.table('fen_summary_refresh')):
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation})')):
            raise ValueError('Wait for pending summary work before retiring CPU reads')
    await schema.install_engine_view(session,layout)
    return {'blunder_backend':'cuda_only','model_id':control['active_model'],
            'inference_changed':False,'results_recalculated':0,'historical_rows_deleted':0}
