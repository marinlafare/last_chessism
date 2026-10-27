"""Read published CUDA results only. Missing/unassessed is never zero."""
import os
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .schema import table


def enabled():
    return os.environ.get('CHESSISM_CANONICAL_BLUNDERS','0')=='1'


def engine_table(layout):
    return table(layout,'engine') if enabled() else layout.table('game_player_engine_summary')


async def current(session):
    if not enabled(): return None
    return (await session.execute(text(f'SELECT * FROM {table(storage_for(session),"control")}'))).mappings().one()


async def saved(session,player):
    layout=storage_for(session); t=lambda name:table(layout,name)
    control=await current(session)
    if not control or not control['active_model']: return None
    if not await session.scalar(text(f'SELECT 1 FROM {layout.table("player")} WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'),{'player':player}):
        raise LookupError('Player was not found.')
    row=(await session.execute(text(f'SELECT payload,updated_at FROM {t("snapshot")} WHERE model_id=:model AND player_name=:player'),
        {'model':control['active_model'],'player':player})).mappings().first()
    pending=await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {t('dirty')} d
        JOIN {layout.table('game_player')} gp ON gp.game_link=d.game_link
        WHERE gp.player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player))'''),{'player':player})
    from ..player_accuracy_distribution import blunder_bins,MODES
    return {'version':1,'player_name':player,'metric':'blunders','bins':blunder_bins(),
        'by_mode':row['payload'] if row else {m:{'counts':[0]*7,'games_count':0} for m in MODES},
        'generated_at':row['updated_at'].isoformat() if row else None,'source':'canonical_cuda_blunders',
        'model_id':control['active_model'],'target_model_id':control['target_model'],
        'approval':'provisional_user_approved','quality_approved':False,
        'summary_pending':row is None,'stale':bool(pending or row is None or control['active_model']!=control['target_model']),
        'fully_analyzed_only':True,'count_unit':'game_player_side','cache_hit':False,'max_age_seconds':15}


async def game_flags(session,game_id):
    control=await current(session)
    if not control or not control['active_model']: return None
    t=lambda name:table(storage_for(session),name)
    row=(await session.execute(text(f'''SELECT flags,status,EXISTS(SELECT 1 FROM {t('dirty')} WHERE game_link=:game) AS pending
        FROM {t('game')} WHERE model_id=:model AND game_link=:game'''),{'model':control['active_model'],'game':game_id})).mappings().first()
    return {'model_id':control['active_model'],'pending':not row or row['pending'] or row['status']!='assessed',
        **(row['flags'] if row else {'blunders':[],'unassessed':[]})}
