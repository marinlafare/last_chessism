"""Read published CUDA results only. Missing/unassessed is never zero."""
from sqlalchemy import text
from .assessment import POLICY, empty_mode


def engine_table():
    # CUDA is the production contract, not an optional CPU/GPU rollout toggle.
    return 'public.canonical_blunder_engine'


async def current(session):
    return (await session.execute(text(f'SELECT * FROM public.canonical_blunder_control'))).mappings().one_or_none()


async def saved(session,player):
    control=await current(session)
    players='public.player'
    if not await session.scalar(text(f'SELECT 1 FROM {players} WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'),{'player':player}):
        raise LookupError('Player was not found.')
    row=None; pending=True
    if control and control['active_model']:
        row=(await session.execute(text(f'SELECT payload,updated_at FROM public.canonical_blunder_snapshot WHERE model_id=:model AND player_name=:player'),
            {'model':control['active_model'],'player':player})).mappings().first()
        pending=await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM (
            SELECT game_link FROM public.canonical_blunder_dirty UNION SELECT game_link FROM public.fen_summary_refresh) d
            JOIN public.game_player gp ON gp.game_link=d.game_link
            WHERE gp.player_id=(SELECT id FROM public.player WHERE player_name=:player))'''),{'player':player})
    if row and any(row['payload'].get(mode, {}).get('assessment_policy') != POLICY for mode in ('bullet','blitz','rapid')):
        row = None  # Never label a pre-migration partial histogram as complete.
    from ..player_accuracy_distribution import blunder_bins,MODES
    return {'version':1,'player_name':player,'metric':'blunders','bins':blunder_bins(),
        'by_mode':row['payload'] if row else {m:empty_mode() for m in MODES},
        'generated_at':row['updated_at'].isoformat() if row else None,'source':'canonical_cuda_blunders',
        'model_id':control['active_model'] if control else None,'target_model_id':control['target_model'] if control else None,
        'approval':'provisional_user_approved','quality_approved':False,
        'summary_pending':row is None,'stale':bool(pending or row is None or control['active_model']!=control['target_model']),
        'fully_analyzed_only':True,'assessment_policy':POLICY,
        'count_unit':'game_player_side','cache_hit':False,'max_age_seconds':15}


async def game_flags(session,game_id):
    control=await current(session)
    if not control or not control['active_model']: return None
    row=(await session.execute(text(f'''SELECT flags,status,white_assessed,black_assessed,
        white_unassessed,black_unassessed,white_complete,black_complete,white_count,black_count,
        (EXISTS(SELECT 1 FROM public.canonical_blunder_dirty WHERE game_link=:game) OR
         EXISTS(SELECT 1 FROM public.fen_summary_refresh WHERE game_link=:game)) AS pending
        FROM public.canonical_blunder_assessment WHERE model_id=:model AND game_link=:game'''),{'model':control['active_model'],'game':game_id})).mappings().first()
    return {'model_id':control['active_model'],'pending':not row or row['pending'],
        'supported':bool(row and row['status']=='assessed'),
        'coverage':{side:{'assessed_moves':row[side+'_assessed'],'unassessed_moves':row[side+'_unassessed'],
            'complete':row[side+'_complete'],'detected_blunders':row[side+'_count']} for side in ('white','black')} if row else None,
        **(row['flags'] if row else {'blunders':[],'unassessed':[]})}
