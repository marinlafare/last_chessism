"""One-time GPU correction of cached zero-move exclusions, not game inference."""
import copy
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..canonical_blunders.schema import table
from ..matrix_constructor.artifact_files import file_operation
from .score_compute import MODES, check_device, host
from . import schema

POLICY = 'exclude_no_move_player_sides_v1'
RECEIPT = 'public.cuda_selection_correction'


def corrected(xp, snapshots, empty_sides, *, require_cuda=True):
    check_device(xp, require_cuda)
    players = sorted(snapshots); indexes = {p:i for i,p in enumerate(players)}
    groups = [indexes[r['player_name']]*3+MODES.index(r['mode']) for r in empty_sides]
    removed = xp.zeros(len(players)*3,dtype=xp.uint64)
    xp.add.at(removed,xp.asarray(groups,dtype=xp.int64),xp.uint64(1))
    old = xp.asarray([snapshots[p][m]['unassessed_games'] for p in players for m in MODES],dtype=xp.uint64)
    if bool(xp.any(removed>old)):
        raise ValueError('Cached coverage changed; no counters corrected.')
    values = host(xp,old-removed).tolist(); removed = host(xp,removed).tolist()
    result = {}
    for index,player in enumerate(players):
        if any(removed[index*3:index*3+3]):
            payload = copy.deepcopy(snapshots[player])
            for mode,value in zip(MODES,values[index*3:index*3+3]): payload[mode]['unassessed_games']=int(value)
            result[player]=payload
    return result


async def upgrade(session, xp):
    """Caller owns transaction. Guard admission, keep old snapshots in a receipt."""
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    layout = storage_for(session); t=lambda n:table(layout,n)
    for relation,predicate in (
        (schema.RUN,"status IN ('queued','running','retrying')"),
        ('algorithm_run',"status IN ('queued','running','cancelling')"),
        ('cloud_analysis_job',"status NOT IN ('complete','cancelled','limit_reached','failed')"),
        ('ingestion_pipeline_run',"status IN ('queued','running')"),
        (t('dirty'),'true'), (layout.table('fen_summary_refresh'),'true'),
    ):
        await session.execute(text(f'LOCK TABLE {relation} IN SHARE MODE NOWAIT'))
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
            raise ValueError('Wait for pending work before correcting zero-move selection.')
    control=(await session.execute(text(f'SELECT active_model,target_model FROM {t("control")} WHERE id FOR UPDATE'))).mappings().one()
    model=control['active_model']
    if not model or model != control['target_model']: raise ValueError('A fully published model is required.')
    await session.execute(text(f'LOCK TABLE {t("game")} IN SHARE MODE NOWAIT'))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {RECEIPT}(
        policy text,model_id text,before_snapshots jsonb NOT NULL,after_snapshots jsonb NOT NULL,
        excluded_sides bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(policy,model_id))'''))
    params={'policy':POLICY,'model':model}
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {RECEIPT} WHERE policy=:policy AND model_id=:model)'),params):
        return {'already_applied':True,'policy':POLICY}
    snapshots=dict((await session.execute(text(f'''SELECT b.player_name,b.payload FROM {t('snapshot')} b
        JOIN {layout.table('player')} p USING(player_name) WHERE model_id=:model
        AND p.joined>0 AND p.deleted_at IS NULL FOR UPDATE OF b'''),params)).all())
    queries=[f'''SELECT {side}_owner AS player_name,mode FROM {t('assessment')}
        WHERE model_id=:model AND status='assessed' AND {side}_assessed=0 AND {side}_unassessed=0
        AND {side}_owner=ANY(CAST(:players AS text[])) AND mode IN ('bullet','blitz','rapid')''' for side in ('white','black')]
    rows=[dict(r) for r in (await session.execute(text(' UNION ALL '.join(queries)),
        {**params,'players':list(snapshots)})).mappings()]
    changed=await file_operation(corrected,xp,snapshots,rows)
    from .blunder_summary import publish
    for player,payload in changed.items(): await publish(session,{'model_id':model},player,payload)
    await session.execute(text(f'''INSERT INTO {RECEIPT}(policy,model_id,before_snapshots,after_snapshots,excluded_sides)
        VALUES(:policy,:model,CAST(:before AS jsonb),CAST(:after AS jsonb),:excluded)'''),
        {**params,'before':json.dumps({p:snapshots[p] for p in changed}),'after':json.dumps(changed),'excluded':len(rows)})
    return {'policy':POLICY,'updated_players':len(changed),'excluded_sides':len(rows),
            'game_results_recalculated':0,'fen_rows_changed':0,'backend':'cuda'}
