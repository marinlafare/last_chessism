"""Reduce saved per-game CUDA results once per player, on CUDA as well."""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..canonical_blunders.schema import table
from ..canonical_blunders.assessment import empty_mode
from ..matrix_constructor.artifact_files import file_operation
from .score_compute import check_device, host, MODES


def reduce(xp, rows, *, require_cuda=True):
    check_device(xp,require_cuda)
    result = {mode:empty_mode() for mode in MODES}
    # Historical snapshots can still contain an owner who never made a move.
    # Exclude that side before device upload, not as an unassessed game.
    rows = [r for r in rows if r['status'] != 'assessed' or (r['assessed'] or 0)+(r['unassessed'] or 0)>0]
    if not rows: return result
    modes = xp.asarray([MODES.index(r['mode']) for r in rows])
    assessed = xp.asarray([r['assessed'] or 0 for r in rows],dtype=xp.int64)
    unassessed = xp.asarray([r['unassessed'] or 0 for r in rows],dtype=xp.int64)
    counts = xp.asarray([r['n'] or 0 for r in rows],dtype=xp.int64)
    complete = xp.asarray([r['complete'] for r in rows],dtype=xp.bool_)
    eligible = xp.asarray([r['status']=='assessed' for r in rows],dtype=xp.bool_)
    # CUDA atomic add supports uint64, not int64. These counters are nonnegative;
    # retain exact 64-bit integers (not float weights or overflowing int32).
    histogram = xp.zeros(21,dtype=xp.uint64)
    xp.add.at(histogram,modes[complete]*7+xp.minimum(6,counts[complete]),xp.uint64(1))
    histogram = host(xp,histogram.reshape(3,7))
    columns = {'games_count':complete,'partial_games':eligible & (assessed>0) & ~complete,
        'unassessed_games':eligible & (assessed==0),'ineligible_games':~eligible,
        'assessed_moves':assessed,'unassessed_moves':unassessed}
    for index,mode in enumerate(MODES): result[mode]['counts']=histogram[index].tolist()
    for name,values in columns.items():
        summed = xp.zeros(3,dtype=xp.uint64); xp.add.at(summed,modes,values.astype(xp.uint64))
        for mode,value in zip(MODES,host(xp,summed).tolist()): result[mode][name]=value
    return result


async def prepare(session, run, player, xp):
    t = table(storage_for(session),'assessment')
    pieces = [f'''SELECT mode,status,{side}_count AS n,{side}_assessed AS assessed,
        {side}_unassessed AS unassessed,{side}_complete AS complete FROM {t}
        WHERE model_id=:model AND {side}_owner=:player AND mode IN ('bullet','blitz','rapid')
        AND (status<>'assessed' OR {side}_assessed+{side}_unassessed>0)''' for side in ('white','black')]
    rows = [dict(r) for r in (await session.execute(text(' UNION ALL '.join(pieces)),
        {'model':run['model_id'],'player':player})).mappings()]
    return await file_operation(reduce,xp,rows)


async def publish(session, run, player, payload):
    await session.execute(text(f'''INSERT INTO {table(storage_for(session),'snapshot')}(model_id,player_name,payload)
        VALUES(:model,:player,CAST(:payload AS jsonb)) ON CONFLICT(model_id,player_name)
        DO UPDATE SET payload=EXCLUDED.payload,updated_at=clock_timestamp()'''),
        {'model':run['model_id'],'player':player,'payload':json.dumps(payload)})
