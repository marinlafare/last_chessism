"""Compare to retained current summaries; never recalculate a CPU reference."""
import json
import math
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table

BASELINE='public.cuda_metadata_baseline'
IDENTITY=('mode','date','weekday','hour','kind','color','key','rating','link')


def equivalent(a,b):
    if isinstance(a,dict) and isinstance(b,dict):
        return a.keys()==b.keys() and all(equivalent(a[k],b[k]) for k in a)
    if isinstance(a,list) and isinstance(b,list):
        if len(a)!=len(b):return False
        key=lambda r:json.dumps([r.get(k) for k in IDENTITY],sort_keys=True) if isinstance(r,dict) else json.dumps(r,sort_keys=True)
        return all(equivalent(x,y) for x,y in zip(sorted(a,key=key),sorted(b,key=key)))
    if isinstance(a,(int,float)) and isinstance(b,(int,float)):
        return a==b if isinstance(a,int) and isinstance(b,int) else math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-9)
    return a==b


async def validate_and_retain(session,run,scopes,snapshots):
    """Initial CPU→CUDA migration gate; changed/stale baselines are not evidence."""
    table=lambda n:analytics_table(storage_for(session),n)
    checked=[];skipped=[]
    for row in scopes:
        if not row['behavior'] or row['player_name'] not in snapshots:continue
        player=row['player_name'];params={'p':player,'id':run['id']}
        revisions=(await session.execute(text(f'SELECT revision,published_revision FROM {table("state")} WHERE player_name=:p'),params)).one()
        if revisions[0]!=row['behavior_revision'] or revisions[1]!=row['behavior_revision']:
            skipped.append(player);continue
        old=(await session.execute(text(f'SELECT section,payload FROM {table("snapshot")} WHERE player_name=:p'),params)).all()
        if not old:skipped.append(player);continue
        new=json.loads(json.dumps(snapshots[player]))
        for section,payload in old:
            reference={k:v for k,v in payload.items() if k not in ('time_context','backend','algorithm')}
            if section not in new or not equivalent(reference,new[section]):
                raise ValueError(f'Metadata parity failed for {player}/{section}; previous summaries retained.')
            await session.execute(text(f'''INSERT INTO {BASELINE}(run_id,player_name,section,payload)
                VALUES(:id,:p,:section,CAST(:payload AS jsonb)) ON CONFLICT DO NOTHING'''),
                {**params,'section':section,'payload':json.dumps(payload)})
        checked.append(player)
    return {'passed_players':checked,'no_current_baseline':skipped,'counts':'exact','float_absolute_tolerance':1e-9,'float_relative_tolerance':1e-10}
