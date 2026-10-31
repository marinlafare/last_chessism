"""GPU merge and revision-guarded publication of incremental metadata results."""
import json
from datetime import date
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.stockfish_summary_schema import summary_table
from ..matrix_constructor.artifact_files import file_operation
from ..player_timezone import resolve_player_timezone
from . import metadata_store as store,behavior_compute as behavior,ending_compute as endings
from .bulk import temp_rows,json_rows


def blank_behavior():return {'hours':[],'latest':None,'activity':[],'ratings':[],'distribution':[],'catalogs':[]}
def day_string(day):return '0001-01-01' if str(day)=='-infinity' else str(day)


async def stage(session,run,batch,behavior_parts,ending_parts):
    keys=set(behavior_parts)|set(ending_parts)
    if not keys:return
    values=await json_rows(lambda:[(p,d,json.dumps(behavior_parts[(p,d)]) if (p,d) in behavior_parts else None,
        json.dumps(ending_parts[(p,d)]) if (p,d) in ending_parts else None) for p,d in sorted(keys)])
    await temp_rows(session,'cuda_metadata_upload','player_name text,day text,behavior jsonb,endings jsonb',
        ('player_name','day','behavior','endings'),values)
    await session.execute(text(f'''INSERT INTO {store.PART}(run_id,batch,player_name,day,behavior,endings)
        SELECT :id,:batch,player_name,CASE WHEN day='0001-01-01' THEN DATE '-infinity' ELSE day::date END,
            behavior,endings FROM pg_temp.cuda_metadata_upload
        ON CONFLICT(run_id,batch,player_name,day) DO UPDATE SET behavior=EXCLUDED.behavior,endings=EXCLUDED.endings'''),
        {'id':run['id'],'batch':batch})


async def reduce(factory,run,xp):
    p={'id':run['id']}
    async with factory() as s:
        layout=storage_for(s)
        scopes=[dict(r) for r in (await s.execute(text(f'SELECT * FROM {store.SCOPE} WHERE run_id=:id ORDER BY player_name'),p)).mappings()]
        profiles=[r['profile'] for r in scopes]
        behavior_entries=[];ending_entries=[]
        for row in (await s.execute(text(f'SELECT player_name,day::text,behavior,endings FROM {store.PART} WHERE run_id=:id'),p)).mappings():
            key=(row['player_name'],day_string(row['day']))
            if row['behavior'] is not None:behavior_entries.append((*key,row['behavior']))
            if row['endings'] is not None:ending_entries.append((*key,row['endings']))
        for row in (await s.execute(text(f'SELECT player_name,day::text,behavior,endings FROM {store.DAY} WHERE run_id=:id'),p)).mappings():
            key=(row['player_name'],day_string(row['day']))
            if row['behavior']:behavior_entries.append((*key,blank_behavior()))
            if row['endings']:ending_entries.append((*key,endings.blank()))
        existing={}
        for kind,relation in (('behavior',analytics_table(layout,'partition')),('endings',store.ENDING_PART)):
            existing[kind]=[(r[0],day_string(r[1]),r[2]) for r in (await s.execute(text(f'''SELECT x.player_name,x.day::text,x.payload
                FROM {relation} x JOIN {store.SCOPE} q ON q.run_id=:id AND q.player_name=x.player_name
                WHERE q.{kind} AND NOT q.full_{kind} AND NOT EXISTS(SELECT 1 FROM {store.DAY} d
                    WHERE d.run_id=q.run_id AND d.player_name=q.player_name AND d.day=x.day AND d.{kind})'''),p)).all()]
    parts=await file_operation(behavior.combine,xp,behavior_entries,profiles,by_day=True)
    end_parts=await file_operation(endings.combine,xp,ending_entries,profiles,by_day=True)
    b_all=[*existing['behavior'],*[(p,d,v) for (p,d),v in parts.items()]]
    e_all=[*existing['endings'],*[(p,d,v) for (p,d),v in end_parts.items()]]
    for row in scopes:
        if row['behavior']:b_all.append((row['player_name'],'0001-01-01',blank_behavior()))
        if row['endings']:e_all.append((row['player_name'],'0001-01-01',endings.blank()))
    snapshots=await file_operation(behavior.combine,xp,b_all,profiles)
    ending_snapshots=await file_operation(endings.combine,xp,e_all,profiles)
    async with factory() as s,s.begin():
        from .metadata_validation import validate_and_retain
        validation=await validate_and_retain(s,run,scopes,snapshots)
        from .schema import RUN
        await s.execute(text(f"UPDATE {RUN} SET timings=jsonb_set(timings,'{{metadata_validation}}',CAST(:report AS jsonb)) WHERE id=:id"),
            {**p,'report':json.dumps(validation)})
        for row in scopes:
            player=row['player_name']
            payload={'behavior':snapshots.get(player),'endings':ending_snapshots.get(player),
                'behavior_parts':{d:v for (p,d),v in parts.items() if p==player},
                'ending_parts':{d:v for (p,d),v in end_parts.items() if p==player}}
            serialized=await json_rows(lambda:json.dumps(payload,allow_nan=False))
            await s.execute(text(f"UPDATE {store.SCOPE} SET result=CAST(:payload AS jsonb),status='ready' WHERE run_id=:id AND player_name=:player"),
                {**p,'player':player,'payload':serialized})


async def write_partitions(s,relation,player,parts,full):
    if full:await s.execute(text(f'DELETE FROM {relation} WHERE player_name=:p'),{'p':player})
    if not parts:return
    values=await json_rows(lambda:[(d,json.dumps(v)) for d,v in parts.items()])
    temporary='cuda_ending_parts' if relation==store.ENDING_PART else 'cuda_behavior_parts'
    await temp_rows(s,temporary,'day text,payload jsonb',('day','payload'),values)
    await s.execute(text(f'''INSERT INTO {relation}(player_name,day,payload)
        SELECT :p,CASE WHEN day='0001-01-01' THEN DATE '-infinity' ELSE day::date END,payload FROM pg_temp.{temporary}
        ON CONFLICT(player_name,day) DO UPDATE SET payload=EXCLUDED.payload'''),{'p':player})


async def publish(factory,run):
    async with factory() as s,s.begin():
        await s.execute(text("SET LOCAL lock_timeout='2s'"));await s.execute(text("SET LOCAL statement_timeout='30s'"))
        row=(await s.execute(text(f"SELECT * FROM {store.SCOPE} WHERE run_id=:id AND status='ready' ORDER BY player_name LIMIT 1 FOR UPDATE"),{'id':run['id']})).mappings().first()
        if not row:return False
        layout=storage_for(s);a=lambda k:analytics_table(layout,k);p={'id':run['id'],'player':row['player_name']}
        ar=await s.scalar(text(f'SELECT revision FROM {a("state")} WHERE player_name=:player FOR UPDATE'),p)
        sr=await s.scalar(text(f'SELECT revision FROM {summary_table(layout,"state")} WHERE player_name=:player FOR UPDATE'),p)
        preparing=await s.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} f
            JOIN {layout.table('game_player')} gp ON gp.game_link=f.game_link JOIN {layout.table('player')} p ON p.id=gp.player_id
            WHERE p.player_name=:player)'''),p) if row['endings'] else False
        if ar!=row['behavior_revision'] or (row['endings'] and (sr!=row['score_revision'] or preparing)):
            await s.execute(text(f"UPDATE {store.SCOPE} SET status='superseded',result=NULL WHERE run_id=:id AND player_name=:player"),p)
            return True
        profile=row['profile'];eligible=profile['joined'] and profile['joined']>0 and not profile['deleted_at']
        result=row['result']
        if not eligible:
            for relation in (a('snapshot'),a('partition'),store.ENDING_PART,store.STATE):
                await s.execute(text(f'DELETE FROM {relation} WHERE player_name=:player'),p)
        else:
            if row['behavior']:
                await write_partitions(s,a('partition'),row['player_name'],result['behavior_parts'],row['full_behavior'])
                for section,payload in result['behavior'].items():
                    payload={**payload,'time_context':resolve_player_timezone(profile).payload(),
                        'backend':'cuda_cupy','algorithm':behavior.VERSION}
                    await s.execute(text(f'''INSERT INTO {a('snapshot')}(player_name,section,revision,payload)
                        VALUES(:player,:section,:revision,CAST(:payload AS jsonb)) ON CONFLICT(player_name,section)
                        DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload,generated_at=clock_timestamp()'''),
                        {**p,'section':section,'revision':ar,'payload':json.dumps(payload)})
            if row['endings']:
                await write_partitions(s,store.ENDING_PART,row['player_name'],result['ending_parts'],row['full_endings'])
            old=await s.scalar(text(f'SELECT payload FROM {store.STATE} WHERE player_name=:player'),p)
            payload=result['endings'] if row['endings'] else old
            await s.execute(text(f'''INSERT INTO {store.STATE}(player_name,behavior_revision,score_revision,behavior_version,ending_version,payload)
                VALUES(:player,:ar,:sr,:bv,:ev,CAST(:payload AS jsonb)) ON CONFLICT(player_name) DO UPDATE SET
                behavior_revision=EXCLUDED.behavior_revision,score_revision=EXCLUDED.score_revision,
                behavior_version=EXCLUDED.behavior_version,ending_version=EXCLUDED.ending_version,
                payload=EXCLUDED.payload,generated_at=clock_timestamp()'''),
                {**p,'ar':ar,'sr':sr,'bv':behavior.VERSION,'ev':endings.VERSION,'payload':json.dumps(payload)})
        if row['behavior']:
            await s.execute(text(f'DELETE FROM {a("dirty")} WHERE player_name=:player'),p)
            await s.execute(text(f'''UPDATE {a('state')} SET published_revision=revision,full_rebuild=:full,
                attempted_at=clock_timestamp(),error=NULL WHERE player_name=:player'''),{**p,'full':not eligible})
        await s.execute(text(f"UPDATE {store.SCOPE} SET status='published',result=NULL WHERE run_id=:id AND player_name=:player"),p)
    return True
