"""Idempotent game writes and atomic histogram/model publication."""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .schema import table
from .assessment import empty_mode

MODES=('bullet','blitz','rapid')


async def summaries(session, model_id, players=None):
    layout=storage_for(session); t=lambda name:table(layout,name)
    owners=(await session.execute(text(f'''SELECT player_name FROM {layout.table('player')}
        WHERE joined>0 AND deleted_at IS NULL AND (:all_players OR player_name=ANY(CAST(:players AS text[])))'''),
        {'all_players':players is None,'players':list(players or [])})).scalars().all()
    payloads={p:{m:empty_mode() for m in MODES} for p in owners}
    if not owners: return
    # Small aggregation of GPU-produced integer results; no FENs or equations.
    query=f'''WITH sides AS (
        SELECT white_owner AS player,mode,white_count AS n,status,white_complete AS complete,
            white_assessed AS assessed,white_unassessed AS unassessed
        FROM {t('assessment')} WHERE model_id=:model AND white_owner=ANY(CAST(:players AS text[]))
        UNION ALL SELECT black_owner,mode,black_count,status,black_complete,black_assessed,black_unassessed
        FROM {t('assessment')} WHERE model_id=:model AND black_owner=ANY(CAST(:players AS text[])))
        SELECT player,mode,CASE WHEN complete THEN LEAST(6,n) END AS bin,
            count(*) FILTER(WHERE complete) AS games,
            count(*) FILTER(WHERE status='assessed' AND assessed>0 AND NOT complete) AS partial_games,
            count(*) FILTER(WHERE status='assessed' AND assessed=0) AS unassessed_games,
            count(*) FILTER(WHERE status<>'assessed') AS ineligible_games,
            COALESCE(sum(assessed),0) AS assessed_moves,COALESCE(sum(unassessed),0) AS unassessed_moves
        FROM sides WHERE mode IN ('bullet','blitz','rapid') AND (status<>'assessed' OR assessed+unassessed>0)
        GROUP BY player,mode,CASE WHEN complete THEN LEAST(6,n) END'''
    for row in (await session.execute(text(query),{'model':model_id,'players':owners})).mappings():
        mode=payloads[row['player']][row['mode']]
        if row['bin'] is not None:
            mode['counts'][row['bin']]=int(row['games'])
        mode['games_count']+=int(row['games'])
        for key in ('partial_games','unassessed_games','ineligible_games','assessed_moves','unassessed_moves'):
            mode[key]+=int(row[key])
    await session.execute(text(f'''INSERT INTO {t('snapshot')}(model_id,player_name,payload)
        SELECT :model,r.player,r.payload FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(player text,payload jsonb)
        ON CONFLICT(model_id,player_name) DO UPDATE SET payload=EXCLUDED.payload,updated_at=clock_timestamp()'''),
        {'model':model_id,'rows':json.dumps([{'player':p,'payload':v} for p,v in payloads.items()])})


async def publish(session, model_id, items, records, info, *, refresh_snapshots=True):
    layout=storage_for(session); t=lambda name:table(layout,name)
    await session.execute(text("SET LOCAL lock_timeout='2s'")); await session.execute(text("SET LOCAL statement_timeout='30s'"))
    control=(await session.execute(text(f'SELECT * FROM {t("control")} WHERE id FOR UPDATE'))).mappings().one()
    if control['target_model']!=model_id: return {'status':'model_changed','written':0}
    versions={r['game_link']:r['revision'] for r in items}
    current=dict((await session.execute(text(f'''SELECT d.game_link,d.revision FROM {t('dirty')} d
        WHERE d.game_link=ANY(CAST(:links AS bigint[]))
          AND NOT EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} r WHERE r.game_link=d.game_link)
        ORDER BY d.game_link FOR UPDATE OF d'''),{'links':sorted(versions)})).all())
    accepted={link for link,rev in current.items() if versions.get(link)==rev}
    if len(records)!=len(items) or {r['game_link'] for r in records}!=set(versions) or len(versions)!=len(items):
        raise ValueError('Missing or duplicate canonical output; no partial batch published.')
    old=(await session.execute(text(f'''SELECT white_owner,black_owner FROM {t('game')}
        WHERE model_id=:model AND game_link=ANY(CAST(:links AS bigint[]))'''),{'model':model_id,'links':sorted(accepted)})).all()
    owners={name for row in old for name in row if name}
    fields=('game_link','revision','mode','played_at','white_owner','black_owner','status','white_count','black_count','white_assessed','black_assessed','flags','source_sha256')
    kept=[{k:r.get(k) for k in fields} for r in records if r['game_link'] in accepted]
    owners.update(r.get(k) for r in kept for k in ('white_owner','black_owner') if r.get(k))
    if kept:
        from ..cuda_summaries.bulk import temp_rows, json_rows
        values = await json_rows(lambda: [tuple(json.dumps(r[k],allow_nan=False) if k=='flags' else r[k]
                                               for k in fields) for r in kept])
        await temp_rows(session, 'cuda_blunders_upload', '''game_link bigint,revision bigint,mode text,played_at timestamptz,
            white_owner text,black_owner text,status text,white_count int,black_count int,
            white_assessed int,black_assessed int,flags jsonb,source_sha256 text''', fields, values)
        await session.execute(text(f'''INSERT INTO {t('game')}(model_id,{','.join(fields)})
            SELECT :model,{','.join('r.'+k for k in fields)} FROM pg_temp.cuda_blunders_upload r
            ON CONFLICT(model_id,game_link) DO UPDATE SET {','.join(k+'=EXCLUDED.'+k for k in fields if k!='game_link')}'''),
            {'model':model_id})
        await session.execute(text(f'DELETE FROM {t("dirty")} WHERE game_link=ANY(CAST(:links AS bigint[]))'),{'links':sorted(accepted)})
    remaining=await session.scalar(text(f'''SELECT COUNT(*) FROM (
        SELECT game_link FROM {t('dirty')} UNION
        SELECT game_link FROM {layout.table('fen_summary_refresh')}) pending'''))
    activating=not remaining and control['active_model']!=model_id
    if activating: await summaries(session,model_id)
    elif refresh_snapshots and control['active_model']==model_id and owners: await summaries(session,model_id,owners)
    total=(control['progress'] or {}).get('processed_games',0)+len(kept)
    progress={**info,'processed_games':total,'remaining_games':remaining,'batch_written':len(kept),
        'stale_inputs_requeued':len(items)-len(kept),'phase':'ready' if not remaining else 'building'}
    await session.execute(text(f'''UPDATE {t('control')} SET
        previous_model=CASE WHEN :activate THEN active_model ELSE previous_model END,
        active_model=CASE WHEN :activate THEN :model ELSE active_model END,status=:status,
        progress=CAST(:progress AS jsonb),error=NULL,updated_at=clock_timestamp() WHERE id'''),
        {'activate':activating,'model':model_id,'status':'ready' if not remaining else 'building','progress':json.dumps(progress)})
    return {'status':'ready' if not remaining else 'building','written':len(kept),'remaining':remaining,'activated':activating}
