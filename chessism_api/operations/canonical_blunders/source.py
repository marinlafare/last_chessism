"""Unique-game, mixed-player extraction; bounded queries and source-version checks."""
import itertools
import time
import numpy as np
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..research_algorithms.blunder_training import game_blocks,positions_sql
from . import compute
from .schema import table
from .eligibility import moving_owners

MAX_GAMES = 32768
MAX_ROWS = 2097152


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL jit=off'))


async def load(session, report=lambda *a:None, *, selected_items=None, timings=None, check=None, position_reader='legacy'):
    # Optional read-only selection/timers are used by the isolated benchmark.
    # The normal dirty-queue path and immutable inference implementation stay unchanged.
    def add(key, started):
        if timings is not None: timings[key]=timings.get(key,0.)+time.perf_counter()-started
    started=time.perf_counter()
    if position_reader not in ('legacy','binary_v1'):raise ValueError('Unknown canonical source reader.')
    layout=storage_for(session); t=lambda name:table(layout,name)
    control=(await session.execute(text(f'SELECT * FROM {t("control")}'))).mappings().one()
    model_id=control['target_model']
    if not model_id: return None
    config=await session.scalar(text(f'SELECT config FROM {t("model")} WHERE id=:id'),{'id':model_id})
    if selected_items is None:
        dirty=[dict(r) for r in (await session.execute(text(f'''SELECT d.game_link,d.revision FROM {t('dirty')} d
            WHERE NOT EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} r WHERE r.game_link=d.game_link)
            ORDER BY d.game_link LIMIT :limit'''),{'limit':MAX_GAMES})).mappings()]
    else:
        dirty=[dict(r) for r in selected_items]
        if len(dirty)>MAX_GAMES or len({r['game_link'] for r in dirty})!=len(dirty):
            raise ValueError('Benchmark selection must be bounded and contain unique games.')
    add('source_queries_seconds',started)
    if not dirty: return model_id,config,[],[],np.empty(0,dtype=compute.DTYPE)
    links=[r['game_link'] for r in dirty]
    started=time.perf_counter()
    data=[dict(r) for r in (await session.execute(text(f'''SELECT g.link AS game_link,m.name AS mode,g.played_at,
        g.white_elo,g.black_elo,c.total_positions,COALESCE(c.is_fully_analyzed,false) AS analyzed,
        CASE WHEN w.joined>0 AND w.deleted_at IS NULL AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp WHERE gp.game_link=g.link AND gp.player_id=w.id AND gp.is_white) THEN w.player_name END AS white_owner,
        CASE WHEN b.joined>0 AND b.deleted_at IS NULL AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp WHERE gp.game_link=g.link AND gp.player_id=b.id AND NOT gp.is_white) THEN b.player_name END AS black_owner
        FROM {layout.table('game')} g JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
        JOIN {layout.table('player')} w ON w.id=g.white_player_id JOIN {layout.table('player')} b ON b.id=g.black_player_id
        LEFT JOIN {layout.table('game_analysis_coverage')} c ON c.link=g.link
        WHERE g.link=ANY(CAST(:links AS bigint[])) ORDER BY g.link'''),{'links':links})).mappings()]
    add('source_queries_seconds',started); started=time.perf_counter()
    by_id={g['game_link']:g for g in data}; accepted=[]; unsupported=[]; taken=[]; total=0
    for item in dirty:
        g=moving_owners(by_id.get(item['game_link']))
        eligible=bool(g and g['analyzed'] and (g['total_positions'] or 0)>0 and g['mode'] in ('bullet','blitz','rapid')
            and (g['white_owner'] or g['black_owner']) and all(isinstance(g[k],int) and 1<=g[k]<=4000 for k in ('white_elo','black_elo')))
        if eligible and accepted and total+g['total_positions']>MAX_ROWS: break
        taken.append(item)
        if eligible:
            g={**g,**item,'link':g['game_link']}; total+=g['total_positions']; accepted.append(g)
        else:
            unsupported.append({**(g or {}),**item,'status':'unsupported_or_ineligible','white_count':None,'black_count':None,
                'white_assessed':0,'black_assessed':0,'flags':{'blunders':[],'unassessed':[]},'source_sha256':'ineligible'})
    add('source_pack_seconds',started)
    if position_reader=='binary_v1':
        from .binary_source import load as binary_load
        rows=await binary_load(session,accepted,timings=timings,check=check)
        return model_id,config,taken,(accepted,unsupported),rows
    parts=[]; index=0
    for block in game_blocks(accepted):
        if check: await check()
        started=time.perf_counter()
        records=(await session.execute(text(positions_sql(session)),{'links':[g['game_link'] for g in block],
            'row_limit':sum(g['total_positions'] for g in block)+1})).all()
        add('source_queries_seconds',started); started=time.perf_counter()
        grouped={link:[(r[1],r[2],r[3]) for r in rows] for link,rows in itertools.groupby(records,lambda r:r[0])}
        for g in block:
            parts.append(compute.pack(g,grouped.pop(g['game_link'],[]),index)); index+=1
        if grouped: raise ValueError('Unexpected source game.')
        add('source_pack_seconds',started)
    started=time.perf_counter()
    rows=np.concatenate(parts) if parts else np.empty(0,dtype=compute.DTYPE)
    add('source_pack_seconds',started)
    return model_id,config,taken,(accepted,unsupported),rows
