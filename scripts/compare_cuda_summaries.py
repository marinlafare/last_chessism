"""Read-only comparison of an explicit CUDA rerun with its preserved baseline."""
import argparse
import asyncio
import copy
import json
import os
import re
from pathlib import Path
from sqlalchemy import text
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.cuda_summaries.accuracy_compute import FIELDS


def normalized_scores(payload):
    payload=copy.deepcopy(payload)
    for mode in payload.values():
        mode['mates']=sorted(mode.get('mates',[]),key=lambda r:(r['side'],r['moves'] if r['moves'] is not None else -1))
    return payload


async def compare(session,run_id):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='120s'"))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    run=(await session.execute(text('SELECT * FROM public.cuda_summary_run WHERE id=:id'),{'id':run_id})).mappings().one()
    if run['status']!='complete': raise ValueError('Run is not complete.')
    note=run['timings']['comparison']; namespace=note['baseline_schema']
    if not re.fullmatch(r'cuda_compare_[0-9a-f]{32}',namespace): raise ValueError('Invalid baseline schema')
    report={'run_id':str(run_id),'baseline':namespace,'players':note['players'],'checks':{}}
    async def check(name,query):
        report['checks'][name]=dict((await session.execute(text(query),{'model':run['model_id']})).mappings().one())
        print(name,json.dumps(report['checks'][name]),flush=True)
    rows=(await session.execute(text(f'''SELECT b.player_name,b.payload AS before,a.payload AS after
        FROM {namespace}.score_snapshot b FULL JOIN public.player_stockfish_snapshot a USING(player_name)
        WHERE COALESCE(b.player_name,a.player_name) IN (SELECT player_name FROM {namespace}.players)'''))).mappings()
    mismatches=[r['player_name'] for r in rows if r['before'] is None or r['after'] is None
                or normalized_scores(r['before'])!=normalized_scores(r['after'])]
    report['checks']['score_histograms']={'mismatches':len(mismatches),'players':mismatches}
    print('score_histograms',report['checks']['score_histograms'],flush=True)
    await check('blunder_games',f'''SELECT count(*) AS compared,
        count(*) FILTER(WHERE b.game_link IS NULL OR a.game_link IS NULL) AS missing,
        count(*) FILTER(WHERE (to_jsonb(b)-ARRAY['revision','source_sha256']) IS DISTINCT FROM
            (to_jsonb(a)-ARRAY['revision','source_sha256'])) AS mismatches
        FROM {namespace}.blunder_game b FULL JOIN public.canonical_blunder_game a
        ON a.model_id=b.model_id AND a.game_link=b.game_link
        WHERE COALESCE(b.model_id,a.model_id)=:model AND COALESCE(b.game_link,a.game_link)
            IN(SELECT game_link FROM {namespace}.games)''')
    await check('accuracy_games',f'''SELECT count(*) AS compared,
        count(*) FILTER(WHERE b.game_link IS NULL OR a.game_link IS NULL) AS missing,
        count(*) FILTER(WHERE (to_jsonb(b)-'accuracy') IS DISTINCT FROM (to_jsonb(a)-'accuracy')
            OR (b.accuracy IS NULL)<>(a.accuracy IS NULL)
            OR abs(b.accuracy-a.accuracy)>greatest(1e-9,1e-10*abs(b.accuracy))) AS mismatches,
        max(abs(b.accuracy-a.accuracy)) AS max_absolute_difference
        FROM {namespace}.accuracy_game b FULL JOIN public.cuda_accuracy_game a USING(player_name,game_link)
        WHERE COALESCE(b.player_name,a.player_name) IN(SELECT player_name FROM {namespace}.players)''')
    await check('daily_accuracy',f'''SELECT count(*) AS compared,
        count(*) FILTER(WHERE b.player_name IS NULL OR a.player_name IS NULL) AS missing,
        count(*) FILTER(WHERE (b.payload-'daily_salience') IS DISTINCT FROM (a.payload-'daily_salience')
            OR (b.payload->>'daily_salience' IS NULL)<>(a.payload->>'daily_salience' IS NULL)
            OR abs((b.payload->>'daily_salience')::float8-(a.payload->>'daily_salience')::float8)
                >greatest(1e-9,1e-10*abs((b.payload->>'daily_salience')::float8))) AS mismatches
        FROM {namespace}.accuracy_daily b FULL JOIN public.cuda_accuracy_daily a USING(player_name,day,mode_mask)
        WHERE COALESCE(b.player_name,a.player_name) IN(SELECT player_name FROM {namespace}.players)''')
    floats={'own_move_cp_gain','own_move_cp_loss','game_efficiency','mean_win_percent_loss','median_win_percent_loss','final_player_cp'}
    changed=['b.player_id IS NULL','a.player_id IS NULL']
    for field in FIELDS:
        changed.append(f'''((b.{field} IS NULL)<>(a.{field} IS NULL) OR
            abs(b.{field}-a.{field})>greatest(1e-9,1e-10*abs(b.{field})))''' if field in floats
            else f'b.{field} IS DISTINCT FROM a.{field}')
    await check('engine_statistics',f'''WITH current AS (
        SELECT gp.player_id,gp.game_link,gp.is_white,{','.join('s.'+f for f in FIELDS)}
        FROM public.game_player_engine_summary s JOIN public.game_player gp ON gp.id=s.game_player_id
        JOIN {namespace}.players p ON p.id=gp.player_id)
        SELECT count(*) AS compared,count(*) FILTER(WHERE {' OR '.join(changed)}) AS mismatches
        FROM {namespace}.engine_summary b FULL JOIN current a USING(player_id,game_link,is_white)''')
    # A reviewed zero-move exclusion correction intentionally changed only
    # cached unassessed counters, not inference. Compare to its retained input.
    rows=(await session.execute(text(f'''SELECT b.player_name,b.payload AS before,a.payload AS after,
        r.before_snapshots->b.player_name AS correction_before
        FROM {namespace}.blunder_snapshot b LEFT JOIN public.canonical_blunder_snapshot a USING(model_id,player_name)
        LEFT JOIN public.cuda_selection_correction r ON r.model_id=b.model_id
            AND r.policy='exclude_no_move_player_sides_v1' '''))).mappings()
    bad=[]; corrected=0
    for r in rows:
        if r['correction_before'] is not None: corrected+=1
        if r['before'] != (r['correction_before'] if r['correction_before'] is not None else r['after']): bad.append(r['player_name'])
    report['checks']['blunder_histograms']={'mismatches':len(bad),'players':bad,'reviewed_zero_move_corrections':corrected}
    report['passed']=all(v.get('mismatches',0)==0 and v.get('missing',0)==0 for v in report['checks'].values())
    return report


async def main(args):
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with AsyncDBSession() as s: result=await compare(s,args.run)
        print(json.dumps(result,indent=2),flush=True)
        if args.output:
            with Path(args.output).open('x') as stream: json.dump(result,stream,indent=2)
    finally: await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',required=True);parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
