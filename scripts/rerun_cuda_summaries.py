"""Explicit full-cohort CUDA comparison run; preserve published baselines.

Administrative one-shot launcher, not an automatic ingestion hook. Run in the
configured application environment with --start. It creates a uniquely named
comparison schema, never deletes current results, and delegates calculation to
the existing manual CUDA worker. No source game/FEN rows or model are changed.
"""
import argparse
import asyncio
import json
import os
import uuid
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.relational_storage import identifier
from chessism_api.database.stockfish_summary_schema import summary_table
from chessism_api.operations.canonical_blunders.schema import table
from chessism_api.operations.canonical_blunders.scheduler import schedule
from chessism_api.operations.cuda_summaries import runs, schema, accuracy_compute
from chessism_api.redis_client import get_redis_pool, close_redis_pool


async def start(factory):
    namespace = 'cuda_compare_' + uuid.uuid4().hex
    base = identifier(namespace)
    layout = configured_storage()
    if layout is None:
        raise ValueError('Full-cohort CUDA comparisons require configured normalized storage.')
    async with factory() as s, s.begin():
        await s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
        await s.execute(text("SET LOCAL lock_timeout='3s'"))
        await s.execute(text("SET LOCAL statement_timeout='120s'"))
        await s.execute(text("SET LOCAL work_mem='64MB'"))
        control = (await s.execute(text(f'SELECT * FROM {schema.CONTROL} WHERE id FOR UPDATE'))).mappings().one()
        if not all(control[k] for k in ('manual','scores','accuracy')):
            raise ValueError('All three manual CUDA writers must be enabled.')
        for relation, predicate in (
            (schema.RUN, "status IN ('queued','running','retrying')"),
            ('algorithm_run', "status IN ('queued','running','cancelling')"),
            ('cloud_analysis_job', "status NOT IN ('complete','cancelled','limit_reached','failed')"),
            ('ingestion_pipeline_run', "status IN ('queued','running')"),
            (layout.table('fen_summary_refresh'), 'true'),
            (table(layout,'dirty'), 'true'),
            (schema.PENDING, 'true'),
        ):
            if await s.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
                raise ValueError(f'Existing work in {relation}; comparison not started.')
        canonical = (await s.execute(text(f'SELECT target_model,active_model FROM {table(layout,"control")} WHERE id FOR UPDATE'))).mappings().one()
        if not canonical['active_model'] or canonical['active_model'] != canonical['target_model']:
            raise ValueError('Canonical model must be fully published before comparison.')
        previous = (await s.execute(text(f"SELECT * FROM {schema.RUN} WHERE status='complete' ORDER BY created_at DESC LIMIT 1"))).mappings().one()
        await s.execute(text(f'CREATE SCHEMA {base}'))
        await s.execute(text(f"COMMENT ON SCHEMA {base} IS 'Explicit CUDA full-cohort comparison baseline; retain until reviewed'"))
        print('Saving baseline in '+namespace, flush=True)
        await s.execute(text(f'''CREATE TABLE {base}.players AS
            SELECT p.id,p.player_name,st.revision AS score_revision FROM {layout.table('player')} p
            JOIN {summary_table(layout,'state')} st USING(player_name)
            WHERE p.joined>0 AND p.deleted_at IS NULL AND st.revision=st.published_revision'''))
        players = await s.scalar(text(f'SELECT count(*) FROM {base}.players'))
        eligible = await s.scalar(text(f'SELECT count(*) FROM {layout.table("player")} WHERE joined>0 AND deleted_at IS NULL'))
        if not players or players != eligible:
            raise ValueError('Every eligible player must have a current baseline.')
        await s.execute(text(f'''CREATE TABLE {base}.games AS SELECT DISTINCT g.link AS game_link
            FROM {layout.table('game_player')} gp JOIN {base}.players p ON p.id=gp.player_id
            JOIN {layout.table('game')} g ON g.link=gp.game_link
            JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
            WHERE m.name IN ('bullet','blitz','rapid') AND
                ((gp.is_white AND g.white_player_id=p.id) OR (NOT gp.is_white AND g.black_player_id=p.id))'''))
        await s.execute(text(f'ALTER TABLE {base}.games ADD PRIMARY KEY(game_link)'))
        games = await s.scalar(text(f'SELECT count(*) FROM {base}.games'))
        queries = {
            'score_snapshot': f'SELECT x.* FROM {summary_table(layout,"snapshot")} x JOIN {base}.players p USING(player_name)',
            'accuracy_state': f'SELECT x.* FROM public.cuda_accuracy_state x JOIN {base}.players p USING(player_name)',
            'accuracy_game': f'SELECT x.* FROM public.cuda_accuracy_game x JOIN {base}.players p USING(player_name)',
            'accuracy_daily': f'SELECT x.* FROM public.cuda_accuracy_daily x JOIN {base}.players p USING(player_name)',
            'engine_summary': f'''SELECT gp.player_id,gp.game_link,gp.is_white,{','.join('x.'+f for f in accuracy_compute.FIELDS)}
                FROM {layout.table('game_player_engine_summary')} x
                JOIN {layout.table('game_player')} gp ON gp.id=x.game_player_id JOIN {base}.players p ON p.id=gp.player_id''',
            'blunder_game': f'''SELECT x.* FROM {table(layout,'game')} x JOIN {base}.games g USING(game_link)
                WHERE x.model_id=:model''',
            'blunder_snapshot': f'''SELECT x.* FROM {table(layout,'snapshot')} x JOIN {base}.players p USING(player_name)
                WHERE x.model_id=:model''',
        }
        for name, query in queries.items():
            await s.execute(text(f'CREATE TABLE {base}.{name} AS {query}'), {'model': canonical['active_model']})
            print('Saved '+name, flush=True)
        # Mark summary caches for rebuild, not the game/FEN data. The canonical
        # dirty insert's existing trigger queues all three metrics by revision.
        await s.execute(text(f'''UPDATE {summary_table(layout,'state')} st SET revision=revision+1,
            full_rebuild=true,error=NULL FROM {base}.players p WHERE st.player_name=p.player_name'''))
        await s.execute(text(f'''INSERT INTO {table(layout,'dirty')}(game_link)
            SELECT game_link FROM {base}.games ORDER BY game_link'''))
        run = await runs.request_run(s, 'explicit-user-full-cohort-comparison')
        note = {'baseline_schema': namespace, 'previous_run': str(previous['id']), 'players': players,
                'games': games, 'full_blunder_recalculation': True,
                'previous_blunder_games': await s.scalar(text(f'''SELECT count(*) FROM {schema.ITEM}
                    WHERE run_id=:id AND blunders'''), {'id':previous['id']}),
                'source_hash_note': 'Requeue advances revisions; compare blunder flags/counts, not revision-dependent source hashes.'}
        await s.execute(text(f'''UPDATE {schema.RUN} SET timings=timings || jsonb_build_object('comparison',CAST(:note AS jsonb))
            WHERE id=:id'''), {'id':run['id'], 'note':json.dumps(note)})
        await s.execute(text(f'CREATE TABLE {base}.metadata(key text PRIMARY KEY,payload jsonb NOT NULL)'))
        await s.execute(text(f'INSERT INTO {base}.metadata VALUES(:key,CAST(:payload AS jsonb))'), [
            {'key':'previous_run','payload':json.dumps(dict(previous),default=str)},
            {'key':'comparison','payload':json.dumps({**note,'new_run':str(run['id'])})}])
        print(json.dumps({'run_id':str(run['id']), **note}), flush=True)
    return run


async def main():
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        run = await start(AsyncDBSession)
        redis = await get_redis_pool()
        await schedule({'redis': redis})
        print('Committed and queued CUDA run '+str(run['id']), flush=True)
    finally:
        await close_redis_pool()
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', action='store_true', required=True)
    parser.parse_args()
    asyncio.run(main())
