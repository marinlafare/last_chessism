"""Disposable check of the full-cohort launcher; never runs a live GPU job."""
import asyncio
from unittest.mock import patch
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base, check_accuracy_cuda_postgres as accuracy
from scripts import rerun_cuda_summaries as launcher
from chessism_api.operations.cuda_summaries import runs


async def check(factory, connection, layout):
    await accuracy.check(factory, connection, layout)
    await connection.execute('CREATE TABLE IF NOT EXISTS ingestion_pipeline_run(status text)')
    await connection.execute('CREATE TABLE IF NOT EXISTS algorithm_run(status text)')
    before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(s) ORDER BY player_name)::text FROM candidate.player_stockfish_snapshot s')
    fens = await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM candidate.fen f')
    with patch.object(launcher, 'configured_storage', return_value=layout):
        run = await launcher.start(factory)
        async with factory() as s:
            saved = await runs.progress(s, run['id'])
        note = saved['timings']['comparison']
        assert saved['status'] == 'queued' and note['players'] == 2 and note['games'] > 0
        assert saved['total_games'] == note['games']
        assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(s) ORDER BY player_name)::text FROM candidate.player_stockfish_snapshot s')
        assert fens == await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM candidate.fen f')
        assert await connection.fetchval(f'SELECT count(*) FROM "{note["baseline_schema"]}".score_snapshot') == 2
        async with factory() as s:
            assert await s.scalar(text('SELECT count(*) FROM public.cuda_summary_item WHERE run_id=:id AND blunders'), {'id':run['id']}) == note['games']
        try: await launcher.start(factory)
        except ValueError as e: assert 'Existing work' in str(e)
        else: raise AssertionError('Duplicate full-cohort run was accepted')
    print('PASS full-cohort launcher: durable baselines, all three metrics queued, no source/result deletion, duplicate guard')


if __name__ == '__main__':
    original = base.check
    async def wrapped(*args):
        base.check = original
        await check(*args)
    base.check = wrapped
    asyncio.run(base.main())
