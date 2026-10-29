"""Disposable PostgreSQL: manual scheduling, revision safety, no CPU/GPU auto-work."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations.cuda_summaries import schema, runs
from chessism_api.operations.canonical_blunders import worker, binary_source
from chessism_api.operations import player_game_scores, player_stockfish_summary


async def check(factory, connection, layout):
    await base.check(factory, connection, layout, position_reader='binary_v1')
    async with factory() as s, s.begin():
        await schema.install(s, layout)
        assert not await schema.enabled(s)
        await schema.activate(s, layout)
        assert await schema.enabled(s)
    device = SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda: (2**33, 2**33))),
        get_default_memory_pool=lambda: SimpleNamespace(free_all_blocks=lambda: None, set_limit=lambda **kw: None))
    calls = []
    async def boundary(function, *args, **kwargs):
        calls.append(function)
        if function is worker.cuda_backend:
            return device, {'name': 'NVIDIA GeForce RTX 4060'}
        assert function is worker.compute.infer and args[0] is device
        return function(np, *args[1:], require_cuda=False)
    async def request():
        async with factory() as s, s.begin():
            return await runs.request_run(s, 'synthetic-admin')
    async def saved(run_id):
        async with factory() as s: return await runs.progress(s, run_id)
    async def change(game):
        await connection.execute('UPDATE candidate.game SET white_elo=white_elo WHERE link=$1', game)
    with patch.object(worker, 'AsyncDBSession', factory), patch.object(worker, 'file_operation', side_effect=boundary):
        await change(1); await change(1); await change(2)
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_summary_pending') == 6
        before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(g)) FROM candidate.canonical_blunder_game g')
        result = await worker.canonical_tick({})
        assert result['status'] == 'awaiting_user' and not calls
        assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(g)) FROM candidate.canonical_blunder_game g')
        # Calling legacy CPU entry points cannot compute or mutate existing data.
        with patch.object(player_game_scores, 'AsyncDBSession', factory):
            assert (await player_game_scores.refresh_game_player_engine_summaries((1,)))['deferred']
        assert (await player_stockfish_summary.refresh_player('alice', session_factory=factory))['status'] == 'awaiting_cuda_migration'
        with patch.object(player_stockfish_summary, 'AsyncDBSession', factory):
            assert await player_stockfish_summary.refresh_pending() is None
        first, duplicate = await asyncio.gather(request(), request())
        assert first['id'] == duplicate['id'] and first['total_games'] == 2
        await change(1); await change(3)  # Neither new revision belongs to the request.
        result = await worker.canonical_tick({})
        first = await saved(first['id'])
        assert first['status'] == 'complete' and first['processed_games'] == first['superseded_games'] == 1, first
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_summary_pending WHERE metric='blunders'") == 2
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_summary_pending WHERE metric='accuracy'") == 3
        assert result['remaining'] == 0
        calls.clear()
        assert (await worker.canonical_tick({}))['status'] == 'awaiting_user' and not calls
        second = await request()
        # A source change after loading is rejected during publication as well.
        original = binary_source.load
        async def racing_load(*args, **kwargs):
            rows = await original(*args, **kwargs)
            await change(1)
            return rows
        with patch.object(binary_source, 'load', side_effect=racing_load):
            await worker.canonical_tick({})
        second = await saved(second['id'])
        assert second['status'] == 'complete' and second['superseded_games'] == 1
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_summary_pending WHERE metric='blunders'") == 1
        third = await request()
        with patch.object(binary_source, 'load', AsyncMock(side_effect=ValueError('synthetic failure'))), patch.object(worker.LOG, 'exception'):
            for attempt in range(3):
                await connection.execute("UPDATE public.cuda_summary_run SET retry_after=NULL WHERE id=$1", third['id'])
                assert (await worker.canonical_tick({}))['status'] == 'retrying'
        assert (await saved(third['id']))['status'] == 'failed'
        calls.clear()
        assert (await worker.canonical_tick({}))['status'] == 'awaiting_user' and not calls
        async with factory() as s, s.begin(): await runs.retry_run(s, third['id'])
        await worker.canonical_tick({})
        assert (await saved(third['id']))['status'] == 'complete'
        async with factory() as s:
            state = await runs.status(s)
            assert state['active'] is None and len(state['history']) == 3
            assert not state['pending_games'].get('blunders')
            assert state['pending_games']['accuracy'] == 3
            assert (await runs.status(s, player='alice'))['pending_metrics'] == ['accuracy','score_distribution']
        # Preparation still runs automatically, without requesting inference.
        await connection.execute("""INSERT INTO candidate.fen_summary_refresh(game_link,refresh_engine)
            VALUES(1,false) ON CONFLICT(game_link) DO UPDATE SET refresh_engine=false""")
        calls.clear()
        result = await worker.canonical_tick({})
        assert result['input_recovery']['source_only'] == 1 and not calls
        assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 0
    print('PASS manual CUDA: no automatic inference/CPU summaries, coalescing, concurrent clicks, captured revisions, extraction races, exact acknowledgements, failure/retry, durable history and independent preparation')


if __name__ == '__main__':
    original = base.check
    async def wrapped(*args):
        base.check = original
        await check(*args)
    base.check = wrapped
    asyncio.run(base.main())
