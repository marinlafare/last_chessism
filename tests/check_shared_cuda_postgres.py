"""Disposable PostgreSQL: shared extraction, CPU oracle parity and revision safety."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.database.stockfish_summary_schema import install_stockfish_summaries
from chessism_api.operations.cuda_summaries import schema, runs, score_runs, shared_source, shared_worker, score_compute, score_publication
from chessism_api.operations.cuda_summaries import batch_budget, blunder_compute, device_inputs, input_snapshot, blunder_summary, snapshot_flow
from chessism_api.operations.canonical_blunders import source, worker, compute, publication
from chessism_api.operations import player_stockfish_summary as old


async def check(factory, connection, layout):
    await base.check(factory, connection, layout, position_reader='binary_v1')
    await connection.execute('''ALTER TABLE candidate.moves ADD COLUMN n_move int,
        ADD COLUMN white_move text,ADD COLUMN black_move text;
        UPDATE candidate.player SET deleted_at=NULL WHERE player_name='bob';
        UPDATE candidate.game SET played_at='2025-01-02 10:00:00+00';
        UPDATE candidate.game SET played_at=NULL WHERE link=3;
        UPDATE candidate.game SET white_result=.5,black_result=.5,white_str_result='stalemate',black_str_result='stalemate' WHERE link=2;
        INSERT INTO candidate.moves VALUES(2,11,'Qh6','--');
        UPDATE candidate.game SET black_str_result='checkmated' WHERE link=1;
        INSERT INTO candidate.fen VALUES(6,NULL,NULL),(7,-9996,'stockfish'),(8,9998,NULL),(9,0,'stockfish');
        UPDATE candidate.game_fen_association SET fen_id=6 WHERE game_link=4 AND n_move=1 AND is_white;
        UPDATE candidate.game_fen_association SET fen_id=7 WHERE game_link=3 AND n_move=1 AND is_white;
        UPDATE candidate.game_fen_association SET fen_id=8 WHERE game_link=3 AND n_move=2 AND is_white;
        UPDATE candidate.game_fen_association SET fen_id=9 WHERE game_link=3 AND n_move=3 AND is_white;
        UPDATE candidate.game_analysis_coverage SET is_fully_analyzed=false WHERE link=4;
        DELETE FROM candidate.fen_summary_refresh;''')
    async with factory() as s, s.begin():
        await install_stockfish_summaries(s, layout)
        await schema.install(s, layout); await schema.activate(s, layout)
        await s.execute(text(f'UPDATE {schema.CONTROL} SET scores=true'))
    device = SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda: (2**33, 2**33))),
        get_default_memory_pool=lambda: SimpleNamespace(free_all_blocks=lambda: None, set_limit=lambda **kw: None))
    calls = []
    async def boundary(function, *args, **kwargs):
        calls.append(function)
        if function is shared_worker.cuda_backend:
            return device, {'name': 'NVIDIA GeForce RTX 4060'}
        if function is input_snapshot.load:
            return function(*args, **kwargs)
        if function in (device_inputs.upload, blunder_summary.reduce, blunder_compute.infer, score_compute.calculate, score_compute.reduce):
            assert args[0] is device
            return function(np, *args[1:], require_cuda=False, **kwargs)
        raise AssertionError(function)
    async def request():
        async with factory() as s, s.begin(): return await runs.request_run(s, 'synthetic-admin')
    async def finish(run):
        for _ in range(100):
            result = await worker.canonical_tick({})
            async with factory() as s: saved = await runs.progress(s, run['id'])
            assert saved['status'] not in ('failed', 'retrying'), saved
            if saved['status'] == 'complete': return saved
        raise AssertionError('Run never finished')
    async def prepare(run):
        while True:
            async with factory() as s: current = await runs.progress(s, run['id'])
            if current['timings'].get('input_snapshot', {}).get('ready'): return
            await snapshot_flow.prepare(factory, current)
    async def oracle(player):
        async with factory() as s:
            days = (await s.execute(text('''SELECT DISTINCT COALESCE((g.played_at AT TIME ZONE 'UTC')::date,DATE '-infinity')
                FROM candidate.game g JOIN candidate.game_player gp ON gp.game_link=g.link
                JOIN candidate.player p ON p.id=gp.player_id WHERE p.player_name=:player'''), {'player': player})).scalars().all()
            return old.combine([await old.calculate_day(s, player, day) for day in days])
    async def compare():
        for player in ('alice', 'bob'):
            expected = await oracle(player)
            actual = json.loads(await connection.fetchval('SELECT payload::text FROM candidate.player_stockfish_snapshot WHERE player_name=$1', player))
            # Mate lists have no UI ordering contract; compare their keyed counts.
            for values in (actual, expected):
                for row in values.values():
                    row['mates'].sort(key=lambda m: (m['side'], -1 if m['moves'] is None else m['moves']))
            assert actual == expected, (player, actual, expected)
        # Compact histogram reduction on CUDA must match the established SQL,
        # including partial/unassessed/ineligible coverage (test oracle only).
        async with factory() as s:
            model = await s.scalar(text('SELECT active_model FROM candidate.canonical_blunder_control WHERE id'))
            before = (await s.execute(text('SELECT player_name,payload FROM candidate.canonical_blunder_snapshot WHERE model_id=:model ORDER BY player_name'), {'model':model})).all()
            await publication.summaries(s,model)
            after = (await s.execute(text('SELECT player_name,payload FROM candidate.canonical_blunder_snapshot WHERE model_id=:model ORDER BY player_name'), {'model':model})).all()
            assert before == after, (before,after)
            await s.rollback()
    with patch.object(worker, 'AsyncDBSession', factory), patch.object(shared_worker, 'file_operation', side_effect=boundary), \
            patch.object(score_publication, 'file_operation', side_effect=boundary), \
            patch.object(blunder_summary, 'file_operation', side_effect=boundary):
        # Idle reads do not initialize CUDA or modify summaries.
        assert (await worker.canonical_tick({}))['status'] == 'awaiting_user' and not calls
        run, duplicate = await asyncio.gather(request(), request())
        assert run['id'] == duplicate['id'] and run['metrics'] == ['blunders','score_distribution']
        # Compare transition bytes with the established reader before publication.
        async with factory() as s, s.begin():
            while await score_runs.plan(s, run): pass
            selected = await runs.next_batch(s)
        async with factory() as s:
            await source.prepare(s)
            data = await shared_source.load(s, selected)
            legacy = await source.load(s, selected_items=[i for i in data[1] if i['blunders']], position_reader='binary_v1')
            np.testing.assert_array_equal(data[-1], legacy[-1])
        # Metadata paging is not a GPU batch boundary. Fill beyond the first page.
        async with factory() as s:
            await source.prepare(s)
            paged = {**selected, 'items': selected['items'][:2],
                     'batch_budget': batch_budget.BatchBudget(2**30, 2**30)}
            expanded = await shared_source.load(s, paged)
            assert len(expanded[1]) == len(selected['items']) > 2
            for field in shared_source.RAW.names:
                np.testing.assert_array_equal(expanded[4][field], data[4][field])
        small_budget = batch_budget.BatchBudget(44*batch_budget.DEVICE_ROW+2*batch_budget.DEVICE_GAME,
                                               44*batch_budget.HOST_ROW+2*batch_budget.HOST_GAME)
        with patch.object(batch_budget, 'plan', return_value=small_budget), patch.object(source, 'MAX_ROWS', 44):
            saved = await finish(run)
        assert saved['timings']['source_positions'] == 8*22, saved  # unique eligible-owned games, never once per owner
        assert saved['timings']['batches'] == (saved['total_games']+1)//2, saved
        assert saved['timings']['last_batch']['games'] == 2
        await compare()
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_summary_pending WHERE metric='score_distribution'") == 0
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_score_part') == 0
        calls.clear(); assert (await worker.canonical_tick({}))['status'] == 'awaiting_user' and not calls
        # Metadata-only change queues the score day even without a new FEN.
        await connection.execute("UPDATE candidate.game SET black_str_result='resigned' WHERE link=1")
        run = await request(); await finish(run); await compare()
        # CUDA OOM shrinks the cached batch without re-extraction or skipped games.
        await connection.execute("UPDATE candidate.player_stockfish_state SET revision=revision+1,full_rebuild=true")
        run = await request(); oom_calls = 0
        class SyntheticOOM(Exception): pass
        device.cuda.memory = SimpleNamespace(OutOfMemoryError=SyntheticOOM)
        async def with_oom(function, *args, **kwargs):
            nonlocal oom_calls
            if function is blunder_compute.infer:
                oom_calls += 1
                if oom_calls == 1: raise SyntheticOOM('synthetic allocation pressure')
            return await boundary(function, *args, **kwargs)
        with patch.object(batch_budget, 'plan', return_value=batch_budget.BatchBudget(small_budget.device_bytes*2, small_budget.host_bytes*2)), \
                patch.object(source, 'MAX_ROWS', 44), \
                patch.object(shared_worker, 'file_operation', side_effect=with_oom):
            saved = await finish(run)
        assert saved['timings']['cuda_memory_retries'] == 1
        assert saved['timings']['source_positions'] == 8*22
        await compare()
        # A write failure retains previous output and every input checkpoint.
        await connection.execute("UPDATE candidate.player_stockfish_state SET revision=revision+1,full_rebuild=true")
        run = await request()
        async with factory() as s, s.begin():
            while await score_runs.plan(s, run): pass
        original_stage = score_runs.stage; writes = 0
        async def fail_second(*args):
            nonlocal writes
            writes += 1
            await original_stage(*args)
            if writes == 2: raise RuntimeError('synthetic second-checkpoint rollback')
        calls.clear()
        with patch.object(source, 'MAX_ROWS', 44): await prepare(run)
        assert not calls, 'Input preparation unexpectedly initialized CUDA'
        with patch.object(batch_budget, 'plan', return_value=small_budget), \
                patch.object(score_runs, 'stage', side_effect=fail_second), \
                patch.object(shared_source, 'load', side_effect=AssertionError('Unexpected DB re-extraction')), \
                patch.object(worker.LOG, 'exception'):
            assert (await worker.canonical_tick({}))['status'] == 'building'
            assert (await worker.canonical_tick({}))['status'] == 'retrying'
        async with factory() as s:
            checkpoint = await runs.progress(s, run['id'])
            assert checkpoint['processed_games'] == 2 and checkpoint['timings']['batches'] == 1, checkpoint
        await connection.execute('UPDATE public.cuda_summary_run SET retry_after=NULL WHERE id=$1', run['id'])
        with patch.object(shared_source, 'load', side_effect=AssertionError('Retry re-extracted inputs')):
            saved = await finish(run)
        assert saved['timings']['source_positions'] == 8*22
        await compare()
        # Score-only pending work remains runnable after blunder work is already consumed.
        await connection.execute("UPDATE candidate.player_stockfish_state SET revision=revision+1,full_rebuild=true WHERE player_name='alice'")
        run = await request(); await finish(run); await compare()
        # Changes during extraction retain the previous snapshot and the newer revision.
        before = await connection.fetchval("SELECT payload::text FROM candidate.player_stockfish_snapshot WHERE player_name='alice'")
        await connection.execute('UPDATE candidate.game SET white_elo=white_elo WHERE link=1')
        run = await request(); original = shared_source.load
        async def racing(*args):
            result = await original(*args)
            await connection.execute('UPDATE candidate.game SET white_result=.5 WHERE link=1')
            return result
        with patch.object(shared_source, 'load', side_effect=racing): await finish(run)
        assert before == await connection.fetchval("SELECT payload::text FROM candidate.player_stockfish_snapshot WHERE player_name='alice'")
        assert await connection.fetchval("SELECT revision<>published_revision FROM candidate.player_stockfish_state WHERE player_name='alice'")
        run = await request(); await finish(run); await compare()
        # No CPU fallback on a failed GPU batch; retries cannot double-add staged counts.
        await connection.execute('UPDATE candidate.game SET white_elo=white_elo WHERE link=1')
        run = await request()
        async with factory() as s, s.begin():
            while await score_runs.plan(s, run): pass
        await prepare(run)
        with patch.object(shared_worker, 'file_operation', side_effect=RuntimeError('Synthetic CUDA failure')), patch.object(worker.LOG, 'exception'):
            for _ in range(3):
                await connection.execute('UPDATE public.cuda_summary_run SET retry_after=NULL WHERE id=$1', run['id'])
                await worker.canonical_tick({})
        async with factory() as s, s.begin():
            assert (await runs.progress(s, run['id']))['status'] == 'failed'
            await runs.retry_run(s, run['id'])
        await finish(run); await compare()
        # Moving a game to a different UTC day clears its old contribution too.
        await connection.execute("UPDATE candidate.game SET played_at='2025-02-03 10:00:00+00' WHERE link=3")
        run = await request(); await finish(run); await compare()
        # Deleted games disappear from their old partitions; no stale counts survive.
        await connection.execute('DELETE FROM candidate.game WHERE link=3')
        run = await request(); await finish(run); await compare()
        # Eligibility changes remove snapshots without treating missing input as zero blunders.
        await connection.execute("UPDATE candidate.player SET deleted_at=now() WHERE player_name='bob'")
        run = await request(); await finish(run)
        assert await connection.fetchval("SELECT count(*) FROM candidate.player_stockfish_snapshot WHERE player_name='bob'") == 0
        await connection.execute("UPDATE candidate.player SET deleted_at=NULL WHERE player_name='bob'")
        run = await request(); await finish(run); await compare()
    print('PASS shared CUDA: exact CPU histograms, partial/unknown/mate/tablebase/zero scores, shared owners, null dates, endings, unchanged blunder transitions, score-only work, idle safety, concurrent revisions and explicit retries')


if __name__ == '__main__':
    original = base.check
    async def wrapped(*args):
        base.check = original
        await check(*args)
    base.check = wrapped
    asyncio.run(base.main())
