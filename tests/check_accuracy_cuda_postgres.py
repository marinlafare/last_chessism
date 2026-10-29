"""Disposable SQL-oracle parity, full manual pipeline, read-only daily bands."""
import asyncio
import json
import math
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base, check_shared_cuda_postgres as shared
from chessism_api.operations import player_game_scores as legacy
from chessism_api.operations.canonical_blunders import worker, compute, source
from chessism_api.operations.cuda_summaries import runs, shared_worker, shared_source, score_compute, score_publication, accuracy_compute, accuracy_store, accuracy_reader
from chessism_api.operations.cuda_summaries import batch_budget, blunder_compute, device_inputs, input_snapshot, blunder_summary


async def check(factory, connection, layout):
    await shared.check(factory, connection, layout)
    # These are synthetic compatibility views in the private disposable database.
    await connection.execute('''
        ALTER TABLE candidate.player ADD COLUMN country text, ADD COLUMN location text,
            ADD COLUMN timezone text DEFAULT 'America/Mexico_City', ADD COLUMN timezone_source text;
        ALTER TABLE candidate.game_player_engine_summary ADD UNIQUE(game_player_id),
            ADD COLUMN own_move_cp_gain float8, ADD COLUMN own_move_cp_loss float8,
            ADD COLUMN mean_win_percent_loss float8,ADD COLUMN median_win_percent_loss float8,
            ADD COLUMN mate_for_positions int,ADD COLUMN mate_against_positions int,
            ADD COLUMN final_player_cp float8,ADD COLUMN result text,ADD COLUMN end_by text;
        CREATE TABLE candidate.game_player_salience(game_player_id bigint PRIMARY KEY,salience float8);
        INSERT INTO candidate.game_player_salience SELECT id,.5 FROM candidate.game_player;
        DROP TABLE public.player,public.game,public.game_player,public.fen,public.game_fen_association,public.game_analysis_summary;
        CREATE VIEW public.player AS SELECT * FROM candidate.player;
        CREATE VIEW public.game AS SELECT g.*,m.name AS mode FROM candidate.game g JOIN candidate.game_mode m ON m.id=g.mode_id;
        CREATE VIEW public.game_player AS SELECT gp.game_link AS link,p.player_name,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
            CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
            g.played_at,g.n_moves,m.name AS mode
            FROM candidate.game_player gp JOIN candidate.player p ON p.id=gp.player_id
            JOIN candidate.game g ON g.link=gp.game_link JOIN candidate.game_mode m ON m.id=g.mode_id;
        CREATE VIEW public.fen AS SELECT * FROM candidate.fen;
        CREATE VIEW public.game_analysis_summary AS SELECT * FROM candidate.game_analysis_coverage;
        CREATE VIEW public.game_fen_association AS SELECT game_link,n_move,
            CASE WHEN is_white THEN 'white' ELSE 'black' END AS move_color,fen_id FROM candidate.game_fen_association;
        UPDATE public.cuda_summary_control SET accuracy=true;
    ''')
    device = SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda: (2**33,2**33))),
        get_default_memory_pool=lambda: SimpleNamespace(free_all_blocks=lambda:None,set_limit=lambda **kw:None))
    calls = []
    async def boundary(function,*args,**kwargs):
        calls.append(function)
        if function is shared_worker.cuda_backend: return device,{'name':'NVIDIA GeForce RTX 4060'}
        if function is input_snapshot.load: return function(*args, **kwargs)
        assert function in (device_inputs.upload,blunder_summary.reduce,blunder_compute.infer,score_compute.calculate,score_compute.reduce,accuracy_compute.calculate,accuracy_compute.daily)
        assert args[0] is device
        return function(np,*args[1:],require_cuda=False,**kwargs)
    async def execute():
        async with factory() as s,s.begin(): run = await runs.request_run(s,'synthetic-admin')
        assert 'accuracy' in run['metrics']
        for _ in range(100):
            await worker.canonical_tick({})
            async with factory() as s: state = await runs.progress(s,run['id'])
            assert state['status'] not in ('failed','retrying'),state
            if state['status'] == 'complete': return state
        raise AssertionError('Run did not complete')
    async def snapshots():
        return {r['game_player_id']:dict(r) for r in await connection.fetch('''SELECT e.* FROM candidate.game_player_engine_summary e
            JOIN candidate.game_player gp ON gp.id=e.game_player_id JOIN candidate.player p ON p.id=gp.player_id
            JOIN candidate.game g ON g.link=gp.game_link
            WHERE p.joined>0 AND p.deleted_at IS NULL''')}
    # Execute the exact existing SQL only as a test oracle; never in production.
    from chessism_api.operations.cuda_summaries import schema
    with patch.object(legacy,'AsyncDBSession',factory),patch.object(schema,'enabled',return_value=False):
        await legacy.refresh_game_player_engine_summaries()
    expected = await snapshots()
    with patch.object(worker,'AsyncDBSession',factory),patch.object(shared_worker,'file_operation',side_effect=boundary), \
            patch.object(score_publication,'file_operation',side_effect=boundary),patch.object(accuracy_store,'file_operation',side_effect=boundary), \
            patch.object(blunder_summary,'file_operation',side_effect=boundary), \
            patch.object(accuracy_reader,'AsyncDBSession',factory):
        assert (await worker.canonical_tick({}))['status']=='awaiting_user' and not calls
        async with factory() as s:
            status=await runs.status(s)
            assert status['pending_accuracy_players']==2 and status['deferred_metrics']==[]
        small_budget=batch_budget.BatchBudget(44*batch_budget.DEVICE_ROW+2*batch_budget.DEVICE_GAME,
                                               44*batch_budget.HOST_ROW+2*batch_budget.HOST_GAME)
        with patch.object(batch_budget,'plan',return_value=small_budget), patch.object(source,'MAX_ROWS',44): saved=await execute()
        actual=await snapshots();assert expected.keys()==actual.keys(),(expected.keys(),actual.keys())
        for key,row in expected.items():
            for field in accuracy_compute.FIELDS:
                value,other=row[field],actual[key][field]
                assert (math.isclose(value,other,rel_tol=1e-10,abs_tol=1e-9) if isinstance(value,float)
                        else value==other),(key,field,value,other)
            assert row['blunder_count']==actual[key]['blunder_count'],'Retired CPU counts changed'
        assert saved['timings']['accuracy_algorithm']==accuracy_compute.VERSION
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_summary_pending WHERE metric='accuracy'")==0
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_accuracy_stage')==0
        result=await accuracy_reader.daily_chart('alice')
        assert not result['summary_pending'] and not result['stale'] and result['points'],result
        assert result['backend']=='saved_cuda_daily' and all(sum(p[2])==p[4] for p in result['points'])
        converted=await accuracy_reader.daily_chart('alice',timezone_name='UTC')
        assert converted['backend']=='saved_game_timezone_adapter' and sum(p[4] for p in converted['points'])==sum(p[4] for p in result['points'])
        assert (await accuracy_reader.daily_chart('alice',mode='rapid'))['points']==[]
        calls.clear();assert (await worker.canonical_tick({}))['status']=='awaiting_user' and not calls
        # Existing snapshots remain visible until an explicit rerun; then both writers advance together.
        await connection.execute("UPDATE candidate.game SET played_at='2025-01-02 01:00:00+00' WHERE link=1")
        stale=await accuracy_reader.daily_chart('alice');assert stale['stale'] and stale['points']==result['points']
        await execute()
        result=await accuracy_reader.daily_chart('alice');assert not result['stale']
        assert any(p[0]=='2025-01-01' for p in result['points']),result
        async with factory() as s:
            assert (await runs.status(s))['pending_accuracy_players']==0
        # Input changes after extraction cannot replace any saved accuracy snapshot.
        before=await snapshots()
        await connection.execute('UPDATE candidate.game SET white_elo=white_elo WHERE link=1')
        original=shared_source.load
        async def racing(*args):
            result=await original(*args)
            await connection.execute("UPDATE candidate.game SET black_str_result='timeout' WHERE link=1")
            return result
        with patch.object(shared_source,'load',side_effect=racing): await execute()
        assert before==await snapshots()
        assert (await accuracy_reader.daily_chart('alice'))['stale']
        await execute()
        assert not (await accuracy_reader.daily_chart('alice'))['stale']
        # Removing a game removes both its cached scalar and its daily contribution.
        old_count=sum(p[4] for p in (await accuracy_reader.daily_chart('alice'))['points'])
        await connection.execute('DELETE FROM candidate.game WHERE link=2')
        await execute()
        assert sum(p[4] for p in (await accuracy_reader.daily_chart('alice'))['points'])==old_count-1
    print('PASS CUDA accuracy SQL parity, preserved retired counts, mixed manual batches, daily cache, timezone adapter, stale reads, incremental dates, no automatic work')


if __name__=='__main__':
    original=base.check
    async def wrapped(*args):
        base.check=original
        await check(*args)
    base.check=wrapped
    asyncio.run(base.main())
