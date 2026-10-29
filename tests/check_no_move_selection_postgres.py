"""Disposable database: zero-move correction, audit, rollback and idle guards."""
import asyncio
import copy
import json
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations.canonical_blunders import publication
from chessism_api.operations.cuda_summaries import no_move_upgrade, blunder_summary


async def check(factory, connection, layout):
    await base.check(factory, connection, layout, position_reader='binary_v1')
    # Minimal admission tables: this migration never launches or edits jobs.
    await connection.execute('''CREATE TABLE public.cuda_summary_run(status text);
        CREATE TABLE algorithm_run(status text);
        CREATE TABLE ingestion_pipeline_run(status text);
        CREATE TABLE IF NOT EXISTS cloud_analysis_job(status text);
        DELETE FROM candidate.fen_summary_refresh;
        UPDATE candidate.canonical_blunder_game SET status='assessed',white_owner=NULL,
          black_owner='alice',white_assessed=1,black_assessed=0,white_count=0,black_count=NULL,
          flags='{"unassessed":[]}' WHERE game_link=1;''')
    model=await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control WHERE id')
    async with factory() as s,s.begin(): await publication.summaries(s,model)
    async def snapshot():
        return json.loads(await connection.fetchval('''SELECT payload::text FROM candidate.canonical_blunder_snapshot
            WHERE model_id=$1 AND player_name='alice' ''',model))
    expected=await snapshot()
    legacy=copy.deepcopy(expected); legacy['blitz']['unassessed_games']+=1
    await connection.execute('''UPDATE candidate.canonical_blunder_snapshot SET payload=$1::jsonb
        WHERE model_id=$2 AND player_name='alice' ''',json.dumps(legacy),model)
    frozen=await connection.fetchval('SELECT jsonb_agg(to_jsonb(g) ORDER BY model_id,game_link)::text FROM candidate.canonical_blunder_game g')
    fens=await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM candidate.fen f')

    async def boundary(fn,xp,*args):
        assert fn is no_move_upgrade.corrected and xp is np
        return fn(np,*args,require_cuda=False)
    async def run():
        async with factory() as s,s.begin(): return await no_move_upgrade.upgrade(s,np)

    with patch.object(no_move_upgrade,'file_operation',side_effect=boundary):
        await connection.execute("INSERT INTO algorithm_run VALUES('running')")
        try: await run()
        except ValueError as e: assert 'pending work' in str(e)
        else: raise AssertionError('Allowed correction during active work')
        assert await snapshot()==legacy
        await connection.execute('DELETE FROM algorithm_run')
        real_publish=blunder_summary.publish
        async def fail_after_write(*args):
            await real_publish(*args)
            raise RuntimeError('synthetic rollback')
        with patch.object(blunder_summary,'publish',side_effect=fail_after_write):
            try: await run()
            except RuntimeError as e: assert 'synthetic rollback' in str(e)
            else: raise AssertionError('Expected failure')
        assert await snapshot()==legacy
        result=await run()
        assert result['updated_players']==1 and result['excluded_sides']==1,result
        assert result['game_results_recalculated']==result['fen_rows_changed']==0
        assert await snapshot()==expected
        audit=await connection.fetchrow('SELECT before_snapshots::text,after_snapshots::text FROM public.cuda_selection_correction')
        assert json.loads(audit['before_snapshots'])=={'alice':legacy}
        assert json.loads(audit['after_snapshots'])=={'alice':expected}
        assert (await run())['already_applied']
        assert await snapshot()==expected
    assert frozen==await connection.fetchval('SELECT jsonb_agg(to_jsonb(g) ORDER BY model_id,game_link)::text FROM candidate.canonical_blunder_game g')
    assert fens==await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM candidate.fen f')
    print('PASS zero-move correction: idle guard, rollback, exact cached counter update, retained before/after audit, idempotency, no game or FEN changes')


if __name__=='__main__':
    original=base.check
    async def wrapped(*args):
        base.check=original
        await check(*args)
    base.check=wrapped
    asyncio.run(base.main())
