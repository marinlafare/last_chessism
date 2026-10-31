"""Private PostgreSQL: CUDA metadata selection, parity, retries and revisions."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base,check_accuracy_cuda_postgres as accuracy
from tests.test_cuda_behavior import normalize
from chessism_api.database.player_analytics_schema import install_player_analytics
from chessism_api.operations import player_analytics_summary as legacy,player_unexpected_endings as ending_api
from chessism_api.operations.canonical_blunders import worker
from chessism_api.operations.cuda_summaries import runs,schema,metadata_store as store,metadata_worker as mw
from chessism_api.operations.cuda_summaries import metadata_source as source,metadata_files as files,metadata_publication as pub
from chessism_api.operations.cuda_summaries import behavior_compute as b,ending_compute as e,shared_worker,score_publication
from chessism_api.operations.cuda_summaries import score_compute,accuracy_compute,accuracy_store,blunder_compute,blunder_summary,device_inputs,input_snapshot


async def check(factory,connection,layout):
    await accuracy.check(factory,connection,layout)
    await connection.execute('''ALTER TABLE candidate.game ADD COLUMN time_elapsed float8 DEFAULT 600,ADD COLUMN eco text DEFAULT 'Test';
        ALTER TABLE candidate.fen ADD COLUMN tablebase_wdl integer;
        ALTER TABLE candidate.game_analysis_coverage ADD COLUMN analyzed_positions int DEFAULT 22,ADD COLUMN unscored_positions int DEFAULT 0;
        CREATE TABLE IF NOT EXISTS algorithm_run(status text);
        CREATE TABLE IF NOT EXISTS ingestion_pipeline_run(status text);''')
    async with factory() as s,s.begin():await install_player_analytics(s,layout)
    for player in ('alice','bob'):
        result=await legacy.refresh_player(player,session_factory=factory)
        while result['status']=='building':result=await legacy.refresh_player(player,session_factory=factory)
        assert result['status']=='ready',result
    before={(r['player_name'],r['section']):json.loads(r['payload']) for r in await connection.fetch(
        'SELECT player_name,section,payload::text FROM candidate.player_analytics_snapshot')}
    async with factory() as s,s.begin():await store.activate(s)
    assert await legacy.refresh_pending(session_factory=factory) is None
    assert (await legacy.refresh_player('alice',session_factory=factory))['status']=='awaiting_cuda'
    # A newly ingested owner has no cache yet: return pending, never scan games.
    from chessism_api.operations.player_rating_distribution import percentage_distribution
    async with factory() as s:
        await s.execute(text("DELETE FROM candidate.player_analytics_snapshot WHERE player_name='alice' AND section='distribution'"))
        with patch.object(legacy,'enabled',return_value=True):
            saved=await legacy.read_snapshot(s,'alice','distribution')
        assert saved['summary_pending'] and saved['generated_at'] is None
        assert percentage_distribution('alice','all',saved)['games_count']==0
        await s.rollback()
    device=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**33,2**33))),
        get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None,set_limit=lambda **kw:None))
    async def boundary(fn,*args,**kwargs):
        if fn is shared_worker.cuda_backend:return device,{'name':'NVIDIA GeForce RTX 4060'}
        if fn in (files.load,files.save,input_snapshot.load):return fn(*args,**kwargs)
        assert fn in (b.partitions,b.combine,e.partitions,e.combine,score_compute.calculate,score_compute.reduce,
            accuracy_compute.calculate,accuracy_compute.daily,blunder_compute.infer,blunder_summary.reduce,device_inputs.upload),fn
        assert args[0] is device
        return fn(np,*args[1:],require_cuda=False,**kwargs)
    async def request():
        async with factory() as s,s.begin():return await runs.request_run(s,'synthetic-admin')
    async def finish(run):
        for _ in range(100):
            await worker.canonical_tick({})
            async with factory() as s:saved=await runs.progress(s,run['id'])
            assert saved['status'] not in ('failed','retrying'),saved
            if saved['status']=='complete':return saved
        raise AssertionError('Metadata did not complete')
    async def compare():
        for player in ('alice','bob'):
            async with factory() as s:
                rows=(await s.execute(legacy.source_rows_sql(s),legacy.day_params(player,all_days=True))).mappings().all()
                profile=(await s.execute(text('SELECT * FROM candidate.player WHERE player_name=:p'),{'p':player})).mappings().one()
                expected=legacy.combine_partitions([legacy.summarize_day(rows,legacy.resolve_player_timezone(profile))])
                actual={r[0]:r[1] for r in (await s.execute(text('SELECT section,payload FROM candidate.player_analytics_snapshot WHERE player_name=:p'),{'p':player})).all()}
                for section,payload in expected.items():
                    got={k:v for k,v in actual[section].items() if k not in ('backend','algorithm','time_context')}
                    assert normalize(got)==normalize(payload),(player,section,got,payload)
                query=ending_api.ending_rows_sql(s)+''' SELECT mode,ending,count(*) AS eligible_games,
                    count(*) FILTER(WHERE score_kind='cp') AS assessable_games,
                    count(*) FILTER(WHERE score_band='ahead') AS ahead_games,
                    count(*) FILTER(WHERE score_band='balanced') AS balanced_games,
                    count(*) FILTER(WHERE score_band='behind') AS behind_games,
                    count(*) FILTER(WHERE score_kind='missing_final_position') AS missing_final_position,
                    count(*) FILTER(WHERE score_kind='missing_final_score') AS missing_final_score,
                    count(*) FILTER(WHERE score_kind='mate') AS mate_games,
                    count(*) FILTER(WHERE score_kind='tablebase') AS tablebase_games FROM classified GROUP BY mode,ending'''
                reference=e.blank()['by_mode']
                for row in (await s.execute(text(query),{'player':player,'modes':list(b.MODES),'threshold_cp':200,'ending':None})).mappings():
                    reference[row['mode']][row['ending']]={k:row[k] for k in e.COUNTS}
                got=await s.scalar(text(f'SELECT payload FROM {store.STATE} WHERE player_name=:p'),{'p':player})
                assert got['by_mode']==reference,(got,reference)
    original_read=source.read
    async def small_read(s,r,a):return await original_read(s,r,a,limit=3)
    with patch.object(worker,'AsyncDBSession',factory),patch.object(shared_worker,'file_operation',side_effect=boundary), \
        patch.object(score_publication,'file_operation',side_effect=boundary),patch.object(accuracy_store,'file_operation',side_effect=boundary), \
        patch.object(blunder_summary,'file_operation',side_effect=boundary),patch.object(mw,'file_operation',side_effect=boundary), \
        patch.object(pub,'file_operation',side_effect=boundary),patch.object(source,'read',side_effect=small_read):
        assert (await worker.canonical_tick({}))['status']=='awaiting_user'
        async with factory() as s:
            status=await runs.status(s);assert status['pending_behavioral_players']==2 and status['pending_unexpected_endings_players']==2,status
        run=await request();assert 'behavioral' in run['metrics']
        # A wrong candidate must not pass the baseline gate or replace anything.
        from chessism_api.operations.cuda_summaries.metadata_validation import validate_and_retain
        import copy
        async with factory() as s:
            scopes=[dict(r) for r in (await s.execute(text(f'SELECT * FROM {store.SCOPE} WHERE run_id=:id'),{'id':run['id']})).mappings()]
            candidates={}
            for (player,section),payload in before.items():
                candidates.setdefault(player,{})[section]={k:v for k,v in copy.deepcopy(payload).items()
                    if k not in ('time_context','backend','algorithm')}
            candidates['alice']['hours']['modes']['blitz']['games']+=1
            try:await validate_and_retain(s,run,scopes,candidates)
            except ValueError as error:assert 'previous summaries retained' in str(error)
            else:raise AssertionError('Incorrect CUDA summary passed baseline comparison')
        assert before=={(r['player_name'],r['section']):json.loads(r['payload']) for r in await connection.fetch(
            'SELECT player_name,section,payload::text FROM candidate.player_analytics_snapshot')}
        saved=await finish(run);await compare()
        assert saved['timings']['metadata']['batches']==1 and len(saved['timings']['metadata']['chunks'])>1
        async with factory() as s:assert (await runs.status(s))['pending_behavioral_players']==0
        # Changed-day replacement must not double counts and must be manual.
        await connection.execute("UPDATE candidate.game SET played_at='2025-01-03 01:00:00+00',eco='New-Opening' WHERE link=1")
        assert await legacy.refresh_pending(session_factory=factory) is None
        await finish(await request());await compare()
        # A new revision after extraction retains the previous complete result.
        await connection.execute("UPDATE candidate.game SET time_elapsed=601 WHERE link=1")
        run=await request();old=await connection.fetchval('SELECT jsonb_agg(payload ORDER BY player_name)::text FROM public.cuda_metadata_state')
        real_reduce=pub.reduce
        async def racing(*args):
            await real_reduce(*args)
            await connection.execute("UPDATE candidate.game SET time_elapsed=602 WHERE link=1")
        with patch.object(pub,'reduce',side_effect=racing):saved=await finish(run)
        assert saved['timings']['metadata']['superseded_players']==2
        assert old==await connection.fetchval('SELECT jsonb_agg(payload ORDER BY player_name)::text FROM public.cuda_metadata_state')
        await finish(await request());await compare()
        # Interrupted extraction retries reuse already saved chunks.
        await connection.execute("UPDATE candidate.game SET time_elapsed=603 WHERE link=1")
        run=await request();real_save=files.save;failed=False
        def fail_once(*args):
            nonlocal failed
            if not failed:failed=True;raise RuntimeError('synthetic input interruption')
            return real_save(*args)
        with patch.object(files,'save',side_effect=fail_once),patch.object(worker.LOG,'exception'):
            for _ in range(100):
                await worker.canonical_tick({})
                async with factory() as s:state=await runs.progress(s,run['id'])
                if state['status']=='retrying':break
            assert state['status']=='retrying',state
        await connection.execute('UPDATE public.cuda_summary_run SET retry_after=NULL WHERE id=$1',run['id'])
        await finish(run);await compare()
    print('PASS CUDA metadata: SQL parity, shared batches, incremental dates, manual-only CPU deferral, superseded revisions, failed input retry, unchanged old snapshots')


if __name__=='__main__':
    original=base.check
    async def wrapped(*args):
        base.check=original
        await check(*args)
    base.check=wrapped
    asyncio.run(base.main())
