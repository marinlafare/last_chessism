"""Synthetic PostgreSQL only; no live data, GPU or cloud credentials."""
import asyncio
import json
import tempfile
import uuid
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock,patch
import numpy as np
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations.canonical_blunders import compute,source,benchmark_source,benchmark_job
from chessism_api.operations.canonical_blunders.benchmark_store import Scratch,NAMES,recover_interrupted
from chessism_api.operations.canonical_blunders.assessment import empty_mode

original=base.check


async def check(factory,connection,layout):
    await original(factory,connection,layout)
    before={name:await connection.fetchval(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text) FROM candidate.canonical_blunder_{name} t')
        for name in ('control','dirty','game','snapshot')}
    mid=await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')
    config=json.loads(await connection.fetchval('SELECT config FROM candidate.canonical_blunder_model WHERE id=$1',mid))
    settings={**benchmark_job.configuration(mid,config),'games':3}
    reporter=AsyncMock(); reporter.run_id=str(uuid.uuid4())
    with tempfile.TemporaryDirectory() as directory:
        info,timings,items,players,_=await benchmark_source.freeze(settings,reporter,Path(directory),factory=factory)
        assert info['games']==3 and timings['source_queries_seconds']>0 and timings['source_pack_seconds']>0
        rows,data=benchmark_source.read(Path(directory),0)
        records,_=compute.infer(np,rows,data['games'],config,require_cuda=False)
        assert benchmark_source.FIELDS
        from chessism_api.operations.canonical_blunders.benchmark_compute import compare
        assert not compare(records,data['expected'])
    # Real binary COPY in the same repeatable-read, read-only transaction.
    async with factory() as s:
        await source.prepare(s)
        first,pair=await benchmark_source.compare_readers(s,items,0,reporter)
        second,reverse=await benchmark_source.compare_readers(s,items,1,reporter)
        assert first[4].tobytes()==second[4].tobytes()==rows.tobytes()
        assert pair['order']==list(reversed(reverse['order']))
        assert await s.scalar(text("SELECT current_setting('transaction_read_only')"))=='on'
        assert await s.scalar(text("SELECT current_setting('transaction_isolation')"))=='repeatable read'
    with tempfile.TemporaryDirectory() as directory:
        paired={**benchmark_job.configuration(mid,config,reader_comparison=True),'games':3}
        info,_,_,_,_=await benchmark_source.freeze(paired,reporter,Path(directory),factory=factory)
        assert info['reader_comparison']['status']=='passed' and info['reader_comparison']['input_byte_agreement']
    scratch=Scratch(reporter.run_id,layout,factory=factory)
    await scratch.create(mid,items,players)
    assert await connection.fetchval('SELECT bool_and(relpersistence=\'p\') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1',scratch.name)
    # Never adopt a pre-existing schema; only the original owner may clean it.
    intruder=Scratch(reporter.run_id,layout,factory=factory)
    try:await intruder.create(mid,items,players)
    except Exception:pass
    else:raise AssertionError('Existing namespace accepted')
    await intruder.cleanup()
    assert not intruder.created
    # Missing output rolls back queue acknowledgement and publications.
    try:await scratch.write(mid,items,records[:-1])
    except ValueError:pass
    else:raise AssertionError('Malformed records accepted')
    result,timing=await scratch.write(mid,items,records)
    assert result['activated'] and result['written']==3 and timing['publication_commit_seconds']>0
    hist={p:{m:empty_mode() for m in ('bullet','blitz','rapid')} for p in players}
    benchmark_job.add_histograms(hist,data['expected'])
    assert await scratch.snapshots(mid)==hist
    # Reject changed contents; do not erase unknown objects.
    await connection.execute(f'CREATE TABLE "{scratch.name}".unexpected(id int)')
    try:await scratch.cleanup()
    except ValueError:pass
    else:raise AssertionError('Unexpected objects erased')
    await connection.execute(f'DROP TABLE "{scratch.name}".unexpected')
    await scratch.cleanup()
    assert not await connection.fetchval('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)',scratch.name)
    # Simulate a worker crash; recovery requires a durable matching failed run.
    await connection.execute('CREATE TABLE algorithm_run(id text,config jsonb,status text)')
    scratch=Scratch(str(uuid.uuid4()),layout,factory=factory); await scratch.create(mid,items,players)
    await connection.execute('INSERT INTO algorithm_run VALUES($1,$2,\'failed\')',scratch.marker.split(':')[1],json.dumps(settings))
    with patch('chessism_api.operations.canonical_blunders.benchmark_store.AsyncDBSession',factory):
        assert await recover_interrupted()==1
        assert await recover_interrupted()==0
    # Full orchestration on the synthetic DB; CUDA math is checked separately
    # on the real card. Here only the device boundary is mocked, never live DB.
    original_infer=compute.infer
    device=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**33,2**33))),
        get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None,set_limit=lambda **kw:None))
    def cpu_infer(xp,rows,games,config):return original_infer(np,rows,games,config,require_cuda=False)
    def cpu_batch(xp,rows,data,config):return cpu_infer(xp,rows,data['games'],config)[0],{}
    with tempfile.TemporaryDirectory() as directory, \
            patch('chessism_api.operations.research_algorithms.storage.WORK_ROOT',Path(directory)), \
            patch.object(benchmark_job,'GAMES',3),patch.object(benchmark_job,'ensure_capacity'), \
            patch.object(benchmark_job,'cuda_backend',return_value=(device,{'name':config['device']})), \
            patch.object(compute,'implementation_hash',return_value=config['kernel_sha256']), \
            patch.object(compute,'infer',side_effect=cpu_infer),patch.object(benchmark_job,'batch',side_effect=cpu_batch):
        reporter.run_id=str(uuid.uuid4())
        result=await benchmark_job.execute(paired,reporter,factory=factory)
        assert result['status']=='passed' and result['scratch_removed'] and not result['production_changed']
        assert result['checks']['reader_inputs']=='passed'
        assert not list(Path(directory).iterdir())
        # A computation failure must still clean its owned tables and files.
        reporter.run_id=str(uuid.uuid4())
        with patch.object(benchmark_job,'batch',side_effect=RuntimeError('synthetic failure')):
            try:await benchmark_job.execute(settings,reporter,factory=factory)
            except RuntimeError as error:assert 'synthetic failure' in str(error)
            else:raise AssertionError('Failed compute accepted')
        assert not list(Path(directory).iterdir())
        assert not await connection.fetchval('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)',
            'cbbench_'+uuid.UUID(reporter.run_id).hex)
    for name,old in before.items():
        current=await connection.fetchval(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text) FROM candidate.canonical_blunder_{name} t')
        assert current==old,name
    async with factory() as s:
        await source.prepare(s)
        try:await source.load(s,selected_items=[items[0],items[0]])
        except ValueError:pass
        else:raise AssertionError('Duplicate source selection accepted')
    print('PASS benchmark: frozen source, logged isolated writes/commits, rollback, exact histograms, ownership/contents guards, full orchestration/failure/crash cleanup, unchanged canonical state')


if __name__=='__main__':
    base.check=check
    asyncio.run(base.main())
