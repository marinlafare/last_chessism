"""Real canonical worker transactions/checkpoints with an explicit array test device."""
import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import numpy as np
from sqlalchemy import text

from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.canonical_blunders import worker, source, binary_source
from chessism_api.operations.cuda_summaries import (shared_worker, input_snapshot, snapshot_flow,
    device_inputs, blunder_summary, blunder_compute, score_compute, accuracy_compute,
    score_publication, score_runs, runs, batch_budget, shared_source)
from tests.check_canonical_cuda_postgres import CanonicalCudaTests, ArrayDevice


class CanonicalWorkerLifecycleTests(CanonicalCudaTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.test_atomic_score_accuracy_and_blunder_publication_and_saved_readers()
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.device=ArrayDevice()
        self.device.cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**33,2**33)))
        self.device.get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None,set_limit=lambda **kw:None)
        self.calls=[]
        changes=[patch.object(worker,'AsyncDBSession',self.factory),
            patch.object(input_snapshot,'ROOT',Path(self.temp.name)),
            patch.object(input_snapshot,'FREE_FLOOR',0),patch.object(source,'MAX_ROWS',22),
            patch.object(batch_budget,'plan',return_value=batch_budget.BatchBudget(10000,14000)),
            patch.object(worker.LOG,'exception')]
        for module in (worker,shared_worker,score_publication,blunder_summary):
            changes.append(patch.object(module,'file_operation',side_effect=self.boundary))
        for change in changes:change.start();self.addCleanup(change.stop)

    async def boundary(self,function,*args,**kwargs):
        self.calls.append(function)
        if function in (worker.cuda_backend,shared_worker.cuda_backend):
            return self.device,{'name':'NVIDIA GeForce RTX 4060'}
        if function is input_snapshot.load:return function(*args,**kwargs)
        if function in (device_inputs.upload,blunder_summary.reduce,blunder_compute.infer,worker.compute.infer,
                score_compute.calculate,score_compute.reduce,accuracy_compute.calculate):
            self.assertIs(args[0],self.device)
            return function(np,*args[1:],require_cuda=False,**kwargs)
        raise AssertionError(function)

    async def saved(self,run):
        async with read_transaction(self.factory) as s:return await runs.progress(s,run['id'])

    async def prepare(self):
        await self.db.execute('UPDATE game SET white_elo=white_elo+1 WHERE link=1')
        async with write_transaction(self.factory) as s:
            run=await runs.request_run(s,'native-lifecycle-check')
            while await score_runs.plan(s,run):pass
        for _ in range(6):
            run=await self.saved(run)
            state=await snapshot_flow.prepare(self.factory,run)
            if state['ready']:return await self.saved(run)
        self.fail('Input preparation failed to finish')

    async def snapshots(self):
        return await self.db.fetchval('SELECT jsonb_agg(to_jsonb(s) ORDER BY player_name)::text FROM player_stockfish_snapshot s')

    async def finish(self,run):
        for _ in range(20):
            await worker.canonical_tick({})
            saved=await self.saved(run)
            self.assertNotIn(saved['status'],('failed','retrying'),saved)
            if saved['status']=='complete':return saved
        self.fail('Worker failed to finish within bounded fixture steps')

    async def test_shared_checkpoint_rollback_retry_does_not_reextract_or_double_count(self):
        # Dirty both games so the bounded device consumes separate input chunks.
        await self.db.execute('UPDATE game SET black_elo=black_elo+1 WHERE link=$1',self.links[1])
        before=await self.snapshots();run=await self.prepare()
        self.assertEqual(len(run['timings']['input_snapshot']['chunks']),2)
        self.calls.clear();writes=0;original=score_runs.stage
        async def fail_second(*args):
            nonlocal writes
            writes+=1;await original(*args)
            if writes==2:raise RuntimeError('synthetic checkpoint failure')
        with patch.object(score_runs,'stage',side_effect=fail_second), \
             patch.object(shared_source,'load',AsyncMock(side_effect=AssertionError('unexpected re-extraction'))):
            self.assertEqual((await worker.canonical_tick({}))['status'],'building')
            self.assertEqual((await worker.canonical_tick({}))['status'],'retrying')
        saved=await self.saved(run)
        self.assertEqual(saved['processed_games'],1)
        self.assertEqual(saved['timings']['input_snapshot']['computed'],1)
        self.assertEqual(await self.snapshots(),before)
        await self.db.execute('UPDATE cuda_summary_run SET retry_after=NULL WHERE id=$1',run['id'])
        with patch.object(shared_source,'load',AsyncMock(side_effect=AssertionError('retry re-extracted inputs'))):
            saved=await self.finish(run)
        self.assertEqual(saved['processed_games'],2)
        self.assertEqual(saved['timings']['input_snapshot']['computed'],2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player_engine_summary'),4)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cuda_accuracy_game'),4)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cuda_summary_pending'),0)
        stable=await self.snapshots()
        self.assertEqual((await worker.canonical_tick({}))['status'],'awaiting_user')
        self.assertEqual(await self.snapshots(),stable)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_cancelled_publication_keeps_prior_results_input_and_unacknowledged_work(self):
        before=await self.snapshots();run=await self.prepare();original=score_runs.stage
        async def cancel_after_write(*args):
            await original(*args);raise asyncio.CancelledError
        with patch.object(score_runs,'stage',side_effect=cancel_after_write):
            with self.assertRaises(asyncio.CancelledError):await worker.canonical_tick({})
        saved=await self.saved(run)
        self.assertEqual(saved['processed_games'],0)
        self.assertEqual(saved['timings']['input_snapshot']['computed'],0)
        self.assertEqual(saved['status'],'retrying')
        self.assertEqual(await self.snapshots(),before)
        self.assertTrue(list(Path(self.temp.name).rglob('*.npz')))
        self.assertGreater(await self.db.fetchval('SELECT count(*) FROM cuda_summary_pending'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)
        await self.db.execute('UPDATE cuda_summary_run SET retry_after=NULL WHERE id=$1',run['id'])
        await self.finish(run)

    async def test_automatic_binary_reader_failure_preserves_results_and_retries(self):
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        self.assertEqual((await worker.canonical_tick({}))['status'],'idle')
        before=await self.snapshots()
        await self.db.execute('UPDATE game SET white_elo=900 WHERE link=1')
        with patch.object(binary_source,'load',AsyncMock(side_effect=ValueError('synthetic binary failure'))) as load:
            self.assertEqual((await worker.canonical_tick({}))['status'],'retrying');load.assert_awaited_once()
        self.assertEqual(await self.snapshots(),before)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM canonical_blunder_dirty'),1)
        self.assertEqual((await worker.canonical_tick({}))['status'],'retry_backoff')
        await self.db.execute("UPDATE canonical_blunder_control SET updated_at=now()-interval '61 seconds'")
        result=await worker.canonical_tick({})
        self.assertEqual(result['input_reader'],worker.INPUT_READER)
        self.assertEqual(result['written'],1)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM canonical_blunder_dirty'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)
