"""Isolated reservation/restart/stop tests. No cloud client or live database."""
import hashlib
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from sqlalchemy import select, text, func
import test_cloud_analysis_database as existing
import test_cloud_game_drafts_database as draft_tests
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import (CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim,
    CloudInputPreparation, CloudInputChunk, CloudJobCancellation, CloudGameDraftCache, Fen)
from chessism_api.operations.cloud_analysis import input_preparation as prep, input_validation as validation
from chessism_api.operations.cloud_analysis import game_fen_queue, cancellation
from chessism_api.operations.cloud_analysis.migrate_input_preparation import upgrade
from chessism_api.routers.cloud_game_drafts import submit_draft, SubmitDraft


@unittest.skipUnless(existing.TEST_URL, 'requires disposable PostgreSQL')
class InputPreparationTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = draft_tests.DraftDatabaseTests.asyncTearDown

    async def asyncSetUp(self):
        await draft_tests.DraftDatabaseTests.asyncSetUp(self)
        async with AsyncDBSession() as s, s.begin():
            job = await s.get(CloudAnalysisJob, self.job.id)
            job.selection = {**job.selection, 'backend':'cloud_run', 'execution_mode':'cloud_run_v1',
                             'mode':'games','game_links':[101,102,103]}
            job.target = 3
            self.job = job
        self.worker = SimpleNamespace(continue_immediately=False)
        self.validation = patch.object(validation, 'prepare', new=AsyncMock(side_effect=validation.validate))
        self.validation.start(); self.addCleanup(self.validation.stop)

    async def root(self, documents=()):
        async with AsyncDBSession() as s:
            return (await s.scalars(select(CloudAnalysisRun).where(CloudAnalysisRun.job_id == self.job.id)
                .execution_options(cloud_documents=documents))).one()

    async def step(self):
        await prep.advance(self.worker, self.job, await self.root())

    async def counts(self):
        async with AsyncDBSession() as s:
            return tuple([await s.scalar(select(func.count()).select_from(model))
                          for model in (CloudFenClaim, CloudInputChunk, CloudInputPreparation)])

    async def test_bounded_checkpoints_restart_and_exact_sealed_document(self):
        await prep.begin(self.job, 3)
        with patch.object(prep, 'PAGE_SIZE', 2):
            await self.step()
            first = await self.root()
            self.assertEqual((first.status,first.position_count), ('selecting',2))
            self.assertEqual(await self.counts(), (2,1,1))
            # A fresh controller/session resumes the committed prefix.
            self.worker = SimpleNamespace(continue_immediately=False)
            await self.step()
            self.assertEqual((await self.root()).position_count,3)
            await self.step()
        run = await self.root(('positions',))
        self.assertEqual(run.status,'preparing')
        self.assertEqual({p['fen'] for p in run.positions},set(self.fens))
        self.assertTrue(all(p['id']==hashlib.sha256(p['fen'].encode()).hexdigest() for p in run.positions))
        self.assertEqual(await self.counts(),(3,0,0))
        self.assertTrue(self.worker.continue_immediately)
        await self.assert_importable_packs(run, run.positions)

    async def assert_importable_packs(self, run, positions):
        from chessism_api.database.postgresql import cloud_columns
        table = cloud_columns.TABLES['position']
        async with AsyncDBSession() as s:
            for start in sorted({0, min(499, len(positions)-1), min(500, len(positions)-1),
                                 max(0, len(positions)-500)}):
                end = min(start+500, len(positions))
                # Match import_batch's bounded reads, including offset tasks.
                packs = (await s.execute(select(table.c.id, table.c.fen).where(
                    table.c.document_id == run._positions_ref,
                    table.c.ordinal.between(start//500, (end-1)//500))
                    .order_by(table.c.ordinal))).all()
                actual = [{'id':ident,'fen':fen} for pack in packs for ident,fen in zip(pack.id,pack.fen)]
                self.assertEqual(actual[start%500:start%500+end-start], positions[start:end])

    async def test_failed_bulk_claim_rolls_back_page_and_queue_for_retry(self):
        await prep.begin(self.job,3)
        original = validation.claim
        async def fail(*args):
            await original(*args)
            raise RuntimeError('synthetic writer failure')
        with patch.object(validation,'claim',side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,'writer failure'): await self.step()
        self.assertEqual(await self.counts(),(0,0,1))
        self.assertEqual((await self.root()).position_count,0)
        await self.step(); await self.step()
        self.assertEqual(len((await self.root(('positions',))).positions),3)

    async def test_cancellation_arriving_during_validation_prevents_commit(self):
        await prep.begin(self.job,3)
        async def stop(fens):
            async with AsyncDBSession() as s,s.begin():
                s.add(CloudJobCancellation(job_id=self.job.id))
            return validation.validate(fens)
        with patch.object(validation,'prepare',side_effect=stop): await self.step()
        self.assertEqual(await self.counts(),(0,0,1))
        self.assertEqual((await self.root()).position_count,0)

    async def test_controller_shutdown_rolls_back_only_current_page_and_resumes(self):
        await prep.begin(self.job,3)
        entered = asyncio.Event()
        async def pending(fens):
            entered.set()
            await asyncio.Future()
        with patch.object(prep,'PAGE_SIZE',1):
            await self.step()
            with patch.object(validation,'prepare',side_effect=pending):
                task=asyncio.create_task(self.step())
                await asyncio.wait_for(entered.wait(),5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
            self.assertEqual((await self.root()).position_count,1)
            self.assertEqual(await self.counts(),(1,1,1))
            for _ in range(3): await self.step()
        run=await self.root(('positions',))
        self.assertEqual(run.status,'preparing')
        self.assertEqual({p['fen'] for p in run.positions},set(self.fens))
        await self.assert_importable_packs(run,run.positions)

    async def test_statistics_refresh_precedes_reservation(self):
        await prep.begin(self.job,3)
        with patch.object(prep,'refresh_claim_statistics',side_effect=RuntimeError('statistics failed')), \
             patch.object(prep,'get_game_queue_fens',new_callable=AsyncMock) as select_fens:
            with self.assertRaisesRegex(RuntimeError,'statistics failed'): await self.step()
            select_fens.assert_not_awaited()
        self.assertEqual(await self.counts(),(0,0,1))

    async def test_stop_releases_only_own_checkpoint_and_never_reads_positions(self):
        await prep.begin(self.job,3)
        with patch.object(prep,'PAGE_SIZE',1): await self.step()
        run = await self.root()
        async with AsyncDBSession() as s,s.begin():
            other_job = CloudAnalysisJob(id='a'*32,status='complete',selection=self.job.selection,target=1,imported=0)
            s.add(other_job); await s.flush()
            other = CloudAnalysisRun(id='f'*32,job_id=other_job.id,status='complete',positions=[],launch={},receipts={})
            s.add(other); await s.flush()
            s.add(CloudFenClaim(fen=self.fens[2],run_id=other.id))
            s.add(CloudJobCancellation(job_id=self.job.id,phase='finishing'))
        from chessism_api.database.postgresql import cloud_codec
        original = cloud_codec.read_document
        def checked(connection, ident):
            if ident.endswith(':positions'): raise AssertionError('Loaded entire input during stop')
            return original(connection,ident)
        with patch.object(cloud_codec,'read_document',side_effect=checked):
            await cancellation.finish(self.job.id,[run.id])
        self.assertEqual(await self.counts(),(1,0,0))
        self.assertEqual((await self.root_without_other()).status,'cancelled')

    async def test_controller_restarts_between_pages_without_launching_early(self):
        from unittest.mock import Mock
        from chessism_api.operations.cloud_analysis.controller import Controller
        cloud=Mock()
        for _ in range(3):
            await Controller(cloud,'unused').tick()
        self.assertEqual((await self.root()).status,'preparing')
        self.assertEqual(cloud.mock_calls,[])

    async def root_without_other(self):
        async with AsyncDBSession() as s:
            return (await s.scalars(select(CloudAnalysisRun).where(CloudAnalysisRun.id!='f'*32)
                .execution_options(cloud_documents=()))).one()

    async def test_scored_and_claimed_fens_excluded_after_queue_build(self):
        await prep.begin(self.job,3)
        await game_fen_queue.ensure_queue(self.job.id,[101,102,103])
        async with AsyncDBSession() as s,s.begin():
            (await s.get(Fen,self.fens[0])).score=0
            other=CloudAnalysisRun(id='e'*32,job_id=self.job.id,status='complete',positions=[],launch={},receipts={})
            s.add(other);await s.flush()
            s.add(CloudFenClaim(fen=self.fens[2],run_id=other.id))
        async with AsyncDBSession() as s:
            root=(await s.scalars(select(CloudAnalysisRun).where(CloudAnalysisRun.status=='selecting')
                .execution_options(cloud_documents=()))).one()
        await prep.advance(self.worker,self.job,root)
        async with AsyncDBSession() as s:
            root=await s.get(CloudAnalysisRun,root.id,execution_options={'cloud_documents':()})
        self.assertEqual(root.position_count,1)
        await prep.advance(self.worker,self.job,root)
        async with AsyncDBSession() as s:
            sealed=await s.get(CloudAnalysisRun,root.id)
            self.assertEqual([p['fen'] for p in sealed.positions],[self.fens[1]])

    async def test_combined_queue_reuses_cached_ids_without_game_scan(self):
        await draft_tests.DraftDatabaseTests.add(self,(101,102,103))
        await draft_tests.DraftDatabaseTests.available(self)
        result=await submit_draft(self.draft['id'],SubmitDraft(revision=self.draft['revision'],backend='cloud_run'))
        with patch.object(game_fen_queue,'candidate_sql',side_effect=AssertionError('Scanned games again')):
            await game_fen_queue.ensure_queue(result['id'],[101,102,103])
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(text('SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id=:id'),{'id':result['id']}),3)
            self.assertIsNone(await s.get(CloudGameDraftCache,self.draft['id']))

    async def test_sealing_rejects_missing_staged_input_and_migration_repeats(self):
        async with self.engine.begin() as c:
            await c.run_sync(upgrade); await c.run_sync(upgrade)
        await prep.begin(self.job,3);await self.step()
        async with AsyncDBSession() as s,s.begin():
            await s.execute(text('DELETE FROM cloud_input_chunk'))
        with self.assertRaisesRegex(ValueError,'counts disagree'): await self.step()
        self.assertEqual((await self.root()).status,'selecting')

    async def test_streamed_seal_matches_codec_across_reader_and_array_boundaries(self):
        from chessism_api.operations.cloud_analysis.input_seal import seal
        await prep.begin(self.job,50_001)
        run = await self.root()
        positions=[{'fen':self.fens[0]+f' {n} {n+1}', 'id':str(n)} for n in range(50_001)]
        async with AsyncDBSession() as s,s.begin():
            owner=await s.get(CloudAnalysisRun,run.id,execution_options={'cloud_documents':()})
            owner.position_count=len(positions)
            s.add_all([CloudInputChunk(run_id=run.id,ordinal=n,
                ids=[p['id'] for p in positions[n:n+500]],fens=[p['fen'] for p in positions[n:n+500]])
                for n in range(0,len(positions),500)])
            await s.flush()
            self.assertTrue(await seal(s,owner))
        # The existing reader verifies the canonical checksum as well as order.
        sealed = await self.root(('positions',))
        self.assertEqual(sealed.positions,positions)
        await self.assert_importable_packs(sealed,positions)
