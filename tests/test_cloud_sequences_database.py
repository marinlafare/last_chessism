"""Real isolated PostgreSQL, mocked Google. Never launch billable test jobs."""
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
from sqlalchemy import select, func, text
import test_cloud_analysis_database as existing
import test_cloud_work_units_database as units_test
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    CloudAnalysisJob, CloudAnalysisRun, CloudBatchSequence, CloudBatchCycle,
    CloudFenClaim, CloudPhaseTiming, CloudControllerHeartbeat, Fen,
)
from chessism_api.operations.cloud_analysis import controller, sequence, importer
from chessism_api.operations.cloud_analysis import work_units_controller as fleet
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.operations.cloud_analysis.migrate_sequences import upgrade
from chessism_api.routers.cloud_analysis import create_cloud_job, cloud_jobs, stop_repeating


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL database')
class SequenceDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown
    reload = units_test.WorkUnitDatabaseTests.reload
    objects = units_test.WorkUnitDatabaseTests.objects

    async def start(self, *, per_loop=2, times=2, add_fourth=True):
        await controller.save_job(self.job.id, status='complete')
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
            if add_fourth:
                session.add(Fen(fen='8/8/8/8/8/8/3k4/6K1 w - -', n_games=1, moves_counter='{}'))
        created = await create_cloud_job(CloudJobRequest(total_fens=per_loop, repeat_count=times), AsyncMock())
        async with AsyncDBSession() as session:
            self.job = await session.get(CloudAnalysisJob, created['id'])
        self.cloud = Mock()
        return created

    async def tick(self):
        # New controller object each tick mimics a restart, not in-memory state.
        await controller.Controller(self.cloud, 'test-image').tick()

    async def freeze(self):
        for _ in range(10):
            await self.tick()
            async with AsyncDBSession() as session:
                saved = await session.get(CloudBatchSequence, self.job.id)
            if saved.frozen_at:
                return saved
        self.fail('Selection did not freeze')

    async def job_state(self):
        async with AsyncDBSession() as session:
            return await session.get(CloudAnalysisJob, self.job.id)

    async def test_api_two_million_has_native_sequence_state_and_no_huge_single_job(self):
        await self.start(per_loop=500000, times=4)
        self.assertEqual(self.job.target, 2000000)
        self.assertEqual(self.job.selection['execution_mode'], sequence.MODE)
        view = await cloud_jobs()
        row = next(j for j in view['jobs'] if j['id'] == self.job.id)
        self.assertEqual(row['sequence']['times'], 4)
        self.assertEqual(row['sequence']['requested'], 2000000)
        self.assertFalse(row['sequence']['frozen'])
        self.assertEqual(row['runs'], [])

    async def test_game_preview_is_divided_not_multiplied_by_times(self):
        await controller.save_job(self.job.id, status='complete')
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
        redis = AsyncMock()
        redis.get.return_value = json.dumps({'game_links':[1], 'player_name':'hikaru', 'fens_to_analyze':1800000})
        created = await create_cloud_job(CloudJobRequest(mode='games', plan_id='a'*32, repeat_count=4), redis)
        async with AsyncDBSession() as session:
            job = await session.get(CloudAnalysisJob, created['id'])
            seq = await session.get(CloudBatchSequence, job.id)
        self.assertEqual(job.target, 1800000)
        await sequence.initialize(job, seq)
        self.assertEqual([c.target_count for c, _ in await sequence.cycles(job.id)], [450000]*4)

    async def test_frozen_selection_claims_every_loop_before_any_google_call(self):
        await self.start()
        saved = await self.freeze()
        self.assertEqual(saved.reserved_count, 4)
        self.assertEqual(self.cloud.mock_calls, [])
        rows = await sequence.cycles(self.job.id)
        self.assertEqual([r.position_count for _,r in rows], [2,2])
        self.assertEqual([r.status for _,r in rows], ['reserved','reserved'])
        local,fens = await existing.get_fens_for_analysis(10, raise_errors=True)
        self.assertIsNone(local)
        self.assertFalse(fens)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)),4)

    async def test_short_selection_is_sealed_without_repeated_attempts_to_find_replacements(self):
        await self.start(per_loop=2, times=4, add_fourth=False)
        saved=await self.freeze()
        self.assertEqual((saved.requested_count,saved.reserved_count),(8,3))
        rows=await sequence.cycles(self.job.id)
        self.assertEqual([r.position_count for _,r in rows],[2,1,0,0])
        self.assertEqual((await self.job_state()).target,3)
        self.assertTrue(all(r.status=='complete' for _,r in rows[2:]))

    async def test_stop_during_selection_releases_only_this_sequences_unstarted_claims(self):
        await self.start()
        await self.tick()
        await stop_repeating(self.job.id)
        await self.tick()
        self.assertEqual((await self.job_state()).status,'cancelled')
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)),0)
        self.assertEqual(self.cloud.mock_calls,[])

    def cloud_stubs(self, objects):
        stack=ExitStack()
        stack.enter_context(patch.object(fleet, 'verify_clean_workspace'))
        stack.enter_context(patch.object(fleet, 'check_bucket'))
        stack.enter_context(patch.object(fleet.batch_spot, 'check_quota', return_value={}))
        stack.enter_context(patch.object(fleet, 'inspect_worker', return_value=('sha256:'+'a'*64,{})))
        stack.enter_context(patch.object(fleet, 'prepare', return_value={}))
        stack.enter_context(patch.object(fleet, 'cleaning_job', return_value={'complete':True}))
        stack.enter_context(patch.object(fleet, 'plan_logs', return_value={}))
        stack.enter_context(patch.object(fleet, 'remove_logs', return_value={'complete':True}))
        stack.enter_context(patch.object(importer, 'refresh_global_projections', new_callable=AsyncMock))
        self.cloud.publish.return_value=units_test.IMAGE
        self.cloud.storage.return_value.create.return_value=True
        self.cloud.storage.return_value.read.side_effect=lambda uri,*args: objects.get(uri)
        self.cloud.start_recovery.return_value='owner'
        remotes={}
        def snapshot(ident,task):
            name='job-'+ident
            remotes[name]={**deepcopy(task['spec']), 'uid':ident, 'status':{'state':'SUCCEEDED'}}
            return {'jobs':[name],'uids':{name:ident},'current_job':name,'execution':'owner','status':'SUCCEEDED'}, {'state':'SUCCEEDED'}
        self.cloud.recovery_snapshot.side_effect=snapshot
        self.cloud.job.side_effect=lambda name:remotes[name]
        return stack

    async def test_each_loop_imports_verifies_cleans_and_refreshes_before_next_and_never_reselects(self):
        await self._full_lifecycle(stop=False)

    async def test_stop_after_started_loop_finishes_it_and_releases_future_fens(self):
        await self._full_lifecycle(stop=True)

    async def _full_lifecycle(self, stop):
        await self.start()
        await self.freeze()
        rows=await sequence.cycles(self.job.id)
        objects={}
        for _,root in rows:
            for unit in root.launch['units']:
                objects.update(await self.objects(unit))
        # A FEN appearing after the seal must never enter any later loop.
        new_fen='8/8/8/8/8/8/2k5/6K1 w - -'
        async with AsyncDBSession() as session,session.begin():
            session.add(Fen(fen=new_fen,n_games=1000,moves_counter='{}'))
        with self.cloud_stubs(objects), patch('chessism_api.database.ask_db.get_fens_for_analysis', side_effect=AssertionError('reselected')):
            stopped=False
            for _ in range(60):
                await self.tick()
                current=await sequence.cycles(self.job.id)
                first,second=current[0][1],current[1][1]
                if first.status != 'complete':
                    self.assertEqual(second.status,'reserved')
                    self.assertLessEqual(self.cloud.start_recovery.call_count,1)
                elif second.status not in {'reserved','cancelled'}:
                    self.assertIsNotNone(first.cleanup_completed_at)
                    self.assertTrue(first.details_pruned)
                    async with AsyncDBSession() as session:
                        self.assertTrue(await session.scalar(select(CloudPhaseTiming.id).where(
                            CloudPhaseTiming.run_id==first.id,CloudPhaseTiming.phase=='global_summaries',
                            CloudPhaseTiming.outcome=='complete')))
                if stop and first.status=='running' and not stopped:
                    await stop_repeating(self.job.id)
                    stopped=True
                saved=await self.job_state()
                if saved.status in {'complete','cancelled'}:break
                self.assertNotIn(saved.status,{'paused','failed'},saved.error)
            else:self.fail('Sequence did not finish')
        self.assertEqual(saved.status,'cancelled' if stop else 'complete')
        self.assertEqual(saved.imported,2 if stop else 4)
        self.assertEqual(self.cloud.start_recovery.call_count,1 if stop else 2)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)),0)
            self.assertIsNone((await session.get(Fen,new_fen)).score)

    async def test_failed_or_paused_loop_never_advances_the_sequence(self):
        await self.start()
        await self.freeze()
        rows=await sequence.cycles(self.job.id)
        first=rows[0][1]
        await controller.save_run(first.id,status='failed',error='simulated failure')
        await self.tick()
        self.assertEqual((await self.job_state()).status,'failed')
        await self.tick()
        self.assertEqual((await self.reload(rows[1][1].id)).status,'reserved')
        self.cloud.start_recovery.assert_not_called()

    async def test_failed_cleanup_never_starts_next_loop(self):
        await self.start()
        await self.freeze()
        rows = await sequence.cycles(self.job.id)
        objects = {}
        for _, root in rows:
            for unit in root.launch['units']:
                objects.update(await self.objects(unit))
        with self.cloud_stubs(objects), patch.object(fleet, 'cleaning_job', side_effect=RuntimeError('cleanup unavailable')):
            for _ in range(30):
                await self.tick()
                job = await self.job_state()
                if job.status == 'paused': break
            else: self.fail('Cleanup failure did not pause the sequence')
        self.assertIn('cleanup unavailable', job.error)
        self.assertEqual(self.cloud.start_recovery.call_count, 1)
        self.assertEqual((await self.reload(rows[1][1].id)).status, 'reserved')
        await self.tick()
        self.assertEqual(self.cloud.start_recovery.call_count, 1)

    async def test_additive_migration_refuses_active_work_and_is_repeatable(self):
        with self.assertRaisesRegex(ValueError,'Finish/recover'):
            async with self.engine.begin() as c:await c.run_sync(upgrade)
        await controller.save_job(self.job.id,status='complete')
        async with self.engine.begin() as c:
            await c.execute(text('DROP TABLE cloud_batch_cycle'))
            await c.execute(text('DROP TABLE cloud_batch_sequence'))
        for _ in range(2):
            async with self.engine.begin() as c:await c.run_sync(upgrade)


if __name__=='__main__':unittest.main()
