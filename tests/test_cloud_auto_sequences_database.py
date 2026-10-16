"""Isolated PostgreSQL and fake Google: restart/import/cleanup of autonomous cycles."""
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
from chessism_api.database.models import (CloudAnalysisJob, CloudAnalysisRun, CloudSequenceControl,
    CloudBatchSequence, CloudControllerHeartbeat, CloudFenClaim, Fen)
from chessism_api.operations.cloud_analysis import controller, sequence, auto_sequence as auto, auto_sequence_cleanup as cleanup, importer
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.routers.cloud_analysis import create_cloud_job, cloud_jobs, retry_cloud_job, stop_repeating, cancel_cloud_job, resume_cloud_job
from chessism_api.operations.cloud_analysis.migrate_auto_sequences import upgrade
from cloud_job.recovery import WORKFLOW, RecoveryClient
from stockfish_batch.checkpoints import digest, encode

EXECUTION=WORKFLOW+'/executions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'


@unittest.skipUnless(existing.TEST_URL,'requires isolated PostgreSQL database')
class AutoSequenceDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown=existing.CloudDatabaseTests.asyncTearDown
    reload=units_test.WorkUnitDatabaseTests.reload
    objects=units_test.WorkUnitDatabaseTests.objects

    async def start(self,total=4):
        await controller.save_job(self.job.id,status='complete')
        async with AsyncDBSession() as s,s.begin():
            s.add(CloudControllerHeartbeat(id=1,seen_at=datetime.now(timezone.utc)))
            s.add(Fen(fen='8/8/8/8/8/8/3k4/6K1 w - -',n_games=1,moves_counter='{}'))
        with patch('chessism_api.operations.cloud_analysis.schemas.MAX_BATCH_FENS_PER_VM',2):
            result=await create_cloud_job(CloudJobRequest(total_fens=total),AsyncMock())
        async with AsyncDBSession() as s:self.job=await s.get(CloudAnalysisJob,result['id'])
        self.cloud=Mock()
        self.data,self.states,self.remotes={},{},{}
        self.remote='RUNNING'
        self.completed=0
        def create(uri,raw):
            if uri in self.data:return False
            self.data[uri]=raw
            return True
        self.cloud.storage.return_value.create.side_effect=create
        self.cloud.storage.return_value.read.side_effect=lambda uri,*args:self.data.get(uri)
        self.cloud.publish.return_value=units_test.IMAGE
        self.cloud.start_sequence.return_value=EXECUTION
        def snapshot(c):
            return {'version':2,'run_id':c.control_run_id,'session':c.session,'execution':EXECUTION,
                'plan_hash':c.plan_sha256,'status':self.remote,'completed_cycles':self.completed}, {
                'state':'ACTIVE' if self.remote=='RUNNING' else 'SUCCEEDED'}
        self.cloud.sequence_snapshot.side_effect=snapshot
        self.cloud.recovery_document.side_effect=lambda ident,session,name:self.states.get((ident,session))
        self.cloud.job.side_effect=lambda name:self.remotes.get(name)
        self.cloud.request.return_value={'state':'SUCCEEDED'}
        self.cloud.recovery_executions.return_value=[]
        self.cloud.require_recovery_stopped.side_effect=lambda ident,launch: RecoveryClient.require_recovery_stopped(self.cloud,ident,launch)

    def stubs(self):
        stack=ExitStack()
        stack.enter_context(patch('cloud_job.sequencing.LIMIT',2))
        stack.enter_context(patch.object(auto,'verify_clean_workspace'))
        stack.enter_context(patch.object(auto,'check_bucket'))
        stack.enter_context(patch.object(auto.batch_spot,'check_quota',return_value={}))
        stack.enter_context(patch.object(auto,'inspect_worker',return_value=('sha256:'+'a'*64,{})))
        self.plan=stack.enter_context(patch.object(cleanup,'prepare',return_value={'jobs':[]}))
        self.clean=stack.enter_context(patch.object(cleanup,'cleaning_job',return_value={'complete':True}))
        stack.enter_context(patch.object(cleanup,'plan_logs',return_value={}))
        stack.enter_context(patch.object(cleanup,'remove_logs',return_value={'complete':True}))
        stack.enter_context(patch.object(importer,'refresh_global_projections',new_callable=AsyncMock))
        return stack

    async def tick(self):
        await controller.Controller(self.cloud,'test-image').tick()
        async with AsyncDBSession() as s:
            job=await s.get(CloudAnalysisJob,self.job.id)
        self.assertNotEqual(job.status,'paused',job.error)
        return job

    async def launch(self):
        for _ in range(30):
            await self.tick()
            async with AsyncDBSession() as s:c=await s.get(CloudSequenceControl,self.job.id)
            if c and c.accepted_at:return c
        self.fail('Sequence not acknowledged')

    async def cloud_finishes(self,cycles=None):
        roots=[r for _,r in await sequence.cycles(self.job.id) if r.position_count]
        for root in roots[:cycles]:
            for unit in root.launch['units']:self.data.update(await self.objects(unit))
            for task in root.launch['tasks']:
                ident,session=task['run_id'],task['recovery_session']
                name=f'chessism-ui-{ident}-1'
                self.states[(ident,session)]={'version':1,'run_id':ident,'session':session,'execution':EXECUTION,
                    'spec_hash':digest(encode(task['spec'])),'status':'SUCCEEDED','jobs':[name],
                    'uids':{name:ident},'current_job':name,'preemptions':0,'application_failures':0,'last_exit_code':None}
                self.remotes[name]={**deepcopy(task['spec']),'uid':ident,'status':{'state':'SUCCEEDED'}}
        self.completed=len(roots[:cycles])
        self.remote='SUCCEEDED' if self.completed==len(roots) else 'RUNNING'

    async def test_whole_sequence_uploaded_before_single_submission_and_ack(self):
        await self.start()
        with self.stubs():
            control=await self.launch()
        rows=await sequence.cycles(self.job.id)
        self.assertEqual([r.position_count for _,r in rows],[2,2])
        for _,root in rows:
            self.assertTrue(all(u['uploaded'] and u['config']['input'] in self.data for u in root.launch['units']))
        self.cloud.start_sequence.assert_called_once()
        self.cloud.publish.assert_called_once()
        self.cloud.start_recovery.assert_not_called()
        self.assertIsNotNone(control.accepted_at)
        view=await cloud_jobs()
        row=next(j for j in view['jobs'] if j['id']==self.job.id)
        self.assertTrue(row['sequence']['safe_to_power_off'])
        # Every VM definition is sealed independently; cycle descriptors remain
        # small. All definitions must exist before the sequence is submitted.
        from cloud_job.sequencing import prefix, checksum64
        base = prefix(control.control_run_id, control.session)
        for index, (_, root) in enumerate(rows):
            cycle = json.loads(json.loads(self.data[base + f'cycle-{index}.json'])['payload'])
            for task_index, ref in enumerate(cycle['tasks']):
                self.assertNotIn('spec', ref)
                payload = json.loads(self.data[base + f'task-{index}-{task_index}.json'])['payload']
                self.assertEqual(ref['checksum'], checksum64(digest(payload.encode())))
                definition = json.loads(payload)
                self.assertEqual(definition['spec'], root.launch['tasks'][task_index]['spec'])
                self.assertEqual(definition['run_id'], ref['run_id'])
        self.assertTrue(row['sequence']['automatic'])
        self.assertEqual(row['sequence']['times'],2)

    async def test_not_safe_to_switch_off_before_google_acknowledges(self):
        await self.start()
        with self.stubs():
            for _ in range(30):
                await self.tick()
                async with AsyncDBSession() as s:c=await s.get(CloudSequenceControl,self.job.id)
                if c and c.phase=='running':break
            else:self.fail('Not submitted')
            self.cloud.sequence_snapshot.return_value=(None,{'state':'ACTIVE'})
            self.cloud.sequence_snapshot.side_effect=None
            await self.tick()
        view=await cloud_jobs()
        row=next(j for j in view['jobs'] if j['id']==self.job.id)
        self.assertFalse(row['sequence']['safe_to_power_off'])
        self.cloud.start_sequence.assert_called_once()

    async def test_cancel_before_publication_releases_claims_without_cloud_writes(self):
        await self.start()
        with self.stubs():
            for _ in range(30):
                await self.tick()
                async with AsyncDBSession() as s:c=await s.get(CloudSequenceControl,self.job.id)
                if c:break
            self.assertEqual(c.phase,'preparing')
            await cancel_cloud_job(self.job.id)
            self.assertEqual((await self.tick()).status,'cancelled')
        self.assertFalse(self.cloud.mock_calls)
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudFenClaim)),0)

    async def test_additive_migration_is_repeatable_and_preserves_old_jobs(self):
        with self.assertRaisesRegex(ValueError,'Finish/recover'):
            async with self.engine.begin() as c:await c.run_sync(upgrade)
        await controller.save_job(self.job.id,status='complete')
        async with self.engine.begin() as c:
            await c.execute(text('DROP TABLE cloud_sequence_control'))
            await c.execute(text('ALTER TABLE cloud_control_work_unit DROP COLUMN uploaded'))
        for _ in range(2):
            async with self.engine.begin() as c:await c.run_sync(upgrade)
        async with AsyncDBSession() as s:
            self.assertEqual((await s.get(CloudAnalysisJob,self.job.id)).status,'complete')

    async def test_restart_after_partial_cleanup_uses_saved_plan_not_cloud_files(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes()
            for _ in range(30):
                await self.tick()
                async with AsyncDBSession() as s:c=await s.get(CloudSequenceControl,self.job.id)
                if c.phase=='cleaning':break
            else:self.fail('Did not reach persisted cleanup plan')
            self.data.clear()
            self.cloud.sequence_snapshot.side_effect=AssertionError('Do not require deleted cloud state')
            self.cloud.recovery_document.side_effect=AssertionError('Do not re-read recovery after deletion starts')
            for _ in range(30):
                job=await self.tick()
                if job.status=='complete':break
            self.assertEqual(job.status,'complete')
        self.clean.assert_called_once()

    async def wait_for_cleanup_planning(self):
        for _ in range(30):
            await self.tick()
            async with AsyncDBSession() as s:c=await s.get(CloudSequenceControl,self.job.id)
            if c.phase=='cleanup_planning':return
        self.fail('Did not reach cleanup planning')

    async def test_batch_success_waits_for_recovery_confirmation_before_finalizing(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes(cycles=1)
            state=next(iter(self.states.values()))
            state['status']='RUNNING'  # Batch has finished; workflow has not checkpointed it yet.
            for _ in range(4):await self.tick()
            first=(await sequence.cycles(self.job.id))[0][1]
            self.assertEqual(first.imported_count,2)
            self.assertEqual(first.status,'running')
            self.assertEqual(first.cloud_state,'RUNNING')
            self.assertIsNone(first.results_verified_at)
            self.assertEqual(first.launch['tasks'][0]['batch_status']['state'],'SUCCEEDED')
            self.plan.assert_not_called()
            self.clean.assert_not_called()
            state['status']='SUCCEEDED'
            await self.tick()
            self.assertEqual((await self.reload(first.id)).status,'finalizing')
            await self.cloud_finishes()
            for _ in range(40):
                job=await self.tick()
                if job.status=='complete':break
            self.assertEqual(job.status,'complete')
            self.assertEqual(job.imported,4)
            self.clean.assert_called_once()
            self.cloud.start_sequence.assert_called_once()
            self.cloud.start_recovery.assert_not_called()

    async def test_cleanup_refreshes_legacy_stale_finalizing_proof_without_reanalysis(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes()
            await self.wait_for_cleanup_planning()
            roots=[r for _,r in await sequence.cycles(self.job.id)]
            proofs=[r.completion_manifest_sha256 for r in roots]
            for root in roots:
                launch=deepcopy(root.launch)
                launch['tasks'][0]['recovery_summary']['status']='RUNNING'
                await controller.save_run(root.id,launch=launch)
            await controller.save_job(self.job.id,status='paused',error='Recovery has not completed; refusing cleanup')
            await resume_cloud_job(self.job.id)
            with patch.object(auto.results,'import_unit',side_effect=AssertionError('Results already committed')):
                await self.tick()
                refreshed=[r for _,r in await sequence.cycles(self.job.id)]
                self.assertEqual([r.completion_manifest_sha256 for r in refreshed],proofs)
                self.assertTrue(all(r.launch['tasks'][0]['recovery_summary']['status']=='SUCCEEDED' for r in refreshed))
                self.plan.assert_called_once()
                for _ in range(40):
                    job=await self.tick()
                    if job.status=='complete':break
            self.assertEqual(job.status,'complete')
            self.assertEqual(job.imported,4)
            self.clean.assert_called_once()
            self.cloud.start_sequence.assert_called_once()
            self.cloud.start_recovery.assert_not_called()

    async def test_cleanup_refresh_fails_closed_for_missing_running_or_changed_owner(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes()
            await self.wait_for_cleanup_planning()
            key=next(iter(self.states))
            original=deepcopy(self.states[key])
            for change in ('missing','running','owner','contract','uid'):
                with self.subTest(change=change):
                    state=deepcopy(original)
                    if change=='missing':state=None
                    elif change=='running':state['status']='RUNNING'
                    elif change=='owner':state['execution']=WORKFLOW+'/executions/bbbbbbbb-cccc-dddd-eeee-ffffffffffff'
                    elif change=='contract':state['spec_hash']='0'*64
                    else:state['uids'][state['current_job']]='wrong-uid'
                    self.states[key]=state
                    await controller.Controller(self.cloud,'test-image').tick()
                    async with AsyncDBSession() as s:job=await s.get(CloudAnalysisJob,self.job.id)
                    self.assertEqual(job.status,'paused')
                    self.assertEqual(job.imported,4)
                    self.plan.assert_not_called()
                    self.clean.assert_not_called()
                    await resume_cloud_job(self.job.id)
            self.states[key]=original
            await self.tick()
            self.plan.assert_called_once()

    async def test_offline_cloud_completion_then_restart_catches_up_and_cleans_once(self):
        await self.start()
        with self.stubs():
            await self.launch()
            # Both tasks finish without another host tick or any import.
            await self.cloud_finishes()
            async with AsyncDBSession() as s:
                self.assertEqual((await s.get(CloudAnalysisJob,self.job.id)).imported,0)
            for _ in range(40):
                job=await self.tick()
                if self.clean.called:self.assertEqual(job.imported,4)
                if job.status=='complete':break
            else:self.fail('Offline catch-up did not complete')
        self.assertEqual(job.imported,4)
        self.clean.assert_called_once()
        self.cloud.start_sequence.assert_called_once()
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudFenClaim)),0)
        self.assertTrue(all(r.results_verified_at and r.cleanup_completed_at and r.details_pruned for _,r in await sequence.cycles(self.job.id)))

    async def test_importing_first_task_does_not_cleanup_shared_image_while_second_runs(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes(cycles=1)
            for _ in range(3):await self.tick()
            roots=[r for _,r in await sequence.cycles(self.job.id)]
            self.assertEqual(roots[0].status,'finalizing')
            self.assertEqual(roots[0].imported_count,2)
            self.assertEqual(roots[1].status,'cloud_queued')
            self.clean.assert_not_called()

    async def test_success_without_all_result_files_does_not_authorize_cleanup(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes()
            missing=next(uri for uri in self.data if '/batches/' in uri)
            del self.data[missing]
            for _ in range(4):await self.tick()
            self.clean.assert_not_called()

    async def test_stop_preserves_checkpoints_and_explicit_retry_keeps_successful_tasks(self):
        await self.start()
        with self.stubs():
            await self.launch()
            await self.cloud_finishes(cycles=1)
            await stop_repeating(self.job.id)
            self.remote='STOPPED'
            for _ in range(4):
                job=await self.tick()
                if job.status=='failed':break
            self.assertEqual(job.status,'failed')
            self.assertEqual(job.imported,2)
            self.clean.assert_not_called()
            self.cloud.signal_sequence.assert_called()
            await retry_cloud_job(self.job.id)
            async with AsyncDBSession() as s:
                c=await s.get(CloudSequenceControl,self.job.id)
                saved=await s.get(CloudBatchSequence,self.job.id)
            self.assertEqual(c.session,2)
            self.assertFalse(saved.stop_requested)
            self.assertIsNone(c.accepted_at)
            roots=[r for _,r in await sequence.cycles(self.job.id)]
            self.assertEqual(roots[0].launch['tasks'][0]['status'],'SUCCEEDED')
            self.assertEqual(roots[1].launch['tasks'][0]['status'],'PENDING')


if __name__=='__main__':unittest.main()
