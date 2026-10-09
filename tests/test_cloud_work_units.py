"""Large logical jobs, bounded workers, no real Google calls."""
from copy import deepcopy
from dataclasses import asdict
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from cloud_job import batch_spot, work_units
from cleaning_job.cleanup import prefixes
from chessism_api.database.cloud_codec import encode_document, decode_document
from chessism_api.operations.cloud_analysis import controller
from chessism_api.operations.cloud_analysis import work_units_controller as fleet
from chessism_api.operations.cloud_analysis import work_units_results as results
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest

IMAGE = 'us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:' + 'a' * 64


def fixture(count=500000, vms=1):
    units = []
    for index, (vm, count) in enumerate(work_units.ranges(count, vms)):
        ident = f'{index + 1:032x}'
        units.append({'index': index, 'vm_index': vm, 'run_id': ident, 'count': count,
                      'config': asdict(batch_spot.config_for(ident, count)),
                      'contract': {'fingerprint': 'b' * 64, 'position_count': count}})
    launch = {'backend': 'batch_spot', 'multi_vm': True, 'workflow_version': 3,
              'n_vms': vms, 'units': units, 'jobs': [], 'image': IMAGE}
    launch['tasks'] = fleet.lanes(launch)
    for task in launch['tasks']:
        task['spec'] = work_units.spec_for([u for u in units if u['vm_index'] == task['index']], IMAGE)
    return SimpleNamespace(id='f' * 32, status='submitting', launch=launch)


class WorkUnitPlanTests(unittest.TestCase):
    def test_500k_means_ten_bounded_units_on_one_vm_not_ten_vms(self):
        run = fixture()
        self.assertEqual(work_units.ranges(500000, 1), [(0, 50000)] * 10)
        self.assertEqual(len(run.launch['tasks']), 1)
        spec = run.launch['tasks'][0]['spec']
        task = spec['taskGroups'][0]
        self.assertEqual((task['taskCount'], task['parallelism']), ('1', '1'))
        self.assertEqual(len(task['taskSpec']['runnables']), 10)
        self.assertNotIn('maxRunDuration', task['taskSpec'])
        self.assertEqual(len(prefixes(spec)), 20)
        self.assertEqual(spec['allocationPolicy']['instances'][0]['policy']['machineType'], 'n2d-highcpu-16')
        for unit, runnable in zip(run.launch['units'], task['taskSpec']['runnables']):
            cfg = unit['config']
            self.assertEqual((cfg['threads'], cfg['nodes'], cfg['hash_mb'], cfg['batch_size']), (1, 100000, 256, 500))
            self.assertEqual((cfg['stall_timeout'], cfg['run_timeout']), (300, 0))
            self.assertIn('--skip-completed-unit', runnable['container']['commands'])
            self.assertFalse(runnable.get('background'))
            self.assertFalse(runnable.get('ignoreExitStatus'))
        work_units.verify_job(spec, spec)

    def test_vm_distribution_balanced_and_limits_fail_closed(self):
        for count in (1, 499, 50001, 200001, 499999, 500000):
            for n in (1, 2, 3, 10):
                units = work_units.ranges(count, n)
                self.assertEqual(sum(size for _, size in units), count)
                self.assertTrue(all(1 <= size <= 50000 for _, size in units))
                totals = [sum(size for vm, size in units if vm == i) for i in range(min(count, n))]
                self.assertLessEqual(max(totals) - min(totals), 1)
        for count in (0, -1, True, 500001):
            with self.assertRaises(ValueError): work_units.ranges(count, 1)
        for n in (0, True, 11):
            with self.assertRaises(ValueError): work_units.ranges(500000, n)
        self.assertEqual(CloudJobRequest(total_fens=500000).target, 500000)
        with self.assertRaises(ValueError): CloudJobRequest(backend='cloud_run', total_fens=200001)

    def test_modified_order_skipped_units_or_unsafe_runnable_rejected(self):
        spec = fixture().launch['tasks'][0]['spec']
        for change in ('order', 'missing', 'background', 'ignoreExitStatus', 'script'):
            altered = deepcopy(spec)
            tasks = altered['taskGroups'][0]['taskSpec']['runnables']
            if change == 'order': tasks.reverse()
            elif change == 'missing': tasks.pop()
            elif change == 'script': tasks[0]['script'] = {'text': 'something else'}
            else: tasks[0][change] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                work_units.verify_job(altered, spec)

    def test_native_columns_roundtrip_parent_has_no_giant_manifest(self):
        launch = fixture().launch
        for unit in launch['units']:
            unit.update(verified=True, refreshed=False, manifest_sha256='c' * 64)
        root, rows = encode_document('parent', 'launch', launch)
        self.assertEqual(len(rows['work_unit']), 10)
        self.assertNotIn('manifest_record', rows)
        self.assertEqual(decode_document(root, rows), launch)
        self.assertEqual(len(results.proof(launch)), 10)


class WorkUnitLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_submission_uses_saved_complete_unit_list_and_recovers_lost_reply(self):
        run, cloud = fixture(vms=2), Mock()
        async def save(_, **changes):
            for key, value in changes.items(): setattr(run, key, deepcopy(value))
        cloud.start_recovery.side_effect = ['owner0', ValueError('lost reply')]
        with patch.object(controller, 'save_run', side_effect=save), patch.object(batch_spot, 'check_quota'):
            with self.assertRaisesRegex(ValueError, 'lost reply'):
                await fleet.advance(controller.Controller(cloud, 'unused'), SimpleNamespace(selection={}), run)
            self.assertEqual(run.launch['tasks'][0]['recovery_execution'], 'owner0')
            self.assertEqual(len(cloud.start_recovery.call_args.args[1]['spec']['taskGroups'][0]['taskSpec']['runnables']), 5)
            cloud.start_recovery.reset_mock(side_effect=True)
            cloud.start_recovery.return_value = 'owner1'
            await fleet.advance(controller.Controller(cloud, 'unused'), SimpleNamespace(selection={}), run)
            cloud.start_recovery.assert_called_once()
            self.assertEqual(run.status, 'running')

    async def test_preemption_keeps_importing_other_units_without_cleaning(self):
        run, cloud = fixture(vms=2), Mock()
        def snapshot(ident, task):
            name = f'job{task["index"]}'
            return {'jobs': [name], 'uids': {name: 'uid'}, 'current_job': name,
                    'execution': 'owner', 'status': 'RETRYING', 'preemptions': 2}, {'state': 'ACTIVE'}
        cloud.recovery_snapshot.side_effect = snapshot
        cloud.job.side_effect = lambda name: {**deepcopy(run.launch['tasks'][int(name[-1])]['spec']),
                                              'uid': 'uid', 'status': {'state': 'FAILED'}}
        async def imported(cloud, unit): return unit
        with patch.object(results, 'import_unit', side_effect=imported) as imports, \
             patch.object(controller, 'save_run', new_callable=AsyncMock) as save, \
             patch.object(controller, 'save_job', new_callable=AsyncMock) as save_job:
            await fleet.poll(controller.Controller(cloud, 'unused'), SimpleNamespace(selection={}), run, deepcopy(run.launch))
            self.assertEqual(imports.await_count, 10)
            self.assertEqual(save.call_args.kwargs['cloud_state'], 'RUNNING')
            self.assertEqual(save.call_args.kwargs['launch']['vm_statuses'][0]['state'], 'RECOVERING')
            save_job.assert_not_called()
            cloud.delete_object.assert_not_called()

    async def test_cleanup_requires_all_unit_proofs_and_stopped_supervisors(self):
        run, cloud = fixture(), Mock()
        run.status = 'cleanup_planning'
        with patch.object(fleet, 'prepare') as prepare:
            with self.assertRaisesRegex(ValueError, 'Not all work units'):
                await fleet.advance(controller.Controller(cloud, 'unused'), None, run)
            prepare.assert_not_called()
        cloud.require_recovery_stopped.side_effect = ValueError('still active')
        with patch.object(results, 'verify_saved', new_callable=AsyncMock), patch.object(fleet, 'prepare') as prepare:
            with self.assertRaisesRegex(ValueError, 'still active'):
                await fleet.advance(controller.Controller(cloud, 'unused'), None, run)
            prepare.assert_not_called()
