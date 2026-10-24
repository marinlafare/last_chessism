import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from benchmark_vm16 import large_finish as finish


class LargeFinishTests(unittest.TestCase):
    def test_summary_retry_does_not_import_scores_and_cleans_only_after_success(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.database, 'execute') as database, \
                 patch.object(finish, 'cleanup', return_value={'complete': True}) as cleanup:
                finish.finalize_import(directory, Mock())
                database.assert_called_once_with(finish.database.finalize_import, directory, plan)
                cleanup.assert_called_once()
                self.assertEqual(json.loads((directory / 'finish-status.json').read_bytes())['state'], 'complete')

    def test_summary_retry_failure_preserves_cloud_results(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.database, 'execute', side_effect=TimeoutError('summary timeout')), \
                 patch.object(finish, 'cleanup') as cleanup:
                with self.assertRaises(TimeoutError):
                    finish.finalize_import(directory, Mock())
                cleanup.assert_not_called()
                self.assertEqual(json.loads((directory / 'finish-status.json').read_bytes())['state'], 'failed')

    def test_waiter_does_not_clean_before_database_receipt_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            (directory / 'import-receipt.json').write_text('{}')
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.database, 'execute', side_effect=[False, True]), \
                 patch.object(finish.time, 'sleep') as sleep, \
                 patch.object(finish, 'cleanup', return_value={'complete': True}) as cleanup:
                finish.wait_cleanup(directory, Mock())
                sleep.assert_called_once()
                cleanup.assert_called_once()
                self.assertEqual(json.loads((directory / 'finish-status.json').read_bytes())['state'], 'complete')

    def test_waiter_timeout_never_deletes_unimported_data(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.time, 'monotonic', side_effect=[0, 2]), \
                 patch.object(finish, 'cleanup') as cleanup:
                with self.assertRaises(TimeoutError):
                    finish.wait_cleanup(directory, Mock(), wait_seconds=1)
                cleanup.assert_not_called()
                self.assertEqual(json.loads((directory / 'finish-status.json').read_bytes())['state'], 'failed')

    def files(self, directory):
        plan = {'id': '20261006d200', 'job': 'chessism-vm200-20261006d200', 'specification': {}}
        job = {'name': plan['job'], 'uid': 'benchmark-uid', 'status': {'state': 'SUCCEEDED'}}
        for filename, value in (('performance-summary.json', {}), ('pre-cleanup-job.json', job),
                                ('submission.json', {'response': job}), ('owned-vm-identities.json', ['us-central1-c/123'])):
            (directory / filename).write_text(json.dumps(value))
        return plan

    def test_no_deletion_before_verified_database_import(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.database, 'execute', side_effect=ValueError('Not imported')), \
                 patch.object(finish, 'cleanup_verified') as cleanup:
                with self.assertRaisesRegex(ValueError, 'Not imported'):
                    finish.cleanup(directory, Mock())
                cleanup.assert_not_called()

    def test_pending_cleanup_does_not_mark_database_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.large, 'check_job'), \
                 patch.object(finish.database, 'execute') as database, \
                 patch.object(finish, 'cleanup_verified', return_value={'complete': False}):
                self.assertFalse(finish.cleanup(directory, Mock())['complete'])
                database.assert_called_once_with(finish.database.verify_import, directory, plan)
                self.assertFalse((directory / 'database-completion.json').exists())

    def test_successful_cleanup_rechecks_database_then_releases_admission_fence(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = self.files(directory)
            complete = {'complete': True}
            with patch.object(finish.large, 'load', return_value=plan), \
                 patch.object(finish.large, 'check_job'), \
                 patch.object(finish.database, 'execute', return_value={'job_status': 'complete'}) as database, \
                 patch.object(finish, 'cleanup_verified', return_value=complete):
                finish.cleanup(directory, Mock())
                self.assertEqual(database.call_count, 2)
                database.assert_called_with(finish.database.verify_import, directory, plan, cleanup=complete)
                self.assertEqual(json.loads((directory / 'database-completion.json').read_text()), {'job_status': 'complete'})

    def test_summary_counts_both_phases_and_separates_resume_metrics(self):
        memory = {'host_used_excluding_reclaimable': {'max_bytes': 8}, 'cgroup_peak_bytes': 7,
                  'cgroup_oom_kills_delta': 0, 'host_swap_used': {'max_bytes': 0}}
        first = {'elapsed_seconds': 100, 'phase': 'interrupt', 'child_exit_code': -9,
                 'supervisor_metrics': {'memory': memory}, 'memory_snapshots': []}
        second = {**first, 'elapsed_seconds': 700, 'phase': 'resume', 'child_exit_code': 0,
                  'checkpoint_recovery_verified': True}
        workers = [{'positions': count, 'last_search_seconds': end} for count, end in ((87500, 698), (87500, 699))]
        metrics = {'vm_cpu_busy_percent': 91, 'fen_per_second': 250, 'engine_save_wait_seconds_sum': .4,
                   'result_pipeline': {}}
        report = {'id': 'test', 'positions': 200000, 'all_result_checksums_and_legal_pvs_verified': True,
                  'interrupted': first, 'resumed': second, 'recovery_scope': 'process only',
                  'performance': {'resumed': 25000, 'analyzed_this_attempt': 175000, 'metrics': metrics, 'workers': workers}}
        plan = {'id': 'test', 'count': 200000}
        job = {'createTime': '2026-10-06T00:00:00Z', 'status': {'state': 'SUCCEEDED',
               'statusEvents': [{'eventTime': '2026-10-06T00:15:00Z'}]}}
        result = finish.summary(plan, report, job, 850, [])
        self.assertEqual(result['total_phase_seconds'], 800)
        self.assertEqual(result['overall_fens_per_second'], 250)
        self.assertEqual(result['submission_to_success_seconds'], 900)
        self.assertEqual(result['resume_mean_engine_queue_wait_seconds'], .2)
        self.assertEqual(result['resume_last_search_finish_spread_seconds'], 1)
        invalid = copy.deepcopy(report)
        invalid['all_result_checksums_and_legal_pvs_verified'] = False
        with self.assertRaises(ValueError):
            finish.summary(plan, invalid, job, 850, [])


class TimeoutBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_timeout_remains_bounded(self):
        for value in (0, 900001, True, '900000'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                async with finish.database.database(statement_timeout_ms=value):
                    self.fail('Unbounded timeout accepted')


if __name__ == '__main__':
    unittest.main()
