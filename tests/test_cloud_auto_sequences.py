"""Pure contract/recovery tests. No Google submissions or local DB writes."""
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from pathlib import Path
os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from cloud_job.launch import Client
from cloud_job.sequencing import cycle_sizes, prefix, sealed, checksum64
from cloud_job.recovery import WORKFLOW
from stockfish_batch.checkpoints import digest
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest, batch_cycle_count, MAX_BATCH_FENS_PER_VM, MAX_BATCH_TOTAL_FENS
from cloud_job.work_units import MAX_FENS_PER_VM

EXECUTION = WORKFLOW + '/executions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'


class AutoSequenceTests(unittest.TestCase):
    def test_deployment_labels_cannot_hide_a_failed_source_update(self):
        self.client.require_recovery=Mock()
        self.client.request.return_value={'labels':{'sequence_schema':'2'},'sourceContents':'old source'}
        with self.assertRaisesRegex(ValueError,'Deployed recovery source differs'):
            self.client.require_sequence()
        self.client.request.return_value['sourceContents']=Path('stockfish-batch-worker/cloud_job/recovery.yaml').read_text()
        self.client.require_sequence()

    def test_split_total_instead_of_multiplying_or_balancing(self):
        self.assertEqual(cycle_sizes(2000000), [1000000]*2)
        self.assertEqual(cycle_sizes(1200000), [1000000,200000])
        self.assertEqual(cycle_sizes(1000001), [1000000,1])
        self.assertEqual(cycle_sizes(1963096, 2), [1963096])
        self.assertEqual(cycle_sizes(4000001, 2), [2000000,2000000,1])
        self.assertEqual(cycle_sizes(1), [1])
        self.assertEqual(sum(cycle_sizes(10000000)), 10000000)
        for value in (0,10000001,True,2.5,'2000000'):
            with self.assertRaises(ValueError): cycle_sizes(value)
        self.assertEqual(CloudJobRequest(total_fens=2000000).target,2000000)
        with self.assertRaises(ValueError): CloudJobRequest(backend='cloud_run',total_fens=200001)
        with self.assertRaises(ValueError): CloudJobRequest(total_fens=2000000,repeat_count=4)

    def test_api_controller_limits_agree_and_total_cap_is_unchanged(self):
        self.assertEqual(MAX_BATCH_FENS_PER_VM, MAX_FENS_PER_VM)
        self.assertEqual(MAX_BATCH_TOTAL_FENS, 10000000)
        for count in (1,500001,1000000,1963096,2000001,10000000):
            for vms in (1,2,3,10):
                sizes = cycle_sizes(count, vms)
                self.assertEqual(batch_cycle_count(count, vms), len(sizes))
                self.assertEqual(sum(sizes), count)
                self.assertLessEqual(max(sizes), vms * 1000000)
        for n in (0,11,True,1.5,'2'):
            with self.assertRaises(ValueError): cycle_sizes(1963096, n)
            with self.assertRaises(ValueError): batch_cycle_count(1963096, n)

    def setUp(self):
        self.client = Client()
        self.client.request = Mock(return_value={'name':EXECUTION,'state':'SUCCEEDED'})
        self.client.pages = Mock(return_value=[])
        self.client.storage = Mock()
        self.control = SimpleNamespace(control_run_id='a'*32,session=1,plan_sha256='b'*64,
            execution=EXECUTION,remote_status='SUCCEEDED',accepted_at=True)

    def test_small_arguments_reference_a_checksum_bound_cloud_plan(self):
        self.assertEqual(self.client.start_sequence(self.control), EXECUTION)
        body=self.client.request.call_args.kwargs['body']
        args=json.loads(body['argument'])
        self.assertEqual(args['mode'],'sequence_v2')
        self.assertEqual(args['plan_checksum'],checksum64('b'*64))
        self.assertLess(len(body['argument']),500)
        self.assertNotIn('spec',args)
        self.assertEqual(body['labels']['sequence_id'],'a'*32)

    def test_lost_submit_response_adopts_existing_execution_and_rejects_different_plan(self):
        self.client.start_sequence(self.control)
        args=self.client.request.call_args.kwargs['body']['argument']
        self.client.request.reset_mock()
        self.client.pages.return_value=[{'executions':[{'name':EXECUTION,'argument':args}]}]
        self.assertEqual(self.client.start_sequence(self.control),EXECUTION)
        self.client.request.assert_not_called()
        self.control.plan_sha256='c'*64
        with self.assertRaisesRegex(ValueError,'different arguments'): self.client.start_sequence(self.control)

    def test_cleanup_needs_terminal_sequence_and_no_delayed_duplicate(self):
        self.client.require_sequence_stopped(self.control)
        self.client.storage.assert_not_called()
        self.client.pages.return_value=[{'executions':[{'state':'QUEUED'}]}]
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.client.require_sequence_stopped(self.control)
        self.control.remote_status='RUNNING'
        with self.assertRaisesRegex(ValueError,'not succeeded'):self.client.require_sequence_stopped(self.control)

    def test_stop_and_cancel_are_distinct_create_only_markers(self):
        self.client.signal_sequence(self.control)
        self.assertTrue(self.client.storage.return_value.create.call_args.args[0].endswith('/sequence-1/stop.json'))
        self.client.signal_sequence(self.control,cancel=True)
        self.assertTrue(self.client.storage.return_value.create.call_args.args[0].endswith('/sequence-1/cancel.json'))
        self.client.request.assert_not_called()
        for ident,session in [('../escape',1),('a'*32,4),('a'*32,True)]:
            with self.assertRaises(ValueError):prefix(ident,session)

    def test_plan_hash_is_of_exact_payload_bytes_not_provider_json_formatting(self):
        value={'version':2,'cycles':[{'count':500000}],'label':'déjà'}
        raw,sha=sealed(value)
        payload=json.loads(raw)['payload']
        self.assertEqual(json.loads(payload),value)
        self.assertEqual(digest(payload.encode()),sha)
        self.assertNotEqual(digest((payload+' ').encode()),sha)

    def test_snapshot_rejects_unknown_owner_and_nonterminal_completion(self):
        self.client.sequence_document=Mock(side_effect=[{'execution':EXECUTION,'plan_hash':'wrong'},None])
        with self.assertRaisesRegex(ValueError,'different plan'):self.client.sequence_snapshot(self.control)
        self.client.sequence_document=Mock(return_value=None)
        with self.assertRaisesRegex(ValueError,'without terminal proof'):self.client.sequence_snapshot(self.control)


if __name__=='__main__':unittest.main()
