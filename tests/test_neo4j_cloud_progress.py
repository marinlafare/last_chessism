import unittest
from types import SimpleNamespace
from chessism_api.database.neo4j.sync_cloud import progress


class CloudGraphProgressTests(unittest.TestCase):
    def test_unknown_candidates_are_not_reported_as_graph_positions(self):
        job=SimpleNamespace(status='running',imported=500,target=1000)
        row={'captured':500,'total':12,'processed':10,'pending':2,'unclassified':1}
        result=progress(job,row,{})
        self.assertIsNone(result['total']);self.assertEqual(result['processed'],10)
        self.assertIn('identifying',result['detail'])

    def test_zero_eligible_and_missing_evidence_are_different(self):
        job=SimpleNamespace(status='complete',imported=500,target=500)
        self.assertEqual(progress(job,{'captured':500,'total':0},{})['status'],'complete')
        self.assertEqual(progress(job,{}, {})['status'],'unavailable')

    def test_retry_and_catchup_during_cloud_work(self):
        job=SimpleNamespace(status='running',imported=500,target=1000)
        row={'captured':500,'total':20,'processed':20,'pending':0}
        self.assertEqual(progress(job,row,{})['status'],'waiting')
        self.assertEqual(progress(job,{**row,'error':'offline','pending':1},{})['status'],'retrying')
        job.imported=1000;row['captured']=1000
        self.assertEqual(progress(job,row,{})['status'],'complete')

    def test_backup_wait_preserves_pending_counts_and_error_precedence(self):
        job=SimpleNamespace(status='complete',imported=10,target=10)
        row={'captured':10,'total':10,'processed':5,'pending':1}
        control={'phase':'waiting','detail':'Waiting for the database backup snapshot to finish.'}
        result=progress(job,row,control)
        self.assertEqual((result['status'],result['processed']),('waiting',5))
        self.assertIn('backup snapshot',result['detail'])
        self.assertEqual(progress(job,{**row,'error':'offline'},control)['status'],'retrying')
        self.assertEqual(progress(job,{**row,'pending':0,'processed':10},control)['status'],'complete')
