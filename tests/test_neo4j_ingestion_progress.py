import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from chessism_api.database.neo4j.sync_ingestion import progress_row

class GraphIngestionProgressTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime.now(timezone.utc)
        self.run=SimpleNamespace(status='running')
        self.control={'heartbeat':self.now}

    def row(self,kind,total,processed,error=None):
        return dict(kind=kind,total=total,processed=processed,error=error,
            started_at=self.now-timedelta(seconds=20),finished_at=self.now-timedelta(seconds=2))

    def test_waits_for_more_ingestion_after_catching_up(self):
        result=progress_row(self.run,[self.row('game',25,25)],self.control,self.now)
        self.assertEqual(result['status'],'waiting')
        self.assertFalse(result['fixed_total'])

    def test_game_and_evaluation_counts_continue_after_ingestion(self):
        self.run.status='complete'
        result=progress_row(self.run,[self.row('game',25,25),self.row('score',10,4)],self.control,self.now)
        self.assertEqual(result['status'],'working')
        self.assertEqual((result['processed'],result['total']),(29,35))
        self.assertIn('25 / 25 games',result['detail'])
        self.assertIn('4 / 10 evaluations',result['detail'])

    def test_retry_is_not_shown_as_success(self):
        result=progress_row(self.run,[self.row('game',25,0,'temporary failure')],self.control,self.now)
        self.assertEqual(result['status'],'retrying')
        self.assertIn('temporary failure',result['detail'])

    def test_completion_and_no_changes(self):
        self.run.status='complete'
        result=progress_row(self.run,[self.row('game',25,25)],self.control,self.now)
        self.assertEqual(result['status'],'complete')
        self.assertEqual(result['elapsed_seconds'],18)
        self.assertEqual(progress_row(self.run,[],self.control,self.now)['status'],'complete')

    def test_backup_wait_is_not_a_stall_or_a_completed_run(self):
        control={**self.control,'phase':'waiting','detail':'Waiting for the database backup snapshot to finish.'}
        rows=[self.row('game',25,10)]
        result=progress_row(self.run,rows,control,self.now)
        self.assertEqual(result['status'],'waiting')
        self.assertIn('backup snapshot',result['detail'])
        self.assertEqual(result['processed'],10)
        rows[0]['error']='temporary failure'
        self.assertEqual(progress_row(self.run,rows,control,self.now)['status'],'retrying')
        self.run.status='complete'
        self.assertEqual(progress_row(self.run,[self.row('game',25,25)],control,self.now)['status'],'complete')
