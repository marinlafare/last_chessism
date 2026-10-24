from datetime import datetime,timedelta,timezone
import unittest
from unittest.mock import AsyncMock,patch

from scripts.compact_db_live_evidence import validate_snapshot
from scripts.migrate_compact_db import apply,parser
from scripts.watch_compact_db import render
from scripts.compact_db_preflight import require_private,require_target


class EvidenceTests(unittest.TestCase):
    def test_only_recent_exact_verified_compressed_snapshot(self):
        now=datetime.now(timezone.utc)
        backup={'backup_id':'snapshot-test','storage_mode':'compressed_snapshot_v1',
            'restore_tested':True,'restore_test_status':'success','archive_sha256':'a'*64,
            'completed_at':(now-timedelta(hours=1)).isoformat(),'restore_tested_at':now.isoformat()}
        manifest={'format':'chessism-pg-dump-custom','relational_storage':{'version':'relational_v2'},
                  'archive_sha256':'a'*64}
        expected={'backup_id':'snapshot-test','archive_sha256':'a'*64}
        validate_snapshot(backup,manifest,expected,now=now)
        for key,value in [('backup_id','wrong'),('storage_mode','wal'),('restore_tested',False),
                          ('restore_test_status','failed'),('archive_sha256','b'*64),
                          ('completed_at',(now-timedelta(days=2)).isoformat())]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                validate_snapshot({**backup,key:value},manifest,expected,now=now)

    def test_live_progress_does_not_claim_isolation(self):
        output=render({'mode':'live_compact','state':'succeeded','committed':True})
        self.assertIn('LIVE DB COMPACT MIGRATION',output)
        self.assertNotIn('Live database unchanged',output)
        self.assertIn('activation/backup verification still pending',output)


class GuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_paths_never_authorize_live_data(self):
        for check in (require_private,require_target):
            with self.assertRaises(RuntimeError):
                await check(AsyncMock(),'/var/lib/postgresql/data')

    async def test_missing_approval_does_not_connect(self):
        args=parser().parse_args(['apply','--plan','absent.json','--output','absent'])
        with self.assertRaisesRegex(ValueError,'Explicit live approval'):
            await apply(args)


if __name__=='__main__':unittest.main()
