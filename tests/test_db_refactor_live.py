from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock,patch

from scripts.db_refactor_live_evidence import prepare_plan,verify_plan,validate_backup
from scripts.migrate_game_player import apply,parser
from scripts.watch_db_refactor import render
from scripts.db_refactor_live_target import (
    require_strict_archiving,STRICT_ARCHIVE_COMMAND,shared_memory_mount_bytes,require_shared_memory,
)


class LiveEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,13,12,tzinfo=timezone.utc)
        self.backup={'backup_id':'tested','restore_tested':True,'restore_test_status':'success',
            'completed_at':(self.now-timedelta(hours=1)).isoformat(),'restore_tested_at':self.now.isoformat()}
        self.info=[{'status':{'code':0},'db':[{'id':1,'repo-key':1,'system-id':123}],
                    'backup':[{'label':'tested','database':{'id':1,'repo-key':1}}]}]

    def test_backup_must_be_recent_tested_and_same_cluster(self):
        validate_backup(self.backup,'tested','123',self.info,now=self.now)
        for key,value in (('backup_id','wrong'),('restore_tested',False),('restore_test_status','failed'),
                          ('completed_at',(self.now-timedelta(days=2)).isoformat()),('restore_test_error','failed')):
            with self.subTest(key=key),self.assertRaises(ValueError):
                validate_backup({**self.backup,key:value},'tested','123',self.info,now=self.now)
        with self.assertRaises(ValueError):validate_backup(self.backup,'tested','other',self.info,now=self.now)
        with self.assertRaises(ValueError):validate_backup(self.backup,'tested','123',[],now=self.now)

    def test_plan_binds_code_and_evidence_without_granting_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch('scripts.db_refactor_live_evidence.evidence',return_value={'proof':'sha'}), \
                 patch('scripts.db_refactor_live_evidence.fingerprint',side_effect=lambda _: {'code.py':'sha'}):
                plan=prepare_plan(root,root)
                self.assertFalse(plan['live_authorized'])
                verify_plan(plan,root)
                plan['source_files']['code.py']='changed'
                with self.assertRaises(ValueError):verify_plan(plan,root)

    def test_live_watch_does_not_claim_no_live_changes(self):
        output=render({'mode':'live_game_player','state':'running','phase':'copying_numeric_fen_associations'})
        self.assertIn('LIVE DB',output)
        self.assertNotIn('Live database unchanged',output)


class ApprovalTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_memory_checks_server_mount_and_rejects_old_live_limit(self):
        for size,expected in (('65536k',None),('262144k',256*1024**2),('1g',1024**3),('268435456',256*1024**2)):
            with self.subTest(size=size):
                connection=AsyncMock()
                connection.fetchval.side_effect=['posix',
                    f'690 685 0:91 / /dev/shm rw,nosuid,nodev - tmpfs shm rw,size={size},inode64\n']
                if expected is None:
                    with self.assertRaisesRegex(RuntimeError,'at least 256 MiB'):
                        await require_shared_memory(connection)
                else:
                    self.assertEqual(await require_shared_memory(connection),expected)
                self.assertEqual(connection.fetchval.call_args.args,("SELECT pg_read_file('/proc/self/mountinfo')",))

    async def test_unreviewed_shared_memory_backend_is_rejected(self):
        connection=AsyncMock()
        connection.fetchval.return_value='mmap'
        with self.assertRaisesRegex(RuntimeError,'POSIX'):
            await require_shared_memory(connection)

    def test_unverified_or_ambiguous_mount_is_rejected(self):
        for mountinfo in ('',
            '1 2 0:3 / /dev/shm rw - ext4 disk rw,size=1g',
            '1 2 0:3 / /dev/shm rw - tmpfs shm rw',
            '1 2 0:3 / /dev/shm rw - tmpfs shm rw,size=50%',
            '1 2 0:3 / /dev/shm rw - tmpfs shm rw,size=1g,size=64m',
            '1 2 0:3 / /dev/shm rw - tmpfs shm rw,size=1g\n' * 2,
            '1 2 0:3 / /not-dev-shm rw - tmpfs shm rw,size=1g'):
            with self.subTest(mountinfo=mountinfo),self.assertRaises(RuntimeError):
                shared_memory_mount_bytes(mountinfo)

    async def test_discarding_archive_wrapper_is_refused(self):
        connection=AsyncMock()
        connection.fetchval.side_effect=['on',STRICT_ARCHIVE_COMMAND]
        await require_strict_archiving(connection)
        connection.fetchval.side_effect=['on','/usr/local/bin/pgbackrest-chessism-archive-push %p']
        with self.assertRaisesRegex(RuntimeError,'strict synchronous'):await require_strict_archiving(connection)

    async def test_missing_approval_fails_before_importing_connections(self):
        args=parser().parse_args(['apply','--plan','plan.json','--backup-id','backup','--expect-database','db',
            '--expect-system-id','123','--expect-pgdata','/not-live','--output','out'])
        with patch('sqlalchemy.ext.asyncio.create_async_engine',side_effect=AssertionError('Connected')):
            with self.assertRaisesRegex(ValueError,'Explicit'):await apply(args)
