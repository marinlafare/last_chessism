"""Offline cleanup safety checks: no production database or cloud access."""
import json
import os
from contextlib import ExitStack
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations import backup_files, database_backups, database_restore_tests
from chessism_api.operations.redis_ownership import release_owned
from chessism_api.operations.restore_workspace import workspace_lock, remove_workspace
from chessism_api.operations.ingestion_pipeline import progress, fen_orchestrator, fen_workers
from chessism_api.operations.cloud_analysis.result_receipts import task_receipts, completion_manifest
from cleaning_job.local_artifacts import plan, encode, digest


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_release_is_a_single_atomic_operation(self):
        redis = AsyncMock()
        redis.eval.return_value = 0
        self.assertFalse(await release_owned(redis, 'key', 'old-owner'))
        redis.get.assert_not_awaited()
        redis.delete.assert_not_awaited()
        script, count, key, owner = redis.eval.call_args.args
        self.assertIn("redis.call('GET', KEYS[1]) == ARGV[1]", script)
        self.assertEqual((count, key, owner), (1, 'key', 'old-owner'))

    async def test_ingestion_release_uses_atomic_owner_check(self):
        redis = AsyncMock()
        await fen_orchestrator._release_fen_pipeline_coordination(redis, 'job')
        redis.eval.assert_awaited_once()
        redis.get.assert_not_awaited()

    async def test_shared_progress_preserves_fields_and_ttl(self):
        redis = AsyncMock()
        result = await progress.write_job_progress(
            redis, 'job', kind='game_update', total=5, processed=2,
            phase='parsing', ingestion_run_id='run', player_name='player', result=None)
        self.assertEqual(result['processed'], 2)
        self.assertEqual(result['ingestion_run_id'], 'run')
        key, payload = redis.set.call_args.args
        self.assertEqual(key, 'chessism:job_progress:job')
        self.assertEqual(json.loads(payload), result)
        self.assertEqual(redis.set.call_args.kwargs['ex'], 86400)

    async def test_restore_cannot_run_while_workspace_is_locked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (patch.object(database_restore_tests, 'RESTORE_TEST_ROOT', root),
                  patch.object(database_restore_tests, '_run_database_restore_test_job', new_callable=AsyncMock) as run,
                  workspace_lock(root) as locked):
                self.assertTrue(locked)
                with self.assertRaisesRegex(database_restore_tests.RestoreTestUnavailableError, 'already in use'):
                    await database_restore_tests.run_database_restore_test_job({}, backup_id='backup')
                run.assert_not_awaited()

    async def exercise_restore(self, root, *, existing_cluster=False, stop_fails=False,
                               restore_fails=False, start_fails=False):
        workspace = root / 'rehearsal-job'
        if existing_cluster:
            (workspace / 'pgdata').mkdir(parents=True)
            (workspace / 'pgdata/postmaster.pid').write_text('999999\n')
        async def restore(**kwargs):
            if restore_fails:
                raise RuntimeError('restore failed')
            if stop_fails:
                (kwargs['pgdata'] / 'postmaster.pid').write_text('999999\n')
        sockets = []
        create_socket = database_restore_tests._create_restore_socket_directory
        def capture_socket():
            directory = create_socket()
            sockets.append(directory)
            # Even tests simulating an unkillable PG must remove their empty
            # socket fixture afterward; no actual PostgreSQL runs here.
            self.addCleanup(lambda: directory.rmdir() if directory.exists() else None)
            return directory
        with ExitStack() as stack:
            for name, value in {
                'RESTORE_TEST_ROOT': root, 'RESTORE_TEST_HEADROOM_BYTES': 0,
                'latest_backup_for_restore_test': lambda: {'backup_id': 'backup', 'database_bytes': 0},
                '_read_json': lambda *_: {}, '_validate_probe': lambda _: None,
                '_validate_fen_identity': AsyncMock(return_value={'version': 'text_v1'}),
                '_catalog_with_restore_result': lambda **_: None, '_update_manifest': lambda **_: None,
                '_run_restore': AsyncMock(side_effect=restore),
                '_run_command_capture': AsyncMock(return_value=(0, '{}'),
                    side_effect=RuntimeError('start failed') if start_fails else None),
                '_create_restore_socket_directory': capture_socket,
                '_write_progress': AsyncMock(), 'ensure_backup_reservation': AsyncMock(),
                'release_backup': AsyncMock(),
            }.items():
                stack.enter_context(patch.object(database_restore_tests, name, value))
            writes = stack.enter_context(patch.object(database_restore_tests, '_atomic_write_json'))
            stop = stack.enter_context(patch.object(database_restore_tests, '_stop_restored_postgres', new_callable=AsyncMock))
            if existing_cluster or stop_fails:
                with self.assertRaises(database_restore_tests.RestoreTestUnavailableError):
                    await database_restore_tests.run_database_restore_test_job({'job_id': 'job'}, backup_id='backup')
                self.assertTrue((workspace / 'pgdata/postmaster.pid').exists())
                if existing_cluster:
                    stop.assert_not_awaited()
                    self.assertEqual(sockets, [])
                else:
                    self.assertTrue(sockets[0].exists())
                self.assertNotIn('success', [call.args[1]['status'] for call in writes.call_args_list])
            elif restore_fails or start_fails:
                with self.assertRaisesRegex(RuntimeError, 'restore failed|start failed'):
                    await database_restore_tests.run_database_restore_test_job({'job_id': 'job'}, backup_id='backup')
                self.assertFalse(workspace.exists())
                self.assertFalse(sockets[0].exists())
            else:
                result = await database_restore_tests.run_database_restore_test_job({'job_id': 'job'}, backup_id='backup')
                self.assertTrue(result['restore_tested'])
                self.assertFalse(workspace.exists())
                self.assertFalse(sockets[0].exists())
                self.assertEqual(writes.call_args.args[1]['status'], 'success')
            database_restore_tests.release_backup.assert_awaited_once()

    async def test_restore_success_removes_copy_before_reporting_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            await self.exercise_restore(Path(temporary))

    async def test_restore_nested_root_uses_short_socket(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / ('deep-workspace-' * 10) / 'restore-tests'
            await self.exercise_restore(root)

    async def test_restore_copy_failure_removes_private_socket(self):
        with tempfile.TemporaryDirectory() as temporary:
            await self.exercise_restore(Path(temporary), restore_fails=True)

    async def test_restore_start_failure_removes_private_socket(self):
        with tempfile.TemporaryDirectory() as temporary:
            await self.exercise_restore(Path(temporary), start_fails=True)

    async def test_invalid_socket_path_fails_before_copying_backup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            socket_dir = root / ('x' * 120)
            socket_dir.mkdir(mode=0o700)
            with ExitStack() as stack:
                for name, value in {
                    'RESTORE_TEST_ROOT': root, 'RESTORE_TEST_HEADROOM_BYTES': 0,
                    'latest_backup_for_restore_test': lambda: {'backup_id': 'backup', 'database_bytes': 0},
                    '_create_restore_socket_directory': lambda: socket_dir,
                    '_atomic_write_json': lambda *_: None,
                    '_catalog_with_restore_result': lambda **_: None,
                    '_update_manifest': lambda **_: None,
                    '_write_progress': AsyncMock(), 'ensure_backup_reservation': AsyncMock(),
                    'release_backup': AsyncMock(), '_run_restore': AsyncMock(),
                    '_run_command_capture': AsyncMock(),
                }.items():
                    stack.enter_context(patch.object(database_restore_tests, name, value))
                with self.assertRaisesRegex(database_restore_tests.RestoreTestUnavailableError, '107-byte'):
                    await database_restore_tests.run_database_restore_test_job({'job_id': 'job'}, backup_id='backup')
                database_restore_tests._run_restore.assert_not_awaited()
                database_restore_tests._run_command_capture.assert_not_awaited()
                database_restore_tests.release_backup.assert_awaited_once()
                self.assertFalse(socket_dir.exists())
                self.assertFalse((root / 'rehearsal-job').exists())

    async def test_restore_failed_stop_preserves_cluster_and_reports_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            await self.exercise_restore(Path(temporary), stop_fails=True)

    async def test_restore_does_not_stop_preexisting_unowned_cluster(self):
        with tempfile.TemporaryDirectory() as temporary:
            await self.exercise_restore(Path(temporary), existing_cluster=True)

    def test_restore_startup_preserves_locked_and_possibly_live_workspaces(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale, live = root / 'rehearsal-stale', root / 'rehearsal-live'
            stale.mkdir()
            (live / 'pgdata').mkdir(parents=True)
            (live / 'pgdata/postmaster.pid').write_text('999999\n')
            with patch.object(database_restore_tests, 'RESTORE_TEST_ROOT', root):
                with workspace_lock(root):
                    self.assertEqual(database_restore_tests.cleanup_stale_restore_workspaces(), [])
                self.assertTrue(stale.exists())
                self.assertEqual(database_restore_tests.cleanup_stale_restore_workspaces(), ['rehearsal-stale'])
                self.assertTrue(live.exists())

    def test_workspace_cleanup_does_not_follow_symlinks_or_accept_other_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / 'keep'
            outside.mkdir()
            (outside / 'important').write_text('keep')
            link = root / 'rehearsal-link'
            link.symlink_to(outside, target_is_directory=True)
            with workspace_lock(root):
                self.assertTrue(remove_workspace(root, link))
                with self.assertRaises(ValueError):
                    remove_workspace(root, outside)
            self.assertTrue((outside / 'important').exists())

    def test_backup_helpers_are_shared_and_failed_replace_preserves_original(self):
        self.assertIs(database_backups._atomic_write_json, database_restore_tests._atomic_write_json)
        self.assertIs(database_backups._sha256, database_restore_tests._sha256)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'status.json'
            backup_files.atomic_write_json(path, {'status': 'original'})
            original_hash = backup_files.sha256(path)
            with patch.object(backup_files.os, 'replace', side_effect=OSError('disk error')):
                with self.assertRaises(OSError):
                    backup_files.atomic_write_json(path, {'status': 'replacement'})
            self.assertEqual(backup_files.sha256(path), original_hash)
            self.assertEqual(list(path.parent.iterdir()), [path])
            self.assertEqual(backup_files.read_json(path, {}), {'status': 'original'})

    def test_failures_go_to_worker_logging_not_repository(self):
        with self.assertLogs(fen_workers.logger, level='ERROR') as logs:
            fen_workers._write_failures('worker', [{'game': 123, 'error': 'bad move'}])
        self.assertIn('worker', logs.output[0])
        self.assertIn('bad move', logs.output[0])

    def test_task_receipts_are_isolated_and_original_not_modified(self):
        first = {'sha256': 'batch', 'records': [{'id': 'a', 'object': 'tasks/000000/batches/000000.json', 'sha256': 'a'}]}
        second = {'sha256': 'batch2', 'records': [{'id': 'b', 'object': 'tasks/000001/batches/000000.json', 'sha256': 'b'}]}
        receipts = {'tasks/000001/batches/000000.json': second, 'tasks/000000/batches/000000.json': first}
        selected = task_receipts(receipts, 0)
        self.assertEqual(selected['batches/000000.json']['records'][0]['object'], 'batches/000000.json')
        self.assertTrue(first['records'][0]['object'].startswith('tasks/'))
        manifest = completion_manifest({'fingerprint': 'fp'}, 2, receipts)
        self.assertEqual([r['id'] for r in manifest['records']], ['a', 'b'])
        first['records'][0]['object'] = 'tasks/000001/batches/000000.json'
        with self.assertRaisesRegex(ValueError, 'another task'):
            task_receipts(receipts, 0)

    def test_legacy_restore_requires_explicit_confirmation_before_docker(self):
        script = Path(__file__).resolve().parents[1] / 'restore_db_from_backup.sh'
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(['bash', str(script)], cwd=temporary, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('--confirm-database', result.stderr)


class LocalArtifactTests(unittest.TestCase):
    def fixture(self, root):
        (root / 'download/batches').mkdir(parents=True)
        engine = {'score': 17}
        result_hash = digest(encode(engine))
        batch = {'fingerprint': 'fp', 'records': [{'id': 'a', 'fingerprint': 'fp',
                 'engine_result': engine, 'result_sha256': result_hash}]}
        manifest = {'status': 'complete', 'fingerprint': 'fp', 'position_count': 1,
                    'records': [{'id': 'a', 'sha256': result_hash, 'object': 'batches/000000.json'}]}
        for name, value in {
            'finish-status.json': {'state': 'complete'},
            'cleanup-result.json': {'complete': True, 'remaining_compute': []},
            'import-receipt.json': {'cleanup_ready': True, 'database_receipts_verified': True,
                'remaining_claims': 0, 'imported': 1, 'db_job_id': 'job', 'manifest_sha256': digest(encode(manifest))},
            'download/manifest.json': manifest, 'download/batches/000000.json': batch,
        }.items():
            (root / name).write_bytes(encode(value))

    def test_dry_run_lists_only_verified_batch_files_and_is_retryable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            unknown = root / 'download/batches/unlisted.json'
            unknown.write_text('important')
            report = plan(root)
            self.assertEqual(len(report['files']), 1)
            self.assertTrue((root / 'download/batches/000000.json').exists())
            (root / 'download/batches/000000.json').unlink()
            self.assertEqual(plan(root)['files'], [])
            self.assertTrue(unknown.exists())

    def test_corruption_or_missing_proof_blocks_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            (root / 'cleanup-result.json').write_text('{"complete": false}')
            with self.assertRaises(ValueError):
                plan(root)
            (root / 'cleanup-result.json').write_text('{"complete": true}')
            path = root / 'download/batches/000000.json'
            data = json.loads(path.read_bytes())
            data['records'][0]['engine_result']['score'] = 99
            path.write_bytes(encode(data))
            with self.assertRaises(ValueError):
                plan(root)

    def test_symlink_batch_is_never_deleted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            path = root / 'download/batches/000000.json'
            path.unlink()
            path.symlink_to(root / 'finish-status.json')
            with self.assertRaises(ValueError):
                plan(root)
            self.assertTrue((root / 'finish-status.json').exists())


if __name__ == '__main__':
    unittest.main()
