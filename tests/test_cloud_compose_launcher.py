"""Offline Compose bootstrap tests: fake credentials, no Docker/cloud/real DB."""
import asyncio
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

FILE = Path(__file__).resolve().parents[1] / "stockfish-batch-worker/cloud_job/run_compose.py"
SPEC = importlib.util.spec_from_file_location("cloud_compose_launcher", FILE)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ComposeCredentialsTests(unittest.TestCase):
    def test_local_connection_does_not_probe_roots_certificates_or_override_tls(self):
        url = "postgresql+asyncpg://test:secret@db/example"
        self.assertEqual(launcher.compose_connection_args(url), {"ssl": False})
        self.assertEqual(launcher.compose_connection_args(url + "?ssl=verify-full"), {})
        with self.assertRaisesRegex(ValueError, "internal db service"):
            launcher.compose_connection_args(url.replace("@db/", "@remote/"))

    def fake_config(self, root):
        source, runtime = root / "source", root / "runtime"
        source.mkdir()
        runtime.mkdir()
        (source / "configurations").mkdir()
        (source / "configurations/config_default").write_text("[core]\naccount = fake@example.test\n")
        (source / "active_config").write_text("default")
        (source / "application_default_credentials.json").write_text("do-not-copy")
        with closing(sqlite3.connect(source / "credentials.db")) as connection, connection:
            connection.execute("CREATE TABLE test (value TEXT)")
            connection.execute("INSERT INTO test VALUES ('fake-credential')")
        return source, runtime

    def test_config_is_private_copy_and_host_docker_config_not_needed(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ):
            source, runtime = self.fake_config(Path(directory))
            before = (source / "credentials.db").read_bytes()
            launcher.prepare_credentials(source, runtime)
            config = Path(os.environ["CLOUDSDK_CONFIG"])
            self.assertEqual(config.stat().st_mode & 0o777, 0o700)
            self.assertEqual((config / "active_config").read_text(), "default")
            self.assertFalse((config / "application_default_credentials.json").exists())
            with closing(sqlite3.connect(config / "credentials.db")) as connection, connection:
                self.assertEqual(connection.execute("SELECT value FROM test").fetchone()[0], "fake-credential")
                connection.execute("UPDATE test SET value = 'refreshed-private-copy'")
            self.assertEqual((source / "credentials.db").read_bytes(), before)
            docker = json.loads((Path(os.environ["DOCKER_CONFIG"]) / "config.json").read_text())
            self.assertEqual(docker, {"credHelpers": {"us-central1-docker.pkg.dev": "gcloud"}})

    def test_sqlite_snapshot_includes_committed_wal(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ):
            source, runtime = self.fake_config(Path(directory))
            connection = sqlite3.connect(source / "credentials.db")
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("UPDATE test SET value = 'committed-in-wal'")
                connection.commit()
                launcher.prepare_credentials(source, runtime)
                with closing(sqlite3.connect(runtime / "gcloud/credentials.db")) as copied:
                    self.assertEqual(copied.execute("SELECT value FROM test").fetchone()[0], "committed-in-wal")
            finally:
                connection.close()

    def test_missing_login_and_symlinks_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "gcloud auth login"):
                launcher.prepare_credentials(root, root)
            source, runtime = self.fake_config(root)
            (source / "configurations/linked").symlink_to(source / "active_config")
            with self.assertRaisesRegex(ValueError, "Symlinked"):
                launcher.prepare_credentials(source, runtime)

    def test_privileges_drop_to_config_owner_with_only_socket_group(self):
        source, socket = Mock(), Mock()
        source.stat.return_value = SimpleNamespace(st_uid=1234, st_gid=5678)
        source.is_dir.return_value = True
        socket.stat.return_value = SimpleNamespace(st_mode=stat.S_IFSOCK, st_gid=965)
        with patch.object(launcher.os, "geteuid", side_effect=[0, 1234]), \
             patch.object(launcher.os, "setgroups") as groups, \
             patch.object(launcher.os, "setgid") as gid, patch.object(launcher.os, "setuid") as uid:
            launcher.drop_privileges(source, socket)
            groups.assert_called_once_with([965])
            gid.assert_called_once_with(5678)
            uid.assert_called_once_with(1234)
        socket.stat.return_value.st_mode = stat.S_IFDIR
        with self.assertRaises(ValueError):
            launcher.drop_privileges(source, socket)

    def test_health_requires_fresh_local_marker_not_another_controllers_db_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "heartbeat"
            with patch.object(launcher, "HEARTBEAT_FILE", marker):
                self.assertFalse(launcher.healthy())
                marker.touch()
                self.assertTrue(launcher.healthy())
                os.utime(marker, (time.time()-120, time.time()-120))
                self.assertFalse(launcher.healthy())


class ComposeStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_for_api_recovery(self):
        client = AsyncMock()
        client.get.side_effect = [httpx.ConnectError("starting"), Mock(status_code=503), Mock(status_code=200)]
        with patch.object(launcher.httpx, "AsyncClient") as factory, \
             patch.object(launcher.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            factory.return_value.__aenter__.return_value = client
            await launcher.wait_for_api("http://api/")
            self.assertEqual(client.get.await_count, 3)
            self.assertEqual(sleep.await_count, 2)

    async def test_api_readiness_is_bounded(self):
        with self.assertRaisesRegex(RuntimeError, "API did not become ready"):
            await launcher.wait_for_api("http://not-contacted/", timeout=0)

    async def test_image_check_happens_after_api_and_before_controller(self):
        os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
        from chessism_api.operations.cloud_analysis import __main__ as daemon
        from cloud_job import launch
        calls = []
        async def api(*args):
            calls.append("api")
        def image(*args):
            calls.append("image")
        async def serve(*args):
            calls.append("controller")
        with patch.object(launcher, "wait_for_api", side_effect=api), \
             patch.object(launch, "inspect_worker", side_effect=image), \
             patch.object(daemon, "serve", side_effect=serve):
            await launcher.run(SimpleNamespace(api_url="http://api/", local_image="worker"))
        self.assertEqual(calls, ["api", "image", "controller"])

    async def test_shutdown_cancels_controller_and_runs_finally(self):
        entered, finished = asyncio.Event(), asyncio.Event()
        async def run(args):
            try:
                entered.set()
                await asyncio.Event().wait()
            finally:
                finished.set()
        with patch.object(launcher, "run", side_effect=run):
            task = asyncio.create_task(launcher.run_until_stopped(None))
            await entered.wait()
            task.cancel()
            await task
            self.assertTrue(finished.is_set())


if __name__ == "__main__":
    unittest.main()
