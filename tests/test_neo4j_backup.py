import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from chessism_api.database.neo4j import backup as graph
from chessism_api.database.neo4j.backup_controller import Controller
from chessism_api.operations.backup_files import atomic_write_json, sha256
from tests import test_database_snapshot as snapshot_tests


class GraphArchiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.key = "snapshot-20261030T000000-abcdef123456"

    def archives(self):
        directory = graph.archive_directory(self.root, self.key)
        directory.mkdir(parents=True)
        metadata = {"format": "neo4j-admin-dump-v1", "archives": {}}
        for name, path in zip(graph.DATABASES, graph.archive_files(self.root, self.key)):
            path.write_bytes(name.encode())
            metadata["archives"][name] = {"file": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
        return metadata

    def test_both_archives_and_checksums_required(self):
        metadata = self.archives()
        graph.verify_archives(self.root, self.key, metadata)
        graph.archive_files(self.root, self.key)[1].write_text("corrupt")
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            graph.verify_archives(self.root, self.key, metadata)
        metadata["archives"].pop("system")
        with self.assertRaisesRegex(RuntimeError, "both"):
            graph.verify_archives(self.root, self.key, metadata)

    def test_paths_reject_traversal_and_symlinks(self):
        with self.assertRaises(ValueError):
            graph.archive_directory(self.root, "../outside")
        directory = graph.archive_directory(self.root, self.key)
        directory.parent.mkdir()
        directory.symlink_to(self.root)
        with self.assertRaises(ValueError):
            graph.archive_files(self.root, self.key)

    async def test_legacy_is_explicitly_postgres_only_and_new_missing_graph_fails(self):
        result = await graph.test_companion({"backup_format_version": 3}, AsyncMock())
        self.assertEqual(result["status"], "not_included")
        with self.assertRaisesRegex(RuntimeError, "missing"):
            await graph.test_companion({"backup_format_version": 4}, AsyncMock())

    async def test_unavailable_controller_fails_closed(self):
        with patch.dict(os.environ, {"CHESSISM_NEO4J_BACKUP_RPC": str(self.root)}):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                await graph.request("dump", self.key)

    async def test_request_receives_result_and_removes_lease(self):
        (self.root / "heartbeat").touch()
        async def respond(_detail):
            path = next(self.root.glob("*.request.json"))
            token = path.name.split(".")[0]
            atomic_write_json(self.root / f"{token}.response.json", {"ok": True, "result": {"passed": True}})
        with patch.dict(os.environ, {"CHESSISM_NEO4J_BACKUP_RPC": str(self.root)}), \
                patch.object(graph.asyncio, "sleep", new=AsyncMock()):
            result = await graph.request("dump", self.key, progress=respond)
        self.assertTrue(result["passed"])
        self.assertEqual([p.name for p in self.root.iterdir()], ["heartbeat"])

    async def test_cancelled_request_removes_lease(self):
        (self.root / "heartbeat").touch()
        with patch.dict(os.environ, {"CHESSISM_NEO4J_BACKUP_RPC": str(self.root)}):
            with self.assertRaises(asyncio.CancelledError):
                await graph.request("dump", self.key, progress=AsyncMock(side_effect=asyncio.CancelledError))
        self.assertFalse(list(self.root.glob("*.lease")))


class GraphRetentionTests(unittest.IsolatedAsyncioTestCase):
    setUp = snapshot_tests.SnapshotTests.setUp
    write_snapshot = snapshot_tests.SnapshotTests.write_snapshot
    def companion(self, row, *, tested=True):
        from chessism_api.operations import database_backups as b
        directory = graph.archive_directory(b.DATABASE_BACKUP_ROOT, row["backup_id"])
        directory.mkdir(parents=True)
        row["neo4j"] = {"format": "neo4j-admin-dump-v1", "archives": {}}
        for name, path in zip(graph.DATABASES, graph.archive_files(b.DATABASE_BACKUP_ROOT, row["backup_id"])):
            path.write_bytes(b"test graph")
            row["neo4j"]["archives"][name] = {"file": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
        manifest = {**row, "restore_test_validation": {"neo4j": {"status": "success" if tested else "failed"}}}
        b._atomic_write_json(b.MANIFEST_DIRECTORY / f"{row['backup_id']}.json", manifest)
        return row

    async def test_graph_failure_prevents_retirement_even_if_postgres_passed(self):
        from chessism_api.operations import database_backups as b, database_snapshot_retention as r
        new = self.companion(self.write_snapshot(), tested=False)
        old = self.write_snapshot("snapshot-20261012T030000-abcdef123456")
        b._write_catalog([new, old], updated_at="now")
        with self.assertRaisesRegex(RuntimeError, "Neo4j companion"):
            await r.retire_superseded(self.backup_id, archive_mode="off")
        self.assertTrue(r.snapshot_path(old["backup_id"]).exists())

    async def test_only_retires_graph_after_both_pass(self):
        from chessism_api.operations import database_backups as b, database_snapshot_retention as r
        new = self.companion(self.write_snapshot())
        old = self.companion(self.write_snapshot("snapshot-20261012T030000-abcdef123456"))
        b._write_catalog([new, old], updated_at="now")
        await r.retire_superseded(self.backup_id, archive_mode="off")
        self.assertFalse(graph.archive_directory(b.DATABASE_BACKUP_ROOT, old["backup_id"]).exists())
        graph.verify_archives(b.DATABASE_BACKUP_ROOT, new["backup_id"], new["neo4j"])

    async def test_postgres_only_cannot_retire_graph_recovery_point(self):
        from chessism_api.operations import database_backups as b, database_snapshot_retention as r
        new = self.write_snapshot()
        old = self.companion(self.write_snapshot("snapshot-20261012T030000-abcdef123456"))
        b._write_catalog([new, old], updated_at="now")
        with self.assertRaisesRegex(RuntimeError, "cannot replace"):
            await r.retire_superseded(self.backup_id, archive_mode="off")


class ControllerSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        with patch.dict(os.environ, {"CHESSISM_COMPOSE_PROJECT": "isolated", "BACKUP_VOLUME_UUID": "test"}):
            self.controller = Controller(Mock())
        self.controller.rpc = Path(self.temp.name)
        self.controller.journal = self.controller.rpc / "active.json"
        self.controller.token = "a" * 32

    def test_only_exact_project_service_can_be_selected(self):
        self.controller.docker.containers.list.return_value = []
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            self.controller.live()
        self.controller.docker.containers.list.assert_called_once_with(all=True, filters={"label": [
            "com.docker.compose.project=isolated", "com.docker.compose.service=neo4j"]})

    def test_recovery_releases_children_before_restart(self):
        atomic_write_json(self.controller.journal, {"token": "a" * 32, "restart_live": True, "container_id": "source"})
        target = Mock(id="source", status="exited")
        events = []
        target.start.side_effect = lambda: events.append("restart")
        with patch.object(self.controller, "owned_cleanup", side_effect=lambda: events.append("cleanup")), \
                patch.object(self.controller, "live", return_value=target), \
                patch.object(self.controller, "wait_ready"):
            self.controller.recover()
        self.assertEqual(events, ["cleanup", "restart"])
        self.assertFalse(self.controller.journal.exists())

    def test_recovery_refuses_replaced_live_container(self):
        atomic_write_json(self.controller.journal, {"token": "a" * 32, "restart_live": True, "container_id": "source"})
        target = Mock(id="different", status="exited")
        with patch.object(self.controller, "owned_cleanup"), patch.object(self.controller, "live", return_value=target):
            with self.assertRaisesRegex(RuntimeError, "changed"):
                self.controller.recover()
        target.start.assert_not_called()
        self.assertTrue(self.controller.journal.exists())


if __name__ == "__main__":
    unittest.main()
