import asyncio
from contextlib import ExitStack
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy.engine import URL

from chessism_api.operations import database_backups as b
from chessism_api.operations import database_snapshot as s
from chessism_api.operations import database_snapshot_retention as retention


class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        app = root / "chessism"
        db = app / "database"
        db.mkdir(parents=True)
        for key, value in {
            "APP_ID": "chessism", "BACKUP_VOLUME_ROOT": root, "APP_BACKUP_ROOT": app,
            "DATABASE_BACKUP_ROOT": db, "BACKUP_CATALOG_PATH": db / "catalog.json",
            "BACKUP_STATUS_PATH": app / "backup-status.json", "BACKUP_USAGE_PATH": app / "backup-usage.json",
            "MANIFEST_DIRECTORY": db / "manifests", "PGBACKREST_BACKUP_DIRECTORY": db / "pgbackrest" / "backup",
            "PGBACKREST_ARCHIVE_DIRECTORY": db / "pgbackrest" / "archive",
            "FEN_ANALYSIS_BACKUP_ROOT": app / "fen-analysis", "RESEARCH_BACKUP_ROOT": app / "research",
        }.items():
            self.stack.enter_context(patch.object(b, key, value))
        self.stack.enter_context(patch.object(b, "storage_snapshot", return_value={"marker_matches": True}))
        self.root = root
        self.backup_id = "snapshot-20261013T030000-abcdef123456"

    def write_snapshot(self, backup_id=None, tested=True):
        backup_id = backup_id or self.backup_id
        archive = s.snapshot_path(backup_id)
        archive.parent.mkdir(exist_ok=True)
        archive.write_bytes(b"fake archive for unit tests")
        row = {"backup_id": backup_id, "storage_mode": s.STORAGE_MODE, "type": "full",
               "repository_bytes": archive.stat().st_size, "archive_sha256": b._sha256(archive),
               "restore_tested": tested, "restore_test_status": "success" if tested else "failed"}
        b._atomic_write_json(b.MANIFEST_DIRECTORY / f"{backup_id}.json", row)
        return row

    def legacy(self):
        path = b.DATABASE_BACKUP_ROOT / "pgbackrest" / "archive"
        path.mkdir(parents=True)
        (path / "wal-test").write_bytes(b"history")
        row = {"backup_id": "20261012-035736F", "storage_mode": "block_incremental_v1"}
        b._atomic_write_json(b.MANIFEST_DIRECTORY / f"{row['backup_id']}.json", row)
        return row

    async def test_only_retires_after_success_and_preserves_other_data(self):
        new = self.write_snapshot()
        old = self.write_snapshot("snapshot-20261012T030000-abcdef123456")
        legacy = self.legacy()
        b._write_catalog([new, old, legacy], updated_at="now")
        sentinels = [self.root / "timeshift" / "keep", self.root / "another-app" / "keep",
                     b.FEN_ANALYSIS_BACKUP_ROOT / "keep", b.RESEARCH_BACKUP_ROOT / "keep"]
        for path in sentinels:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("must remain")
        result = await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertEqual(result["state"], "complete")
        self.assertTrue(s.snapshot_path(self.backup_id).exists())
        self.assertFalse(s.snapshot_path(old["backup_id"]).exists())
        self.assertFalse((b.DATABASE_BACKUP_ROOT / "pgbackrest").exists())
        self.assertEqual(b._read_json(b.BACKUP_CATALOG_PATH, {})["backups"], [new])
        self.assertTrue(all(p.read_text() == "must remain" for p in sentinels))

    async def test_archive_on_preserves_legacy_chain_and_wal(self):
        new, legacy = self.write_snapshot(), self.legacy()
        b._write_catalog([new, legacy], updated_at="now")
        result = await retention.retire_superseded(self.backup_id, archive_mode="on")
        self.assertTrue(result["legacy_retained"])
        self.assertTrue((b.DATABASE_BACKUP_ROOT / "pgbackrest" / "archive" / "wal-test").exists())
        self.assertEqual(len(b._read_json(b.BACKUP_CATALOG_PATH, {})["backups"]), 2)

    async def test_failed_test_prevents_all_cleanup(self):
        new, legacy = self.write_snapshot(tested=False), self.legacy()
        b._write_catalog([new, legacy], updated_at="now")
        with self.assertRaisesRegex(RuntimeError, "successfully restored"):
            await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertTrue((b.DATABASE_BACKUP_ROOT / "pgbackrest").exists())

    async def test_corrupt_replacement_prevents_cleanup(self):
        row = self.write_snapshot()
        b._write_catalog([row, self.legacy()], updated_at="now")
        s.snapshot_path(self.backup_id).write_bytes(b"corrupt")
        with self.assertRaisesRegex(RuntimeError, "integrity"):
            await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertTrue((b.DATABASE_BACKUP_ROOT / "pgbackrest").exists())

    async def test_partial_cleanup_can_resume_from_its_journal(self):
        new, old = self.write_snapshot(), self.legacy()
        b._write_catalog([new, old], updated_at="now")
        with patch.object(retention.shutil, "rmtree", side_effect=OSError("test interruption")):
            with self.assertRaises(OSError):
                await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertEqual(b._read_json(b.BACKUP_CATALOG_PATH, {})["backups"], [new])
        result = await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertIn(old["backup_id"], result["removed_backup_ids"])
        self.assertFalse((b.DATABASE_BACKUP_ROOT / "pgbackrest").exists())

    async def test_symlink_repository_is_never_removed(self):
        new = self.write_snapshot()
        outside = self.root / "timeshift"
        outside.mkdir()
        (b.DATABASE_BACKUP_ROOT / "pgbackrest").symlink_to(outside, target_is_directory=True)
        b._write_catalog([new], updated_at="now")
        with self.assertRaisesRegex(RuntimeError, "exact Chessism"):
            await retention.retire_superseded(self.backup_id, archive_mode="off")
        self.assertTrue(outside.exists())

    def test_paths_reject_traversal_and_symlink(self):
        for key in ("../database", "", "/tmp/anything", "snapshot-20261013T030000-../foo"):
            with self.assertRaises(ValueError):
                s.snapshot_path(key)
        (b.DATABASE_BACKUP_ROOT / "snapshots").symlink_to(self.root)
        with self.assertRaises(ValueError):
            s.snapshot_path(self.backup_id)

    def test_snapshot_sizes_are_in_database_component(self):
        row = self.write_snapshot()
        measured = b._measure_application_usage()
        self.assertEqual(measured["database_backup_bytes"], row["repository_bytes"])

    def test_secret_is_not_in_cli_arguments_or_inherited_pg_environment(self):
        url = URL.create("postgresql+asyncpg", username="u", password="secret", host="db", database="data")
        with patch.dict(os.environ, {"PGOPTIONS": "unsafe", "PGSERVICE": "elsewhere"}):
            env = s.pg_environment(url)
        self.assertEqual(env["PGPASSWORD"], "secret")
        self.assertEqual(env["PGHOST"], "db")
        self.assertNotIn("PGOPTIONS", env)
        self.assertNotIn("PGSERVICE", env)

    async def test_subprocess_failure_and_capacity_failure_propagate(self):
        with self.assertRaisesRegex(RuntimeError, "failed"):
            await s.run_progress_command("/bin/sh", "-c", "exit 3", progress=AsyncMock())
        def no_space():
            raise b.BackupCapacityError("out of space")
        with self.assertRaises(b.BackupCapacityError):
            await s.run_progress_command("/bin/sleep", "20", progress=AsyncMock(), capacity_check=no_space)


if __name__ == "__main__":
    unittest.main()
