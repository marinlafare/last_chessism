import gzip
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from chessism_api.operations.matrix_constructor.arrays import TypedArrayWriter, validate_typed_arrays
from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.artifact_files import copy_snapshot, inspect_snapshot, sha256
from chessism_api.operations.matrix_constructor.storage import relative_manifest_path
from chessism_api.operations.matrix_backups import backup_completed_matrices, validate_matrix_bundle
from chessism_api.operations import database_backups as backup
from chessism_api.routers import research_matrices


def fixture_snapshot(root: Path, artifact_id: str) -> dict:
    folder = root / artifact_id
    folder.mkdir(parents=True)
    writer = TypedArrayWriter(folder, ROW_TYPES["game_player"], {
        "feature_columns": ["moves", "result"], "label_columns": [],
    }, 2)
    writer.write([{"moves": 20, "result": 1.0}, {"moves": 30, "result": 0.5}])
    profiles = writer.finish()
    writer.close()
    with gzip.open(folder / "row_keys.jsonl.gz", "wt") as target:
        target.write('"10:white"\n"11:black"\n')
    with gzip.open(folder / "dictionaries.json.gz", "wt") as target:
        target.write("{}")
    manifest = {
        "format": "chessism_matrix", "version": 2, "artifact_id": artifact_id,
        "row_count": 2, "feature_count": 2, "label_count": 0,
        "columns": writer.layouts, "profiles": profiles,
        "validation": validate_typed_arrays(folder, rows=2, layouts=writer.layouts),
        "files": [{"name": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
                  for path in sorted(folder.iterdir())],
    }
    (folder / "manifest.json").write_text(json.dumps(manifest))
    return manifest


class MatrixFileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source, self.destination = self.root / "working", self.root / "backup"
        self.artifact_id = str(uuid.uuid4())
        fixture_snapshot(self.source, self.artifact_id)

    def copy(self, **kwargs):
        return copy_snapshot(self.source, self.destination, self.artifact_id, **kwargs)

    def test_copy_is_verified_and_repeat_backup_writes_nothing(self):
        first = self.copy()
        paths = sorted((self.destination / self.artifact_id).iterdir())
        before = [(path.stat().st_ino, path.stat().st_mtime_ns) for path in paths]
        self.assertTrue(first["copied"])
        second = self.copy(capacity_check=MagicMock(side_effect=AssertionError("must not allocate")))
        self.assertFalse(second["copied"])
        self.assertEqual(before, [(path.stat().st_ino, path.stat().st_mtime_ns) for path in paths])
        self.assertEqual(first["manifest_sha256"], inspect_snapshot(self.destination, self.artifact_id, verify=True)["manifest_sha256"])

    def test_corrupt_source_cleans_its_partial_copy(self):
        path = self.source / self.artifact_id / "features_int32.npy"
        data = bytearray(path.read_bytes()); data[-1] ^= 1; path.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.copy()
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_corrupt_existing_backup_is_found_by_restore_test(self):
        result = self.copy()
        path = self.destination / self.artifact_id / "features_int32.npy"
        data = bytearray(path.read_bytes()); data[-1] ^= 1; path.write_bytes(data)
        bundle = {"schema_version": 1, "count": 1, "snapshots": [result]}
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            validate_matrix_bundle(bundle, self.destination, [self.artifact_id])

    def test_existing_mismatched_manifest_is_never_overwritten(self):
        self.copy()
        path = self.destination / self.artifact_id / "manifest.json"
        changed = json.loads(path.read_text()); changed["extra"] = "different snapshot"
        path.write_text(json.dumps(changed))
        with self.assertRaisesRegex(ValueError, "overwrite"):
            self.copy()
        self.assertEqual(json.loads(path.read_text())["extra"], "different snapshot")

    def test_capacity_failure_cleans_only_new_partial(self):
        other_id = str(uuid.uuid4())
        fixture_snapshot(self.destination, other_id)
        checks = MagicMock(side_effect=[None, RuntimeError("full")])
        with self.assertRaisesRegex(RuntimeError, "full"):
            self.copy(capacity_check=checks)
        self.assertEqual([path.name for path in self.destination.iterdir()], [other_id])

    def test_same_root_and_bad_uuid_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "different directories"):
            copy_snapshot(self.source, self.source, self.artifact_id)
        with self.assertRaises(ValueError):
            relative_manifest_path("../../elsewhere")

    def test_symlink_artifact_or_file_is_rejected(self):
        self.destination.mkdir()
        (self.destination / self.artifact_id).symlink_to(self.source / self.artifact_id, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            self.copy()
        path = self.source / self.artifact_id / "features_int32.npy"
        path.rename(self.root / "other.npy"); path.symlink_to(self.root / "other.npy")
        with self.assertRaisesRegex(ValueError, "changed matrix file"):
            inspect_snapshot(self.source, self.artifact_id)

    def test_manifest_path_traversal_is_rejected(self):
        path = self.source / self.artifact_id / "manifest.json"
        payload = json.loads(path.read_text()); payload["files"][0]["name"] = "../secret"
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            self.copy()

    def test_restore_bundle_matches_database_and_exact_hash(self):
        result = self.copy()
        bundle = {"schema_version": 1, "count": 1, "snapshots": [result]}
        self.assertEqual(validate_matrix_bundle(bundle, self.destination, [self.artifact_id])["status"], "passed")
        with self.assertRaisesRegex(ValueError, "metadata"):
            validate_matrix_bundle(bundle, self.destination, [])
        result["manifest_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "Changed matrix"):
            validate_matrix_bundle(bundle, self.destination, [self.artifact_id])


class MatrixBackupTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_companion_backup_copies_new_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            working, research = root / "working", root / "research"
            ids = [str(uuid.uuid4()), str(uuid.uuid4())]
            for artifact_id in ids:
                fixture_snapshot(working, artifact_id)
            session = AsyncMock()
            result = MagicMock(); result.scalars.return_value = ids
            session.execute.return_value = result
            with (
                patch("chessism_api.operations.matrix_backups.ARTIFACT_ROOT", working),
                patch.object(backup, "RESEARCH_BACKUP_ROOT", research),
                patch.object(backup, "BACKUP_VOLUME_ROOT", root),
                patch.object(backup, "FREE_FLOOR_BYTES", 0),
                patch.object(backup, "STOP_MARGIN_BYTES", 0),
                patch.object(backup, "require_storage", return_value={"application_bytes": 0}),
            ):
                first = await backup_completed_matrices(session, publish_progress=AsyncMock())
                second = await backup_completed_matrices(session, publish_progress=AsyncMock())
            self.assertEqual(first["copied_count"], 2)
            self.assertEqual(second["copied_count"], 0)
            self.assertEqual(second["bytes_added"], 0)
            self.assertEqual(second["reused_count"], 2)
            self.assertEqual(validate_matrix_bundle(first, research / "matrices", ids)["count"], 2)

    async def test_deletion_removes_only_working_copy(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            working, archived = root / "working", root / "backup"
            artifact_id = str(uuid.uuid4())
            fixture_snapshot(working, artifact_id)
            copy_snapshot(working, archived, artifact_id)
            session = AsyncMock()
            session.get.return_value = SimpleNamespace(id=artifact_id, status="complete")
            factory = MagicMock(); factory.return_value.__aenter__.return_value = session
            with patch.object(research_matrices, "ARTIFACT_ROOT", working), patch.object(research_matrices, "AsyncDBSession", factory):
                await research_matrices._delete_working_artifact(artifact_id)
            self.assertFalse((working / artifact_id).exists())
            self.assertEqual(inspect_snapshot(archived, artifact_id, verify=True)["artifact_id"], artifact_id)
            session.delete.assert_awaited_once()

    async def test_manual_backup_manifest_includes_matrices(self):
        from chessism_api.operations import database_backup_job as job
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            matrices = {"schema_version": 1, "count": 0, "copied_count": 0, "reused_count": 0,
                        "bytes_added": 25, "snapshots": []}
            newest = {"backup_id": "testF", "type": "full", "database_bytes": 50,
                      "database_delta_bytes": 50, "repository_bytes": 10, "repository_delta_bytes": 10}
            calls = []
            async def save_matrices(*args, **kwargs):
                calls.append("matrices"); return matrices
            async def pg_backup(*args, **kwargs):
                calls.append("database"); self.assertEqual(kwargs["baseline_app_bytes"], 125)
            with (
                patch.object(job, "ensure_backup_reservation", AsyncMock()),
                patch.object(job, "release_backup", AsyncMock()),
                patch.object(job, "backup_completed_matrices", side_effect=save_matrices),
                patch.object(job, "definition_backup_manifest", AsyncMock(return_value={"count": 0, "storage": "postgresql"})),
                patch.object(backup, "require_storage", return_value={"application_bytes": 100}),
                patch.object(backup, "DATABASE_BACKUP_ROOT", root),
                patch.object(backup, "MANIFEST_DIRECTORY", root / "manifests"),
                patch.object(backup, "BACKUP_STATUS_PATH", root / "status.json"),
                patch.object(backup, "BACKUP_CATALOG_PATH", root / "catalog.json"),
                patch.object(backup, "PGBACKREST_GAP_PATH", root / "gap"),
                patch.object(backup, "_write_progress", AsyncMock()),
                patch.object(backup, "_pgbackrest_info", AsyncMock(return_value=[{"status": {"code": 0}}])),
                patch.object(backup, "_backup_rows", return_value=[]),
                patch.object(backup, "_catalog_rows", return_value=[newest]),
                patch.object(backup, "_run_command_capture", AsyncMock()),
                patch.object(backup, "_run_backup_command", side_effect=pg_backup),
                patch.object(backup, "record_application_usage", return_value=135),
            ):
                result = await job._run_database_backup_job({"job_id": "test", "redis": AsyncMock()}, AsyncMock())
            self.assertEqual(calls, ["matrices", "database"])
            self.assertEqual(result["matrices"], matrices)
            manifest = json.loads((root / "manifests" / "testF.json").read_text())
            self.assertEqual(manifest["schema_version"], 4)
            self.assertEqual(manifest["matrices"], matrices)
            self.assertEqual(manifest["matrix_definitions"], {"count": 0, "storage": "postgresql"})


if __name__ == "__main__":
    unittest.main()
