import gzip
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from fastapi import FastAPI
import httpx
import numpy as np

from chessism_api.operations.matrix_constructor.arrays import TypedArrayWriter, validate_typed_arrays
from chessism_api.operations.matrix_constructor.artifact_files import sha256
from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.preview import PreviewUnavailable, read_matrix_preview
from chessism_api.routers import research_matrices


def update_manifest(folder, manifest):
    manifest["files"] = [
        {"name": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
        for path in sorted(folder.iterdir()) if path.name != "manifest.json"
    ]
    (folder / "manifest.json").write_text(json.dumps(manifest))


def create_fixture(root, artifact_id):
    folder = root / artifact_id
    folder.mkdir()
    writer = TypedArrayWriter(folder, ROW_TYPES["game_player"], {
        "feature_columns": ["moves", "elapsed_seconds", "mode", "started_at"],
        "label_columns": ["result"],
    }, 3)
    writer.write([
        {"moves": 20, "elapsed_seconds": 12.5, "mode": "bullet", "started_at": 2 ** 53 + 1, "result": 1},
        {"moves": 30, "elapsed_seconds": None, "mode": "rapid", "started_at": 0, "result": 0},
        {"moves": 40, "elapsed_seconds": 30.25, "mode": None, "started_at": None, "result": 0.5},
    ])
    profiles = writer.finish()
    writer.close()
    with gzip.open(folder / "row_keys.jsonl.gz", "wt") as output:
        output.write('"a"\n"b"\n"c"\n')
    with gzip.open(folder / "dictionaries.json.gz", "wt") as output:
        output.write('{"mode":["bullet","rapid"]}')
    manifest = {
        "format": "chessism_matrix", "version": 2, "artifact_id": artifact_id,
        "row_count": 3, "columns": writer.layouts, "profiles": profiles,
        "validation": validate_typed_arrays(folder, rows=3, layouts=writer.layouts),
    }
    update_manifest(folder, manifest)
    return folder, manifest


class MatrixPreviewTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.artifact_id = str(uuid.uuid4())
        self.folder, self.manifest = create_fixture(self.root, self.artifact_id)

    def preview(self, **kwargs):
        return read_matrix_preview(self.root, self.artifact_id, **kwargs)

    def test_pages_keep_original_column_order_nulls_and_exact_integers(self):
        page = self.preview(limit=2)
        self.assertEqual(page["shape"], [3, 5])
        self.assertEqual([c["key"] for c in page["columns"]], ["moves", "elapsed_seconds", "mode", "started_at", "result"])
        self.assertEqual(page["rows"], [[20, 12.5, 0, str(2 ** 53 + 1), 1.0], [30, None, 1, 0, 0.0]])
        self.assertTrue(page["has_more"])
        last = self.preview(offset=2, limit=2)
        self.assertEqual(last["rows"], [[40, 30.25, None, None, 0.5]])
        self.assertFalse(last["has_more"])
        json.dumps(page, allow_nan=False)

    def test_feature_and_label_selection(self):
        self.assertEqual(self.preview(role="features")["shape"], [3, 4])
        self.assertEqual(self.preview(role="labels")["rows"], [[1.0], [0.0], [0.5]])
        self.assertEqual(self.preview(offset=20)["rows"], [])

    def test_bounded_requests_and_2d_slice_validation(self):
        for invalid in ({"limit": 101}, {"limit": 0}, {"offset": -1}, {"role": "secrets"}, {"slice_index": -1}, {"slice_index": 1}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.preview(**invalid)

    def test_3d_slice_is_a_2d_dataframe_page(self):
        for path in self.folder.glob("*.npy"):
            array = np.load(path, allow_pickle=False)
            second = array.copy()
            if path.name == "features_int32.npy":
                second[:, 0] += 100
            np.save(path, np.stack((array, second)), allow_pickle=False)
        update_manifest(self.folder, self.manifest)
        page = self.preview(slice_index=1, limit=1)
        self.assertEqual(page["dimensions"], 3)
        self.assertEqual(page["shape"], [2, 3, 5])
        self.assertEqual(page["slice_count"], 2)
        self.assertEqual(page["rows"][0][0], 120)
        with self.assertRaisesRegex(ValueError, "Slice index"):
            self.preview(slice_index=2)

    def test_shape_and_dtype_mismatch_are_rejected(self):
        np.save(self.folder / "features_int32.npy", np.zeros((4, 2), dtype="int32"))
        update_manifest(self.folder, self.manifest)
        with self.assertRaises(PreviewUnavailable):
            self.preview()

    def test_invalid_missing_mask_is_rejected(self):
        path = self.folder / "features_missing.npy"
        mask = np.load(path); mask[0, 0] = 2
        np.save(path, mask)
        update_manifest(self.folder, self.manifest)
        with self.assertRaisesRegex(PreviewUnavailable, "missing-value mask"):
            self.preview()

    def test_preview_does_not_unpickle_or_follow_symlinks(self):
        path = self.folder / "features_int32.npy"
        np.save(path, np.array([[object()]], dtype=object))
        update_manifest(self.folder, self.manifest)
        with self.assertRaises(PreviewUnavailable):
            self.preview()
        path.unlink()
        path.symlink_to(self.folder / "features_float32.npy")
        with self.assertRaises(PreviewUnavailable):
            self.preview()

    def test_preview_is_read_only_and_uses_memory_mapping(self):
        before = [(path.name, path.stat().st_mtime_ns, sha256(path)) for path in sorted(self.folder.iterdir())]
        with patch('chessism_api.operations.matrix_constructor.preview.np.load', wraps=np.load) as load:
            self.preview(offset=2, limit=1)
            self.assertTrue(load.called)
            for call in load.call_args_list:
                self.assertEqual(call.kwargs, {"mmap_mode": "r", "allow_pickle": False})
        after = [(path.name, path.stat().st_mtime_ns, sha256(path)) for path in sorted(self.folder.iterdir())]
        self.assertEqual(before, after)


class MatrixPreviewEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_success_missing_active_and_query_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact_id = str(uuid.uuid4())
            create_fixture(root, artifact_id)
            app = FastAPI()
            app.include_router(research_matrices.router, prefix="/matrices")
            session = AsyncMock()
            session.get.return_value = SimpleNamespace(status="complete")
            factory = MagicMock(); factory.return_value.__aenter__.return_value = session
            with patch.object(research_matrices, "AsyncDBSession", factory), patch.object(research_matrices, "ARTIFACT_ROOT", root):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                    url = f"/matrices/{artifact_id}/preview"
                    response = await client.get(url, params={"limit": 2})
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(len(response.json()["rows"]), 2)
                    self.assertEqual((await client.get(url, params={"limit": 101})).status_code, 422)
                    self.assertEqual((await client.get(url, params={"slice_index": 1})).status_code, 422)
                    self.assertEqual((await client.get('/matrices/not-a-uuid/preview')).status_code, 422)
                    session.get.return_value = None
                    self.assertEqual((await client.get(url)).status_code, 404)
                    session.get.return_value = SimpleNamespace(status="running")
                    self.assertEqual((await client.get(url)).status_code, 409)
                    session.get.return_value = SimpleNamespace(status="complete")
                    (root / artifact_id / "features_int32.npy").unlink()
                    self.assertEqual((await client.get(url)).status_code, 409)


if __name__ == "__main__":
    unittest.main()
