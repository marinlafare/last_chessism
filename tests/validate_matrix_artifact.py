"""Read-only integrity validator for completed Chessism matrix artifacts."""

from __future__ import annotations

import gzip
import hashlib
import json
import sys
from pathlib import Path

import numpy as np


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate(manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = manifest_path.parent
    assert manifest["format"] == "chessism_matrix"
    assert manifest["version"] == 2
    assert manifest["validation"]["status"] == "passed"
    for file_row in manifest["files"]:
        path = root / file_row["name"]
        assert path.stat().st_size == file_row["bytes"], path
        assert sha256(path) == file_row["sha256"], path
    with gzip.open(root / "row_keys.jsonl.gz", "rt", encoding="utf-8") as source:
        row_keys = sum(1 for _ in source)
    assert row_keys == manifest["row_count"]
    for role in ("features", "labels"):
        layout = manifest["columns"][role]
        if not layout:
            continue
        mask = np.load(root / f"{role}_missing.npy", mmap_mode="r", allow_pickle=False)
        assert mask.shape == (manifest["row_count"], len(layout))
        for dtype in {column["storage_dtype"] for column in layout}:
            columns = [column for column in layout if column["storage_dtype"] == dtype]
            matrix = np.load(root / f"{role}_{dtype}.npy", mmap_mode="r", allow_pickle=False)
            assert matrix.shape == (manifest["row_count"], len(columns))
            assert str(matrix.dtype) == dtype
    return {
        "artifact_id": manifest["artifact_id"],
        "rows": manifest["row_count"],
        "features": manifest["feature_count"],
        "labels": manifest["label_count"],
        "files": len(manifest["files"]),
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Pass at least one manifest path.")
    print(json.dumps([validate(Path(value)) for value in sys.argv[1:]], indent=2))
