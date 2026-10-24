"""Small read-only DataFrame-style pages from saved NumPy arrays, never SQL.

The constructor writes 2D arrays. A 3D array is viewed as [slice, row, column]
with matching 3D missing masks; this reader does not create tensor snapshots.
Category values deliberately remain encoded, avoiding a full dictionary load.
"""

from contextlib import ExitStack
import math
from pathlib import Path

import numpy as np

from .artifact_files import artifact_directory, inspect_snapshot


MAX_PREVIEW_ROWS = 100
MAX_PREVIEW_COLUMNS = 64
MAX_SAFE_JSON_INTEGER = 2 ** 53 - 1


class PreviewUnavailable(ValueError):
    """A snapshot is missing, unsafe, or inconsistent with its manifest."""


def read_matrix_preview(root: Path, artifact_id: str, *, offset: int = 0,
                        limit: int = 50, role: str = "all", slice_index: int = 0) -> dict:
    if offset < 0 or not 1 <= limit <= MAX_PREVIEW_ROWS or slice_index < 0:
        raise ValueError("Invalid preview row range or slice.")
    if role not in {"all", "features", "labels"}:
        raise ValueError("Choose all columns, features, or labels.")
    try:
        folder = artifact_directory(root, artifact_id)
        if (folder / "manifest.json").stat().st_size > 1024 * 1024:
            raise PreviewUnavailable("Snapshot manifest is too large to preview safely.")
        try:
            manifest = inspect_snapshot(root, artifact_id)["manifest"]
        except ValueError as error:
            raise PreviewUnavailable("Snapshot files do not match their saved manifest.") from error
        columns = []
        for group in ("features", "labels"):
            if role in {"all", group}:
                columns.extend({**item, "role": group} for item in manifest["columns"][group])
        if len(columns) > MAX_PREVIEW_COLUMNS:
            raise PreviewUnavailable("Snapshot has too many columns to preview safely.")
        return _read_page(folder, manifest, columns, offset, limit, slice_index)
    except (OSError, KeyError, TypeError, IndexError, OverflowError) as error:
        raise PreviewUnavailable("Working snapshot files are missing or invalid.") from error


def _read_page(folder, manifest, columns, offset, limit, slice_index):
    arrays = {}
    with ExitStack() as handles:
        def open_array(filename):
            if filename not in arrays:
                try:
                    array = np.load(folder / filename, mmap_mode="r", allow_pickle=False)
                except (ValueError, EOFError) as error:
                    raise PreviewUnavailable("Snapshot contains an invalid numeric array.") from error
                if not isinstance(array, np.memmap):
                    if hasattr(array, "close"):
                        array.close()
                    raise PreviewUnavailable("Only memory-mapped NumPy arrays can be previewed.")
                handles.callback(array._mmap.close)
                if array.ndim not in {2, 3} or array.dtype.kind not in "biuf":
                    raise PreviewUnavailable("Preview supports numeric 2D or 3D arrays only.")
                arrays[filename] = array
            return arrays[filename]

        prefix = None
        sources = []
        for column in columns:
            array = open_array(column["array_file"])
            mask = open_array(column["missing_mask_file"])
            if prefix is None:
                prefix = array.shape[:-1]
            value_index, mask_index = column["array_column"], column["missing_mask_column"]
            if (array.shape[:-1] != prefix or mask.shape[:-1] != prefix
                    or str(array.dtype) != column["storage_dtype"] or mask.dtype != np.uint8
                    or not 0 <= value_index < array.shape[-1]
                    or not 0 <= mask_index < mask.shape[-1]):
                raise PreviewUnavailable("Snapshot array dimensions do not match its column layout.")
            sources.append((array, mask, value_index, mask_index))

        prefix = prefix or (int(manifest["row_count"]),)
        total_rows, dimensions = prefix[-1], len(prefix) + 1
        slice_count = prefix[0] if dimensions == 3 else 1
        if total_rows != manifest["row_count"]:
            raise PreviewUnavailable("Snapshot row count does not match its manifest.")
        if slice_index >= slice_count:
            raise ValueError("Slice index is outside this snapshot.")
        stop = min(total_rows, offset + limit)
        values = [[] for _ in range(offset, stop)]
        for array, mask, value_index, mask_index in sources:
            plane = array[slice_index] if dimensions == 3 else array
            missing_plane = mask[slice_index] if dimensions == 3 else mask
            page = plane[offset:stop, value_index]
            missing = missing_plane[offset:stop, mask_index]
            for row, value, absent in zip(values, page, missing):
                if int(absent) not in {0, 1}:
                    raise PreviewUnavailable("Snapshot has an invalid missing-value mask.")
                scalar = value.item()
                if absent or (isinstance(scalar, float) and not math.isfinite(scalar)):
                    scalar = None
                elif isinstance(scalar, int) and abs(scalar) > MAX_SAFE_JSON_INTEGER:
                    scalar = str(scalar)  # Preserve int64 precision in the browser.
                row.append(scalar)

        return {
            "artifact_id": manifest["artifact_id"], "dimensions": dimensions,
            "shape": [*prefix, len(columns)], "slice_index": slice_index,
            "slice_count": slice_count, "offset": offset, "limit": limit,
            "total_rows": total_rows, "has_more": stop < total_rows,
            "columns": [{key: column[key] for key in ("key", "role", "storage_dtype", "encoding")}
                        for column in columns],
            "rows": values, "category_values": "stored_dictionary_codes",
        }
