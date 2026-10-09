"""Bounded NumPy encoding, stable column statistics, and artifact contracts."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np


def array_layout(keys, *, role, row_type):
    groups: dict[str, list[str]] = {}
    columns = row_type.columns_by_key
    for key in keys:
        groups.setdefault(columns[key].numpy_dtype, []).append(key)
    layout = [{
        "key": key, "role": role, "data_type": columns[key].data_type,
        "encoding": "dictionary" if columns[key].data_type == "category" else "numeric",
        "storage_dtype": columns[key].numpy_dtype,
        "array_file": f"{role}_{columns[key].numpy_dtype}.npy",
        "array_column": groups[columns[key].numpy_dtype].index(key),
        "missing_mask_file": f"{role}_missing.npy", "missing_mask_column": index,
    } for index, key in enumerate(keys)]
    return groups, layout


class TypedArrayWriter:
    """Encode one bounded block at a time; profiles describe stored values."""

    def __init__(self, root: Path, row_type, config: dict, rows: int):
        self.rows = rows
        self.processed = 0
        self.columns = row_type.columns_by_key
        self.layouts = {}
        self.arrays = {}
        self.masks = {}
        self.profiles = {}
        self.dictionaries = {}
        self.targets = []
        for role, keys in (("features", config["feature_columns"]), ("labels", config["label_columns"])):
            groups, layout = array_layout(keys, role=role, row_type=row_type)
            self.layouts[role] = layout
            self.profiles[role] = {}
            if not keys:
                continue
            self.masks[role] = np.lib.format.open_memmap(
                root / f"{role}_missing.npy", mode="w+", dtype="uint8", shape=(rows, len(keys)),
            )
            for dtype, grouped_keys in groups.items():
                self.arrays[(role, dtype)] = np.lib.format.open_memmap(
                    root / f"{role}_{dtype}.npy", mode="w+", dtype=dtype, shape=(rows, len(grouped_keys)),
                )
            for item in layout:
                key = item["key"]
                column = self.columns[key]
                profile = {
                    "key": key, "data_type": column.data_type, "storage_dtype": column.numpy_dtype,
                    "missing_count": 0, "non_missing_count": 0,
                    "minimum": None, "maximum": None, "mean": 0.0, "m2": 0.0,
                }
                self.profiles[role][key] = profile
                dictionary = self.dictionaries.setdefault(key, {}) if column.data_type == "category" else None
                self.targets.append((item, column, dictionary, profile))

    def write(self, block: list[Any]) -> None:
        start, stop = self.processed, self.processed + len(block)
        if stop > self.rows:
            raise ValueError("Matrix query returned more rows than the snapshot estimate.")
        for item, column, dictionary, profile in self.targets:
            values = [row[item["key"]] for row in block]
            missing = np.fromiter((value is None for value in values), dtype=bool, count=len(values))
            dtype = np.dtype(column.numpy_dtype)
            if dictionary is not None:
                encoded = np.fromiter(
                    (dictionary.setdefault(str(value), len(dictionary)) if value is not None else 0 for value in values),
                    dtype=dtype, count=len(values),
                )
            elif dtype.kind in "iu":
                limits = np.iinfo(dtype)
                integers = []
                for value in values:
                    integer = 0 if value is None else int(value)
                    if value is not None and (integer != value or not limits.min <= integer <= limits.max):
                        raise ValueError(f"{column.key} cannot be represented exactly as {dtype}: {value}")
                    integers.append(integer)
                encoded = np.asarray(integers, dtype=dtype)
            else:
                # Casting may overflow float32 even when the source float64 is
                # finite. Such values are explicitly missing, never silent inf.
                with np.errstate(over="ignore", invalid="ignore"):
                    encoded = np.asarray([0 if value is None else value for value in values], dtype=dtype)
                missing |= ~np.isfinite(encoded)
                encoded[missing] = 0
            self.arrays[(item["role"], column.numpy_dtype)][start:stop, item["array_column"]] = encoded
            self.masks[item["role"]][start:stop, item["missing_mask_column"]] = missing
            profile["missing_count"] += int(missing.sum())
            count = len(values) - int(missing.sum())
            previous = profile["non_missing_count"]
            profile["non_missing_count"] += count
            if count and dictionary is None:
                valid = encoded[~missing].astype(np.float64)
                batch_mean = float(valid.mean())
                delta = batch_mean - profile["mean"]
                total = previous + count
                profile["m2"] += float(np.square(valid - batch_mean).sum()) + delta * delta * previous * count / total
                profile["mean"] += delta * count / total
                minimum, maximum = valid.min().item(), valid.max().item()
                profile["minimum"] = minimum if previous == 0 else min(profile["minimum"], minimum)
                profile["maximum"] = maximum if previous == 0 else max(profile["maximum"], maximum)
        self.processed = stop

    def finish(self) -> dict:
        if self.processed != self.rows:
            raise ValueError(f"Snapshot expected {self.rows} rows but wrote {self.processed}.")
        profiles = {}
        for role, entries in self.profiles.items():
            profiles[role] = []
            for key, working in entries.items():
                profile = dict(working)
                m2 = profile.pop("m2")
                count = profile["non_missing_count"]
                profile["missing_fraction"] = profile["missing_count"] / self.rows
                if profile["data_type"] == "category":
                    profile.pop("mean")
                    profile["category_count"] = len(self.dictionaries[key])
                elif count:
                    profile["standard_deviation"] = math.sqrt(max(0.0, m2 / count))
                else:
                    profile.pop("mean")
                profiles[role].append(profile)
        for array in [*self.arrays.values(), *self.masks.values()]:
            array.flush()
        return profiles

    def close(self) -> None:
        for array in [*self.arrays.values(), *self.masks.values()]:
            array._mmap.close()
        self.arrays.clear()
        self.masks.clear()


def validate_typed_arrays(root: Path, *, rows: int, layouts: dict) -> dict:
    validated = set()
    for role, layout in layouts.items():
        if not layout:
            continue
        files = [(f"{role}_missing.npy", "uint8", len(layout))]
        for dtype in sorted({item["storage_dtype"] for item in layout}):
            files.append((f"{role}_{dtype}.npy", dtype, sum(item["storage_dtype"] == dtype for item in layout)))
        for filename, dtype, columns in files:
            array = np.load(root / filename, mmap_mode="r", allow_pickle=False)
            try:
                if array.shape != (rows, columns) or str(array.dtype) != dtype:
                    raise RuntimeError(f"Invalid array contract: {filename}")
            finally:
                array._mmap.close()
            validated.add(filename)
    return {"status": "passed", "row_count": rows, "validated_array_files": sorted(validated)}
