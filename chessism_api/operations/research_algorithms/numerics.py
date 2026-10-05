"""Stable float64 statistics and bounded sampling for the CPU reference."""

import numpy as np


class ColumnStats:
    def __init__(self, size):
        self.count = np.zeros(size, dtype=np.int64)
        self.mean = np.zeros(size)
        self.m2 = np.zeros(size)

    def add(self, block):
        for column in range(block.shape[1]):
            values = block[:, column]
            values = values[np.isfinite(values)]
            n = len(values)
            if not n:
                continue
            old = self.count[column]
            delta = values.mean() - self.mean[column]
            self.m2[column] += np.square(values - values.mean()).sum() + delta * delta * old * n / (old + n)
            self.mean[column] += delta * n / (old + n)
            self.count[column] += n


class Relationships:
    def __init__(self, columns, *, seed, sample_limit):
        self.columns = columns
        size = len(columns)
        self.count = 0
        self.mean = np.zeros(size)
        self.cross = np.zeros((size, size))
        self.minimum = np.full(size, np.inf)
        self.maximum = np.full(size, -np.inf)
        self.rng = np.random.default_rng(seed)
        self.sample_limit = sample_limit
        self.sample = []

    def add(self, block, keys):
        n = len(block)
        if not n:
            return
        old = self.count
        mean = block.mean(axis=0)
        centered = block - mean
        delta = mean - self.mean
        self.cross += centered.T @ centered + np.outer(delta, delta) * old * n / (old + n)
        self.mean += delta * n / (old + n)
        self.minimum = np.minimum(self.minimum, block.min(axis=0))
        self.maximum = np.maximum(self.maximum, block.max(axis=0))
        for index, (row, key) in enumerate(zip(block, keys)):
            seen = old + index + 1
            slot = len(self.sample) if len(self.sample) < self.sample_limit else int(self.rng.integers(0, seen))
            if slot < self.sample_limit:
                item = {"row_key": key, "values": row.tolist()}
                if slot == len(self.sample):
                    self.sample.append(item)
                else:
                    self.sample[slot] = item
        self.count += n

    def finish(self, *, scaling, x, y):
        if self.count < 2:
            raise ValueError("At least two usable rows are required. Check missing values and the selected scope.")
        if not np.isfinite(self.cross).all():
            raise ValueError("The numerical range is too large for a reliable float64 correlation.")
        variance = np.maximum(np.diag(self.cross), 0)
        std = np.sqrt(variance / self.count)
        denominator = np.sqrt(variance[:, None] * variance[None, :])
        correlation = np.full(self.cross.shape, np.nan)
        np.divide(self.cross, denominator, out=correlation, where=denominator > 0)
        correlations = [[float(np.clip(value, -1, 1)) if np.isfinite(value) else None for value in row] for row in correlation]
        points = []
        for item in self.sample:
            values = np.array(item["values"])
            if scaling == "standardize":
                values = np.divide(values - self.mean, std, out=np.zeros_like(values), where=std > 0)
            points.append({"row_key": item["row_key"], "x": float(values[self.columns.index(x)]), "y": float(values[self.columns.index(y)])})
        return {
            "columns": self.columns, "correlations": correlations,
            "summaries": [{"column": key, "count": self.count, "mean": float(self.mean[i]),
                           "std": float(std[i]), "min": float(self.minimum[i]), "max": float(self.maximum[i]),
                           "constant": bool(std[i] == 0)} for i, key in enumerate(self.columns)],
            "scatter": {"x": x, "y": y, "points": points, "sampled": self.count > len(points),
                        "units": "z-score" if scaling == "standardize" else "source values"},
        }


def prepare_block(block, keys, config, means):
    missing = ~np.isfinite(block)
    affected = missing.any(axis=1)
    if config["missing"] == "drop_rows":
        block = block[~affected]
        keys = [key for key, excluded in zip(keys, affected) if not excluded]
    else:
        block = np.where(missing, means, block)
    if config["rating_difference"]:
        columns = config["columns"]
        difference = block[:, columns.index("opponent_rating")] - block[:, columns.index("rating")]
        block = np.column_stack((block, difference))
    return block, keys, int(affected.sum())
