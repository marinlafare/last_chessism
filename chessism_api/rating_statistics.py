"""Weighted rating partitions shared by rating and scored-position summaries."""
from typing import List, Tuple


def _weighted_sse(
    prefix_w: List[float],
    prefix_wx: List[float],
    prefix_wx2: List[float],
    start_idx: int,
    end_idx: int
) -> float:
    """
    Weighted sum of squared errors for 1-based inclusive range [start_idx, end_idx].
    """
    total_w = prefix_w[end_idx] - prefix_w[start_idx - 1]
    if total_w <= 0:
        return 0.0
    total_wx = prefix_wx[end_idx] - prefix_wx[start_idx - 1]
    total_wx2 = prefix_wx2[end_idx] - prefix_wx2[start_idx - 1]
    return float(total_wx2 - ((total_wx * total_wx) / total_w))



def weighted_jenks_breaks(values: List[int], weights: List[int], n_classes: int = 3) -> List[Tuple[int, int]]:
    """
    Weighted Jenks natural breaks for histogram data (unique sorted values + counts).
    Returns contiguous class ranges as [(min, max), ...] with n_classes entries.
    """
    if not values or not weights or len(values) != len(weights):
        return []

    n = len(values)
    k = max(1, int(n_classes))
    if n == 1:
        return [(values[0], values[0]) for _ in range(k)]

    if n < k:
        out = [(values[idx], values[idx]) for idx in range(n)]
        last = out[-1]
        while len(out) < k:
            out.append(last)
        return out

    prefix_w = [0.0] * (n + 1)
    prefix_wx = [0.0] * (n + 1)
    prefix_wx2 = [0.0] * (n + 1)

    for idx in range(1, n + 1):
        w = float(weights[idx - 1])
        v = float(values[idx - 1])
        prefix_w[idx] = prefix_w[idx - 1] + w
        prefix_wx[idx] = prefix_wx[idx - 1] + (w * v)
        prefix_wx2[idx] = prefix_wx2[idx - 1] + (w * v * v)

    inf = float("inf")
    dp = [[inf] * (n + 1) for _ in range(k + 1)]
    back = [[0] * (n + 1) for _ in range(k + 1)]

    for i in range(1, n + 1):
        dp[1][i] = _weighted_sse(prefix_w, prefix_wx, prefix_wx2, 1, i)

    for cls in range(2, k + 1):
        for i in range(cls, n + 1):
            best_cost = inf
            best_start = cls
            for start in range(cls, i + 1):
                cost = dp[cls - 1][start - 1] + _weighted_sse(prefix_w, prefix_wx, prefix_wx2, start, i)
                if cost < best_cost:
                    best_cost = cost
                    best_start = start
            dp[cls][i] = best_cost
            back[cls][i] = best_start

    ranges: List[Tuple[int, int]] = []
    end = n
    for cls in range(k, 0, -1):
        if cls == 1:
            start = 1
        else:
            start = back[cls][end]
            if start <= 0:
                start = cls
        ranges.append((values[start - 1], values[end - 1]))
        end = start - 1

    ranges.reverse()
    if len(ranges) < k:
        last = ranges[-1]
        while len(ranges) < k:
            ranges.append(last)
    return ranges

