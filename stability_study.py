# stability_study.py
import asyncio
import statistics
from typing import List, Dict, Any

import constants
from chessism_api.database.engine import init_db, AsyncDBSession
from sqlalchemy import text

from stockfish_test import analyze_fens


async def _fetch_random_fens(limit: int) -> List[str]:
    async with AsyncDBSession() as session:
        result = await session.execute(
            text("SELECT fen FROM fen ORDER BY random() LIMIT :limit"),
            {"limit": limit}
        )
        return [row[0] for row in result.fetchall()]


def _extract_pv1(analysis: Any) -> Dict[str, Any]:
    if isinstance(analysis, list):
        return analysis[0] if analysis else {}
    if isinstance(analysis, dict):
        return analysis
    return {}

def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return float(ordered[f])
    return float(ordered[f] + (ordered[c] - ordered[f]) * (k - f))


async def run_stability_study(
    sample_size: int = 200,
    nodes_base: int = 500000,
    multipv: int = 1,
    batch_size: int = 50
) -> Dict[str, Any]:
    """
    Runs a quick stability study comparing nodes_base vs nodes_base*4.
    Returns summary stats for eval stability and PV1 move agreement.
    """
    await init_db(constants.CONN_STRING)
    fens = await _fetch_random_fens(sample_size)

    def batched(items: List[str], size: int) -> List[List[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    results_base = []
    results_high = []

    for chunk in batched(fens, batch_size):
        results_base.extend(analyze_fens(chunk, nodes_limit=nodes_base, multipv=multipv))
        results_high.extend(analyze_fens(chunk, nodes_limit=nodes_base * 4, multipv=multipv))

    diffs = []
    move_matches = 0
    total = 0

    for r_base, r_high in zip(results_base, results_high):
        if not r_base.get("is_valid") or not r_high.get("is_valid"):
            continue
        pv1_base = _extract_pv1(r_base.get("analysis"))
        pv1_high = _extract_pv1(r_high.get("analysis"))

        score_base = pv1_base.get("score")
        score_high = pv1_high.get("score")
        if score_base is None or score_high is None:
            continue

        diffs.append(abs(float(score_base) - float(score_high)))
        total += 1

        pv_base = pv1_base.get("pv", [])
        pv_high = pv1_high.get("pv", [])
        if pv_base and pv_high and pv_base[0] == pv_high[0]:
            move_matches += 1

    summary = {
        "sample_size": sample_size,
        "nodes_base": nodes_base,
        "nodes_high": nodes_base * 4,
        "evaluated": total,
        "move_agreement_rate": (move_matches / total) if total else 0.0,
        "mean_abs_cp_diff": statistics.mean(diffs) if diffs else 0.0,
        "median_abs_cp_diff": statistics.median(diffs) if diffs else 0.0,
        "p90_abs_cp_diff": _percentile(diffs, 90.0),
        "p95_abs_cp_diff": _percentile(diffs, 95.0),
        "p99_abs_cp_diff": _percentile(diffs, 99.0),
    }
    return summary


if __name__ == "__main__":
    print(asyncio.run(run_stability_study()))
