"""Pure presentation of maintained scored-position statistics."""
from typing import Any, Dict


SCORED_BUCKET_DEFINITIONS = [
    ("equal", "Equal", 1),
    ("small", "Small", 2),
    ("clear", "Clear", 3),
    ("decisive", "Decisive", 4),
    ("mate", "Mate", 5),
]


def _scored_bucket_payload(row: Any, key: str, label: str, order: int) -> Dict[str, Any]:
    positions = int(row.get(f"{key}_positions") or 0)
    appearances = int(row.get(f"{key}_appearances") or 0)
    abs_score_sum = float(row.get(f"{key}_abs_score_sum") or 0.0)
    return {
        "key": key,
        "label": label,
        "order": order,
        "positions": positions,
        "appearances": appearances,
        "avg_abs_score": round(abs_score_sum / positions, 2) if positions else 0.0
    }



def summary_payload(row: Any) -> Dict[str, Any]:
    analyzed_fens = int(row.get("analyzed_fens") or 0)
    wdl_positions = int(row.get("wdl_positions") or 0)
    score_sum = float(row.get("score_sum") or 0.0)
    abs_score_sum = float(row.get("abs_score_sum") or 0.0)

    return {
        "total_positions": int(row.get("total_positions") or 0),
        "analyzed_fens": analyzed_fens,
        "scored_positions": int(row.get("scored_positions") or 0),
        "nonzero_scored_fens": int(row.get("nonzero_scored_fens") or 0),
        "unscored_fens": int(row.get("unscored_fens") or 0),
        "buckets": [
            _scored_bucket_payload(row, key, label, order)
            for key, label, order in SCORED_BUCKET_DEFINITIONS
        ],
        "side_balance": {
            "white_better": int(row.get("white_better") or 0),
            "black_better": int(row.get("black_better") or 0),
            "balanced": int(row.get("balanced") or 0),
            "avg_score": round(score_sum / analyzed_fens, 2) if analyzed_fens else 0.0,
            "avg_abs_score": round(abs_score_sum / analyzed_fens, 2) if analyzed_fens else 0.0
        },
        "wdl": {
            "positions": wdl_positions,
            "avg_white_win": round(float(row.get("wdl_win_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0,
            "avg_draw": round(float(row.get("wdl_draw_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0,
            "avg_white_loss": round(float(row.get("wdl_loss_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0
        }
    }

