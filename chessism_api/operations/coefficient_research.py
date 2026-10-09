"""Reproducible research for calibrating CP-to-outcome sigmoid coefficients."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select, text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CoefficientResearchExperiment


LICHESS_COEFFICIENT = 0.00368208
RESEARCH_PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
RESEARCH_ARTIFACT_DIR = Path(
    os.environ.get(
        "COEFFICIENT_RESEARCH_DIR",
        os.path.join(os.environ.get("FEN_ANALYSIS_BACKUP_DIR", "/tmp"), "research"),
    )
)
RESEARCH_ARTIFACT_DISPLAY_DIR = Path(
    os.environ.get("COEFFICIENT_RESEARCH_DISPLAY_DIR", str(RESEARCH_ARTIFACT_DIR))
)
RATING_BINS = (
    {"key": "under-1200", "label": "<1200", "lower": None, "upper": 1199},
    {"key": "1200-1399", "label": "1200–1399", "lower": 1200, "upper": 1399},
    {"key": "1400-1599", "label": "1400–1599", "lower": 1400, "upper": 1599},
    {"key": "1600-1799", "label": "1600–1799", "lower": 1600, "upper": 1799},
    {"key": "1800-1999", "label": "1800–1999", "lower": 1800, "upper": 1999},
    {"key": "2000-2199", "label": "2000–2199", "lower": 2000, "upper": 2199},
    {"key": "2200-2399", "label": "2200–2399", "lower": 2200, "upper": 2399},
    {"key": "2400-2599", "label": "2400–2599", "lower": 2400, "upper": 2599},
    {"key": "2600-plus", "label": "2600+", "lower": 2600, "upper": None},
)
BIN_BY_KEY = {item["key"]: item for item in RATING_BINS}
RATING_BIN_SQL = """
CASE
    WHEN game.avg_elo < 1200 THEN 'under-1200'
    WHEN game.avg_elo < 1400 THEN '1200-1399'
    WHEN game.avg_elo < 1600 THEN '1400-1599'
    WHEN game.avg_elo < 1800 THEN '1600-1799'
    WHEN game.avg_elo < 2000 THEN '1800-1999'
    WHEN game.avg_elo < 2200 THEN '2000-2199'
    WHEN game.avg_elo < 2400 THEN '2200-2399'
    WHEN game.avg_elo < 2600 THEN '2400-2599'
    ELSE '2600-plus'
END
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalized_config(config: dict[str, Any] | None) -> dict[str, Any]:
    source = config or {}
    selected_modes = [
        mode for mode in ("bullet", "blitz", "rapid")
        if mode in {str(item).lower() for item in source.get("modes", [])}
    ]
    if not selected_modes:
        selected_modes = ["bullet", "blitz", "rapid"]
    return {
        "modes": selected_modes,
        "max_rating_gap": max(0, min(1000, int(source.get("max_rating_gap", 200)))),
        "opening_moves_excluded": max(
            0, min(40, int(source.get("opening_moves_excluded", 10)))
        ),
        "minimum_games_per_fit": max(
            50, min(100_000, int(source.get("minimum_games_per_fit", 500)))
        ),
        "cp_ceiling": max(100, min(2000, int(source.get("cp_ceiling", 1000)))),
        "split_seed": max(1, min(2_147_483_000, int(source.get("split_seed", 20260930)))),
        "rating_basis": "average_game_rating",
        "game_weighting": "equal_total_weight_per_game",
        "split": {"training": 70, "validation": 15, "test": 15},
    }


def _common_game_filters() -> str:
    return """
        coverage.is_fully_analyzed
        AND coverage.total_positions > 0
        AND game.avg_elo IS NOT NULL
        AND game.white_elo > 0
        AND game.black_elo > 0
        AND ABS(game.white_elo - game.black_elo) <= :max_rating_gap
        AND game.mode = ANY(CAST(:modes AS text[]))
        AND game.n_moves > :opening_moves_excluded
        AND LOWER(game.white_str_result) NOT IN ('timeout', 'abandoned', 'timevsinsufficient')
        AND LOWER(game.black_str_result) NOT IN ('timeout', 'abandoned', 'timevsinsufficient')
    """


def query_params(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "modes": list(config["modes"]),
        "max_rating_gap": int(config["max_rating_gap"]),
        "opening_moves_excluded": int(config["opening_moves_excluded"]),
        "cp_ceiling": int(config["cp_ceiling"]),
        "split_seed": int(config["split_seed"]),
        "fold_seed": int(config["split_seed"]) + 1,
    }


async def get_coefficient_research_overview(
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safe_config = normalized_config(config)
    sql = text(f"""
        SELECT
            {RATING_BIN_SQL} AS rating_bin,
            game.mode,
            COUNT(*)::bigint AS games,
            COALESCE(SUM(coverage.total_positions), 0)::bigint AS candidate_positions
        FROM game
        JOIN game_analysis_summary coverage ON coverage.link = game.link
        WHERE {_common_game_filters()}
        GROUP BY 1, 2
        ORDER BY MIN(game.avg_elo), game.mode
    """)
    async with AsyncDBSession() as session:
        result = await session.execute(sql, query_params(safe_config))
        rows = result.mappings().all()

    grouped = {
        item["key"]: {
            **item,
            "games": 0,
            "candidate_positions": 0,
            "modes": {
                mode: {"games": 0, "candidate_positions": 0}
                for mode in safe_config["modes"]
            },
        }
        for item in RATING_BINS
    }
    for row in rows:
        key = str(row["rating_bin"])
        mode = str(row["mode"])
        if key not in grouped or mode not in grouped[key]["modes"]:
            continue
        games = int(row["games"] or 0)
        positions = int(row["candidate_positions"] or 0)
        grouped[key]["games"] += games
        grouped[key]["candidate_positions"] += positions
        grouped[key]["modes"][mode] = {
            "games": games,
            "candidate_positions": positions,
        }

    bins = list(grouped.values())
    return {
        "lichess_coefficient": LICHESS_COEFFICIENT,
        "config": safe_config,
        "bins": bins,
        "totals": {
            "games": sum(item["games"] for item in bins),
            "candidate_positions": sum(item["candidate_positions"] for item in bins),
        },
        "position_note": (
            "Candidate positions are from fully analyzed games; the experiment excludes "
            "openings, mate values, and non-Stockfish rows before fitting."
        ),
    }


def expected_result(cp: float, coefficient: float) -> float:
    """Return the symmetric expected result in [-1, 1]."""
    return math.tanh(float(coefficient) * float(cp) / 2.0)


def weighted_error(
    observations: Iterable[dict[str, Any]],
    coefficient: float,
    *,
    metric: str = "mse",
) -> float | None:
    weighted_total = 0.0
    weight_total = 0.0
    for row in observations:
        weight = float(row["weight"])
        residual = float(row["outcome"]) - expected_result(row["cp"], coefficient)
        weighted_total += weight * (abs(residual) if metric == "mae" else residual * residual)
        weight_total += weight
    return weighted_total / weight_total if weight_total > 0 else None


def fit_coefficient(observations: list[dict[str, Any]]) -> float | None:
    """Minimize weighted MSE for the one-parameter sigmoid without SciPy."""
    if not observations or sum(float(row["weight"]) for row in observations) <= 0:
        return None
    lower = 0.0001
    upper = 0.0100
    ratio = (math.sqrt(5.0) - 1.0) / 2.0
    left = upper - ratio * (upper - lower)
    right = lower + ratio * (upper - lower)
    left_value = weighted_error(observations, left)
    right_value = weighted_error(observations, right)
    left_error = left_value if left_value is not None else math.inf
    right_error = right_value if right_value is not None else math.inf
    for _ in range(80):
        if left_error <= right_error:
            upper, right, right_error = right, left, left_error
            left = upper - ratio * (upper - lower)
            left_value = weighted_error(observations, left)
            left_error = left_value if left_value is not None else math.inf
        else:
            lower, left, left_error = left, right, right_error
            right = lower + ratio * (upper - lower)
            right_value = weighted_error(observations, right)
            right_error = right_value if right_value is not None else math.inf
    return (lower + upper) / 2.0


def approximate_confidence_interval(
    observations: list[dict[str, Any]],
    coefficient: float,
) -> list[float] | None:
    """Return a transparent curvature-based 95% interval, clustered only by weight."""
    game_weight = sum(float(row["weight"]) for row in observations)
    if game_weight <= 1:
        return None
    mse = weighted_error(observations, coefficient)
    if mse is None:
        return None
    information = 0.0
    for row in observations:
        cp = float(row["cp"])
        prediction = expected_result(cp, coefficient)
        derivative = (cp / 2.0) * (1.0 - prediction * prediction)
        information += float(row["weight"]) * derivative * derivative
    if information <= 0:
        return None
    standard_error = math.sqrt(mse / information)
    return [
        max(0.000001, coefficient - 1.96 * standard_error),
        coefficient + 1.96 * standard_error,
    ]


def _rounded(value: float | None, digits: int = 10) -> float | None:
    return round(float(value), digits) if value is not None and math.isfinite(value) else None


def _metric_payload(
    observations: list[dict[str, Any]],
    fitted: float,
) -> dict[str, float | None]:
    fitted_mse = weighted_error(observations, fitted)
    baseline_mse = weighted_error(observations, LICHESS_COEFFICIENT)
    improvement = None
    if baseline_mse and fitted_mse is not None:
        improvement = ((baseline_mse - fitted_mse) / baseline_mse) * 100.0
    return {
        "mse": _rounded(fitted_mse),
        "mae": _rounded(weighted_error(observations, fitted, metric="mae")),
        "lichess_mse": _rounded(baseline_mse),
        "improvement_percent": _rounded(improvement, 4),
    }


def _sample_payload(observations: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "games": int(round(sum(float(row["weight"]) for row in observations))),
        "positions": sum(int(row["positions"]) for row in observations),
    }


def build_fit_results(
    rows: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    scopes = [("all", {item["key"] for item in RATING_BINS})]
    scopes.extend((item["key"], {item["key"]}) for item in RATING_BINS)
    mode_scopes = ["all", *config["modes"]]

    for rating_key, bin_keys in scopes:
        rating = (
            {"key": "all", "label": "All ratings", "lower": None, "upper": None}
            if rating_key == "all" else BIN_BY_KEY[rating_key]
        )
        for mode in mode_scopes:
            selected = [
                row for row in rows
                if row["rating_bin"] in bin_keys and (mode == "all" or row["mode"] == mode)
            ]
            partitions = {
                name: [row for row in selected if row["split"] == name]
                for name in ("training", "validation", "test")
            }
            samples = {name: _sample_payload(items) for name, items in partitions.items()}
            training_games = samples["training"]["games"]
            if training_games < int(config["minimum_games_per_fit"]):
                results.append({
                    "rating_bin": rating,
                    "mode": mode,
                    "status": "insufficient_games",
                    "minimum_games": int(config["minimum_games_per_fit"]),
                    "samples": samples,
                })
                continue

            coefficient = fit_coefficient(partitions["training"])
            if coefficient is None:
                continue
            interval = approximate_confidence_interval(partitions["training"], coefficient)
            results.append({
                "rating_bin": rating,
                "mode": mode,
                "status": "fitted",
                "coefficient": _rounded(coefficient),
                "difference_from_lichess": _rounded(coefficient - LICHESS_COEFFICIENT),
                "confidence_interval_95": (
                    [_rounded(interval[0]), _rounded(interval[1])] if interval else None
                ),
                "confidence_note": "Approximate curvature interval using equal-game weights.",
                "samples": samples,
                "training": _metric_payload(partitions["training"], coefficient),
                "validation": _metric_payload(partitions["validation"], coefficient),
                "test": _metric_payload(partitions["test"], coefficient),
            })
    return results


async def _load_aggregated_observations(
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    sql = text(f"""
        WITH eligible_positions AS MATERIALIZED (
            SELECT
                game.link AS game_link,
                {RATING_BIN_SQL} AS rating_bin,
                game.mode,
                CASE
                    WHEN game.white_result = 1 THEN 1
                    WHEN game.white_result = 0.5 THEN 0
                    ELSE -1
                END AS outcome,
                LEAST(
                    :cp_ceiling,
                    GREATEST(0 - :cp_ceiling, ROUND(fen.score)::int)
                ) AS cp,
                CASE
                    WHEN ((hashtextextended(game.link::text, :split_seed) & 9223372036854775807) % 100) < 70
                        THEN 'training'
                    WHEN ((hashtextextended(game.link::text, :split_seed) & 9223372036854775807) % 100) < 85
                        THEN 'validation'
                    ELSE 'test'
                END AS split,
                ((hashtextextended(game.link::text, :fold_seed) & 9223372036854775807) % 20)::int AS fold,
                COUNT(*) OVER (PARTITION BY game.link)::double precision AS eligible_positions_in_game
            FROM game
            JOIN game_analysis_summary coverage ON coverage.link = game.link
            JOIN game_fen_association association ON association.game_link = game.link
            JOIN fen ON fen.fen = association.fen_fen
            WHERE {_common_game_filters()}
              AND association.n_move > :opening_moves_excluded
              AND fen.score IS NOT NULL
              AND ABS(fen.score) < 9000
              AND COALESCE(fen.analysis_source, '') LIKE 'stockfish%'
        )
        SELECT
            rating_bin,
            mode,
            outcome,
            cp,
            split,
            fold,
            COUNT(*)::bigint AS positions,
            SUM(1.0 / eligible_positions_in_game)::double precision AS weight
        FROM eligible_positions
        GROUP BY rating_bin, mode, outcome, cp, split, fold
        ORDER BY rating_bin, mode, split, fold, cp, outcome
    """)
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 1"))
        result = await session.execute(sql, query_params(config))
        return [dict(row) for row in result.mappings().all()]


def _dataset_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    digest = hashlib.sha256()
    total_positions = 0
    total_weight = 0.0
    for row in rows:
        canonical = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        digest.update(canonical.encode("utf-8"))
        total_positions += int(row["positions"])
        total_weight += float(row["weight"])
    return {
        "eligible_games": int(round(total_weight)),
        "eligible_position_appearances": total_positions,
        "aggregated_rows": len(rows),
        "sha256": digest.hexdigest(),
    }


async def _write_progress(ctx: dict[str, Any], experiment_id: str, **payload: Any) -> None:
    redis = ctx.get("redis")
    if not redis:
        return
    body = {"experiment_id": experiment_id, "updated_at": utc_now().timestamp(), **payload}
    await redis.set(
        f"chessism:coefficient_research:{experiment_id}",
        json.dumps(body),
        ex=RESEARCH_PROGRESS_TTL_SECONDS,
    )


def _write_artifact(experiment_id: str, payload: dict[str, Any]) -> str:
    RESEARCH_ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = RESEARCH_ARTIFACT_DIR / f"chessism-coefficient-{experiment_id}.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
    return str(RESEARCH_ARTIFACT_DISPLAY_DIR / path.name)


async def run_chessism_coefficient_experiment(
    ctx: dict[str, Any],
    experiment_id: str,
    **_job_options: Any,
) -> dict[str, Any]:
    """Extract equal-game-weighted observations, fit candidates, and persist results."""
    async with AsyncDBSession() as session:
        experiment = await session.get(CoefficientResearchExperiment, experiment_id)
        if experiment is None:
            raise ValueError(f"Unknown coefficient experiment {experiment_id}")
        experiment.status = "running"
        experiment.started_at = utc_now()
        experiment.error = None
        await session.commit()
        config = normalized_config(experiment.config)

    try:
        await _write_progress(
            ctx, experiment_id, phase="extracting", processed=0, total=3,
            detail="Aggregating eligible Stockfish positions with equal game weights.",
        )
        rows = await _load_aggregated_observations(config)
        dataset = _dataset_summary(rows)
        await _write_progress(
            ctx, experiment_id, phase="fitting", processed=1, total=3,
            detail=f"Fitting rating bins from {dataset['eligible_games']} games.",
        )
        fits = build_fit_results(rows, config)
        result = {
            "lichess_coefficient": LICHESS_COEFFICIENT,
            "fits": fits,
            "fitted_at": utc_now().isoformat(),
            "production_changed": False,
        }
        artifact_payload = {
            "experiment_id": experiment_id,
            "config": config,
            "dataset_summary": dataset,
            "result": result,
        }
        await _write_progress(
            ctx, experiment_id, phase="saving", processed=2, total=3,
            detail="Saving the reproducible experiment and external artifact.",
        )
        artifact_path = _write_artifact(experiment_id, artifact_payload)
        result["artifact_path"] = artifact_path

        async with AsyncDBSession() as session:
            experiment = await session.get(CoefficientResearchExperiment, experiment_id)
            if experiment is None:
                raise ValueError(f"Experiment {experiment_id} disappeared while running")
            experiment.status = "complete"
            experiment.dataset_summary = dataset
            experiment.result = result
            experiment.finished_at = utc_now()
            await session.commit()

        await _write_progress(
            ctx, experiment_id, phase="complete", processed=3, total=3,
            detail="Coefficient research complete; live accuracy was not changed.",
        )
        return {"experiment_id": experiment_id, "dataset": dataset, "fits": len(fits)}
    except Exception as error:
        async with AsyncDBSession() as session:
            experiment = await session.get(CoefficientResearchExperiment, experiment_id)
            if experiment is not None:
                experiment.status = "failed"
                experiment.error = str(error)
                experiment.finished_at = utc_now()
                await session.commit()
        await _write_progress(
            ctx, experiment_id, phase="failed", processed=0, total=3,
            detail=str(error),
        )
        raise


def experiment_payload(experiment: CoefficientResearchExperiment) -> dict[str, Any]:
    return {
        "id": experiment.id,
        "status": experiment.status,
        "job_id": experiment.job_id,
        "config": experiment.config,
        "dataset_summary": experiment.dataset_summary,
        "result": experiment.result,
        "error": experiment.error,
        "decision": experiment.decision,
        "decision_config": experiment.decision_config,
        "decision_notes": experiment.decision_notes,
        "created_at": experiment.created_at.isoformat() if experiment.created_at else None,
        "started_at": experiment.started_at.isoformat() if experiment.started_at else None,
        "finished_at": experiment.finished_at.isoformat() if experiment.finished_at else None,
        "decided_at": experiment.decided_at.isoformat() if experiment.decided_at else None,
    }


async def list_coefficient_experiments(limit: int = 20) -> list[dict[str, Any]]:
    async with AsyncDBSession() as session:
        result = await session.execute(
            select(CoefficientResearchExperiment)
            .order_by(CoefficientResearchExperiment.created_at.desc())
            .limit(max(1, min(100, int(limit))))
        )
        return [experiment_payload(item) for item in result.scalars().all()]
