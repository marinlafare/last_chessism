"""Create small persistent matrix artifacts against the live Chessism database.

This manual integration utility is intentionally excluded from the unit suite.
It validates the artifact writer with representative dense, move-level, and
period-level data and leaves the completed snapshots visible in the Research UI.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import uuid

import constants
from arq import create_pool
from sqlalchemy import select

from chessism_api.database.engine import AsyncDBSession, init_db
from chessism_api.database.models import MatrixArtifact
from chessism_api.operations.matrix_constructor.jobs import run_matrix_construction_job
from chessism_api.operations.matrix_constructor.queries import ARTIFACT_ROOT
from chessism_api.operations.matrix_constructor.storage import matrix_catalog_lock
from chessism_api.operations.matrix_constructor.queries import (
    estimate_matrix,
    normalize_matrix_config,
)
from chessism_api.redis_client import redis_settings


SAMPLES = (
    {
        "name": "Validation · Lafareto player games",
        "row_type": "game_player",
        "feature_columns": [
            "player_color", "rating", "opponent_rating", "moves",
            "elapsed_seconds", "started_at", "mode", "analyzed_player_moves",
            "cp_gain", "cp_loss", "accuracy", "blunders", "final_player_cp",
            "game_salience", "position_occurrences", "unique_positions",
            "repeated_positions",
        ],
        "label_columns": ["result"],
        "filters": {
            "players": ["lafareto"],
            "modes": ["bullet", "blitz", "rapid"],
            "min_moves": 11,
            "analyzed_only": True,
            "max_rows": 10_000,
        },
    },
    {
        "name": "Validation · Lafareto move salience",
        "row_type": "move",
        "feature_columns": [
            "fullmove", "ply", "move_color", "reaction_seconds",
            "time_left_seconds", "player_rating", "mode", "position_cp",
            "piece_count", "game_salience", "corpus_games_with_position",
            "occurrence_index", "position_salience", "depth_weight",
            "weighted_move_salience",
        ],
        "label_columns": ["player_result"],
        "filters": {
            "players": ["lafareto"],
            "modes": ["bullet", "blitz", "rapid"],
            "min_moves": 11,
            "analyzed_only": True,
            "max_rows": 10_000,
        },
    },
    {
        "name": "Validation · Player monthly trends",
        "row_type": "player_period",
        "feature_columns": [
            "player", "games", "wins", "draws", "losses", "average_rating",
            "analyzed_games", "average_accuracy", "mode", "period_start",
            "effective_games", "salience_weighted_accuracy",
        ],
        "label_columns": [],
        "filters": {
            "players": ["lafareto", "hikaru", "pat_buchanan"],
            "modes": ["bullet", "blitz", "rapid"],
            "analyzed_only": False,
            "max_rows": 10_000,
        },
    },
)


async def main() -> None:
    await init_db(constants.CONN_STRING)
    redis = await create_pool(redis_settings)
    completed = []
    sample_names = [request["name"] for request in SAMPLES]
    async with AsyncDBSession() as session:
        previous = list((await session.execute(
            select(MatrixArtifact).where(
                MatrixArtifact.name.in_(sample_names),
                MatrixArtifact.created_by.is_(None),
                MatrixArtifact.status.notin_(("queued", "running")),
            )
        )).scalars())
    try:
        for request in SAMPLES:
            config = normalize_matrix_config(request)
            estimate = await estimate_matrix(config)
            artifact_id = str(uuid.uuid4())
            job_id = f"matrix-constructor-{artifact_id}"
            async with AsyncDBSession() as session:
                session.add(MatrixArtifact(
                    id=artifact_id,
                    name=config["name"],
                    row_type=config["row_type"],
                    status="queued",
                    job_id=job_id,
                    created_by=None,
                    config=config,
                    estimate=estimate,
                ))
                await session.commit()
            result = await run_matrix_construction_job(
                {"redis": redis, "job_id": job_id}, artifact_id=artifact_id,
            )
            completed.append({
                "id": artifact_id,
                "name": config["name"],
                "rows": result["row_count"],
                "features": result["feature_count"],
                "labels": result["label_count"],
                "bytes": result["size_bytes"],
                "manifest": result["artifact_path"],
            })
        # Only after every replacement succeeds, remove older artifacts made by
        # this same validation utility. User-created snapshots have an account
        # owner and are never included.
        root = ARTIFACT_ROOT.resolve()
        async with matrix_catalog_lock() as session:
            for artifact in previous:
                target = (root / artifact.id).resolve()
                if target.parent != root:
                    raise RuntimeError(f"Unsafe superseded artifact path: {target}")
                shutil.rmtree(target, ignore_errors=True)
                stored = await session.get(MatrixArtifact, artifact.id)
                if stored is not None:
                    await session.delete(stored)
    finally:
        await redis.aclose()
    print(json.dumps(completed, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
