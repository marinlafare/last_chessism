"""Manual database smoke test for the matrix writer.

Run in the API container with MATRIX_ARTIFACT_DIR pointing at temporary space.
This is intentionally not part of the default unit suite because it needs the
live PostgreSQL schema and reads a five-row repeatable snapshot.
"""

import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import constants
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.jobs import _construct_artifact
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from tests.validate_matrix_artifact import validate


class MemoryProgress:
    async def set(self, *_args, **_kwargs):
        return True


async def main() -> None:
    # Never run production schema migrations as a side effect of a smoke test.
    engine = create_async_engine(constants.CONN_STRING.replace("postgresql://", "postgresql+asyncpg://", 1))
    AsyncDBSession.configure(bind=engine)
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout = '120s'"))
        for row_type in ROW_TYPES.values():
            config = normalize_matrix_config({
                "row_type": row_type.key,
                "feature_columns": [
                    column.key for column in row_type.columns
                ],
                "filters": {"players": ["lafareto"], "max_rows": 5},
            })
            count_sql, select_sql, params = matrix_sql(config)
            await session.execute(text(f"EXPLAIN {count_sql}"), params)
            await session.execute(text(f"EXPLAIN {select_sql}"), params)
        for row_type_key, fields in {
            "game_player": ["accuracy", "game_salience", "unique_positions"],
            "move": ["position_salience", "depth_weight", "weighted_move_salience"],
            "game_position": ["position_salience", "game_salience"],
            "player_period": ["effective_games", "salience_weighted_accuracy"],
        }.items():
            config = normalize_matrix_config({
                "row_type": row_type_key,
                "feature_columns": fields,
                "filters": {"players": ["lafareto"], "max_rows": 5},
            })
            _, select_sql, params = matrix_sql(config)
            await session.execute(text(f"EXPLAIN {select_sql}"), params)

    config = normalize_matrix_config({
        "name": "smoke-test",
        "row_type": "game_player",
        "feature_columns": ["rating", "opponent_rating", "mode"],
        "label_columns": ["result"],
        "filters": {"players": ["lafareto"], "max_rows": 5},
    })
    result = await _construct_artifact(
        artifact_id=str(uuid.uuid4()),
        config=config,
        redis=MemoryProgress(),
        job_id="matrix-constructor-smoke",
    )
    assert result["row_count"] == 5, result
    assert result["feature_count"] == 3, result
    manifest = Path(result["artifact_path"])
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["version"] == 2, payload
    assert payload["storage_layout"] == "dtype_grouped_numpy", payload
    assert payload["validation"]["status"] == "passed", payload
    assert len(payload["profiles"]["features"]) == 3, payload
    assert (manifest.parent / "features_int32.npy").exists(), payload
    assert (manifest.parent / "labels_float32.npy").exists(), payload
    move_rows = max(1, int(os.environ.get("MATRIX_SMOKE_MOVE_ROWS", "5")))
    move_config = normalize_matrix_config({
        "name": "move-smoke-test",
        "row_type": "move",
        "feature_columns": [
            "position_cp", "corpus_games_with_position", "occurrence_index",
            "position_salience", "depth_weight", "weighted_move_salience",
        ],
        "filters": {
            "players": ["lafareto"], "analyzed_only": True,
            "max_rows": move_rows,
        },
    })
    move_started = time.monotonic()
    move_result = await _construct_artifact(
        artifact_id=str(uuid.uuid4()),
        config=move_config,
        redis=MemoryProgress(),
        job_id="matrix-constructor-move-smoke",
    )
    assert move_result["row_count"] == move_rows, move_result
    validate(Path(move_result["artifact_path"]))
    move_seconds = round(time.monotonic() - move_started, 3)
    # Exercise actual encoding (not just SQL planning) for every allowlisted
    # field, including category dictionaries, null masks and fractional values.
    for row_type in ROW_TYPES.values():
        sample = await _construct_artifact(
            artifact_id=str(uuid.uuid4()),
            config=normalize_matrix_config({
                "row_type": row_type.key,
                "feature_columns": [column.key for column in row_type.columns],
                "filters": {"players": ["lafareto"], "max_rows": 5},
            }),
            redis=MemoryProgress(), job_id=f"matrix-smoke-{row_type.key}",
        )
        validate(Path(sample["artifact_path"]))
    print({
        "rows": result["row_count"],
        "features": result["feature_count"],
        "labels": result["label_count"],
        "manifest": str(manifest),
        "planned_row_types": len(ROW_TYPES),
        "move_rows": move_result["row_count"],
        "move_seconds": move_seconds,
        "all_column_snapshots_validated": len(ROW_TYPES),
    })
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
