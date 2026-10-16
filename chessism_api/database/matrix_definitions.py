"""One-time import of legacy snapshot recipes, inside schema creation's lock."""

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from .models import MatrixArtifact, MatrixDefinition


async def import_snapshot_definitions(connection) -> int:
    # Called ONLY when matrix_definition did not exist before create_all. Never
    # recreate a definition that the user deliberately deleted on later starts.
    from chessism_api.operations.matrix_constructor.definitions import definition_config

    result = await connection.stream(select(
        MatrixArtifact.id, MatrixArtifact.config, MatrixArtifact.created_by, MatrixArtifact.created_at,
    ).where(MatrixArtifact.status == "complete"))
    imported = 0
    try:
        async for batch in result.mappings().partitions(100):
            rows = []
            for item in batch:
                config = definition_config(item["config"])
                rows.append({
                    "id": item["id"], "name": config["name"], "row_type": config["row_type"],
                    "config": config, "created_by": item["created_by"], "created_at": item["created_at"],
                })
            if rows:
                await connection.execute(insert(MatrixDefinition).values(rows).on_conflict_do_nothing())
                imported += len(rows)
    finally:
        await result.close()
    return imported
