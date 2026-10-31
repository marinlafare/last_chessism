"""Verify recipe records inside PostgreSQL backups, not separate matrix files."""

import hashlib
import json

from sqlalchemy import select

from chessism_api.database.models import MatrixDefinition


def definition_fingerprint(records: list[dict]) -> dict:
    digest = hashlib.sha256()
    seen = set()
    for item in sorted(records, key=lambda row: row["id"]):
        if item["id"] in seen or not isinstance(item["config"], dict):
            raise ValueError("Invalid or duplicate restored matrix definition.")
        seen.add(item["id"])
        record = {key: item[key] for key in ("id", "name", "row_type", "config")}
        digest.update(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        digest.update(b"\n")
    return {"schema_version": 1, "storage": "postgresql", "table": "matrix_definition",
            "count": len(records), "sha256": digest.hexdigest()}


async def definition_backup_manifest(session) -> dict:
    # The caller holds matrix_catalog_lock through the PostgreSQL recovery point.
    result = await session.execute(select(
        MatrixDefinition.id, MatrixDefinition.name, MatrixDefinition.row_type, MatrixDefinition.config,
    ).order_by(MatrixDefinition.id))
    return definition_fingerprint([dict(row) for row in result.mappings()])


def validate_definition_backup(expected: dict, restored: list[dict]) -> dict:
    actual = definition_fingerprint(restored)
    if expected != actual:
        raise ValueError("Restored matrix definitions differ from the backup's recorded instructions.")
    return {"status": "passed", "count": actual["count"], "sha256": actual["sha256"]}
