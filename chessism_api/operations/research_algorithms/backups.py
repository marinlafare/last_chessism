"""Compact integrity checks of definitions and immutable completed results."""

from sqlalchemy import text

# Hash each JSONB record on PostgreSQL itself. This avoids transporting every
# scatter point to the backup worker, and uses identical canonicalization after
# restore. pgBackRest independently verifies the physical backup and WAL.
ALGORITHM_BACKUP_QUERY = """
SELECT json_build_object(
    'schema_version', 1,
    'definitions', (SELECT json_build_object('count', count(*), 'md5',
        md5(COALESCE(string_agg(md5(jsonb_build_object('id', id, 'name', name, 'config', config)::text), '' ORDER BY id), '')))
        FROM algorithm_definition),
    'completed_runs', (SELECT json_build_object('count', count(*), 'md5',
        md5(COALESCE(string_agg(md5(jsonb_build_object('id', id, 'name', name, 'config', config, 'result', result)::text), '' ORDER BY id), '')))
        FROM algorithm_run WHERE status = 'complete')
)
"""


async def algorithm_backup_manifest(session):
    # Caller holds the shared catalog lock. Save/delete and completed-result
    # publication use that lock; live progress need not block or be frozen here.
    return await session.scalar(text(ALGORITHM_BACKUP_QUERY))


def validate_algorithm_backup(expected, restored):
    if not isinstance(expected, dict) or expected.get("schema_version") != 1 or restored != expected:
        raise ValueError("Restored algorithm definitions or completed results differ from the backup manifest.")
    return {"status": "passed", **restored}
