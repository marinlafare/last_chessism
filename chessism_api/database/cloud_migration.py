"""Transactional JSON-to-columns migration; run only with API/controller stopped.

No Google calls. Every document is reconstructed and checksum-verified before
old columns are dropped. PostgreSQL rolls back the entire migration on failure.
"""
from sqlalchemy import text
from . import cloud_columns as columns
from .cloud_codec import write_document, read_document, checksum


def repair_cleanup_resources(connection):
    """Upgrade the initial incorrect TEXT[] mapping with the controller stopped.

    The old encoder rejected resource dictionaries, so its only valid persisted
    inventories were absent/null/empty. Refuse any nonempty legacy array instead
    of guessing resource identities. Existing checksums must remain unchanged.
    Safe for a paused cleanup job; does not change job status or call Google.
    """
    connection.execute(text('SELECT pg_advisory_xact_lock(731946219)'))
    connection.execute(text('LOCK TABLE cloud_control_launch IN ACCESS EXCLUSIVE MODE'))
    existing = set(connection.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='cloud_control_launch'")).scalars())
    obsolete = [columns.column_name(field + '.remaining_compute')
                for field in ('cleanup_report', 'cleanup_verification')]
    obsolete = [name for name in obsolete if name in existing]
    for name in obsolete:
        if connection.scalar(text(f'SELECT EXISTS (SELECT 1 FROM cloud_control_launch WHERE cardinality("{name}") > 0)')):
            raise ValueError('Nonempty legacy compute inventory; schema repair refused without changing data')
    columns.TABLES['compute_resource'].create(connection, checkfirst=True)
    for ident in connection.execute(text("SELECT id FROM cloud_control_document WHERE schema_name='launch'")).scalars():
        # read_document verifies the original digest, including empty/null lists.
        read_document(connection, ident)
    for name in obsolete:
        connection.execute(text(f'ALTER TABLE cloud_control_launch DROP COLUMN "{name}"'))
    return len(obsolete)


def migrate(connection):
    connection.execute(text('SELECT pg_advisory_xact_lock(731946219)'))
    active = connection.scalar(text("SELECT count(*) FROM cloud_analysis_job WHERE status IN ('queued','running','paused')"))
    if active:
        raise RuntimeError('Finish or explicitly pause deployment until all cloud work is terminal')
    connection.execute(text('LOCK TABLE cloud_analysis_job, cloud_analysis_run IN ACCESS EXCLUSIVE MODE'))
    columns.ROOTS.create(connection, checkfirst=True)
    for table in columns.TABLES.values():
        table.create(connection, checkfirst=True)
    repair_cleanup_resources(connection)
    from .models import CloudPhaseTiming, CloudResultBatch, CloudUsage, CloudExecutionAttempt
    for model in (CloudPhaseTiming, CloudResultBatch, CloudUsage, CloudExecutionAttempt):
        model.__table__.create(connection, checkfirst=True)
    specs = {'cloud_analysis_job': {'selection': 'selection'}, 'cloud_analysis_run': {
        'positions': 'positions', 'receipts': 'receipts', 'launch': 'launch', 'contract': 'contract', 'cleanup_plan': 'cleanup'}}
    migrated = 0
    for table, fields in specs.items():
        existing = set(connection.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=:name"), {'name': table}).scalars())
        for field, kind in fields.items():
            ref = field + '_record_id'
            connection.execute(text(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {ref} VARCHAR(96)'))
            if field not in existing:
                continue
            for row in connection.execute(text(f'SELECT id,{field} FROM {table}')).mappings():
                ident = f"{row['id']}:{field}"
                write_document(connection, ident, kind, row[field])
                if checksum(read_document(connection, ident)) != checksum(row[field]):
                    raise ValueError('Lossy cloud migration; original JSON retained by rollback')
                connection.execute(text(f'UPDATE {table} SET {ref}=:ref WHERE id=:id'), {'ref': ident, 'id': row['id']})
                migrated += 1
        if table == 'cloud_analysis_run':
            for definition in ('position_count INTEGER NOT NULL DEFAULT 0', 'imported_count INTEGER NOT NULL DEFAULT 0',
                               'details_pruned BOOLEAN NOT NULL DEFAULT false',
                               'results_verified_at TIMESTAMPTZ', 'cleanup_completed_at TIMESTAMPTZ',
                               'completion_manifest_sha256 VARCHAR(64)'):
                connection.execute(text(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {definition}'))
            if 'positions' in existing:
                connection.execute(text('UPDATE cloud_analysis_run SET position_count=json_array_length(positions), imported_count=COALESCE((SELECT sum(json_array_length(value->\'records\')) FROM json_each(receipts)),0)'))
        # Drops happen inside the same transaction as all copies/verifications.
        for field in fields:
            if field in existing:
                connection.execute(text(f'ALTER TABLE {table} DROP COLUMN {field}'))
    return migrated
