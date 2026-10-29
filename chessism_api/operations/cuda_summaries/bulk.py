"""Bounded binary COPY transport; all temporary rows belong to the transaction."""
from sqlalchemy import text
from chessism_api.database.relational_storage import driver_connection, identifier
from ..matrix_constructor.artifact_files import file_operation


async def temp_rows(session, name, definition, columns, rows):
    name_sql = identifier(name)
    await session.execute(text(f'CREATE TEMP TABLE {name_sql} ({definition}) ON COMMIT DROP'))
    driver = await driver_connection(session)
    await driver.copy_records_to_table(name, schema_name='pg_temp', columns=columns, records=rows, timeout=120)


async def json_rows(function, *args):
    """Python encoding must not block DB/COPY cancellation or worker heartbeats."""
    return await file_operation(function, *args)
