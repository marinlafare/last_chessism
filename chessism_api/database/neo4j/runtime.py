"""Bounded graph connections; never initializes a schema or imports chess data."""
from contextlib import asynccontextmanager

from .config import GraphConfig


@asynccontextmanager
async def connect(config=None):
    # Keep the optional driver out of PostgreSQL-only startup paths.
    from neo4j import AsyncGraphDatabase

    config = config or GraphConfig.from_env()
    driver = AsyncGraphDatabase.driver(
        config.uri, auth=config.credentials(),
        max_connection_pool_size=4,
        connection_timeout=5,
        connection_acquisition_timeout=8,
        max_transaction_retry_time=5,
        telemetry_disabled=True,
    )
    try:
        await driver.verify_connectivity()
        yield driver
    finally:
        await driver.close()


async def check_connection(config=None):
    from neo4j import Query

    config = config or GraphConfig.from_env()
    async with connect(config) as driver:
        async with driver.session(database=config.database, default_access_mode='READ') as session:
            result = await session.run(Query('RETURN 1 AS ok', timeout=3))
            record = await result.single(strict=True)
            if record['ok'] != 1:
                raise RuntimeError('Unexpected graph health response.')
    return {'status': 'ready', 'database': config.database, 'schema_changes': False}
