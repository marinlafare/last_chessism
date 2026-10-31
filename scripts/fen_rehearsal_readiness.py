"""Bounded readiness check for the private restored migration database."""

import asyncio


async def wait_for_recovery_completion(connection, *, timeout_seconds=300, poll_seconds=1):
    """Wait for natural recovery completion; never promote a server ourselves.

    pg_ctl -w can return while a restored server only accepts read-only
    connections. Neither WAL progress functions nor migration DDL are safe yet.
    The caller must pass its private restored-database connection.
    """
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError('Recovery timeout and polling interval must be positive')

    async def wait_until_ready():
        while True:
            state = await connection.fetchrow("""
                SELECT pg_is_in_recovery() AS in_recovery,
                       current_setting('transaction_read_only') = 'on' AS read_only
            """)
            if state is None or state['in_recovery'] not in (True, False):
                raise RuntimeError('Restored PostgreSQL returned an invalid recovery state')
            if state['in_recovery'] is False:
                if state['read_only'] is not False:
                    raise RuntimeError('Restored PostgreSQL finished recovery but remains read-only')
                return
            await asyncio.sleep(poll_seconds)

    try:
        await asyncio.wait_for(wait_until_ready(), timeout=timeout_seconds)
    except asyncio.TimeoutError as error:
        raise RuntimeError('Timed out waiting for restored PostgreSQL to finish recovery') from error
