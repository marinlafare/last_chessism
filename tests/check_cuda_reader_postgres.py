"""Interrupted COPY regression: disposable PostgreSQL, no network/live data."""
import asyncio
from unittest.mock import AsyncMock, patch
import asyncpg
from sqlalchemy import text
from chessism_api.database.postgresql.relational_storage import driver_connection
from chessism_api.operations.cuda_summaries import snapshot_flow
from tests import check_canonical_blunders_postgres as base


async def check(factory, connection, layout):
    engine = factory.kw['bind']
    seen = []
    started = asyncio.Event()

    async def discard(data):
        pass

    async def copy(session, run, *, failure):
        driver = await driver_connection(session)
        seen.append(driver.get_server_pid())
        if failure == 'disconnect':
            driver.terminate()
            raise RuntimeError('synthetic disconnect during COPY')
        if failure == 'server':
            await session.execute(text("SET LOCAL statement_timeout='25ms'"))
        started.set()
        return await driver.copy_from_query('SELECT pg_sleep(2)', output=discard,
            format='binary', timeout=.025 if failure == 'client' else 5)

    with patch.object(snapshot_flow.score_runs, 'pending_items', new=AsyncMock(return_value=[{'game_link':1}])):
        for failure, error in [('server', asyncpg.QueryCanceledError), ('client', TimeoutError), ('disconnect', RuntimeError), ('cancel', asyncio.CancelledError)]:
            started.clear()
            async def load(session, run):
                return await copy(session, run, failure=failure)
            with patch.object(snapshot_flow.shared_source, 'load', side_effect=load):
                task = asyncio.create_task(snapshot_flow.read_chunk(factory, {}, None))
                if failure == 'cancel':
                    await started.wait(); task.cancel()
                try:
                    await asyncio.wait_for(task, 10)
                except error as actual:
                    assert 'rollback' not in str(actual), actual
                else:
                    raise AssertionError(f'{failure} did not preserve its error')
            assert engine.pool.checkedout() == 0, engine.pool.status()
            assert await connection.fetchval('SELECT 1') == 1  # unrelated connection untouched
            async def retry(session, run):
                driver = await driver_connection(session)
                assert driver.get_server_pid() != seen[-1], 'Broken connection reused'
                return await driver.copy_from_query('SELECT 42', output=discard, format='binary', timeout=5)
            with patch.object(snapshot_flow.shared_source, 'load', side_effect=retry):
                assert await snapshot_flow.read_chunk(factory, {}, None) == 'COPY 1'
            assert engine.pool.checkedout() == 0
    print('PASS: real COPY server/client timeouts, cancellation and disconnect preserve original errors; no pool leak; fresh-connection retries; unrelated connections untouched')


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
