"""Durable game-summary refresh requests committed with local FEN changes.

Triggers only queue affected identities. Bounded background recovery rebuilds
absolute coverage/engine summaries and acknowledges the exact captured revision.
Cloud-reserved FENs retain their existing durable cloud projection/retry workflow.
"""
import logging
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import summary_refresh
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction

LOG = logging.getLogger(__name__)
BATCH_GAMES = 500


async def recover_batch(*, factory=None, refresh=None):
    """Refresh absolute projections, then acknowledge exactly the captured revisions.

    No database transaction is held during the refresh callback. If the process
    stops after a projection commit, the saved request safely repeats that work.
    """
    factory = factory or AsyncDBSession
    if refresh is None:
        from functools import partial
        from chessism_api.operations.game_analysis_summary import refresh_game_analysis_summary
        refresh = partial(refresh_game_analysis_summary, factory=factory)
    async with write_transaction(factory) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        rows = await summary_refresh.pending_batch(await driver_connection(session), BATCH_GAMES)
    error = None
    if rows:
        try:
            await refresh(tuple(row['game_link'] for row in rows))
        except Exception as failure:
            error = f'{type(failure).__name__}: {failure}'[:1000]
            LOG.exception('Game summary refresh retained for retry; FEN scores are safe.')
        async with write_transaction(factory) as session:
            await summary_refresh.finish_batch(await driver_connection(session), rows, error)
    async with write_transaction(factory) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        pending = await summary_refresh.pending_count(await driver_connection(session))
    processed = len(rows) if error is None else 0
    # Every queued revision receives the same complete projection refresh.
    return {'processed': processed, 'source_only': 0, 'engine_refreshed': processed,
            'pending': pending, 'error': error}
