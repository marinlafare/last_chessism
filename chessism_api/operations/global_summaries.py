"""Explicit refresh operations; initialized caches are never rebuilt by readers."""
from chessism_api.database.postgresql import summary_writes
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.game_coverage import refresh_pipeline_summary
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction
from chessism_api.scored_positions import summary_payload


async def refresh_database_summary(*, session_factory=None):
    async with write_transaction(session_factory or AsyncDBSession) as session:
        connection = await driver_connection(session)
        await summary_writes.refresh_game_counts(connection)
        database, _ = await summary_writes.refresh_fen_summaries(connection)
    return database


async def refresh_database_summary_game_counts(*, session_factory=None):
    async with write_transaction(session_factory or AsyncDBSession) as session:
        result = await summary_writes.refresh_game_counts(await driver_connection(session))
    return result


async def refresh_scored_position_summary(*, session_factory=None):
    async with write_transaction(session_factory or AsyncDBSession) as session:
        _, scored = await summary_writes.refresh_fen_summaries(await driver_connection(session))
    return summary_payload(scored)


async def refresh_fen_pipeline_summary(*, mark_tablebase_current=False, session_factory=None):
    async with write_transaction(session_factory or AsyncDBSession) as session:
        result = dict(await refresh_pipeline_summary(await driver_connection(session),
            mark_tablebase_current=mark_tablebase_current))
    result['refreshed_at'] = result['refreshed_at'].isoformat()
    return result
