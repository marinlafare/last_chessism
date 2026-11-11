"""Explicit application transactions for rebuilding maintained player rankings."""
from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.mode_summary import refresh_modes
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction


async def _refresh(names, factory):
    async with write_transaction(factory or AsyncDBSession) as session:
        # The adapter may wait for an importer; each subsequent statement must
        # see its commit, rather than retain a snapshot from before the wait.
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL READ COMMITTED'))
        return await refresh_modes(await driver_connection(session), names)


async def refresh_main_character_mode_summary(*, session_factory=None):
    return await _refresh(None, session_factory)


async def refresh_main_character_mode_summary_for_players(player_names, *, session_factory=None):
    names = sorted({str(name).strip().lower() for name in player_names or () if str(name or '').strip()})
    if not names: return {'bullet':0,'blitz':0,'rapid':0,'total':0}
    return await _refresh(names, session_factory)
