"""One persistence path for local engines and downloaded cloud results.

The caller owns the transaction, including any import receipts or leases.
"""
from sqlalchemy import delete, insert

from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Fen, FenContinuation


async def stage_fen_results(session, formatted, continuations, *, interface=None):
    await (interface or DBInterface(Fen)).update_fen_analysis_data(session, formatted)
    await session.execute(delete(FenContinuation).where(
        FenContinuation.fen_fen.in_([row["fen"] for row in formatted])
    ))
    if continuations:
        await session.execute(insert(FenContinuation.__table__), continuations)
