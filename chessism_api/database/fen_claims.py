from sqlalchemy import select
from chessism_api.database.models import CloudFenClaim, Fen


async def unclaimed_locked_fens(session, fens):
    """Recheck in a fresh READ COMMITTED snapshot AFTER taking Fen row locks.

    A cloud reservation may have committed between the initial selection snapshot
    and its row lock. Holding these locks makes the second check race-free.
    """
    if not fens:
        return []
    eligible = set()
    for offset in range(0, len(fens), 1000):
        eligible.update((await session.scalars(select(Fen.fen).where(
            Fen.fen.in_(fens[offset:offset + 1000]), Fen.score.is_(None),
            ~select(CloudFenClaim.fen).where(CloudFenClaim.fen == Fen.fen).exists(),
        ))).all())
    return [fen for fen in fens if fen in eligible]
