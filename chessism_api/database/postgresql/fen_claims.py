from contextlib import nullcontext
from sqlalchemy import text


RECHECK_BATCH_SIZE = 5000


async def locked_eligible_fens(session, statement, parameters=None, *, timings=None):
    """Keep lock acquisition and the fresh-snapshot safety check separate."""
    measure = timings.measure if timings is not None else lambda _: nullcontext()
    with measure('prepare_select'):
        result = await session.execute(statement, parameters or {})
        fens = result.scalars().all()
    with measure('prepare_recheck'):
        return await unclaimed_locked_fens(session, fens)


async def unclaimed_locked_fens(session, fens):
    """Recheck in a fresh READ COMMITTED snapshot AFTER taking Fen row locks.

    A cloud reservation may have committed between the initial selection snapshot
    and its row lock. Holding these locks makes the second check race-free.
    """
    if not fens:
        return []
    eligible = set()
    for offset in range(0, len(fens), RECHECK_BATCH_SIZE):
        # A single array bind keeps parsing/planning bounded instead of expanding
        # thousands of IN parameters. Locks from the first query remain held.
        eligible.update((await session.scalars(text("""
            SELECT f.fen
            FROM unnest(CAST(:fens AS text[])) wanted(fen)
            JOIN fen f ON f.fen = wanted.fen
            WHERE f.score IS NULL
              AND NOT EXISTS (SELECT 1 FROM cloud_fen_claim c WHERE c.fen = f.fen)
        """), {'fens': fens[offset:offset + RECHECK_BATCH_SIZE]})).all())
    return [fen for fen in fens if fen in eligible]
