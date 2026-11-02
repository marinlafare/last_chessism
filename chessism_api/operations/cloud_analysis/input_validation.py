"""Bounded CPU-only input validation; no database/cloud access in children."""
import atexit
import asyncio
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

import chess

BLOCK = 12_500
_pool = None


def validate(fens):
    rows = []
    seen = set()
    for fen in fens:
        if not isinstance(fen, str) or len(fen) > 256 or len(fen.split()) not in (4, 6):
            raise ValueError('A four-field position key or six-field FEN is required')
        if fen in seen:
            raise ValueError('Duplicate FEN in reservation input')
        seen.add(fen)
        if not chess.Board(fen).is_valid():
            raise ValueError('Invalid chess position in reservation input')
        rows.append({'id': hashlib.sha256(fen.encode()).hexdigest(), 'fen': fen})
    return rows


async def prepare(fens):
    global _pool
    if not fens:
        raise ValueError('Empty reservation input')
    if len(fens) > 50_000 or len(set(fens)) != len(fens):
        raise ValueError('Reservation input must contain at most 50,000 distinct FENs')
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn'))
    loop = asyncio.get_running_loop()
    groups = await asyncio.gather(*(loop.run_in_executor(_pool, validate, fens[n:n + BLOCK])
                                   for n in range(0, len(fens), BLOCK)))
    return [row for group in groups for row in group]


async def claim(session, run_id, fens):
    # COPY retains all PK/FK constraints. Eligibility was rechecked after taking
    # FEN locks in this same transaction; a conflict rolls back the whole page.
    from chessism_api.database.postgresql.relational_storage import driver_connection
    driver = await driver_connection(session)
    await driver.copy_records_to_table('cloud_fen_claim', columns=['fen', 'run_id'],
                                      records=((fen, run_id) for fen in fens))


def shutdown():
    global _pool
    pool, _pool = _pool, None
    if pool:
        pool.shutdown(wait=True, cancel_futures=True)


atexit.register(shutdown)
