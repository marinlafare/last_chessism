"""Synthetic legal-PV benchmark; no Stockfish, DB or network access."""
import asyncio
import time
import json
import chess
from chessism_api.operations.cloud_analysis import batch_validation as validation
from stockfish_batch.checkpoints import BatchCheckpoints, encode


def fixture():
    board = chess.Board()
    positions = [{'id': str(i), 'fen': board.fen()} for i in range(500)]
    contract = {'fingerprint': 'offline-benchmark', 'settings': {'multipv': 4}}
    lines = []
    for rank, first in enumerate(list(board.legal_moves)[:4], 1):
        current, pv = board.copy(), [first.uci()]
        current.push(first)
        for _ in range(15):
            move = next(iter(current.legal_moves), None)
            if move is None: break
            pv.append(move.uci()); current.push(move)
        lines.append({'multipv': rank, 'nodes': 100000, 'score': 20-rank, 'wdl': [200, 700, 100], 'pv': pv})
    checkpoints = BatchCheckpoints(None, '', contract, positions, 500)
    records = [checkpoints.prepare(row, {'fen': row['fen'], 'is_valid': True, 'analysis': lines}, 1) for row in positions]
    return positions, contract, encode({'fingerprint': contract['fingerprint'], 'records': records})


async def main():
    rows, contract, raw = fixture()
    count = 32
    started = time.monotonic()
    reference = None
    for _ in range(count): reference = validation.prepare_batch(raw, rows, contract)
    serial = time.monotonic() - started
    await asyncio.gather(*(validation.prepare_in_process(raw, rows, contract) for _ in range(4)))
    slots = asyncio.Semaphore(4)
    async def one():
        async with slots:
            actual = await validation.prepare_in_process(raw, rows, contract)
            assert actual.receipt == reference.receipt and actual.results == reference.results
    started = time.monotonic()
    await asyncio.gather(*(one() for _ in range(count)))
    parallel = time.monotonic() - started
    await asyncio.to_thread(validation.shutdown_pool)
    print(json.dumps({'fens': count*500, 'serial_seconds': round(serial, 3),
        'parallel_seconds': round(parallel, 3), 'speedup': round(serial/parallel, 2), 'identical_results': True}))


if __name__ == '__main__': asyncio.run(main())
