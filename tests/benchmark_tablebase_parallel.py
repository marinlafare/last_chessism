"""Read-only Syzygy benchmark: synthetic legal positions, no database/network."""
import argparse
import asyncio
import json
import random
from time import perf_counter
import chess
from chessism_api.operations.tablebase_parallel import TablebaseProber


def positions(count):
    rng=random.Random(20261029);rows=[];seen=set()
    materials=('KRPkr','KQPkp','KBPkb','KNPkn','KRkp','KQkr')
    while len(rows)<count:
        board=chess.Board.empty();pieces=materials[len(rows)%len(materials)]
        for symbol,square in zip(pieces,rng.sample(range(64),len(pieces))):
            board.set_piece_at(square,chess.Piece.from_symbol(symbol))
        board.turn=bool(rng.getrandbits(1))
        if not board.is_valid():continue
        fen=' '.join(board.fen().split()[:4])
        if fen in seen:continue
        seen.add(fen);rows.append({'fen':fen,'piece_count':len(pieces)})
    return rows


async def main(args):
    rows=positions(args.rows);reference=None;reports=[]
    for workers in (1,2,4,4,2,1):
        setup=perf_counter()
        async with TablebaseProber(args.path,workers) as prober:
            await prober.probe(rows[:min(150,len(rows))])
            warmup=perf_counter()-setup;start=perf_counter()
            actual=await prober.probe(rows)
            seconds=perf_counter()-start
            if reference is None:reference=actual
            assert actual==reference,'Parallel probe changed exact output or ordering'
            row={'workers':prober.workers,'rows':len(rows),'solved':len(actual[0]),
                'unavailable':len(actual[1]),'warmup_seconds':round(warmup,3),'probe_seconds':round(seconds,3)}
            reports.append(row);print(json.dumps(row),flush=True)
    print('PASS exact WDL, DTZ, scores, best moves and unavailable records across 1/2/4 processes',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--path',default='/syzygy');parser.add_argument('--rows',type=int,default=1500)
    args=parser.parse_args()
    if not 1<=args.rows<=10000:parser.error('rows must be between 1 and 10000')
    asyncio.run(main(args))
