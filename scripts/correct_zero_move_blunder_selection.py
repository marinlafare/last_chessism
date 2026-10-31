"""Explicit idle-only correction; preserve old snapshots, never reanalyze FENs."""
import argparse
import asyncio
import json
import os
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
from chessism_api.operations.cuda_summaries.no_move_upgrade import upgrade


async def main():
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        xp,hardware=cuda_backend(0)
        if hardware['name']!='NVIDIA GeForce RTX 4060': raise ValueError('RTX 4060 required')
        async with AsyncDBSession() as session,session.begin():
            result=await upgrade(session,xp)
        print(json.dumps(result),flush=True)
    finally:
        await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',required=True,action='store_true')
    parser.parse_args()
    asyncio.run(main())
