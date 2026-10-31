"""Explicit upgrade for a completed, exclusively-run legacy repair.

Default is a preview. --apply changes scheduling reasons, never evaluations.
Run with the algorithm worker stopped; all durable work survives the upgrade.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path

from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.evaluation_repair.refresh_routing import route
from scripts.repair_legacy_provenance import read_plan


async def main(args):
    plan = read_plan(args.plan)
    progress = json.loads(args.plan.with_suffix('.progress.json').read_text())
    if progress.get('run_id') != plan['sha256'] or progress.get('phase') != 'repair_complete':
        raise ValueError('The exact repair must have completed before routing its follow-up work')
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+asyncpg://', 1))
    try:
        async with AsyncDBSession() as session, session.begin():
            result = await route(session, plan['sha256'], apply=args.apply)
        print(json.dumps(result, default=str), flush=True)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    asyncio.run(main(parser.parse_args()))
