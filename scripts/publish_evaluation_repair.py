"""Explicit reviewed repair publisher. No engines, jobs or service restart."""
import argparse
import asyncio
import json
import os
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.evaluation_repair import publish
from chessism_api.operations.evaluation_repair.artifacts import read


async def main(args):
    if not args.install and not all((args.plan, args.shadow, args.fen_ids, args.plan_sha256,
                                    args.shadow_sha256, args.actor, args.acknowledgement)):
        raise ValueError('Publication requires explicit artifacts, hashes, IDs, reviewer and acknowledgement')
    plan = read(args.plan, 'plan') if not args.install else None
    shadow = read(args.shadow, 'shadow') if not args.install else None
    confirmation = read(args.confirmation, 'shadow') if args.confirmation else None
    endgame_review = read(args.endgame_review, 'endgame_review') if args.endgame_review else None
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+asyncpg://', 1))
    try:
        async with AsyncDBSession() as session, session.begin():
            if args.install:
                result = await publish.install(session)
            else:
                result = await publish.publish(session, plan, shadow, args.fen_ids, actor=args.actor,
                    plan_sha256=args.plan_sha256, shadow_sha256=args.shadow_sha256,
                    acknowledgement=args.acknowledgement, policy=args.policy,
                    confirmation=confirmation, endgame_review=endgame_review)
        # Only report success after commit. Durable receipts survive lost output.
        print(json.dumps(result))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', action='store_true', help='Explicitly install only the additive receipt table')
    parser.add_argument('--plan'); parser.add_argument('--shadow')
    parser.add_argument('--plan-sha256'); parser.add_argument('--shadow-sha256')
    parser.add_argument('--fen-ids', type=int, nargs='+')
    parser.add_argument('--actor'); parser.add_argument('--acknowledgement')
    parser.add_argument('--policy', choices=(publish.POLICY, publish.REPLACEMENT_POLICY), default=publish.POLICY)
    parser.add_argument('--confirmation'); parser.add_argument('--endgame-review')
    asyncio.run(main(parser.parse_args()))
