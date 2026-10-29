"""Read-only legacy repair pilot. Does not install tables, analyze or publish."""
import argparse
import asyncio
import json
import os
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.evaluation_repair import plan
from chessism_api.operations.evaluation_repair.artifacts import read, write


async def main(args):
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+asyncpg://', 1))
    try:
        async with AsyncDBSession() as session:
            await plan.prepare(session)
            if args.recheck:
                report = await plan.recheck(session, read(args.recheck, 'plan'))
            elif args.fen_ids:
                report = await plan.preview_ids(session, args.fen_ids)
            else:
                report = await plan.preview(session, after_id=args.after_id, window=args.window,
                                            limit=args.limit, mates_only=args.mates_only, cp_only=args.cp_only)
        report = write(args.output, report)
        print(json.dumps({'report': args.output, 'sha256': report['sha256'],
            'fens': len(report.get('items', [])), 'affected_games': len(report.get('affected_games', [])),
            'eligible_players': report.get('eligible_players'), 'changed_ids': report.get('changed_ids'),
            'scores_changed': 0}))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--after-id', type=int, default=0)
    parser.add_argument('--window', type=int, default=plan.MAX_WINDOW)
    parser.add_argument('--limit', type=int, default=8)
    parser.add_argument('--mates-only', action='store_true')
    parser.add_argument('--cp-only', action='store_true')
    parser.add_argument('--fen-ids', type=int, nargs='+')
    parser.add_argument('--recheck', help='Recheck a saved plan without changing any data')
    parser.add_argument('--output', required=True)
    asyncio.run(main(parser.parse_args()))
