"""Fresh-process DB startup probe; explicitly private Unix socket only."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def main(args):
    from scripts.db_refactor_release_rehearsal import check_runtime
    from scripts.db_refactor_workload import digest
    workspace=args.workspace
    if (workspace.parent!=Path('/restore-test') or workspace.resolve()!=workspace
        or not workspace.name.startswith('db-refactor-baseline-')):
        raise ValueError('Only an isolated baseline workspace is accepted')
    result=await check_runtime(workspace/'socket',workspace/'pgdata',database=args.database)
    print(json.dumps({'checks':result['checks'],'contract_hash':digest(result['contract']),
                      'cloud_calls':0,'queue_consumers_started':0}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace',type=Path,required=True)
    parser.add_argument('--database',choices=('chessism_db','postgres'),default='chessism_db')
    asyncio.run(main(parser.parse_args()))
