"""Explicit canonical model adoption/rollback; never called by API startup.

Run in the API environment against an initialized canonical database. Adoption
keeps the previous published model until the new unique-game rebuild finishes.
"""
import argparse
import asyncio
import json
import os
from sqlalchemy import text
from chessism_api.database.postgresql.runtime import bind_runtime_engine
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from chessism_api.operations.canonical_blunders import registry


async def main(args):
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://','postgresql+asyncpg://',1))
    try:
        async with write_transaction(AsyncDBSession) as session:
            if args.adopt_run:
                if args.acknowledgement!=registry.ACK:raise ValueError('Explicit provisional acknowledgement required.')
                owner=await session.scalar(text('SELECT created_by FROM algorithm_run WHERE id=:id'),{'id':args.adopt_run})
                if not owner:raise ValueError('Source run must have an administrator owner.')
                model=await registry.adopt(session,args.adopt_run,owner,args.acknowledgement)
                print('Selected canonical target: '+model)
            elif args.rollback_to:
                if args.acknowledgement!='explicit_canonical_rollback':raise ValueError('Explicit rollback acknowledgement required.')
                await session.execute(text('SELECT id FROM public.canonical_blunder_control WHERE id FOR UPDATE'))
                if args.rollback_to=='legacy_cpu':
                    raise ValueError('CPU blunders are retired; choose a retained CUDA model instead')
                else:await registry.select_target(session,args.rollback_to)
            result=await registry.status(session)
            # Do not print the large source report, identities, credentials or raw parameters by default.
            print(json.dumps({k:result.get(k) for k in ('installed','target_model','active_model','previous_model',
                'approval','status','pending_games','progress','error')},default=str,indent=2))
    finally:await finish_analysis_cleanup(engine.dispose())


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--adopt-run')
    action.add_argument('--rollback-to',help='Retained CUDA model ID; CPU fallback is not supported')
    parser.add_argument('--acknowledgement',default='')
    asyncio.run(main(parser.parse_args()))
