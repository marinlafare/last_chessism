"""Explicit admin migration/adoption/rollback; never called by API startup.

Run in the API environment. Installation is additive and transactional. Adoption
keeps the previous published model until the new unique-game rebuild finishes.
"""
import argparse
import asyncio
import json
import os
from sqlalchemy import text
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.canonical_blunders import schema,registry


async def main(args):
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://','postgresql+asyncpg://',1))
    try:
        async with AsyncDBSession() as session,session.begin():
            if args.install:
                await schema.install(session,storage_for(session)); print('Canonical schema installed; source games unchanged.')
            elif args.adopt_run:
                if args.acknowledgement!=registry.ACK:raise ValueError('Explicit provisional acknowledgement required.')
                owner=await session.scalar(text('SELECT created_by FROM algorithm_run WHERE id=:id'),{'id':args.adopt_run})
                if not owner:raise ValueError('Source run must have an administrator owner.')
                model=await registry.adopt(session,args.adopt_run,owner,args.acknowledgement)
                print('Selected canonical target: '+model)
            elif args.rollback_to:
                if args.acknowledgement!='explicit_canonical_rollback':raise ValueError('Explicit rollback acknowledgement required.')
                t=lambda name:schema.table(storage_for(session),name)
                await session.execute(text(f'SELECT id FROM {t("control")} WHERE id FOR UPDATE'))
                if args.rollback_to=='legacy_cpu':
                    await session.execute(text(f'''UPDATE {t('control')} SET previous_model=active_model,
                        active_model=NULL,target_model=NULL,status='legacy_rollback',error=NULL,updated_at=now() WHERE id'''))
                    print('Legacy CPU reads restored; GPU caches and immutable evidence retained.')
                else:await registry.select_target(session,args.rollback_to)
            result=await registry.status(session)
            # Do not print the large source report, identities, credentials or raw parameters by default.
            print(json.dumps({k:result.get(k) for k in ('installed','target_model','active_model','previous_model',
                'approval','status','pending_games','progress','error')},default=str,indent=2))
    finally:await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--install',action='store_true')
    action.add_argument('--adopt-run')
    action.add_argument('--rollback-to',help='Retained model ID or legacy_cpu')
    parser.add_argument('--acknowledgement',default='')
    asyncio.run(main(parser.parse_args()))
