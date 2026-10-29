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
            elif args.upgrade_coverage:
                from chessism_api.operations.canonical_blunders.coverage_upgrade import upgrade
                print(json.dumps(await upgrade(session)))
            elif args.upgrade_input_pipeline:
                from chessism_api.operations.fen_summary_recovery import upgrade
                print(json.dumps(await upgrade(session)))
            elif args.retire_cpu_blunders:
                from chessism_api.operations.canonical_blunders.cpu_retirement import upgrade
                print(json.dumps(await upgrade(session)))
            elif args.repair_retired_counter:
                from chessism_api.operations.canonical_blunders.retired_counter import repair
                print(json.dumps(await repair(session,storage_for(session))))
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
                    raise ValueError('CPU blunders are retired; choose a retained CUDA model instead')
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
    action.add_argument('--upgrade-coverage',action='store_true',help='Atomically exclude partial games and install durable FEN refresh requests; no GPU recalculation.')
    action.add_argument('--upgrade-input-pipeline',action='store_true',help='Idle-only durable input-trigger upgrade; no FEN repair or summary recalculation.')
    action.add_argument('--retire-cpu-blunders',action='store_true',help='Remove CPU fallback from compatibility reads; preserve GPU model and all saved results.')
    action.add_argument('--repair-retired-counter',action='store_true',help='Allow missing retired CPU counts only after verifying CUDA-only readers; metadata-only, no recalculation.')
    action.add_argument('--adopt-run')
    action.add_argument('--rollback-to',help='Retained CUDA model ID; CPU fallback is not supported')
    parser.add_argument('--acknowledgement',default='')
    asyncio.run(main(parser.parse_args()))
