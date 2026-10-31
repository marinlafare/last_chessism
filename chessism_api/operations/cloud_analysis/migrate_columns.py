"""Explicit local migration. Stop API and cloud-controller before --apply.

No Google calls. Uses DATABASE_URL; never prints credentials. Without --apply,
checks every existing legacy document for a lossless column representation.
"""
import argparse
import asyncio
import os
import re
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine
from . import runtime
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudResultBatch, CloudAnalysisJob
from chessism_api.database.cloud_codec import encode_document, decode_document, checksum, write_document
from chessism_api.database.cloud_migration import migrate, repair_cleanup_resources
from . import history


async def backfill(*, compact=False):
    async with AsyncDBSession() as session:
        ids = (await session.scalars(select(CloudAnalysisRun.id))).all()
    for ident in ids:
        async with AsyncDBSession() as session, session.begin():
            run = await session.get(CloudAnalysisRun, ident, with_for_update=True)
            await history.save_usage(session, run, run.launch)
            connection = await session.connection()
            for name, receipt in run.receipts.items():
                match = re.fullmatch(r'(?:tasks/(\d+)/)?batches/(\d+)\.json', name)
                if match is None:
                    raise ValueError('Unsupported legacy receipt name; existing record retained')
                task, index = int(match[1]) if match[1] else -1, int(match[2])
                key = {'run_id': ident, 'task_index': task, 'batch_index': index}
                existing = await session.get(CloudResultBatch, key)
                if existing:
                    if existing.receipt_sha256 != checksum(receipt):
                        raise ValueError('Legacy receipt disagrees with imported batch ledger')
                    continue
                ref = f'{ident}:batch:{task}:{index}'
                await connection.run_sync(write_document, ref, 'receipt', receipt)
                session.add(CloudResultBatch(**key, object_name=name, sha256=receipt['sha256'],
                    receipt_sha256=checksum(receipt), position_count=len(receipt['records']), detail_record_id=ref))
            # All receipts and their immutable detail references are committed
            # atomically before removing the legacy aggregate representation.
            run.receipts = {}
        if compact:
            await history.compact_run(ident, historical=True)
    return len(ids)


async def main(args):
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'ssl': False})
    AsyncDBSession.configure(bind=engine)
    try:
        if getattr(args, 'repair_cleanup_resources', False):
            async with engine.begin() as connection:
                await connection.execute(text("SET LOCAL lock_timeout='10s'"))
                count = await connection.run_sync(repair_cleanup_resources)
            print(f'Repaired cleanup resource columns; removed {count} obsolete empty-array columns. Job state unchanged.')
        elif args.apply:
            async with engine.begin() as connection:
                await connection.execute(text("SET LOCAL lock_timeout='10s'"))
                count = await connection.run_sync(migrate)
            runs = await backfill(compact=args.compact_completed)
            print(f'Migrated {count} control documents; backfilled {runs} run histories. No Google resources changed.')
        else:
            count = 0
            async with engine.connect() as connection:
                for table, mapping in (('cloud_analysis_job', {'selection': 'selection'}), ('cloud_analysis_run', {
                    'positions': 'positions', 'receipts': 'receipts', 'launch': 'launch', 'contract': 'contract', 'cleanup_plan': 'cleanup'})):
                    existing = set((await connection.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=:t"), {'t': table})).scalars())
                    names = [name for name in mapping if name in existing]
                    if not names: continue
                    for row in (await connection.execute(text(f'SELECT id,{",".join(names)} FROM {table}'))).mappings():
                        for name in names:
                            root, rows = encode_document(row['id'] + ':' + name, mapping[name], row[name])
                            decode_document(root, rows)
                            count += 1
            print(f'Validated {count} legacy documents. No database changes made.')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    changes = parser.add_mutually_exclusive_group()
    changes.add_argument('--apply', action='store_true')
    changes.add_argument('--repair-cleanup-resources', action='store_true',
                         help='Repair the initial pending-compute schema, including a paused cleanup run')
    parser.add_argument('--compact-completed', action='store_true',
                        help='After verification, remove temporary FEN identities from completed job history')
    asyncio.run(main(parser.parse_args()))
