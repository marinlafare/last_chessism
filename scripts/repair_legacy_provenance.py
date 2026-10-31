"""Explicit resumable repair; prepare is read-only, apply needs acknowledgement.

Does not start engines, cloud work, or restart services. Existing workers consume
normal durable refresh requests. JSON progress and immutable receipts survive
disconnects. Missing historical timestamps stay NULL on assumed CP evaluations.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import chess.syzygy
from arq.connections import create_pool, RedisSettings
from sqlalchemy import text

from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.backup_files import atomic_write_json
from chessism_api.operations.backup_coordination import ensure_backup_reservation, release_backup
from chessism_api.operations.fen_backup.catalog import invalidate_after_restore
from chessism_api.operations.evaluation_repair import legacy_batch as db, legacy_policy as policy
from chessism_api.operations.evaluation_repair.artifacts import digest


def save_plan(destination, value):
    value = {**value, 'sha256': digest(value)}
    with destination.open('x', encoding='utf-8') as output:
        json.dump(value, output, separators=(',', ':'))
        output.flush()
        os.fsync(output.fileno())
    return value


def read_plan(destination):
    value = json.loads(destination.read_text())
    if value.get('sha256') != digest({k: v for k, v in value.items() if k != 'sha256'}):
        raise ValueError('Repair plan checksum mismatch')
    if value.get('policy') != policy.POLICY or set(value['ids']) != {'tablebase', 'assumed', 'held_mate'}:
        raise ValueError('Unsupported repair plan')
    ids = [number for group in value['ids'].values() for number in group]
    if len(set(ids)) != len(ids) or any(type(number) is not int or number <= 0 for number in ids):
        raise ValueError('Duplicate or invalid planned identities')
    if value['counts'] != {key: len(ids) for key, ids in value['ids'].items()}:
        raise ValueError('Repair plan counts disagree')
    return value


async def main(args):
    if not 1 <= args.batch_size <= policy.MAX_BATCH:
        raise ValueError('Invalid batch size')
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+asyncpg://', 1))
    redis = None
    owner = None
    state = {'phase': 'preparing', 'started_at': datetime.now(timezone.utc).isoformat()}
    status_path = args.plan.with_suffix('.progress.json')
    def progress(**values):
        state.update(values, updated_at=datetime.now(timezone.utc).isoformat())
        atomic_write_json(status_path, state)
        print(json.dumps(state), flush=True)
    try:
        if not args.apply:
            async with AsyncDBSession() as session:
                plan = save_plan(args.plan, await db.discover(session))
            print(json.dumps({'phase': 'planned_read_only', 'plan': str(args.plan),
                              'sha256': plan['sha256'], 'counts': plan['counts']}), flush=True)
            return
        if args.acknowledgement != policy.ACK or not args.actor:
            raise ValueError('Explicit actor and repair acknowledgement required')
        plan = read_plan(args.plan)
        state.update(run_id=plan['sha256'], counts=plan['counts'], changed=0, tablebase=0, assumed=0, skipped=0)
        redis = await create_pool(RedisSettings(host=os.environ.get('REDIS_HOST', 'redis'),
            port=int(os.environ.get('REDIS_PORT', '6379'))))
        owner = 'legacy-repair-' + plan['sha256']
        await ensure_backup_reservation(redis, owner)
        backup_root = Path(os.environ['FEN_ANALYSIS_BACKUP_DIR'])
        if not backup_root.is_dir():
            raise ValueError('Existing backup directory is required')
        # Before any commit, require the next FEN backup to include historical
        # NULL timestamps. Never counterfeit analyzed_at to force a delta export.
        invalidate_after_restore(backup_root)
        async with AsyncDBSession() as session:
            layout = await db.configure(session, readonly=True)
            completed = list((await session.execute(text(f'''SELECT payload->'processed_ids'
                FROM {layout.table('evaluation_repair_receipt')} WHERE payload->>'run_id'=:run'''),
                {'run': plan['sha256']})).scalars())
        done = {number for group in completed for number in group}
        state.update(changed=len(done), tablebase=len(done.intersection(plan['ids']['tablebase'])),
                     assumed=len(done.intersection(plan['ids']['assumed'])))
        skip_log = args.plan.with_suffix('.skipped.jsonl')
        progress(phase='repairing', resumable=True)
        with chess.syzygy.open_tablebase(args.tablebases) as tablebase, skip_log.open('a', encoding='utf-8') as skipped_file:
            for group in ('tablebase', 'assumed'):
                pending = [number for number in plan['ids'][group] if number not in done]
                for offset in range(0, len(pending), args.batch_size):
                    await ensure_backup_reservation(redis, owner)
                    ids = pending[offset:offset + args.batch_size]
                    async with AsyncDBSession() as session:
                        rows = await db.load(session, ids)
                    if len(rows) != len(ids):
                        raise ValueError('Planned FEN disappeared')
                    candidates, skipped = await asyncio.to_thread(policy.prepare, rows, tablebase)
                    if any(policy.classify(c['before'])[0] != group for c in candidates):
                        raise ValueError('Planned repair category changed')
                    result = {'changed': 0, 'tablebase': 0, 'assumed': 0}
                    if candidates:
                        async with AsyncDBSession() as session, session.begin():
                            result = await db.publish(session, candidates, run_id=plan['sha256'], actor=args.actor,
                                                      acknowledgement=args.acknowledgement)
                    for item in skipped:
                        skipped_file.write(json.dumps(item) + '\n')
                    skipped_file.flush()
                    os.fsync(skipped_file.fileno())
                    for key in ('changed', 'tablebase', 'assumed'):
                        state[key] += result.get(key, 0)
                    state['skipped'] += len(skipped)
                    progress(phase='repairing', group=group, last_fen_id=ids[-1],
                             last_receipt=result.get('receipt_id'))
        # The independent workers may still be processing the durable queue.
        async with AsyncDBSession() as session:
            layout = await db.configure(session, readonly=True)
            queues = {name: await session.scalar(text(f'SELECT count(*) FROM {layout.table(name)}'))
                      for name in ('fen_summary_refresh', 'canonical_blunder_dirty')}
        progress(phase='repair_complete', held_mates=len(plan['ids']['held_mate']), refresh_queues=queues,
                 next_fen_backup='full_snapshot_required')
    except BaseException as error:
        if args.apply:
            progress(phase='failed', error=f'{type(error).__name__}: {error}', resumable=True)
        raise
    finally:
        if redis:
            if owner:
                await release_backup(redis, owner)
            await redis.aclose()
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--actor')
    parser.add_argument('--acknowledgement')
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--tablebases', default='/syzygy')
    asyncio.run(main(parser.parse_args()))
