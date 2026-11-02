"""Repair one orphaned endgame completion from verified successful job evidence.

Dry-run by default. Does not analyze positions, enqueue jobs, or infer completion
from elapsed time. Refuses to write while pipeline work or eligible endgames exist.
"""
import argparse
import asyncio
import json
import os

from arq.jobs import Job
from sqlalchemy import select, text
from chessism_api.database.postgresql.runtime import bind_runtime_engine
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import IngestionPipelineRun, IngestionPipelineStageTiming
from chessism_api.operations.tablebase import TABLEBASE_CANDIDATE_SQL, TABLEBASE_COORDINATION_KEY
from chessism_api.redis_client import get_redis_pool


async def main(args):
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        redis = await get_redis_pool()
        evidence = await Job(args.job, redis, _queue_name='pipeline_queue').result_info()
        if (not evidence or not evidence.success or evidence.function != 'run_tablebase_analysis_job'
                or not evidence.finish_time or not isinstance(evidence.result, dict)):
            raise RuntimeError('A retained successful tablebase job result is required.')
        if evidence.kwargs.get('game_links') is not None or evidence.kwargs.get('max_positions') is not None:
            raise RuntimeError('This repair requires an unrestricted shared job, not a partial selection.')
        if await redis.zcard('pipeline_queue') or await redis.get(TABLEBASE_COORDINATION_KEY):
            raise RuntimeError('Pipeline/tablebase work is active; no records changed.')
        async with AsyncDBSession() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            run = await session.get(IngestionPipelineRun, args.run, with_for_update=True)
            if not run or run.status != 'running':
                raise RuntimeError('Expected exactly the specified running ingestion.')
            stages = list((await session.scalars(select(IngestionPipelineStageTiming)
                .where(IngestionPipelineStageTiming.run_id == args.run).with_for_update())).all())
            pending = [row for row in stages if row.status != 'complete']
            if len(pending) != 1 or pending[0].stage != 'tablebase_marking' or pending[0].status != 'running':
                raise RuntimeError('Other ingestion stages are unfinished; no records changed.')
            stage = pending[0]
            if not (run.started_at <= stage.started_at <= evidence.start_time <= evidence.finish_time):
                raise RuntimeError('Shared job timing does not cover this endgame phase.')
            if any(row.finished_at and row.finished_at > evidence.finish_time for row in stages):
                raise RuntimeError('Ingestion work continued after the supplied shared result.')
            remaining = await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM fen f WHERE {TABLEBASE_CANDIDATE_SQL})'))
            if remaining:
                raise RuntimeError('Eligible unscored endgames remain; no records changed.')
            print(json.dumps({'apply': args.apply, 'player':run.player_name, 'run':args.run,
                'evidence_job':args.job, 'job_result':evidence.result,
                'finished_at':evidence.finish_time.isoformat(), 'remaining_eligible_endgames':False}))
            if args.apply:
                stage.status = 'complete'
                stage.finished_at = evidence.finish_time
                stage.elapsed_ms = (evidence.finish_time - stage.started_at).total_seconds() * 1000
                # One verified completion check, not a fabricated per-player
                # FEN count copied from an unscoped job.
                stage.processed = stage.total = 1
                stage.detail = f'Completed by shared tablebase job {args.job}; verified no eligible endgames remain.'
                run.status = 'complete'
                run.finished_at = evidence.finish_time
                run.elapsed_ms = (evidence.finish_time - run.started_at).total_seconds() * 1000
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    parser.add_argument('--job', required=True)
    parser.add_argument('--apply', action='store_true')
    asyncio.run(main(parser.parse_args()))
