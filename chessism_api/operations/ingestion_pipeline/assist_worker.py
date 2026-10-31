"""Drain one existing ingestion chain without moving jobs or interrupting backups.

Explicit recovery only: python -m ...assist_worker --run-id UUID. Jobs retain
their original IDs, queue, coordination keys and normal ARQ atomic claims. The
helper exits once the durable run is terminal and it has no active task.
"""
import argparse
import asyncio
from contextlib import suppress
import logging
import os
from uuid import UUID

from arq.jobs import Job
from arq.worker import Worker, func
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.redis_client import redis_settings
from .fen_orchestrator import run_fen_pipeline
from ..tablebase import run_tablebase_analysis_job

FUNCTIONS = {'run_fen_pipeline', 'run_tablebase_analysis_job'}


def belongs_to_run(info, run_id):
    return bool(info and info.function in FUNCTIONS and info.kwargs.get('ingestion_run_id') == run_id)


class IngestionAssistWorker(Worker):
    def __init__(self, *, run_id, **kwargs):
        self.ingestion_run_id = str(UUID(run_id))
        super().__init__(**kwargs)

    async def start_jobs(self, job_ids):
        # Filter before ARQ's normal atomic claim, not after it has consumed a
        # job. A running backup or another player's chain is never touched.
        for raw_id in job_ids:
            ident = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            info = await Job(ident, self.pool, _queue_name=self.queue_name).info()
            if belongs_to_run(info, self.ingestion_run_id):
                await super().start_jobs([ident.encode()])
                if self.job_counter >= self.max_jobs:
                    break


async def run_status(run_id):
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        return await session.scalar(text('SELECT status FROM ingestion_pipeline_run WHERE run_id=:id'), {'id':run_id})


async def finish_when_terminal(worker, run_id):
    while True:
        await asyncio.sleep(3)
        if worker.job_counter:
            continue
        try:
            status = await run_status(run_id)
        except Exception:
            logging.exception('Could not check ingestion completion; helper stays available')
            continue
        if status != 'running':
            worker.allow_pick_jobs = False
            if worker.main_task:
                worker.main_task.cancel()
            return status


async def run(run_id):
    run_id = str(UUID(run_id))
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    worker = monitor = None
    try:
        status = await run_status(run_id)
        if status != 'running':
            raise ValueError(f'Ingestion run is not running: {status!r}')
        worker = IngestionAssistWorker(run_id=run_id,
            functions=[func(run_fen_pipeline, timeout=86400), func(run_tablebase_analysis_job, timeout=86400)],
            queue_name='pipeline_queue', redis_settings=redis_settings, max_jobs=1,
            queue_read_limit=100, poll_delay=1, health_check_interval=30,
            health_check_key=f'chessism:ingestion_assist:{run_id}:health', log_results=False)
        monitor = asyncio.create_task(finish_when_terminal(worker, run_id))
        try:
            await worker.async_run()
        except asyncio.CancelledError:
            if not monitor.done():
                raise
        if monitor.done():
            print(f'Ingestion helper finished: {monitor.result()}', flush=True)
    finally:
        if monitor:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if worker:
            await worker.close()
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args.run_id))
