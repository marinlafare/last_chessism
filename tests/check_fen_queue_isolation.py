"""Synthetic real-Redis ARQ claims. Use a disposable Redis with no network access."""
import asyncio
import os
from contextlib import suppress
from uuid import uuid4
os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')

from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import JobStatus
from arq.worker import func
from chessism_api.operations.ingestion_pipeline.assist_worker import IngestionAssistWorker


async def main():
    pool=await create_pool(RedisSettings(host='127.0.0.1'))
    run_id=str(uuid4());calls=[];workers=[];tasks=[]
    async def extraction(ctx,ingestion_run_id):
        calls.append(('extract',ingestion_run_id))
        await asyncio.sleep(.05)
        await ctx['redis'].enqueue_job('run_tablebase_analysis_job',ingestion_run_id=ingestion_run_id,_queue_name='pipeline_queue')
    async def endgames(ctx,ingestion_run_id):
        calls.append(('endgames',ingestion_run_id))
        await asyncio.sleep(.05)
    try:
        backup=await pool.enqueue_job('run_fen_analysis_backup_job',_queue_name='pipeline_queue')
        await pool.set('arq:in-progress:'+backup.job_id,b'backup-still-running',ex=120)
        other=await pool.enqueue_job('run_fen_pipeline',ingestion_run_id='other',_queue_name='pipeline_queue')
        target=await pool.enqueue_job('run_fen_pipeline',ingestion_run_id=run_id,_queue_name='pipeline_queue')
        for i in range(2):
            worker=IngestionAssistWorker(run_id=run_id,functions=[func(extraction,name='run_fen_pipeline'),
                func(endgames,name='run_tablebase_analysis_job')],redis_settings=RedisSettings(host='127.0.0.1'),
                queue_name='pipeline_queue',max_jobs=1,queue_read_limit=100,poll_delay=.02,
                handle_signals=False,health_check_key=f'test-assist:{i}')
            workers.append(worker);tasks.append(asyncio.create_task(worker.async_run()))
        for _ in range(400):
            if sum(w.jobs_complete for w in workers)==2 and all(w.job_counter==0 for w in workers):break
            await asyncio.sleep(.025)
        assert calls==[('extract',run_id),('endgames',run_id)],calls
        assert sum(w.jobs_complete for w in workers)==2
        assert await target.status()==JobStatus.complete
        assert await other.status()==JobStatus.queued
        assert await backup.status()==JobStatus.in_progress
        assert await pool.get('arq:in-progress:'+backup.job_id)==b'backup-still-running'
        print('PASS: two competing helpers ran one extraction and its follow-up exactly once; running backup and unrelated ingestion untouched')
    finally:
        for task in tasks:task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):await task
        for worker in workers:await worker.close()
        await pool.close()


if __name__=='__main__':asyncio.run(main())
