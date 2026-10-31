"""One bounded export at a time, with a consistent snapshot and atomic publication."""
import asyncio
from datetime import timedelta
import hashlib
import logging
import os
import time

from arq import cron
from arq.worker import func
from sqlalchemy import select, text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.redis_client import redis_settings
from . import config, files
from .catalog import SOURCES, tables_for
from .models import ExportClient, ExportJob
from .query import ExportSpec, compile_query, effective_order
from .service import WORKER_LOCK, enqueue, now
from .writer import ParquetOutput
from .analytical_spec import parse_request
from .analytical_reader import AnalyticalError

LOG = logging.getLogger(__name__)


class ExportCancelled(Exception):
    pass


async def disk_work(function, *args):
    # Cancellation cannot stop a thread; wait for it before closing/deleting files.
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


async def checkpoint(job_id, **values):
    async with AsyncDBSession() as session:
        job = await session.get(ExportJob, job_id, with_for_update=True)
        client = await session.get(ExportClient, job.client_id)
        if job.cancel_requested or not client or client.revoked_at or client.expires_at <= now():
            raise ExportCancelled()
        for name, value in values.items():
            setattr(job, name, value)
        await session.commit()


async def construct(job):
    document = files.read_request(job.id)
    if hashlib.sha256(files.canonical_bytes(document)).hexdigest() != job.request_sha256:
        raise ValueError('Stored request checksum mismatch.')
    spec = parse_request(document)
    if spec.source in ('sql', 'move_matrix'):
        from .analytical import construct as construct_analytical
        return await construct_analytical(job, spec, document)
    directory = files.job_dir(job.id)
    temporary = directory / 'data.parquet.building'
    if temporary.exists():
        raise ValueError('An interrupted export must be requested again with a new idempotency key.')
    await disk_work(files.ensure_space)
    writer = ParquetOutput(temporary, spec)
    count, truncated, snapshot, snapshot_at = 0, False, None, None
    last_progress = time.monotonic()
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            await session.execute(text("SET LOCAL statement_timeout = '60s'"))
            await session.execute(text("SET LOCAL lock_timeout = '2s'"))
            await session.execute(text('SET LOCAL max_parallel_workers_per_gather = 1'))
            await session.execute(text('SET LOCAL jit = off'))
            snapshot_at, snapshot = (await session.execute(text('SELECT transaction_timestamp(), pg_current_snapshot()::text'))).one()
            await checkpoint(job.id, phase='reading', snapshot_at=snapshot_at)
            query, params = compile_query(spec, tables_for(session))
            stream = await session.stream(query, params, execution_options={'yield_per': config.BATCH_ROWS})
            try:
                async for rows in stream.mappings().partitions(config.BATCH_ROWS):
                    selected = rows[:max(0, spec.limit-count)]
                    truncated = truncated or len(selected) < len(rows)
                    if selected:
                        await disk_work(writer.write, selected)
                        count += len(selected)
                    # Check cancellation/revocation at every bounded batch.
                    progress = time.monotonic() - last_progress >= 1
                    await checkpoint(job.id, **({'rows_written': count} if progress else {}))
                    if progress:
                        last_progress = time.monotonic()
            finally:
                await stream.close()
        # Release the database snapshot before footer validation and hashing.
    finally:
        await disk_work(writer.close)
    await checkpoint(job.id, phase='finalizing', rows_written=count)
    size = temporary.stat().st_size
    if size > config.MAX_BYTES:
        raise ValueError('Export exceeds the configured file-size limit.')
    digest = await disk_work(files.sha256, temporary)
    completed = now()
    expires = completed + timedelta(seconds=config.RETENTION)
    manifest = {
        'format': 'parquet', 'format_version': 1, 'catalog_version': 1, 'export_id': job.id,
        'source': spec.source, 'row_meaning': SOURCES[spec.source].meaning,
        'source_definition': document, 'request_sha256': job.request_sha256,
        'filters': spec.where, 'selection_order': effective_order(spec), 'row_limit': spec.limit,
        'row_count': count, 'limit_reached': truncated,
        'columns': [{'name': key, 'type': SOURCES[spec.source].columns[key].dtype,
                     'nullable': True, 'description': SOURCES[spec.source].columns[key].description} for key in spec.select],
        'snapshot': {'isolation': 'repeatable_read', 'started_at': snapshot_at.isoformat(), 'identifier': snapshot,
                     'note': 'Informational identity, not a reusable PostgreSQL snapshot.'},
        'created_at': job.created_at.isoformat(), 'completed_at': completed.isoformat(),
        'expires_at': expires.isoformat(), 'file': {'name': 'data.parquet', 'size_bytes': size, 'sha256': digest},
    }
    await disk_work(files.write_json, directory/'manifest.json', manifest)
    os.replace(temporary, directory/'data.parquet')
    # The same row lock serializes cancellation and completion publication.
    await checkpoint(job.id, status='complete', phase='complete', rows_written=count,
                     file_bytes=size, file_sha256=digest, limit_reached=truncated,
                     completed_at=completed, expires_at=expires)


async def run_export(ctx, job_id):
    async with AsyncDBSession() as guard:
        locked = await guard.scalar(text('SELECT pg_try_advisory_xact_lock(:key)'), {'key': WORKER_LOCK})
        if not locked:
            return {'status': 'busy'}  # Durable queued row is retried by maintenance.
        async with AsyncDBSession() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if not job or job.status != 'queued':
                return {'status': 'not_queued'}
            job.status, job.phase, job.started_at = 'running', 'starting', now()
            await session.commit()
        try:
            await checkpoint(job.id)
            await asyncio.wait_for(construct(job), timeout=config.TIMEOUT)
            return {'status': 'complete'}
        except (Exception, asyncio.CancelledError) as error:
            cancelled = isinstance(error, (ExportCancelled, asyncio.CancelledError))
            # Never return SQL, credentials, filesystem paths or database rows.
            detail = ('Export cancelled or integration credential revoked.' if cancelled else
                      'Export failed or exceeded its resource limits. Narrow the request and retry with a new key.')
            code = 'EXPORT_CANCELLED' if cancelled else 'EXPORT_FAILED'
            if isinstance(error, AnalyticalError):
                code, detail = error.code, str(error)
            elif isinstance(error, asyncio.TimeoutError):
                code, detail = 'EXPORT_TIMEOUT', 'Export exceeded its execution deadline; no partial file was published.'
            LOG.warning('Export %s stopped (%s)', job.id, type(error).__name__)
            await disk_work(files.remove_files, job.id)
            async with AsyncDBSession() as session:
                row = await session.get(ExportJob, job.id, with_for_update=True)
                row.status = row.phase = 'cancelled' if cancelled else 'failed'
                row.error, row.error_code, row.completed_at = detail[:500], code, now()
                await session.commit()
            if isinstance(error, asyncio.CancelledError):
                raise
            return {'status': 'cancelled' if cancelled else 'failed'}


async def maintain_exports(ctx):
    async with AsyncDBSession() as guard:
        if not await guard.scalar(text('SELECT pg_try_advisory_xact_lock(:key)'), {'key': WORKER_LOCK}):
            return
        # This lock proves no live exporter owns a snapshot/files right now.
        async with AsyncDBSession() as session:
            rows = (await session.scalars(select(ExportJob).where(
                ExportJob.status.in_(('queued', 'running')) |
                ((ExportJob.expires_at <= now()) & ExportJob.status.not_in(('deleted', 'expired')))
            ).limit(100))).all()
            for job in rows:
                if job.status == 'running':
                    job.status = job.phase = 'failed'
                    job.error = 'Export interrupted by a worker restart. Submit a new request key.'
                    job.completed_at = now()
                    await disk_work(files.remove_files, job.id)
                elif job.expires_at and job.expires_at <= now():
                    await disk_work(files.remove_files, job.id)
                    job.status = job.phase = 'expired'
                elif job.status == 'queued':
                    await enqueue(ctx['redis'], job.id)
            await session.commit()
    # Clean abandoned pre-commit directories only after the retention window;
    # the namespace contains no caller-chosen directory names.
    if config.ROOT.exists():
        for path in config.ROOT.iterdir():
            if len(path.name) != 32 or not path.is_dir() or path.is_symlink():
                continue
            try:
                files.job_dir(path.name)
            except ValueError:
                continue
            if path.stat().st_mtime > time.time()-config.RETENTION:
                continue
            async with AsyncDBSession() as session:
                row = await session.get(ExportJob, path.name)
                if row is None:
                    await disk_work(files.remove_files, path.name)


async def startup(ctx):
    ctx['engine'] = await bind_runtime_engine(os.environ['DATABASE_URL'])
    await maintain_exports(ctx)


async def shutdown(ctx):
    await ctx['engine'].dispose()


class WorkerSettings:
    functions = [func(run_export, timeout=config.TIMEOUT+60, keep_result=0, max_tries=1)]
    cron_jobs = [cron(maintain_exports, second={0, 30}, keep_result=0)]
    redis_settings = redis_settings
    queue_name = config.QUEUE
    max_jobs = 1
    on_startup = startup
    on_shutdown = shutdown
