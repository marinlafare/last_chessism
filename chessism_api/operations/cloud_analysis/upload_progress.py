"""Small upload counters only: no FEN payload reads, writes or cloud-job control."""
import httpx
from sqlalchemy import select, text
from fastapi import HTTPException

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudBatchUnit
from chessism_api.database.cloud_columns import TABLES
from chessism_api.database.cloud_codec import read_projections

AFTER_UPLOAD = {'submitting', 'run_creating', 'run_start', 'run_discover', 'running',
                'cloud_queued', 'finalizing', 'cleanup_planning', 'cleaning',
                'log_planning', 'log_cleaning', 'refreshing', 'complete'}


def observed_count(tasks, observation, run_id, expected):
    if observation.get('state') != 'ready':
        return None
    indices = observation.get('tasks')
    if (observation.get('run_id') != run_id or not isinstance(indices, list)
            or len(indices) > 1200 or any(type(i) is not int for i in indices)
            or len(indices) != len(set(indices))):
        raise ValueError('Invalid upload observation')
    counts = {row['index']: row['count'] for row in tasks}
    if (len(counts) != len(tasks) or set(counts) != set(range(len(tasks)))
            or any(type(n) is not int or not 1 <= n <= 5000 for n in counts.values())
            or sum(counts.values()) != expected or not set(indices) <= counts.keys()):
        raise ValueError('Upload observation does not match saved tasks')
    return sum(counts[i] for i in indices)


async def read_progress(job_id):
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='3s'"))
        job = (await session.execute(select(CloudAnalysisJob._selection_ref,
            CloudAnalysisJob.status).where(CloudAnalysisJob.id == job_id))).one_or_none()
        if job is None:
            raise HTTPException(404, 'Cloud job not found')
        runs = (await session.execute(select(CloudAnalysisRun.id, CloudAnalysisRun.status,
            CloudAnalysisRun.position_count, CloudAnalysisRun._launch_ref).where(
            CloudAnalysisRun.job_id == job_id,
            ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.run_id))))).all()
        connection = await session.connection()
        documents = await connection.run_sync(read_projections, [job[0]], {'backend', 'execution_mode'})
        selection = documents[job[0]]
        details = []
        kind = 'task' if selection.get('backend') == 'cloud_run' else 'work_unit'
        table = TABLES[kind]
        for run in runs:
            tasks = []
            if run.status == 'uploading':
                fields = [table.c.index, table.c.count]
                if kind == 'work_unit':
                    fields.append(table.c.uploaded)
                tasks = [dict(row) for row in (await session.execute(select(*fields).where(
                    table.c.document_id == run._launch_ref, table.c.parent_path == '$'))).mappings()]
            details.append((run.id, run.status, run.position_count, tasks))
    total = sum(row[2] for row in details)
    result = {'total_fens': total, 'uploaded_fens': None, 'state': 'unavailable'}
    if not total or job.status in {'cancelled', 'failed'}:
        return result
    uploaded = 0
    for run_id, status, count, tasks in details:
        if status in AFTER_UPLOAD:
            uploaded += count
        elif status in {'preparing', 'publishing'}:
            continue
        elif status != 'uploading':
            return result
        elif kind == 'work_unit':
            if (selection.get('execution_mode') != 'batch_sequence_v2'
                    or not tasks or sum(row['count'] for row in tasks) != count):
                return result
            uploaded += sum(row['count'] for row in tasks if row.get('uploaded') is True)
        else:
            try:
                async with httpx.AsyncClient(timeout=3, trust_env=False, follow_redirects=False) as client:
                    response = await client.get(f'http://cloud-billing:8080/upload-progress/{run_id}')
                    response.raise_for_status()
                    observed = observed_count(tasks, response.json(), run_id, count)
                if observed is None:
                    return {**result, 'state': 'pending'}
                uploaded += observed
            except (httpx.HTTPError, ValueError, TypeError, AttributeError):
                return result
    return {**result, 'uploaded_fens': uploaded, 'state': 'ready'}
