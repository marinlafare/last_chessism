"""Priority cancellation signaling, independent of the result-import loop.

Runs under the same singleton controller lock. It never imports, launches,
deletes, advances phases or overwrites the importer's launch snapshots.
"""
import asyncio
import time
from types import SimpleNamespace
from sqlalchemy import select, text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudBatchUnit, CloudJobCancellation, CloudSequenceControl
from chessism_api.database.cloud_codec import read_documents
from .cancellation_cloud import stop_batch, stop_run

FINALIZING = {'finalizing', 'cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete', 'cancelled'}


async def pending_stops():
    async with AsyncDBSession() as session:
        return (await session.scalars(select(CloudJobCancellation.job_id).where(
            CloudJobCancellation.phase == 'stopping', CloudJobCancellation.finished_at.is_(None)))).all()


async def snapshot(job_id):
    async with AsyncDBSession() as session:
        # Launch documents are immutable versions. Read their references and
        # checksum-verified contents from one snapshot, without locking imports.
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        phase = await session.scalar(select(CloudJobCancellation.phase).where(CloudJobCancellation.job_id == job_id))
        if phase != 'stopping':
            return None
        control = await session.get(CloudSequenceControl, job_id)
        if control and control.phase in FINALIZING:
            return None  # Nothing to interrupt; ordinary cleanup owns its inventory.
        rows = (await session.execute(select(CloudAnalysisRun.id, CloudAnalysisRun.status,
            CloudAnalysisRun._launch_ref.label('launch_ref'), CloudAnalysisRun._cleanup_plan_ref.label('cleanup_ref'))
            .where(CloudAnalysisRun.job_id == job_id,
                   ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.run_id)),
                   ~CloudAnalysisRun.status.in_(FINALIZING)))).all()
        connection = await session.connection()
        launches = await connection.run_sync(read_documents, [row.launch_ref for row in rows if row.launch_ref])
        roots = [SimpleNamespace(id=row.id, status=row.status, launch=launches.get(row.launch_ref, {}),
                                 cleanup_plan=bool(row.cleanup_ref)) for row in rows]
        return roots, control


async def dispatch(controller, cloud, job_id):
    async with controller.stop_signal_lock:
        saved = await snapshot(job_id)
        if saved is None:
            return False
        roots, control = saved
        # Separate client/HTTP slots keep downloads from delaying stop requests.
        # Results are deliberately NOT persisted: the main state machine owns
        # all snapshots and will verify identities again before cleanup.
        await asyncio.to_thread(stop_batch, cloud, roots, control, signal=True, check_supervisors=False)
        for root in roots:
            if root.launch.get('backend') == 'cloud_run':
                await asyncio.to_thread(stop_run, cloud, root, signal=True)
        return True


async def serve(controller, cloud, interval=1):
    reported = set()
    while True:
        try:
            pending = await pending_stops()
            reported.intersection_update(pending)
            for job_id in pending:
                started = time.monotonic()
                if await dispatch(controller, cloud, job_id) and job_id not in reported:
                    print(f'Priority stop dispatched for {job_id} in {time.monotonic() - started:.2f}s.', flush=True)
                    reported.add(job_id)
        except Exception as exc:
            # Durable intent stays pending. The ordinary state machine reports
            # detailed errors; never turn a transient dispatch error into a launch.
            print(f'Priority stop will retry ({type(exc).__name__}).', flush=True)
            await asyncio.sleep(4)
        await asyncio.sleep(interval)
