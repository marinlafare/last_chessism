"""Cheap saved progress for cloud-owned evaluation events; no graph/API calls."""
from sqlalchemy import text
from .sync_schema import available

TERMINAL = {'complete', 'limit_reached', 'cancelled', 'failed'}


def progress(job, row, control):
    terminal = job.status in TERMINAL
    total = int(row.get('total') or 0)
    processed = int(row.get('processed') or 0)
    unknown = int(row.get('unclassified') or 0)
    pending = int(row.get('pending') or 0)
    error = row.get('error')
    captured = int(row.get('captured') or 0)
    all_imported = terminal or job.imported >= job.target
    if error:
        status = 'retrying'
    elif pending:
        status = 'working'
    elif captured < job.imported:
        status = 'unavailable'  # Never invent missing per-job evidence.
    elif all_imported:
        status = 'complete'
    else:
        status = 'waiting' if captured else 'pending'
    detail = (f'{processed:,} graph positions synchronized; identifying remaining affected positions.' if unknown else
              f'{processed:,} / {total:,} affected graph positions synchronized')
    if status in {'pending','waiting'}:
        detail += ' · Waiting for additional imported results.'
    elif status == 'unavailable':
        detail = 'Job-specific synchronization records are incomplete; check System Status.'
    if error:
        detail += f' · Retrying: {error}'
    elif pending and control.get('phase') == 'waiting':
        status = 'waiting'
        detail += ' · ' + (control.get('detail') or 'Waiting for graph publication.')
    return {'status':status,'processed':processed,'total':None if unknown else total,
            'detail':detail,'started_at':row.get('started_at'),
            'finished_at':row.get('finished_at') if status=='complete' else None,
            'heartbeat_at':control.get('heartbeat'), 'scope':'affected_graph_positions'}


async def cloud_progress(session, jobs):
    if not jobs or not await available(session):
        return {}
    if not await session.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='graph_sync.control'::regclass AND attname='cloud_tracking_since' AND NOT attisdropped)")):
        return {}  # Older restored snapshots require explicit schema installation.
    control = dict((await session.execute(text('SELECT enabled,heartbeat,phase,detail,cloud_tracking_since FROM graph_sync.control WHERE id'))).mappings().one())
    if not control['enabled'] or not control['cloud_tracking_since']:
        return {}
    first = await session.scalar(text('SELECT min(created_at) FROM graph_sync.event'))
    recent = [job for job in jobs if job.created_at >= control['cloud_tracking_since']]
    rows = (await session.execute(text('''SELECT source_job_id,
        sum(cardinality(ids)) AS captured,
        sum(COALESCE(cardinality(matched_ids),0)) AS total,
        sum(CASE WHEN completed_at IS NOT NULL THEN COALESCE(cardinality(matched_ids),0) ELSE 0 END) AS processed,
        count(*) FILTER(WHERE matched_ids IS NULL) AS unclassified,
        count(*) FILTER(WHERE completed_at IS NULL) AS pending,
        max(error) FILTER(WHERE completed_at IS NULL) AS error,
        min(COALESCE(started_at,created_at)) AS started_at,max(completed_at) AS finished_at
        FROM graph_sync.event WHERE source_job_id=ANY(CAST(:ids AS text[])) AND kind='score'
        GROUP BY source_job_id'''), {'ids':[job.id for job in recent]})).mappings().all()
    by_job = {r['source_job_id']:r for r in rows}
    result = {job.id:progress(job,by_job.get(job.id,{}),control) for job in recent}
    # Earlier jobs have no durable ownership labels. Report queue catch-up
    # without fabricating per-job counts or matching events by timestamp.
    legacy = [job for job in jobs if first and first <= job.created_at < control['cloud_tracking_since']]
    if legacy:
        pending = await session.scalar(text("SELECT count(*) FROM graph_sync.event WHERE kind='score' AND completed_at IS NULL"))
        for job in legacy:
            result[job.id] = {'status':'complete' if not pending and job.status in TERMINAL else 'unavailable',
                'processed':None,'total':None,'started_at':None,'finished_at':None,
                'detail':('Graph synchronization is current. ' if not pending else '')+
                    'Per-job counts were not recorded for this earlier job.', 'scope':'legacy_queue_status'}
    return result
