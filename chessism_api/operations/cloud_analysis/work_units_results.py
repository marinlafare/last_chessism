"""One bounded unit at a time; parent completion contains only unit proofs."""
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import json

from sqlalchemy import select, func
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudBatchUnit, CloudFenClaim, CloudResultBatch
from chessism_api.database.cloud_codec import checksum
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from stockfish_batch.performance import validate_performance
from .ingestion import ingest_ready
from .importer import validate_manifest


def proof(launch):
    return [{'index': u['index'], 'run_id': u['run_id'], 'count': u['count'],
             'fingerprint': u['contract']['fingerprint'], 'manifest_sha256': u['manifest_sha256']}
            for u in launch['units']]


async def import_unit(cloud, unit):
    storage = await asyncio.to_thread(cloud.storage)
    async with AsyncDBSession() as session:
        run = await session.get(CloudAnalysisRun, unit['run_id'])
    if run.results_verified_at:
        return {**unit, 'verified': True, 'manifest_sha256': run.completion_manifest_sha256,
                'performance': run.launch['performance']}
    await ingest_ready(storage, run, [{'output': unit['config']['output'], 'count': unit['count'],
                                      'contract': unit['contract']}], storage_factory=cloud.storage)
    del run  # Never retain two input/receipt snapshots while catching up.
    async with AsyncDBSession() as session:
        imported = await session.scalar(select(CloudAnalysisRun.imported_count).where(
            CloudAnalysisRun.id == unit['run_id']))
    if imported != unit['count']:
        return unit
    raw = await asyncio.to_thread(storage.read, unit['config']['output'] + '/manifest.json', MAX_MANIFEST_BYTES)
    report = await asyncio.to_thread(storage.read, unit['config']['output'] + '/performance.json', 128 * 1024)
    if raw is None or report is None:
        return unit
    manifest, performance = json.loads(raw), validate_performance(json.loads(report), unit['contract'], 16)
    async with AsyncDBSession() as session, session.begin():
        run = await session.get(CloudAnalysisRun, unit['run_id'], with_for_update=True)
        validate_manifest(manifest, run.positions, run.contract, run.receipts)
        run.launch = {**run.launch, 'manifest': manifest, 'performance': performance}
        run.completion_manifest_sha256 = checksum(manifest)
        run.results_verified_at = datetime.now(timezone.utc)
        run.status = 'unit_verified'
    return {**unit, 'verified': True, 'manifest_sha256': checksum(manifest), 'performance': performance}


async def verify_saved(root, launch):
    """Fail closed using durable unit proofs and committed receipt counts.

    Full input/PV/manifest comparison already happened within each bounded unit.
    Cleanup cannot be authorized by UI counts or an unverified projection.
    """
    units = launch.get('units', [])
    if not units or any(not u.get('verified') or not u.get('manifest_sha256') for u in units):
        raise ValueError('Not all work units have verified results')
    if [u['index'] for u in units] != list(range(len(units))) or len({u['run_id'] for u in units}) != len(units):
        raise ValueError('Work-unit identity/order changed')
    async with AsyncDBSession() as session:
        saved = (await session.execute(select(CloudBatchUnit.unit_index, CloudBatchUnit.run_id,
            CloudBatchUnit.vm_index, CloudBatchUnit.position_count, CloudAnalysisRun.imported_count,
            CloudAnalysisRun.results_verified_at, CloudAnalysisRun.completion_manifest_sha256)
            .join(CloudAnalysisRun, CloudAnalysisRun.id == CloudBatchUnit.run_id)
            .where(CloudBatchUnit.root_run_id == root.id).order_by(CloudBatchUnit.unit_index))).all()
        if len(saved) != len(units):
            raise ValueError('Work-unit inventory changed')
        for row, unit in zip(saved, units):
            if (tuple(row[:5]) != (unit['index'], unit['run_id'], unit['vm_index'], unit['count'], unit['count'])
                    or not row.results_verified_at or row.completion_manifest_sha256 != unit['manifest_sha256']):
                raise ValueError('Work-unit database proof changed')
        run_ids = [u['run_id'] for u in units]
        receipts = await session.scalar(select(func.coalesce(func.sum(CloudResultBatch.position_count), 0))
            .where(CloudResultBatch.run_id.in_(run_ids)))
        claims = await session.scalar(select(func.count()).select_from(CloudFenClaim).where(CloudFenClaim.run_id.in_(run_ids)))
        if claims or receipts != sum(u['count'] for u in units) or receipts != root.position_count or receipts != root.imported_count:
            raise ValueError('Uncommitted work-unit results; cleanup withheld')
    if root.completion_manifest_sha256 and checksum(proof(launch)) != root.completion_manifest_sha256:
        raise ValueError('Parent completion proof changed')


def performance(launch):
    """Unit-success measurements, not billed time; include reused completed units."""
    from cloud_job.batch_spot import MACHINE
    vms, reports = [], []
    for task in launch['tasks']:
        units = [u for u in launch['units'] if u['vm_index'] == task['index']]
        parts = [u['performance'] for u in units]
        seconds = sum(p['metrics']['worker_seconds'] for p in parts)
        analyzed = sum(p['analyzed_this_attempt'] for p in parts)
        workers = defaultdict(lambda: {'positions': 0, 'analysis_seconds': 0.0, 'upload_wait_seconds': 0.0})
        for p in parts:
            for w in p['workers']:
                for key in ('positions', 'analysis_seconds', 'upload_wait_seconds'):
                    workers[w['worker']][key] += w[key]
        memory = [p['metrics'].get('memory', {}) for p in parts]
        metrics = {'worker_seconds': seconds, 'fen_per_second': analyzed / seconds if seconds else 0,
                   'vm_cpu_busy_percent': sum(p['metrics'].get('vm_cpu_busy_percent', 0) * p['metrics']['worker_seconds'] for p in parts) / seconds if seconds else 0,
                   'memory': {'cgroup_peak_bytes': max((m.get('cgroup_peak_bytes', 0) for m in memory), default=0),
                              'cgroup_oom_kills_delta': sum(m.get('cgroup_oom_kills_delta', 0) for m in memory),
                              'host_used_excluding_reclaimable': {'max_bytes': max((m.get('host_used_excluding_reclaimable', {}).get('max_bytes', 0) for m in memory), default=0)}}}
        report = {'analyzed_this_attempt': analyzed, 'resumed': sum(p['resumed'] for p in parts),
                  'position_count': sum(u['count'] for u in units), 'metrics': metrics,
                  'workers': [{'worker': ident, **value} for ident, value in sorted(workers.items())]}
        reports.append(report)
        vms.append({'index': task['index'], 'positions': report['position_count'],
                    **{k: report[k] for k in ('analyzed_this_attempt', 'resumed', 'workers', 'metrics')}})
    return {**launch, 'task_performance': reports, 'performance': {
        'backend': 'batch_spot', 'vm_count': len(vms), 'cpus_per_vm': 16, 'memory_gib_per_vm': 16,
        'machine_type': MACHINE, 'position_count': sum(u['count'] for u in launch['units']),
        'worker_seconds_sum': sum(p['metrics']['worker_seconds'] for p in reports), 'vms': vms,
        'note': 'Successful unit-attempt measurements only; container transitions and interrupted attempts are not billed-time measurements.'}}
