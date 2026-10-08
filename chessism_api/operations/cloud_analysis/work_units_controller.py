"""Large logical job, bounded database runs, sequential Google-owned VM units."""
import asyncio
from copy import deepcopy
from dataclasses import asdict
import hashlib
from uuid import uuid4

from sqlalchemy import insert
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudBatchUnit, CloudFenClaim
from cloud_job import batch_spot, work_units
from cloud_job.launch import inspect_worker, expected_contract
from cleaning_job import cleaning_job
from cleaning_job.cleanup import prepare, check_bucket
from stockfish_batch.checkpoints import encode, parse_input
from stockfish_batch import __version__
from stockfish_batch.storage import MAX_INPUT_BYTES
from stockfish_core import ENGINE_SHA256, PROFILE_ID
from . import history, runtime, work_units_results as results
from .preflight import verify_clean_workspace, parallel_checks
from .preparation import PreparationTimings, refresh_claim_statistics
from .log_cleanup import plan_logs, remove_logs
from .importer import refresh_unit_projections


async def reserve_unit(job, root, launch, vm, limit):
    from chessism_api.database.ask_db import get_fens_for_analysis, get_player_fens_for_analysis, get_game_set_fens_for_analysis
    selection, timings = job.selection, PreparationTimings()
    session, timing_run_id = None, root.id
    try:
        with timings.measure('prepare_statistics'):
            await refresh_claim_statistics()
        if selection['mode'] == 'games':
            session, fens = await get_game_set_fens_for_analysis(selection['game_links'], limit, raise_errors=True, timings=timings)
        elif selection.get('player_name'):
            session, fens = await get_player_fens_for_analysis(selection['player_name'], limit, raise_errors=True, timings=timings)
        else:
            session, fens = await get_fens_for_analysis(limit, raise_errors=True, timings=timings)
        if session is None:
            return 0
        with timings.measure('prepare_encode'):
            positions = [{'id': hashlib.sha256(fen.encode()).hexdigest(), 'fen': fen} for fen in fens]
            raw = b''.join(map(encode, positions))
        with timings.measure('prepare_validate'):
            parse_input(raw, limit)
            ident, index = uuid4().hex, len(launch['units'])
            cfg = batch_spot.config_for(ident, len(positions))
            contract = expected_contract(raw, positions, cfg, {'engine_sha': ENGINE_SHA256,
                'worker_version': __version__, 'chess_version': '1.11.2'})
        with timings.measure('prepare_persist'):
            child = CloudAnalysisRun(id=ident, job_id=job.id, status='unit_pending', positions=positions,
                contract=contract, receipts={}, launch={'backend': 'batch_spot', 'config': asdict(cfg)})
            session.add(child)
            await session.flush()
            session.add(CloudBatchUnit(root_run_id=root.id, unit_index=index, run_id=ident,
                                      vm_index=vm, position_count=len(positions)))
            owner = await session.get(CloudAnalysisRun, root.id, with_for_update=True)
            owner.launch = {**launch, 'units': [*launch['units'], {'index': index, 'vm_index': vm,
                'run_id': ident, 'count': len(positions), 'config': asdict(cfg), 'contract': contract}]}
            owner.position_count += len(positions)
            await session.flush()
        with timings.measure('prepare_claims'):
            for offset in range(0, len(fens), 1000):
                await session.execute(insert(CloudFenClaim), [{'fen': fen, 'run_id': ident} for fen in fens[offset:offset + 1000]])
        with timings.measure('prepare_commit'):
            await session.commit()
        timing_run_id = ident
        return len(positions)
    finally:
        if session is not None:
            await session.close()
        await timings.save(job.id, timing_run_id)


def lanes(launch):
    tasks = []
    for index in sorted({u['vm_index'] for u in launch['units']}):
        group = [u for u in launch['units'] if u['vm_index'] == index]
        # Recovery control documents live inside the first unit's input scope,
        # which is explicitly present in the saved runnable/cleanup plan.
        tasks.append({'index': index, 'start': 0, 'count': sum(u['count'] for u in group),
            'run_id': group[0]['run_id'], 'config': group[0]['config'],
            'status': 'PENDING', 'jobs': [], 'recovery_session': 1})
    return tasks


async def stopped(cloud, launch):
    for task in launch['tasks']:
        await asyncio.to_thread(cloud.require_recovery_stopped, task['run_id'], task)


async def advance(controller, job, run):
    from .controller import save_run, save_job
    cloud, launch = controller.cloud, deepcopy(run.launch)
    if run.status == 'preparing':
        frozen_cycle = job.selection.get('execution_mode') == 'batch_sequence_v1'
        if not launch.get('image_id'):
            timings = PreparationTimings()
            try:
                with timings.measure('prepare_inventory'):
                    await asyncio.to_thread(verify_clean_workspace, cloud)
                with timings.measure('prepare_requirements'):
                    quota, _, _ = await asyncio.to_thread(parallel_checks,
                        lambda: batch_spot.check_quota(cloud, job.selection['n_vms']),
                        cloud.require_recovery, lambda: check_bucket(cloud))
                # Inspect/publish only after every safety check has succeeded.
                with timings.measure('prepare_image_check'):
                    image_id, _ = await asyncio.to_thread(inspect_worker, controller.local_image)
            finally:
                await timings.save(job.id, run.id)
            launch = {**launch, 'backend': 'batch_spot', 'multi_vm': True, 'workflow_version': 3,
                'profile': PROFILE_ID, 'units': [], 'tasks': [], 'jobs': [], 'image_id': image_id,
                'n_vms': job.selection['n_vms'], 'recovery_session': 1, 'quota': quota}
            if frozen_cycle:
                launch['units'] = deepcopy(run.launch['units'])
            await save_run(run.id, launch=launch, error=None)
        if frozen_cycle:
            if not launch['units'] or sum(u['count'] for u in launch['units']) != run.position_count:
                raise ValueError('Frozen cycle membership is incomplete')
            schedule = []  # All FENs were reserved before loop 1; never reselect.
        else:
            schedule = work_units.ranges(job.target, job.selection['n_vms'])
        if not frozen_cycle and len(launch['units']) < len(schedule):
            vm, count = schedule[len(launch['units'])]
            reserved = await reserve_unit(job, run, launch, vm, count)
            if reserved == count:
                controller.continue_immediately = True
                return
            async with AsyncDBSession() as session:
                owner = await session.get(CloudAnalysisRun, run.id)
                launch = owner.launch
            if not launch['units']:
                await save_job(job.id, status='waiting', error='No unclaimed, unscored FENs available. Resume to check again.')
                return
        launch['tasks'] = lanes(launch)
        await save_run(run.id, launch=launch, status='publishing', error=None)
    elif run.status == 'publishing':
        await asyncio.to_thread(check_bucket, cloud)
        launch['image'] = await asyncio.to_thread(cloud.publish, run.id, launch['image_id'])
        for task in launch['tasks']:
            task['spec'] = work_units.spec_for([u for u in launch['units'] if u['vm_index'] == task['index']], launch['image'])
        await save_run(run.id, launch=launch, status='uploading', error=None)
    elif run.status == 'uploading':
        storage = await asyncio.to_thread(cloud.storage)
        # One input in memory, never concatenate the logical job's FEN set.
        for unit in launch['units']:
            async with AsyncDBSession() as session:
                child = await session.get(CloudAnalysisRun, unit['run_id'])
            raw = b''.join(map(encode, child.positions))
            del child
            created = await asyncio.to_thread(storage.create, unit['config']['input'], raw)
            if not created and await asyncio.to_thread(storage.read, unit['config']['input'], MAX_INPUT_BYTES) != raw:
                raise ValueError('Work-unit input already exists with different contents')
            del raw
        await save_run(run.id, status='submitting', error=None)
    elif run.status == 'submitting':
        pending = [t for t in launch['tasks'] if t['status'] != 'SUCCEEDED']
        if pending and not any(t.get('recovery_execution') for t in pending) and not job.selection.get('cancel_requested'):
            await asyncio.to_thread(batch_spot.check_quota, cloud, len(pending))
        for task in pending:
            if job.selection.get('cancel_requested'):
                await asyncio.to_thread(cloud.cancel_recovery, task['run_id'], task['recovery_session'])
            if not task.get('recovery_execution'):
                task['recovery_execution'] = await asyncio.to_thread(cloud.start_recovery, task['run_id'], task)
                await save_run(run.id, launch=deepcopy(launch), error=None)
        await save_run(run.id, launch=launch, status='running', error=None)
    elif run.status == 'running':
        await poll(controller, job, run, launch)
    elif run.status in {'finalizing', 'cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning'}:
        await results.verify_saved(run, launch)
        if run.status == 'finalizing':
            await save_run(run.id, status='cleanup_planning', error=None)
        elif run.status == 'cleanup_planning':
            await stopped(cloud, launch)
            plan = await asyncio.to_thread(prepare, cloud, launch['jobs'], include_failed=True)
            await save_run(run.id, cleanup_plan=plan, status='cleaning', error=None)
        elif run.status == 'cleaning':
            await stopped(cloud, launch)
            report = await asyncio.to_thread(cleaning_job, plan=run.cleanup_plan, execute=True,
                cloud=cloud, wait_seconds=0, plan_directory=runtime.CLEANUP_ROOT)
            launch['cleanup_report'] = report
            await save_run(run.id, launch=launch, status='log_planning' if report['complete'] else 'cleaning', error=None)
        elif run.status == 'log_planning':
            plans = await history.cleanup_plans(run.id)
            launch['log_cleanup_plan'] = await asyncio.to_thread(plan_logs, cloud,
                {j['uid'] for plan in plans if plan for j in plan.get('jobs', [])})
            await save_run(run.id, launch=launch, status='log_cleaning', error=None)
        else:
            report = await asyncio.to_thread(remove_logs, cloud, launch['log_cleanup_plan'])
            launch['log_cleanup_report'] = report
            await save_run(run.id, launch=launch, status='refreshing' if report['complete'] else 'log_cleaning', error=None)
    elif run.status == 'refreshing':
        if not all(unit.get('refreshed') for unit in launch['units']):
            await refresh_unit_projections([unit['run_id'] for unit in launch['units']])
            launch['units'] = [{**unit, 'refreshed': True} for unit in launch['units']]
            await save_run(run.id, launch=launch, error=None)
            controller.continue_immediately = True
            return
        for unit in launch['units']:
            async with AsyncDBSession() as session, session.begin():
                child = await session.get(CloudAnalysisRun, unit['run_id'])
                if child.status == 'complete':
                    continue
                child.cleanup_completed_at = run.cleanup_completed_at
                child.status = 'refreshing'
            del child
            await history.compact_run(unit['run_id'])
            controller.continue_immediately = True
            return
        await history.compact_run(run.id)
    elif run.status == 'failed':
        await save_job(job.id, status='failed', error=run.error)
    else:
        raise ValueError('Unknown work-unit phase: ' + run.status)


async def poll(controller, job, run, launch):
    from .controller import save_run, save_job
    cloud, terminal = controller.cloud, {'SUCCEEDED', 'FAILED', 'CANCELLED'}
    for task in launch['tasks']:
        if job.selection.get('cancel_requested') and task['status'] not in terminal:
            await asyncio.to_thread(cloud.cancel_recovery, task['run_id'], task['recovery_session'])
        state, execution = await asyncio.to_thread(cloud.recovery_snapshot, task['run_id'], task)
        task['status'] = 'RECOVERY_STARTING'
        if state:
            task.update(jobs=state['jobs'], recovery_summary=state, recovery_execution=state['execution'])
            if state['current_job']:
                remote = await asyncio.to_thread(cloud.job, state['current_job'])
                if not remote or remote.get('uid') != state['uids'][state['current_job']]:
                    raise ValueError('Batch work-unit job identity changed')
                work_units.verify_job(remote, task['spec'])
                task['batch_status'] = remote['status']
                task['status'] = remote['status']['state']
            if execution['state'] == 'SUCCEEDED' and state['status'] in terminal:
                task['status'] = state['status']
            elif task['status'] in terminal:
                task['status'] = 'RECOVERING'
    for index, unit in enumerate(launch['units']):
        if not unit.get('verified'):
            launch['units'][index] = await results.import_unit(cloud, unit)
    launch['jobs'] = [name for task in launch['tasks'] for name in task['jobs']]
    launch['vm_statuses'] = [{'index': t['index'], 'positions': t['count'], 'state': t['status'],
        'preemptions': t.get('recovery_summary', {}).get('preemptions', 0),
        'application_failures': t.get('recovery_summary', {}).get('application_failures', 0)} for t in launch['tasks']]
    states = [t['status'] for t in launch['tasks']]
    finished = all(s in terminal for s in states)
    state = 'SUCCEEDED' if all(s == 'SUCCEEDED' for s in states) else 'FAILED' if finished else 'RUNNING'
    if state == 'SUCCEEDED':
        async with AsyncDBSession() as session:
            current = await session.get(CloudAnalysisRun, run.id)
        await results.verify_saved(current, launch)
        launch = results.performance(launch)
        await save_run(run.id, launch=launch, cloud_state=state, status='finalizing', error=None)
    else:
        await save_run(run.id, launch=launch, cloud_state=state, error=None)
        if finished:
            message = 'Batch work units stopped; committed results and completed units are retained for recovery.'
            await save_run(run.id, status='failed', error=message)
            await save_job(job.id, status='failed', error=message)
