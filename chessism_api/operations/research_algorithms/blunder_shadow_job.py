"""500-game smoke followed by up to 6,000 shadow games; never canonical."""
import asyncio
import hashlib
import inspect
import json
import time
import uuid

import numpy as np
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_shadow_source as source, blunder_shadow_compute as compute, blunder_shadow_store as store
from . import blunder_fixed, blunder_v2_model as model
from .blunder_development_reuse import digest
from .blunder_v2_job import cancellation_check
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = 'rating_blunder_shadow_pilot_v1'
FORMAT = 'chessism_blunder_shadow_pilot_v1'


def configuration(parent_id, row, players):
    parent_id = str(uuid.UUID(str(parent_id)))
    if (row is None or row['status'] != 'complete' or (row['result'] or {}).get('status') != 'frozen_final_evaluation'
            or not row['result'].get('cpu_cuda_passed') or row['result'].get('canonical') is not False
            or row['result'].get('production_changed') is not False
            or row['result'].get('final_evaluation', {}).get('refitted') is not False):
        raise ValueError('Choose a completed, numerically verified frozen final evaluation; this pilot does not approve it for production.')
    names = [str(p).strip().lower() for p in players]
    if len(names) != 6 or len(set(names)) != 6 or any(not n or len(n) > 128 for n in names):
        raise ValueError('Choose six different complete player profiles.')
    fits = row['result']['fits']
    if [f['mode'] for f in fits] != list(model.MODES) or any(f['status'] != 'fitted' for f in fits):
        raise ValueError('All three frozen mode parameter sets are required.')
    return {'operation': OPERATION, 'implementation_version': 2, 'summary_version': 2, 'backend': 'cuda_cupy', 'parent_run_id': parent_id,
        'parent_report_sha256': digest(row['result']), 'model_id': row['result']['model_id'], 'fits_sha256': digest(fits),
        'players': names, 'quotas_per_player': dict(source.QUOTAS), 'smoke_games': 500, 'max_games': 6000,
        'max_rows': source.MAX_POSITIONS, 'columns': ['before', 'after', 'player_rating', 'opponent_rating', 'mode'],
        'k0': .00368208, 'threshold': .2, 'mate_rules': 'existing_700cp_v1',
        'sample_only': True, 'independent_calibration_test': False, 'canonical': False}


async def load_parent(session, config):
    row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
        {'id': config['parent_run_id']})).mappings().one_or_none()
    if config != configuration(config['parent_run_id'], row, config['players']):
        raise ValueError('Frozen model or pilot instructions changed.')
    if model.K0 != config['k0'] or model.THRESHOLD != config['threshold']:
        raise ValueError('The inference formula changed; create a separate model version.')
    return row['result']


async def calculate_file(xp, path, games, fits, check):
    rows = np.memmap(path, dtype=blunder_fixed.DTYPE, mode='r')
    try:
        return await file_operation(compute.calculate, xp, rows, games, fits, check)
    finally:
        rows._mmap.close()


async def execute(config, reporter):
    await reporter.check(); started = time.perf_counter()
    xp, hardware = await file_operation(cuda_backend, 0)
    ensure_capacity(config)
    check = cancellation_check(reporter, asyncio.get_running_loop())
    with workspace(reporter.run_id) as folder:
        path = folder/'transitions.bin'
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='30s'"))
            await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
            await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
            if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
                raise ValueError('Wait for the active cloud job; shadow extraction does not compete with it.')
            parent = await load_parent(session, config)
            await reporter.progress('selecting_shadow_games', detail='Six players, deterministic mode quotas, unique games; no outcomes used for selection.')
            selection_start = time.perf_counter()
            games = await source.select_games(session, config['players'], reporter)
            selection_seconds = time.perf_counter()-selection_start
            if len(games) < 500:
                raise ValueError('Fewer than 500 eligible pilot games; smoke test not started.')
            snapshot = str(await session.scalar(text('SELECT transaction_timestamp()')))
            with path.open('xb') as stream:
                smoke_rows, timings = await source.extract(session, games[:500], reporter, stream)
                await file_operation(stream.flush)
                await reporter.progress('checking_shadow_smoke', detail='500-game smoke: saved CPU, independent SQL, scalar CPU and GPU must agree within each rule.')
                smoke = await calculate_file(xp, path, games[:500], parent['fits'], check)
                if smoke['status'] != 'passed':
                    raise ValueError('500-game smoke failed; full pilot not started: '+json.dumps(smoke['checks']))
                saved, first_write = await store.publish(reporter, config['model_id'], smoke['records'], 0)
                # Real database idempotency check: replay the same update safely.
                saved, retry = await store.publish(reporter, config['model_id'], smoke['records'], saved['generation'])
                if retry['changed_games'] != 0:
                    raise ValueError('Shadow retry double-counted unchanged games.')
                await reporter.progress('shadow_smoke_passed', 500, len(games), 'Smoke passed; a separate 500-game snapshot is durable. Expanding the same frozen sample.')
                remaining_rows, rest_timings = await source.extract(session, games[500:], reporter, stream, index_offset=500)
                await file_operation(stream.flush)
            timings = {k: v+rest_timings[k] for k, v in timings.items()}
            await reporter.progress('checking_shadow_pilot', detail='Comparing all sampled games; live summaries are unchanged.')
            result = await calculate_file(xp, path, games, parent['fits'], check)
            if result['status'] != 'passed':
                raise ValueError('Pilot reference mismatch; last complete 500-game shadow retained: '+json.dumps(result['checks']))
            await source.attach_examples(session, result['examples'])
        await reporter.progress('benchmarking_shadow', detail='Same frozen formula on vectorized CPU and CUDA; three warm repeats including GPU transfers.')
        rows = np.memmap(path, dtype=blunder_fixed.DTYPE, mode='r')
        try:
            benchmark = await file_operation(compute.benchmark, xp, rows, games, parent['fits'], check)
        finally:
            rows._mmap.close()
        saved, full_write = await store.publish(reporter, config['model_id'], result['records'][500:], saved['generation'])
        expected, _ = store.merge(None, result['records'], config['model_id'])
        if saved['records'] != expected['records'] or saved['groups'] != expected['groups']:
            raise ValueError('Incremental shadow summary differs from a full rebuild.')
        async with AsyncDBSession() as session:
            recovered = await session.scalar(text('SELECT result FROM algorithm_run WHERE id=:id'), {'id': reporter.run_id})
        if recovered != saved:
            raise ValueError('Durable shadow reload differs from the published snapshot.')
    result.update(format=FORMAT, summary_version=2, status='passed', canonical=False, production_changed=False, promotion_ready=False,
        sample_only=True, independent_calibration_test=False, backend='cuda_cupy', precision='float64',
        config=config, model_id=config['model_id'], fits=parent['fits'], original_quality_gates=parent['gates'],
        hardware=hardware, groups=saved['groups'], benchmark=benchmark,
        smoke={'games': 500, 'moves': smoke_rows, 'status': smoke['status'], 'checks': smoke['checks']},
        source={'games': len(games), 'positions': sum(g['total_positions'] for g in games),
            'own_moves': smoke_rows+remaining_rows, 'taken_at': snapshot, 'sha256': digest(games),
            'selection': 'fixed_seed_mode_quotas_one_assigned_owner_per_unique_game', 'players': config['players']},
        included_rows=smoke_rows+remaining_rows,
        operational_checks={'duplicate_retry': retry, 'initial_publication': first_write,
            'incremental_publication': full_write, 'incremental_equals_rebuild': True,
            'durable_reload': True, 'example_board_transitions_verified': all(x['legal_transition_verified'] for x in result['examples']),
            'automatic_production_scheduler_tested': False},
        implementation_sha256=hashlib.sha256(''.join(inspect.getsource(m) for m in
            (inspect.getmodule(execute), source, compute, store, model, blunder_fixed)).encode()).hexdigest())
    result['timings'].update(timings, selection_seconds=selection_seconds,
        shadow_publication_seconds=first_write['publication_seconds']+full_write['publication_seconds'],
        total_compute_seconds=time.perf_counter()-started)
    result['notes'] = [
        'Shadow pilot only; sampled counts are not full player histories. No live summaries, accuracy, salience or canonical model changed.',
        'One assigned player per unique game; shared games are counted once. At most 1,000 assigned games per player, 400 bullet / 400 blitz / 200 rapid; shortages are not padded.',
        'The first 500 games passed before expansion. Source positions, independent legacy SQL and saved counts share one read-only repeatable snapshot.',
        'Old .15, coefficient-only .20 and frozen rating-aware .20 separate threshold changes from rating effects. Forced-mate rules are unchanged; tablebase transitions remain unassessed.',
        'Summary v2: games with no assessed player moves are counted separately, excluded from all three histograms; they are not zero-blunder observations.',
        'No coefficient fitting or new calibration claim. This outcome-blind operational sample may overlap previous research; all original failed or unsupported quality gates remain visible.',
        'Timing separates source extraction, SQL reference, scalar reference, GPU comparison, same-formula CPU/GPU benchmark and shadow publication. This is not the full production ingestion pipeline.',
        'Incremental append, duplicate retry and durable reload exercised against research snapshots. Synthetic score changes, rollback and cancellation are tested only in disposable PostgreSQL, not by changing live FENs.',
        'Move examples verify source boards and classification arithmetic, not human ground-truth blunder quality. Automatic production scheduling and GPU-worker outage recovery remain untested.']
    json.dumps(result, allow_nan=False)
    return result
