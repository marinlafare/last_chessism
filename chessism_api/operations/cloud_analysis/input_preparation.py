"""One logical Cloud Run job, with bounded, restartable local reservation pages."""
from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import insert, select
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import (CloudAnalysisRun, CloudInputPreparation,
    CloudInputChunk, CloudJobCancellation)
from . import history, input_validation
from .game_fen_queue import get_game_queue_fens, discard_queue
from .input_seal import seal
from .preparation import StageTimings, refresh_claim_statistics

PAGE_SIZE = 50_000


async def begin(job, target):
    async with AsyncDBSession() as session, session.begin():
        if await session.get(CloudJobCancellation, job.id):
            return
        run = CloudAnalysisRun(id=uuid4().hex, job_id=job.id, status='selecting',
                               positions=[], launch={}, receipts={})
        session.add(run)
        await session.flush()
        session.add(CloudInputPreparation(run_id=run.id, target=target))
        await history.transition(session, run, None, 'selecting')


async def checkpoint_phase(run_id, phase):
    async with AsyncDBSession() as session, session.begin():
        state = await session.get(CloudInputPreparation, run_id)
        if state:
            state.phase, state.updated_at = phase, datetime.now(timezone.utc)


async def advance(controller, job, run):
    timings = StageTimings()
    try:
        await _advance(controller, job, run, timings)
    finally:
        await timings.save(job.id, run.id)


async def _advance(controller, job, run, timings):
    from chessism_api.database.postgresql.ask_db import get_fens_for_analysis, get_player_fens_for_analysis
    async with AsyncDBSession() as session:
        state = await session.get(CloudInputPreparation, run.id)
        if state is None:
            raise ValueError('Missing input preparation checkpoint')
    remaining = state.target - run.position_count
    if remaining > 0 and state.phase != 'sealing':
        await checkpoint_phase(run.id, 'reserving')
        # Match Batch Spot's empty-to-populated planner safeguard. Run outside
        # the reservation transaction, before taking any FEN/queue locks.
        with timings.measure('prepare_statistics'):
            await refresh_claim_statistics()
        count = min(PAGE_SIZE, remaining)
        if job.selection['mode'] == 'games':
            session, fens = await get_game_queue_fens(job.id, job.selection['game_links'], count, timings=timings)
        elif job.selection.get('player_name'):
            session, fens = await get_player_fens_for_analysis(job.selection['player_name'], count, raise_errors=True, timings=timings)
        else:
            session, fens = await get_fens_for_analysis(count, raise_errors=True, timings=timings)
        if session is not None:
            try:
                await checkpoint_phase(run.id, 'validating')
                with timings.measure('prepare_validate'):
                    positions = await input_validation.prepare(fens)
                if await session.get(CloudJobCancellation, job.id):
                    await session.rollback()
                    return
                owner = await session.get(CloudAnalysisRun, run.id, with_for_update=True,
                                          execution_options={'cloud_documents': ()})
                if owner.status != 'selecting' or owner.position_count != run.position_count:
                    raise ValueError('Reservation checkpoint changed')
                with timings.measure('prepare_claims'):
                    await input_validation.claim(session, run.id, fens)
                await session.execute(insert(CloudInputChunk), [dict(run_id=run.id,
                    ordinal=owner.position_count + n, ids=[p['id'] for p in positions[n:n + 500]],
                    fens=[p['fen'] for p in positions[n:n + 500]]) for n in range(0, len(positions), 500)])
                owner.position_count += len(positions)
                saved = await session.get(CloudInputPreparation, run.id)
                saved.phase = 'sealing' if len(fens) < count or owner.position_count >= state.target else 'reserving'
                saved.updated_at = datetime.now(timezone.utc)
                with timings.measure('prepare_commit'):
                    await session.commit()
            finally:
                await session.close()
            controller.continue_immediately = True
            return
        await checkpoint_phase(run.id, 'sealing')
    async with AsyncDBSession() as session, session.begin():
        if await session.get(CloudJobCancellation, job.id):
            return
        owner = await session.get(CloudAnalysisRun, run.id, with_for_update=True,
                                  execution_options={'cloud_documents': ()})
        with timings.measure('prepare_seal'):
            sealed = await seal(session, owner)
        if sealed:
            following = 'preparing' if owner.position_count else 'complete'
            await history.transition(session, owner, owner.status, following)
            owner.status = following
            if not owner.position_count:
                owner.details_pruned = True
            await discard_queue(session, job.id)
    controller.continue_immediately = True
