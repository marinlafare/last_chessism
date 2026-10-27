"""Superuser algorithm configuration, preflight, execution and durable results."""

from copy import deepcopy
import logging
import uuid
from typing import Literal

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, AlgorithmDefinition, AlgorithmRun, MatrixDefinition
from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.matrix_constructor.storage import matrix_catalog_lock, MatrixCatalogBusy
from chessism_api.operations.matrix_constructor.definitions import estimate_definition, resolve_definition
from chessism_api.operations.matrix_constructor.defaults import default_formula
from chessism_api.routers.matrices.schemas import MatrixParameters
from chessism_api.operations.research_algorithms.config import ACTIVE, MAX_COLUMNS, MAX_ROWS, QUEUE, algorithm_catalog, normalize_algorithm, source_config
from chessism_api.operations.research_algorithms.repository import definition_payload, list_runs, now, run_payload, terminal
from chessism_api.operations.research_algorithms.storage import capacity

router = APIRouter()
QUEUE_LOCK = 731_946_225


@router.get('/canonical-blunders')
async def canonical_blunders_status():
    from chessism_api.operations.canonical_blunders.registry import status
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        return await status(session)


@router.post('/canonical-blunders/benchmark', status_code=202)
async def benchmark_canonical_blunders(account: Account = Depends(get_current_account),
        redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.canonical_blunders.benchmark_job import current_configuration
    try:
        config = await current_configuration()
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Blunder reader comparison · 100,000 games · RTX 4060', config))


class AlgorithmRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    matrix_definition_id: uuid.UUID
    operation: Literal["feature_relationships"] = "feature_relationships"
    columns: list[str] = Field(min_length=2, max_length=MAX_COLUMNS)
    missing: Literal["drop_rows", "column_mean"] = "drop_rows"
    scaling: Literal["none", "standardize"] = "none"
    rating_difference: bool = False
    x: str
    y: str
    max_rows: int = Field(100000, ge=1, le=MAX_ROWS)
    seed: int = Field(42, ge=0, le=2 ** 32 - 1)


class BuilderRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    operation: Literal['pipeline']
    matrix_definition_id: uuid.UUID | None = None
    builtin_matrix_key: str | None = Field(None, max_length=100)
    matrix_parameters: MatrixParameters | None = None
    parent_definition_id: uuid.UUID | None = None
    columns: list[str] = Field(min_length=1, max_length=12)
    steps: list[dict] = Field(min_length=1, max_length=20)
    outputs: list[dict] = Field(min_length=1, max_length=6)
    missing: Literal['keep', 'drop_rows', 'column_mean'] = 'keep'
    invalid_values: Literal['null', 'error'] = 'null'
    max_rows: int = Field(100000, ge=1, le=100000)
    seed: int = Field(42, ge=0, le=2**32 - 1)


class MatrixInputRequest(BaseModel):
    matrix_definition_id: uuid.UUID | None = None
    builtin_matrix_key: str | None = Field(None, max_length=100)
    matrix_parameters: MatrixParameters


class CudaValidationRequest(BaseModel):
    player: str = Field(min_length=1, max_length=128)
    benchmark: bool = False


class BlunderTrainingRequest(BaseModel):
    games_per_group: int = Field(1000, ge=100, le=3000, strict=True)


class FixedBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    player: str = Field(min_length=1, max_length=128)


class SmoothBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    games_per_group: int = Field(1000, ge=100, le=3000, strict=True)
    sampling: Literal['legacy','balanced_v1','development_reuse_v1'] = 'legacy'
    refinement: Literal['none', 'calibration_v1'] = 'none'


class FinalBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    acknowledgement: Literal['rapid_provisional_evaluation_v1']


class ShadowBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    players: list[str] = Field(min_length=6, max_length=6)
    acknowledgement: Literal['sampled_shadow_only_no_live_changes']


class ThresholdBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    acknowledgement: Literal['compare_thresholds_only_no_live_changes']


class CohortBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    resume_run_id: uuid.UUID | None = None
    acknowledgement: Literal['all_player_shadow_summaries_no_live_changes']


class StrengthBlunderRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    acknowledgement: Literal['compare_rating_strengths_only_no_live_changes']


class FullDevelopmentRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    parent_run_id: uuid.UUID
    acknowledgement: Literal['large_cuda_development_only_keep_tests_untouched']


@router.post('/blunder-full-development', status_code=202)
async def start_full_blunder_development(request: FullDevelopmentRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_full_job import configuration
    from chessism_api.operations.research_algorithms import blunder_development_reuse as history
    class ReadOnlyReporter:
        async def check(self): pass
        async def progress(self, *args, **kwargs):
            raise ValueError('Large development requires verified identity manifests, not historical position replay in the API.')
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            policy = await history.reference_plan(session)
            roles, _, _ = await history.history(session, policy, ReadOnlyReporter())
            config = configuration(request.parent_run_id, row, policy, roles)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · CP-balanced diagnostics · RTX 4060', config))


@router.post('/blunder-strength-comparison', status_code=202)
async def start_blunder_strength_comparison(request: StrengthBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_strength_job import configuration
    from chessism_api.operations.research_algorithms.blunder_development_reuse import reference_plan
    try:
        async with AsyncDBSession() as session:
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            config = configuration(request.parent_run_id, row, await reference_plan(session))
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · weaker adjustments · shared GPU sample', config))


@router.post('/blunder-cohort-summaries', status_code=202)
async def start_blunder_cohort_summaries(request: CohortBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_cohort_job import configuration
    try:
        async with AsyncDBSession() as session:
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            config = configuration(request.parent_run_id, row, request.resume_run_id)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · all-player GPU 0.15 shadow summaries', config))


@router.post('/blunder-threshold-comparison', status_code=202)
async def start_blunder_threshold_comparison(request: ThresholdBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_threshold_compare import configuration
    try:
        async with AsyncDBSession() as session:
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            config = configuration(request.parent_run_id, row)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · 0.15 versus 0.20 · same 6,000 games', config))


@router.post('/blunder-shadow-pilot', status_code=202)
async def start_shadow_blunder_pilot(request: ShadowBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_shadow_job import configuration
    try:
        async with AsyncDBSession() as session:
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            config = configuration(request.parent_run_id, row, request.players)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · 500 → 6,000-game shadow pilot', config))


@router.post('/blunder-final-evaluation', status_code=202)
async def start_final_blunder_evaluation(request: FinalBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_final_evaluation import configuration
    try:
        async with AsyncDBSession() as session:
            row = (await session.execute(text('SELECT status,config,result FROM algorithm_run WHERE id=:id'),
                {'id': str(request.parent_run_id)})).mappings().one_or_none()
            config = configuration(request.parent_run_id, row)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · frozen final evaluation · rapid provisional', config))


@router.post('/blunder-training-v2', status_code=202)
async def start_smooth_blunder_training(request: SmoothBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_v2_job import configuration
    if request.refinement != 'none' and request.sampling not in ('balanced_v1', 'development_reuse_v1'):
        raise HTTPException(422, 'Calibration refinement requires balanced sampling with verified prior-game exclusion.')
    if request.sampling == 'development_reuse_v1' and request.refinement != 'calibration_v1':
        raise HTTPException(422, 'Development reuse requires calibration refinement.')
    plan=None
    if request.sampling in ('balanced_v1', 'development_reuse_v1'):
        from chessism_api.operations.research_algorithms.blunder_balanced_source import reference_plan
        if request.sampling == 'development_reuse_v1':
            from chessism_api.operations.research_algorithms.blunder_development_reuse import reference_plan
        try:
            async with AsyncDBSession() as session:
                plan=await reference_plan(session)
        except ValueError as error:
            raise HTTPException(409,str(error)) from error
    from chessism_api.operations.research_algorithms.blunder_refinement import policy
    refinement = policy() if request.refinement == 'calibration_v1' else None
    name = 'Rating-aware blunders · calibration refinement v1' if refinement else f'Rating-aware blunders v2 · {"balanced v1" if plan else "legacy sample"}'
    if request.sampling == 'development_reuse_v1':
        name += ' · development reuse'
    return await enqueue_run(None, account, redis,
        draft=(name, configuration(request.games_per_group, plan, refinement)))


@router.post('/blunder-validation', status_code=202)
async def start_blunder_validation(request: FixedBlunderRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_validation_job import configuration
    try:
        config = configuration(request.player)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=(f"Fixed-coefficient blunders · CUDA validation · {config['player']}"[:100], config))


@router.post('/blunder-training', status_code=202)
async def start_blunder_training(request: BlunderTrainingRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.blunder_training import configuration
    return await enqueue_run(None, account, redis,
        draft=('Rating-aware blunders · CUDA candidate', configuration(request.games_per_group)))


@router.post('/cuda-validation', status_code=202)
async def start_cuda_validation(request: CudaValidationRequest,
        account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    from chessism_api.operations.research_algorithms.score_validation_job import configuration
    try:
        config = configuration(request.player, benchmark=request.benchmark)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    return await enqueue_run(None, account, redis,
        draft=(f"Score distribution · CUDA validation · {config['player']}"[:100], config))


async def load_input_formula(request, session):
    builtin = getattr(request, 'builtin_matrix_key', None)
    if builtin:
        if request.matrix_definition_id is not None:
            raise HTTPException(422, 'Choose one input formula, not both a default and a saved formula.')
        try:
            return default_formula(builtin)['config']
        except KeyError as error:
            raise HTTPException(404, 'Default matrix formula not found.') from error
    if request.matrix_definition_id is None:
        raise HTTPException(422, 'Select an input formula.')
    matrix = await session.get(MatrixDefinition, str(request.matrix_definition_id))
    if matrix is None:
        raise HTTPException(404, 'Matrix definition not found. Refresh the input list.')
    return matrix.config


@router.post('/input')
async def configure_matrix_input(request: MatrixInputRequest):
    """Resolve column instructions and scope only. No source query or writes."""
    async with AsyncDBSession() as session:
        recipe = await load_input_formula(request, session)
    try:
        config = resolve_definition(recipe, request.matrix_parameters.model_dump(mode='json'))
    except (ValueError, TypeError, KeyError) as error:
        raise HTTPException(422, str(error)) from error
    return {'config': config, 'materialized': False}


async def request_config(request, session):
    parent_id = getattr(request, 'parent_definition_id', None)
    parent = await session.get(AlgorithmDefinition, str(parent_id)) if parent_id else None
    if parent_id and parent is None:
        raise HTTPException(404, 'Parent algorithm not found. Start a new definition instead.')
    builtin = getattr(request, 'builtin_matrix_key', None)
    parameters = getattr(request, 'matrix_parameters', None)
    reuse_parent = parent and not builtin and parameters is None and (
        request.matrix_definition_id is None or str(request.matrix_definition_id) == parent.matrix_definition_id)
    if reuse_parent:
        recipe = parent.config['matrix']
    else:
        recipe = await load_input_formula(request, session)
    try:
        if parameters is not None:
            recipe = resolve_definition(recipe, parameters.model_dump(mode='json'))
        config = normalize_algorithm(request.model_dump(mode="json"), recipe)
        if request.operation == 'pipeline':
            config['revision'] = int(parent.config.get('revision', 1)) + 1 if parent else 1
            config['parent_definition_id'] = str(parent_id) if parent else None
            if reuse_parent and 'matrix_source' in parent.config:
                config['matrix_source'] = deepcopy(parent.config['matrix_source'])
            else:
                config['matrix_source'] = {
                    'builtin_key': builtin,
                    'definition_id': str(request.matrix_definition_id) if request.matrix_definition_id else None,
                    'parameters': parameters.model_dump(mode='json') if parameters is not None else None,
                }
        return config
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise HTTPException(422, str(error)) from error


@router.get("/catalog")
async def catalog():
    return algorithm_catalog()


@router.post("/preflight")
async def preflight(request: AlgorithmRequest | BuilderRequest):
    async with AsyncDBSession() as session:
        config = await request_config(request, session)
    # A bounded count is advisory, never an assertion of an exact total.
    from sqlalchemy.exc import DBAPIError
    try:
        estimate = await estimate_definition(source_config(config))
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) == "57014":
            raise HTTPException(504, "Counting took too long. Narrow the matrix scope; saving does not require a count.") from error
        raise
    return {"estimate": estimate, "resources": capacity(config), "schemas": config.get('schemas'), "backend": "numpy_cpu", "materialized": False}


@router.post('/validate')
async def validate(request: BuilderRequest):
    async with AsyncDBSession() as session:
        config = await request_config(request, session)
    return {'valid': True, 'schemas': config['schemas'], 'resources': capacity(config), 'materialized': False}


@router.get("/definitions")
async def definitions(limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0)):
    async with AsyncDBSession() as session:
        rows = list((await session.scalars(select(AlgorithmDefinition).order_by(AlgorithmDefinition.created_at.desc(), AlgorithmDefinition.id).offset(offset).limit(limit + 1))))
    return {"definitions": [definition_payload(row) for row in rows[:limit]], "has_more": len(rows) > limit}


@router.post("/definitions", status_code=201)
async def save_definition(request: AlgorithmRequest | BuilderRequest, account: Account = Depends(get_current_account)):
    try:
        async with matrix_catalog_lock(wait=False) as session:
            config = await request_config(request, session)
            matrix_id = str(request.matrix_definition_id) if request.matrix_definition_id else None
            row = AlgorithmDefinition(id=str(uuid.uuid4()), name=request.name.strip() or "Custom algorithm",
                                      matrix_definition_id=matrix_id, config=config, created_by=account.id)
            session.add(row)
            await session.flush()
            return definition_payload(row)
    except MatrixCatalogBusy as error:
        raise HTTPException(409, str(error)) from error


@router.delete("/definitions/{definition_id}", status_code=204)
async def delete_definition(definition_id: uuid.UUID):
    try:
        async with matrix_catalog_lock(wait=False) as session:
            row = await session.get(AlgorithmDefinition, str(definition_id))
            if row is None:
                raise HTTPException(404, "Algorithm definition not found.")
            await session.delete(row)  # Existing runs retain their frozen instructions.
    except MatrixCatalogBusy as error:
        raise HTTPException(409, str(error)) from error


@router.post("/definitions/{definition_id}/runs", status_code=202)
async def start_run(definition_id: uuid.UUID, account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    return await enqueue_run(definition_id, account, redis)


@router.post('/sample', status_code=202)
async def test_sample(request: BuilderRequest, account: Account = Depends(get_current_account), redis: ArqRedis = Depends(get_redis_pool)):
    async with AsyncDBSession() as session:
        config = await request_config(request, session)
    config['max_rows'] = min(config['max_rows'], 500)
    config['sample_run'] = True
    return await enqueue_run(None, account, redis, draft=(request.name.strip()[:85] + ' · sample', config))


async def enqueue_run(definition_id, account, redis, *, draft=None):
    run_id = str(uuid.uuid4())
    async with AsyncDBSession() as session, session.begin():
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": QUEUE_LOCK})
        if await session.scalar(select(func.count()).select_from(AlgorithmRun).where(AlgorithmRun.status.in_(ACTIVE))) >= 5:
            raise HTTPException(409, "The algorithm queue already has five active requests. Wait or cancel one.")
        if draft:
            name, config = draft
        else:
            definition = await session.get(AlgorithmDefinition, str(definition_id), with_for_update=True)
            if definition is None:
                raise HTTPException(404, "Algorithm definition not found.")
            name, config = definition.name, definition.config
        if not capacity(config)["safe_to_run"]:
            raise HTTPException(409, "Not enough local disk space above the protected reserve.")
        row = AlgorithmRun(id=run_id, definition_id=str(definition_id) if definition_id else None, name=name, config=deepcopy(config),
                           created_by=account.id, status="queued", progress={"phase": "queued", "detail": "Waiting for the dedicated algorithm worker."})
        session.add(row)
    try:
        function = 'run_large_algorithm_job' if config.get('operation') == 'full_development_rating_blunder_v1' else 'run_algorithm_job'
        job = await redis.enqueue_job(function, run_id=run_id, _queue_name=QUEUE, _job_id=f"algorithm-{run_id}")
        if job is None:
            raise RuntimeError("Unable to register algorithm job.")
    except Exception as error:
        await terminal(run_id, "failed", "Queue submission failed. Please try again.")
        raise HTTPException(503, "Algorithm queue is unavailable; the request was marked failed.") from error
    return {"id": run_id, "status": "queued"}


@router.get("/runs")
async def runs(limit: int = Query(30, ge=1, le=100), offset: int = Query(0, ge=0)):
    return await list_runs(limit, offset)


@router.get("/runs/{run_id}")
async def run_details(run_id: uuid.UUID):
    async with AsyncDBSession() as session:
        row = await session.get(AlgorithmRun, str(run_id))
        if row is None:
            raise HTTPException(404, "Algorithm run not found.")
        return run_payload(row, result=True)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: uuid.UUID, redis: ArqRedis = Depends(get_redis_pool)):
    async with AsyncDBSession() as session, session.begin():
        row = await session.get(AlgorithmRun, str(run_id), with_for_update=True)
        if row is None:
            raise HTTPException(404, "Algorithm run not found.")
        if row.status not in ACTIVE:
            raise HTTPException(409, "This run has already finished.")
        queued = row.status == "queued"
        row.cancel_requested = True
        if queued:
            row.status, row.finished_at = "cancelled", now()
            row.progress = {"phase": "cancelled", "detail": "Cancelled before execution."}
    if queued:
        try:
            await redis.zrem(QUEUE, f"algorithm-{run_id}")
        except Exception:
            # Durable cancellation is authoritative; the worker skips this row
            # even if Redis is temporarily unavailable to remove the ticket.
            logging.getLogger(__name__).warning("Cancelled algorithm ticket remains in Redis: %s", run_id, exc_info=True)
    return {"id": str(run_id), "status": "cancelled" if queued else "cancelling",
            "message": "Cancellation is checked between bounded chunks; a source query may take up to 60 seconds to return."}
