"""Superuser-only endpoints for non-production coefficient experiments."""

from __future__ import annotations

import json
import uuid
from typing import Literal

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from chessism_api.auth import get_current_account
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, CoefficientResearchExperiment
from chessism_api.operations.coefficient_research import (
    experiment_payload,
    get_coefficient_research_overview,
    list_coefficient_experiments,
    normalized_config,
    utc_now,
)
from chessism_api.operations.player_salience import (
    enqueue_player_salience,
    enqueue_stale_player_salience_jobs,
    get_player_salience_report,
    get_salience_overview,
    seed_player_salience_summaries,
)
from chessism_api.redis_client import get_redis_pool


router = APIRouter()
ANALYSIS_JOB_FUNCTIONS = {
    "run_analysis_job",
    "run_player_analysis_job",
    "run_analysis_loop_job",
    "run_player_games_analysis_job",
}


class CoefficientExperimentConfig(BaseModel):
    modes: list[Literal["bullet", "blitz", "rapid"]] = Field(
        default_factory=lambda: ["bullet", "blitz", "rapid"]
    )
    max_rating_gap: int = Field(200, ge=0, le=1000)
    opening_moves_excluded: int = Field(10, ge=0, le=40)
    minimum_games_per_fit: int = Field(500, ge=50, le=100_000)
    cp_ceiling: int = Field(1000, ge=100, le=2000)
    split_seed: int = Field(20260930, ge=1, le=2_147_483_000)


class CoefficientDecisionRequest(BaseModel):
    action: Literal["keep_lichess", "alongside", "replace"]
    strategy: Literal["global", "rating_bins"]
    notes: str = Field("", max_length=2000)


async def _analysis_jobs_active(redis: ArqRedis) -> bool:
    queued_rows = await redis.zrange("analysis_queue", 0, -1)
    for raw_job_id in queued_rows:
        job_id = raw_job_id.decode("utf-8") if isinstance(raw_job_id, bytes) else str(raw_job_id)
        job = Job(job_id, redis, _queue_name="analysis_queue")
        status = await job.status()
        if status not in (JobStatus.queued, JobStatus.deferred, JobStatus.in_progress):
            continue
        info = await job.info()
        if info and info.function in ANALYSIS_JOB_FUNCTIONS:
            return True
    return False


async def _research_progress(redis: ArqRedis, experiment_id: str) -> dict | None:
    raw = await redis.get(f"chessism:coefficient_research:{experiment_id}")
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


@router.post("/coefficient/overview")
async def coefficient_overview(
    config: CoefficientExperimentConfig,
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    payload = await get_coefficient_research_overview(config.model_dump())
    analysis_active = await _analysis_jobs_active(redis)
    payload["resources"] = {
        "analysis_active": analysis_active,
        "safe_to_start": not analysis_active,
    }
    return payload


@router.get("/salience")
async def salience_overview(
    limit: int = Query(100, ge=1, le=1_000),
) -> dict:
    """Return persistent projection state for tracked players."""
    return await get_salience_overview(limit)


@router.post("/salience/backfill", status_code=202)
async def start_salience_backfill(
    limit: int = Query(25, ge=1, le=100),
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    """Seed tracked players and queue a bounded first salience pass."""
    if await _analysis_jobs_active(redis):
        raise HTTPException(
            status_code=409,
            detail="Wait for the Stockfish analysis queue to become idle before starting salience backfill.",
        )
    seeded = await seed_player_salience_summaries()
    jobs = await enqueue_stale_player_salience_jobs(redis, limit=limit)
    return {"seeded_players": seeded, "queued": jobs}


@router.get("/salience/players/{player_name}")
async def salience_player_report(
    player_name: str,
    game_limit: int = Query(10, ge=1, le=50),
) -> dict:
    """Return one player's corpus and salience distribution report."""
    try:
        return await get_player_salience_report(player_name, game_limit=game_limit)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@router.post("/salience/players/{player_name}", status_code=202)
async def start_player_salience(
    player_name: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    """Queue a forced corpus refresh for one tracked player."""
    if await _analysis_jobs_active(redis):
        raise HTTPException(
            status_code=409,
            detail="Wait for the Stockfish analysis queue to become idle before calculating salience.",
        )
    try:
        return await enqueue_player_salience(redis, player_name, force=True)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@router.get("/coefficient/experiments")
async def coefficient_experiments(
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    return {"experiments": await list_coefficient_experiments(limit)}


@router.get("/coefficient/experiments/{experiment_id}")
async def coefficient_experiment(
    experiment_id: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    async with AsyncDBSession() as session:
        experiment = await session.get(CoefficientResearchExperiment, experiment_id)
        if experiment is None:
            raise HTTPException(status_code=404, detail="Coefficient experiment not found")
        payload = experiment_payload(experiment)
    payload["progress"] = await _research_progress(redis, experiment_id)
    return payload


@router.post("/coefficient/experiments", status_code=202)
async def create_coefficient_experiment(
    config: CoefficientExperimentConfig,
    account: Account = Depends(get_current_account),
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict:
    if await _analysis_jobs_active(redis):
        raise HTTPException(
            status_code=409,
            detail="Wait for the Stockfish analysis queue to become idle before starting research.",
        )

    safe_config = normalized_config(config.model_dump())
    experiment_id = str(uuid.uuid4())
    async with AsyncDBSession() as session:
        active = await session.execute(
            CoefficientResearchExperiment.__table__.select()
            .where(CoefficientResearchExperiment.status.in_(("queued", "running")))
            .limit(1)
        )
        if active.first():
            raise HTTPException(status_code=409, detail="A coefficient experiment is already active.")
        experiment = CoefficientResearchExperiment(
            id=experiment_id,
            status="queued",
            created_by=account.id,
            config=safe_config,
        )
        session.add(experiment)
        await session.commit()

    try:
        job = await redis.enqueue_job(
            "run_chessism_coefficient_experiment",
            experiment_id=experiment_id,
            _queue_name="research_queue",
            _job_id=f"coefficient-research-{experiment_id}",
        )
        job_id = str(getattr(job, "job_id", job))
        async with AsyncDBSession() as session:
            experiment = await session.get(CoefficientResearchExperiment, experiment_id)
            if experiment is not None:
                experiment.job_id = job_id
                await session.commit()
    except Exception as error:
        async with AsyncDBSession() as session:
            experiment = await session.get(CoefficientResearchExperiment, experiment_id)
            if experiment is not None:
                experiment.status = "failed"
                experiment.error = str(error)
                experiment.finished_at = utc_now()
                await session.commit()
        raise

    return {
        "experiment_id": experiment_id,
        "job_id": job_id,
        "status": "queued",
        "production_changed": False,
    }


@router.post("/coefficient/experiments/{experiment_id}/decision")
async def decide_coefficient_experiment(
    experiment_id: str,
    decision: CoefficientDecisionRequest,
) -> dict:
    async with AsyncDBSession() as session:
        experiment = await session.get(CoefficientResearchExperiment, experiment_id)
        if experiment is None:
            raise HTTPException(status_code=404, detail="Coefficient experiment not found")
        if experiment.status != "complete":
            raise HTTPException(status_code=409, detail="Only a completed experiment can be reviewed.")
        experiment.decision = decision.action
        experiment.decision_config = {
            "strategy": decision.strategy,
            "production_changed": False,
        }
        experiment.decision_notes = decision.notes.strip() or None
        experiment.decided_at = utc_now()
        await session.commit()
        payload = experiment_payload(experiment)
    payload["message"] = "Decision recorded for research only; production accuracy was not changed."
    return payload
