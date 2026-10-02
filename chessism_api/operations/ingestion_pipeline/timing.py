"""Persistent timing records for complete ingestion runs and their stages."""

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import desc, select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    IngestionPipelineRun,
    IngestionPipelineStageTiming,
)
from .stages import STAGE_ORDER


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _elapsed_ms(started_at: datetime, finished_at: datetime) -> float:
    return max(0.0, (finished_at - started_at).total_seconds() * 1_000)


async def start_ingestion_run(
    *,
    run_id: str | None = None,
    player_name: str | None = None,
    trigger: str = "automatic",
) -> str:
    """Create or reopen an ingestion run and return its stable identifier."""
    stable_run_id = str(run_id or uuid4())
    now = _utcnow()
    try:
        async with AsyncDBSession() as session:
            row = await session.get(IngestionPipelineRun, stable_run_id)
            if row is None:
                session.add(IngestionPipelineRun(
                    run_id=stable_run_id,
                    player_name=(player_name or None),
                    trigger=str(trigger or "automatic")[:32],
                    status="running",
                    started_at=now,
                ))
            else:
                row.status = "running"
                row.finished_at = None
                row.elapsed_ms = None
                row.last_error = None
                if player_name and not row.player_name:
                    row.player_name = player_name
            await session.commit()
    except Exception as error:
        print(
            f"Could not start ingestion timing run {stable_run_id}: {error!r}",
            flush=True,
        )
    return stable_run_id


async def start_ingestion_stage(
    run_id: str | None,
    stage: str,
    *,
    detail: str | None = None,
) -> int | None:
    """Start a stage unless an execution of that stage is already running."""
    if not run_id:
        return None
    try:
        async with AsyncDBSession() as session:
            active = await session.scalar(
                select(IngestionPipelineStageTiming)
                .where(
                    IngestionPipelineStageTiming.run_id == run_id,
                    IngestionPipelineStageTiming.stage == stage,
                    IngestionPipelineStageTiming.status == "running",
                )
                .order_by(desc(IngestionPipelineStageTiming.id))
                .limit(1)
            )
            if active is not None:
                return int(active.id)

            row = IngestionPipelineStageTiming(
                run_id=run_id,
                stage=stage,
                status="running",
                started_at=_utcnow(),
                detail=detail,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return int(row.id)
    except Exception as error:
        print(
            f"Could not start ingestion stage {stage} for {run_id}: {error!r}",
            flush=True,
        )
        return None


async def finish_ingestion_stage(
    run_id: str | None,
    stage: str,
    *,
    processed: int = 0,
    total: int = 0,
    status: str = "complete",
    detail: str | None = None,
) -> None:
    """Finish the latest active execution of a stage."""
    if not run_id:
        return
    try:
        async with AsyncDBSession() as session:
            row = await session.scalar(
                select(IngestionPipelineStageTiming)
                .where(
                    IngestionPipelineStageTiming.run_id == run_id,
                    IngestionPipelineStageTiming.stage == stage,
                    IngestionPipelineStageTiming.status == "running",
                )
                .order_by(desc(IngestionPipelineStageTiming.id))
                .limit(1)
                .with_for_update()
            )
            if row is None:
                return
            finished_at = _utcnow()
            row.status = str(status or "complete")[:16]
            row.finished_at = finished_at
            row.elapsed_ms = _elapsed_ms(row.started_at, finished_at)
            row.processed = max(0, int(processed or 0))
            row.total = max(0, int(total or 0))
            if detail is not None:
                row.detail = detail
            await session.commit()
    except Exception as error:
        print(
            f"Could not finish ingestion stage {stage} for {run_id}: {error!r}",
            flush=True,
        )


async def finish_ingestion_run(
    run_id: str | None,
    *,
    status: str = "complete",
    error: str | None = None,
) -> None:
    """Finish a complete ingestion, including its queue waits."""
    if not run_id:
        return
    try:
        async with AsyncDBSession() as session:
            row = await session.get(
                IngestionPipelineRun,
                run_id,
                with_for_update=True,
            )
            if row is None:
                return
            finished_at = _utcnow()
            row.status = str(status or "complete")[:16]
            row.finished_at = finished_at
            row.elapsed_ms = _elapsed_ms(row.started_at, finished_at)
            row.last_error = str(error) if error else None
            await session.commit()
    except Exception as timing_error:
        print(
            f"Could not finish ingestion timing run {run_id}: {timing_error!r}",
            flush=True,
        )


async def fail_running_ingestion_stages(
    run_id: str | None,
    error: str,
) -> None:
    """Close every still-running stage after a pipeline failure."""
    if not run_id:
        return
    try:
        async with AsyncDBSession() as session:
            rows = list((await session.scalars(
                select(IngestionPipelineStageTiming)
                .where(
                    IngestionPipelineStageTiming.run_id == run_id,
                    IngestionPipelineStageTiming.status == "running",
                )
                .with_for_update()
            )).all())
            finished_at = _utcnow()
            for row in rows:
                row.status = "failed"
                row.finished_at = finished_at
                row.elapsed_ms = _elapsed_ms(row.started_at, finished_at)
                row.detail = str(error)
            await session.commit()
    except Exception as timing_error:
        print(
            f"Could not fail ingestion stages for {run_id}: {timing_error!r}",
            flush=True,
        )


async def get_latest_ingestion_timing() -> dict[str, Any] | None:
    """Return the latest run with repeated stage executions aggregated."""
    async with AsyncDBSession() as session:
        run = await session.scalar(
            select(IngestionPipelineRun)
            .order_by(desc(IngestionPipelineRun.started_at))
            .limit(1)
        )
        if run is None:
            return None
        stage_rows = list((await session.scalars(
            select(IngestionPipelineStageTiming)
            .where(IngestionPipelineStageTiming.run_id == run.run_id)
            .order_by(IngestionPipelineStageTiming.started_at)
        )).all())

    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"elapsed_ms": 0.0, "processed": 0, "total": 0, "passes": 0}
    )
    for row in stage_rows:
        item = grouped[row.stage]
        item["elapsed_ms"] += float(row.elapsed_ms or 0.0)
        item["processed"] += int(row.processed or 0)
        item["total"] += int(row.total or 0)
        item["passes"] += 1
        item["status"] = row.status

    return {
        "run_id": run.run_id,
        "player_name": run.player_name,
        "trigger": run.trigger,
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "elapsed_ms": float(run.elapsed_ms or 0.0) if run.elapsed_ms is not None else None,
        "last_error": run.last_error,
        "stages": [
            {"stage": stage, **grouped[stage]}
            for stage in STAGE_ORDER
            if stage in grouped
        ],
    }
