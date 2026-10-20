"""Read-only, paginated history built from durable ingestion timing records."""

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import Date, cast, func, select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import IngestionPipelineRun, IngestionPipelineStageTiming
from .stages import STAGE_ORDER
from .steps import STEP_LABELS


LEGACY_LABELS = {
    "parsing_games": "Download, parse and save games",
    "fen_extraction": "Extract, save and link positions",
    "saving_positions": "Save unique positions",
    "linking_positions": "Link positions to games",
    "updating_summaries": "Refresh ingestion summaries",
    "tablebase_marking": "Endgame analysis",
}
ISSUE_STATUSES = {"failed", "error", "warning", "cancelled", "canceled", "partial"}


def run_metadata(run):
    return {
        "run_id": run.run_id,
        "player_name": run.player_name,
        "trigger": run.trigger,
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "elapsed_ms": float(run.elapsed_ms) if run.elapsed_ms is not None else None,
        "last_error": run.last_error,
    }


def history_timezone(name):
    try:
        return ZoneInfo(name)
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise ValueError("Unknown history timezone.") from error


async def list_ingestion_history_days(*, limit=20, offset=0, timezone_name="UTC"):
    history_timezone(timezone_name)
    day = cast(func.timezone(timezone_name, IngestionPipelineRun.started_at), Date)
    async with AsyncDBSession() as session:
        rows = (await session.execute(
            select(day.label("day"), func.count().label("run_count"))
            .where(IngestionPipelineRun.status != "running")
            .group_by(day)
            .order_by(day.desc())
            .limit(limit + 1)
            .offset(offset)
        )).mappings().all()
    return {
        "days": [
            {"day": row["day"].isoformat(), "run_count": row["run_count"]}
            for row in rows[:limit]
        ],
        "has_more": len(rows) > limit,
        "timezone": timezone_name,
    }


async def list_ingestion_history(*, limit=10, offset=0, day: date | None = None, timezone_name="UTC"):
    zone = history_timezone(timezone_name)
    query = select(IngestionPipelineRun).where(IngestionPipelineRun.status != "running")
    if day is not None:
        # Local midnight boundaries handle DST and preserve the started_at index.
        start = datetime.combine(day, time.min, zone).astimezone(timezone.utc)
        end = datetime.combine(day + timedelta(days=1), time.min, zone).astimezone(timezone.utc)
        query = query.where(
            IngestionPipelineRun.started_at >= start,
            IngestionPipelineRun.started_at < end,
        )
    async with AsyncDBSession() as session:
        runs = list((await session.scalars(
            query
            .order_by(IngestionPipelineRun.started_at.desc(), IngestionPipelineRun.run_id.desc())
            .limit(limit + 1)
            .offset(offset)
        )).all())
    return {
        "runs": [run_metadata(run) for run in runs[:limit]],
        "has_more": len(runs) > limit,
        "day": day.isoformat() if day is not None else None,
    }


def summarize_stages(run, rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.stage].append(row)
    # New runs also record enclosing phases. Do not display those twice.
    detailed = any(name in grouped for name in STEP_LABELS)
    order = list(STEP_LABELS if detailed else STAGE_ORDER)
    # Preserve errors in enclosing phases or unfamiliar historical stages.
    order.extend(name for name, records in grouped.items() if name not in order and (
        not detailed or any(row.status in ISSUE_STATUSES for row in records)
    ))
    result = []
    for name in order:
        records = grouped.get(name)
        if not records:
            continue
        label = STEP_LABELS[name][0] if name in STEP_LABELS else LEGACY_LABELS.get(
            name, name.replace("_", " ").capitalize()
        )
        if not detailed and name == "fen_extraction":
            label = "Extract positions"
        durations = [float(row.elapsed_ms) for row in records if row.elapsed_ms is not None]
        parallel = name in STEP_LABELS
        elapsed_ms = (max(durations) if parallel else sum(durations)) if durations else None
        issues = []
        for row in records:
            if row.status not in ISSUE_STATUSES:
                continue
            detail = row.detail
            if not detail or detail in (label, name):
                detail = run.last_error or f"{label}: {row.status}."
            if detail not in issues:
                issues.append(detail)
        result.append({
            "stage": name,
            "label": label,
            "elapsed_ms": elapsed_ms,
            "timing_basis": "longest_worker_active_time" if parallel else "phase_elapsed_time",
            "issues": issues,
        })
    return result


def summarize_counts(rows):
    """Recover run-scoped counts without querying today's game/FEN totals.

    Parsing progress counts archive requests, not games. Tablebase progress
    includes finalization units and unavailable positions, not just solved FENs.
    Use the recorded game count and exact-result message instead.
    """
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.stage].append(row)

    def completed_records(stage):
        records = grouped.get(stage, [])
        return records if records and all(row.status == "complete" for row in records) else []

    def processed_count(stage):
        records = completed_records(stage)
        return sum(int(row.processed or 0) for row in records) if records else None

    games_downloaded = processed_count("filter_games")
    imported_counts = []
    parsing = completed_records("parsing_games")
    for row in parsing:
        detail = row.detail or ""
        imported = re.match(r"Successfully processed and inserted (\d+) games\b", detail)
        if imported:
            imported_counts.append(int(imported.group(1)))
        elif (
            detail in ("ALL MONTHS IN DB ALREADY", "All games already at DB")
            or re.fullmatch(r"(?:No (?:new )?games found|No new data to insert) for .+\.", detail)
            or re.fullmatch(r"Player .+ is already up to date\.", detail)
        ):
            imported_counts.append(0)
    games_imported = sum(imported_counts) if parsing and len(imported_counts) == len(parsing) else None

    # Both old and bounded pipelines record occurrences in this enclosing
    # phase. Do not sum it with its new per-worker link counters.
    positions = processed_count("fen_extraction")
    tablebase_counts = []
    tablebase = completed_records("tablebase_marking")
    for row in tablebase:
        detail = row.detail or ""
        solved = re.fullmatch(r"Cached (\d+) exact Syzygy positions\.", detail)
        if solved:
            tablebase_counts.append(int(solved.group(1)))
        elif detail == "No new tablebase positions required marking.":
            tablebase_counts.append(0)
    marked = sum(tablebase_counts) if tablebase and len(tablebase_counts) == len(tablebase) else None
    return {
        "games_downloaded": games_downloaded,
        "games_imported": games_imported,
        "position_occurrences": positions,
        "tablebase_positions_marked": marked,
    }


async def get_ingestion_history_run(run_id):
    async with AsyncDBSession() as session:
        run = await session.get(IngestionPipelineRun, run_id)
        if run is None or run.status == "running":
            return None
        rows = list((await session.scalars(
            select(IngestionPipelineStageTiming)
            .where(IngestionPipelineStageTiming.run_id == run_id)
            .order_by(IngestionPipelineStageTiming.started_at, IngestionPipelineStageTiming.id)
        )).all())
    return {
        **run_metadata(run),
        "stages": summarize_stages(run, rows),
        "counts": summarize_counts(rows),
    }
