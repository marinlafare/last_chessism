import hashlib
from typing import Any

import chess
from sqlalchemy import delete, desc, func, insert, select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import AnalysisTime

MAX_ANALYSIS_TIME_SAMPLES = 10


def _fen_hash(fen: str) -> str:
    return hashlib.sha256(fen.encode("utf-8")).hexdigest()


def _piece_count(fen: str) -> int:
    try:
        return len(chess.Board(fen).piece_map())
    except Exception:
        return 0


def _analysis_lines(result: dict[str, Any]) -> list[dict[str, Any]]:
    analysis = result.get("analysis")
    if isinstance(analysis, list):
        return [line for line in analysis if isinstance(line, dict)]
    if isinstance(analysis, dict):
        return [analysis]
    return []


def _uses_tablebase(result: dict[str, Any]) -> bool:
    return any(int(line.get("tbhits") or 0) > 0 for line in _analysis_lines(result))


async def _prune_analysis_times(session) -> None:
    keep_ids_stmt = (
        select(AnalysisTime.id)
        .order_by(desc(AnalysisTime.created_at), desc(AnalysisTime.id))
        .limit(MAX_ANALYSIS_TIME_SAMPLES)
    )
    keep_ids = [row[0] for row in (await session.execute(keep_ids_stmt)).all()]
    if not keep_ids:
        return

    await session.execute(
        delete(AnalysisTime.__table__).where(AnalysisTime.id.not_in(keep_ids))
    )


async def record_analysis_time(
    *,
    fen: str,
    source: str,
    nodes_limit: int,
    multipv: int,
    elapsed_ms: float,
    engine_result: dict[str, Any],
) -> None:
    if not engine_result.get("is_valid"):
        return
    if _uses_tablebase(engine_result):
        return

    row = {
        "source": source,
        "fen_hash": _fen_hash(fen),
        "n_pieces": _piece_count(fen),
        "nodes_limit": int(nodes_limit),
        "multipv": int(multipv),
        "elapsed_ms": float(elapsed_ms),
    }
    async with AsyncDBSession() as session:
        await session.execute(insert(AnalysisTime.__table__).values(row))
        await _prune_analysis_times(session)
        await session.commit()


async def record_analysis_times(rows: list[dict[str, Any]]) -> int:
    prepared = []
    for row in rows:
        engine_result = row.get("engine_result") or {}
        fen = str(row.get("fen") or "")
        if not fen or not engine_result.get("is_valid") or _uses_tablebase(engine_result):
            continue
        prepared.append({
            "source": str(row.get("source") or "unknown"),
            "fen_hash": _fen_hash(fen),
            "n_pieces": _piece_count(fen),
            "nodes_limit": int(row.get("nodes_limit") or 0),
            "multipv": int(row.get("multipv") or 1),
            "elapsed_ms": float(row.get("elapsed_ms") or 0),
        })

    if not prepared:
        return 0

    async with AsyncDBSession() as session:
        await session.execute(insert(AnalysisTime.__table__), prepared)
        await _prune_analysis_times(session)
        await session.commit()
    return len(prepared)


async def get_analysis_time_summary(limit: int = MAX_ANALYSIS_TIME_SAMPLES) -> dict[str, Any]:
    limit = min(max(1, int(limit)), MAX_ANALYSIS_TIME_SAMPLES)
    async with AsyncDBSession() as session:
        await _prune_analysis_times(session)
        await session.commit()

        total_stmt = select(
            func.count(AnalysisTime.id).label("samples"),
            func.avg(AnalysisTime.elapsed_ms).label("avg_ms"),
            func.min(AnalysisTime.elapsed_ms).label("min_ms"),
            func.max(AnalysisTime.elapsed_ms).label("max_ms"),
        )
        total_row = (await session.execute(total_stmt)).mappings().one()

        by_pieces_stmt = (
            select(
                AnalysisTime.n_pieces.label("n_pieces"),
                func.count(AnalysisTime.id).label("samples"),
                func.avg(AnalysisTime.elapsed_ms).label("avg_ms"),
                func.min(AnalysisTime.elapsed_ms).label("min_ms"),
                func.max(AnalysisTime.elapsed_ms).label("max_ms"),
            )
            .group_by(AnalysisTime.n_pieces)
            .order_by(AnalysisTime.n_pieces.asc())
        )
        by_pieces = [dict(row) for row in (await session.execute(by_pieces_stmt)).mappings().all()]

        recent_stmt = (
            select(
                AnalysisTime.created_at,
                AnalysisTime.source,
                AnalysisTime.n_pieces,
                AnalysisTime.nodes_limit,
                AnalysisTime.multipv,
                AnalysisTime.elapsed_ms,
            )
            .order_by(desc(AnalysisTime.created_at))
            .limit(limit)
        )
        recent = [dict(row) for row in (await session.execute(recent_stmt)).mappings().all()]

    overall = dict(total_row)
    avg_ms = overall.get("avg_ms")
    overall["avg_seconds"] = (float(avg_ms) / 1000) if avg_ms is not None else None
    overall["retention_limit"] = MAX_ANALYSIS_TIME_SAMPLES

    return {
        "overall": overall,
        "by_pieces": by_pieces,
        "recent": recent,
    }
