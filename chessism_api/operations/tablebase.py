import asyncio
import json
import os
import time
from typing import Any, Sequence

import chess
import chess.syzygy
from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus
from sqlalchemy import bindparam, func, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.ask_db import (
    increment_database_summary_fen_counts,
    increment_game_analysis_summary_for_scored_fens,
    increment_scored_position_summary_for_scored_fens,
    refresh_database_summary_fen_counts,
    refresh_fen_pipeline_summary,
    refresh_game_analysis_summary,
    refresh_scored_position_summary,
    refresh_scored_rating_summary,
)
from chessism_api.operations.player_salience import enqueue_stale_player_salience_jobs
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen
from chessism_api.operations.ingestion_pipeline import TABLEBASE_MARKING
from chessism_api.operations.ingestion_pipeline.timing import (
    fail_running_ingestion_stages,
    finish_ingestion_run,
    finish_ingestion_stage,
    start_ingestion_stage,
)


TABLEBASE_PATH = os.getenv("STOCKFISH_SYZYGY_PATH", "/syzygy")
TABLEBASE_MAX_PIECES = 5
TABLEBASE_BATCH_SIZE = 1_000
TABLEBASE_PROGRESS_KIND = "tablebase_analysis"
TABLEBASE_COORDINATION_KEY = "chessism:automatic_tablebase_analysis"
PROGRESS_TTL_SECONDS = 60 * 60 * 24
COORDINATION_TTL_SECONDS = 60 * 60 * 24
RESERVATION_TTL_SECONDS = 60
ACTIVE_JOB_STATUSES = {
    JobStatus.queued,
    JobStatus.deferred,
    JobStatus.in_progress,
}

PIECE_COUNT_SQL = (
    "COALESCE(f.piece_count, "
    "char_length(translate(split_part(f.fen, ' ', 1), '12345678/', '')))"
)
TABLEBASE_CANDIDATE_SQL = f"""
    f.score IS NULL
    AND NOT EXISTS (SELECT 1 FROM cloud_fen_claim c WHERE c.fen = f.fen)
    AND COALESCE(f.analysis_source, '') <> 'tablebase_unavailable'
    AND {PIECE_COUNT_SQL} BETWEEN 2 AND {TABLEBASE_MAX_PIECES}
"""


def _redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value or "")


async def _write_progress(
    redis: ArqRedis | None,
    job_id: str,
    *,
    total: int,
    processed: int,
    failed: int,
    phase: str,
    detail: str,
    ingestion_run_id: str | None = None,
) -> None:
    if redis is None:
        return
    payload = {
        "job_id": job_id,
        "kind": TABLEBASE_PROGRESS_KIND,
        "ingestion_run_id": ingestion_run_id,
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "failed": max(0, int(failed)),
        "phase": phase,
        "detail": detail,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )


async def _read_progress(redis: ArqRedis | None, job_id: str) -> dict[str, Any]:
    if redis is None:
        return {}
    raw = await redis.get(f"chessism:job_progress:{job_id}")
    if not raw:
        return {}
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _clean_game_links(game_links: Sequence[int] | None) -> list[int]:
    return list(dict.fromkeys(
        int(link) for link in (game_links or []) if link is not None
    ))


async def count_tablebase_candidates(game_links: Sequence[int] | None = None) -> int:
    clean_links = _clean_game_links(game_links)
    if game_links is not None and not clean_links:
        return 0

    if clean_links:
        query = f"""
            SELECT COUNT(DISTINCT f.fen)::bigint
            FROM game_fen_association gfa
            JOIN fen f ON f.fen = gfa.fen_fen
            WHERE gfa.game_link = ANY(CAST(:game_links AS BIGINT[]))
              AND {TABLEBASE_CANDIDATE_SQL};
        """
        params = {"game_links": clean_links}
    else:
        query = f"SELECT COUNT(*)::bigint FROM fen f WHERE {TABLEBASE_CANDIDATE_SQL};"
        params = {}

    async with AsyncDBSession() as session:
        return int(await session.scalar(text(query), params) or 0)


async def _lease_tablebase_batch(
    game_links: Sequence[int] | None,
    limit: int,
) -> tuple[AsyncSession | None, list[dict[str, Any]]]:
    clean_links = _clean_game_links(game_links)
    if game_links is not None and not clean_links:
        return None, []

    session = AsyncDBSession()
    try:
        await session.begin()
        if clean_links:
            query = f"""
                WITH candidate_fens AS MATERIALIZED (
                    SELECT f.fen, MIN(gfa.n_move) AS first_move
                    FROM game_fen_association gfa
                    JOIN fen f ON f.fen = gfa.fen_fen
                    WHERE gfa.game_link = ANY(CAST(:game_links AS BIGINT[]))
                      AND {TABLEBASE_CANDIDATE_SQL}
                    GROUP BY f.fen
                )
                SELECT f.fen, {PIECE_COUNT_SQL}::smallint AS piece_count
                FROM candidate_fens candidate
                JOIN fen f ON f.fen = candidate.fen
                ORDER BY candidate.first_move, f.fen
                LIMIT :limit
                FOR UPDATE OF f SKIP LOCKED;
            """
            params = {"game_links": clean_links, "limit": max(1, int(limit))}
        else:
            query = f"""
                SELECT f.fen, {PIECE_COUNT_SQL}::smallint AS piece_count
                FROM fen f
                WHERE {TABLEBASE_CANDIDATE_SQL}
                ORDER BY f.n_games DESC, f.fen
                LIMIT :limit
                FOR UPDATE OF f SKIP LOCKED;
            """
            params = {"limit": max(1, int(limit))}

        rows = [dict(row) for row in (await session.execute(text(query), params)).mappings()]
        from chessism_api.database.fen_claims import unclaimed_locked_fens
        unclaimed = set(await unclaimed_locked_fens(session, [row["fen"] for row in rows]))
        rows = [row for row in rows if row["fen"] in unclaimed]
        if not rows:
            await session.rollback()
            await session.close()
            return None, []
        return session, rows
    except Exception:
        await session.rollback()
        await session.close()
        raise


def _move_rank(
    outcome: int,
    child_dtz: int | None,
) -> tuple[int, int]:
    distance = abs(int(child_dtz or 0))
    if outcome > 0:
        return outcome, -distance
    if outcome < 0:
        return outcome, distance
    return outcome, 0


def _best_tablebase_move(
    tablebase: chess.syzygy.Tablebase,
    board: chess.Board,
) -> str | None:
    best_move = None
    best_rank = None
    for move in sorted(board.legal_moves, key=lambda candidate: candidate.uci()):
        child = board.copy(stack=False)
        child.push(move)
        child_wdl = tablebase.get_wdl(child)
        if child_wdl is None:
            continue
        rank = _move_rank(-int(child_wdl), tablebase.get_dtz(child))
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_move = move.uci()
    return best_move


def _tablebase_score_payload(board: chess.Board, raw_wdl: int) -> dict[str, Any]:
    white_wdl = int(raw_wdl) if board.turn == chess.WHITE else -int(raw_wdl)
    if white_wdl == 2:
        return {
            "score": 1_000.0,
            "wdl_win": 1_000.0,
            "wdl_draw": 0.0,
            "wdl_loss": 0.0,
        }
    if white_wdl == -2:
        return {
            "score": -1_000.0,
            "wdl_win": 0.0,
            "wdl_draw": 0.0,
            "wdl_loss": 1_000.0,
        }
    # Cursed wins and blessed losses are draws under the fifty-move rule.
    return {
        "score": 0.0,
        "wdl_win": 0.0,
        "wdl_draw": 1_000.0,
        "wdl_loss": 0.0,
    }


def _probe_batch_sync(
    tablebase: chess.syzygy.Tablebase,
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    solved = []
    unavailable = []
    for row in rows:
        fen = str(row["fen"])
        piece_count = int(row.get("piece_count") or 0)
        try:
            board = chess.Board(fen)
            if piece_count > TABLEBASE_MAX_PIECES or board.castling_rights:
                raise KeyError("Position is not compatible with the installed tablebases")
            raw_wdl = tablebase.get_wdl(board)
            raw_dtz = tablebase.get_dtz(board)
            if raw_wdl is None or raw_dtz is None:
                raise KeyError("Position is missing from the installed tablebases")
            solved.append({
                "fen": fen,
                "piece_count": piece_count,
                "tablebase_wdl": int(raw_wdl),
                "tablebase_dtz": int(raw_dtz),
                "next_moves": _best_tablebase_move(tablebase, board),
                **_tablebase_score_payload(board, int(raw_wdl)),
            })
        except (ValueError, KeyError, chess.syzygy.MissingTableError):
            unavailable.append({"fen": fen, "piece_count": piece_count})
    return solved, unavailable


async def _save_tablebase_batch(
    session: AsyncSession,
    solved: list[dict[str, Any]],
    unavailable: list[dict[str, Any]],
) -> None:
    if solved:
        prepared = [{
            "p_fen": item["fen"],
            "p_piece_count": item["piece_count"],
            "p_score": item["score"],
            "p_next_moves": item["next_moves"],
            "p_wdl_win": item["wdl_win"],
            "p_wdl_draw": item["wdl_draw"],
            "p_wdl_loss": item["wdl_loss"],
            "p_tablebase_wdl": item["tablebase_wdl"],
            "p_tablebase_dtz": item["tablebase_dtz"],
        } for item in solved]
        statement = (
            update(Fen.__table__)
            .where(Fen.fen == bindparam("p_fen"))
            .values(
                piece_count=bindparam("p_piece_count"),
                score=bindparam("p_score"),
                next_moves=bindparam("p_next_moves"),
                wdl_win=bindparam("p_wdl_win"),
                wdl_draw=bindparam("p_wdl_draw"),
                wdl_loss=bindparam("p_wdl_loss"),
                analysis_source="tablebase",
                tablebase_wdl=bindparam("p_tablebase_wdl"),
                tablebase_dtz=bindparam("p_tablebase_dtz"),
                analyzed_at=func.now(),
            )
        )
        await session.execute(
            statement,
            prepared,
            execution_options={"synchronize_session": False},
        )

    if unavailable:
        prepared_unavailable = [{
            "p_fen": item["fen"],
            "p_piece_count": item["piece_count"],
        } for item in unavailable]
        unavailable_statement = (
            update(Fen.__table__)
            .where(Fen.fen == bindparam("p_fen"))
            .values(
                piece_count=bindparam("p_piece_count"),
                analysis_source="tablebase_unavailable",
            )
        )
        await session.execute(
            unavailable_statement,
            prepared_unavailable,
            execution_options={"synchronize_session": False},
        )


async def analyze_tablebase_positions(
    ctx: dict,
    *,
    game_links: Sequence[int] | None = None,
    candidate_count: int | None = None,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    progress_job_id: str | None = None,
    refresh_projections: bool = True,
    ingestion_run_id: str | None = None,
) -> dict[str, int]:
    """Solve tablebase-eligible positions directly and persist exact results."""
    if not os.path.isdir(TABLEBASE_PATH):
        raise RuntimeError(f"Syzygy tablebase directory is unavailable: {TABLEBASE_PATH}")

    redis = ctx.get("redis")
    job_id = str(progress_job_id or ctx.get("job_id") or "tablebase-analysis")
    available = (
        max(0, int(candidate_count))
        if candidate_count is not None
        else await count_tablebase_candidates(game_links)
    )
    target = min(available, max(0, int(max_positions))) if max_positions is not None else available
    if target <= 0:
        return {"processed": 0, "solved": 0, "unavailable": 0, "target": 0}

    progress_total = target + (1 if refresh_projections else 0)

    await _write_progress(
        redis,
        job_id,
        total=progress_total,
        processed=0,
        failed=0,
        phase="tablebase",
        detail=f"Probing {target} positions directly with Syzygy.",
        ingestion_run_id=ingestion_run_id,
    )

    processed = solved_total = unavailable_total = 0
    safe_batch_size = max(1, min(int(batch_size), 5_000))
    tablebase = chess.syzygy.open_tablebase(TABLEBASE_PATH)
    try:
        while processed < target:
            session, rows = await _lease_tablebase_batch(
                game_links,
                min(safe_batch_size, target - processed),
            )
            if not rows or session is None:
                break
            try:
                solved, unavailable_rows = await asyncio.to_thread(
                    _probe_batch_sync,
                    tablebase,
                    rows,
                )
                await _save_tablebase_batch(session, solved, unavailable_rows)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

            if solved:
                try:
                    await increment_database_summary_fen_counts(
                        analyzed_delta=len(solved),
                        nonzero_scored_delta=sum(
                            1
                            for item in solved
                            if float(item.get("score") or 0) != 0.0
                        ),
                    )
                    await increment_scored_position_summary_for_scored_fens(solved)
                    await increment_game_analysis_summary_for_scored_fens(solved)
                except Exception:
                    # FEN scores were committed immediately before these cached
                    # summaries. Reconcile them before surfacing the failure so a
                    # retry cannot leave counters or per-game coverage incomplete.
                    await refresh_database_summary_fen_counts()
                    await refresh_scored_position_summary()
                    await refresh_game_analysis_summary(
                        _clean_game_links(game_links) if game_links is not None else None
                    )
                    raise

            processed += len(rows)
            solved_total += len(solved)
            unavailable_total += len(unavailable_rows)
            await _write_progress(
                redis,
                job_id,
                total=progress_total,
                processed=processed,
                failed=unavailable_total,
                phase="tablebase",
                detail=f"Solved {solved_total}; {unavailable_total} require Stockfish.",
                ingestion_run_id=ingestion_run_id,
            )
    finally:
        tablebase.close()

    # A scoped automatic run follows freshly inserted game associations. Those
    # associations change rating projections even when every endgame probe is
    # unavailable, so the handoff still needs one projection refresh.
    if refresh_projections and (solved_total > 0 or game_links is not None):
        await _write_progress(
            redis,
            job_id,
            total=progress_total,
            processed=processed,
            failed=unavailable_total,
            phase="refreshing_statistics",
            detail="Refreshing rating statistics for the solved endgames.",
            ingestion_run_id=ingestion_run_id,
        )
        await refresh_scored_rating_summary()
        await _write_progress(
            redis,
            job_id,
            total=progress_total,
            processed=processed + 1,
            failed=unavailable_total,
            phase="tablebase",
            detail="Tablebase and rating projections are current.",
            ingestion_run_id=ingestion_run_id,
        )

    return {
        "processed": processed,
        "solved": solved_total,
        "unavailable": unavailable_total,
        "target": target,
    }


async def _release_coordination(redis: ArqRedis, job_id: str) -> None:
    if _redis_text(await redis.get(TABLEBASE_COORDINATION_KEY)) == job_id:
        await redis.delete(TABLEBASE_COORDINATION_KEY)


async def ensure_tablebase_analysis_enqueued(
    redis: ArqRedis,
    *,
    game_links: Sequence[int] | None = None,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    ingestion_run_id: str | None = None,
) -> dict[str, Any]:
    current_job_id = _redis_text(await redis.get(TABLEBASE_COORDINATION_KEY))
    if current_job_id and current_job_id != "reserving":
        status = await Job(
            current_job_id,
            redis,
            _queue_name="pipeline_queue",
        ).status()
        if status in ACTIVE_JOB_STATUSES:
            return {"status": "already_active", "job_id": current_job_id, "pending": None}
        await redis.delete(TABLEBASE_COORDINATION_KEY)
    elif current_job_id == "reserving":
        return {"status": "already_active", "job_id": None, "pending": None}

    clean_links = _clean_game_links(game_links)
    pending = await count_tablebase_candidates(
        clean_links if game_links is not None else None
    )
    if pending <= 0:
        if game_links is None:
            await refresh_fen_pipeline_summary(mark_tablebase_current=True)
        return {"status": "up_to_date", "job_id": None, "pending": 0}

    reserved = await redis.set(
        TABLEBASE_COORDINATION_KEY,
        "reserving",
        ex=RESERVATION_TTL_SECONDS,
        nx=True,
    )
    if not reserved:
        return {"status": "already_active", "job_id": None, "pending": pending}

    try:
        job = await redis.enqueue_job(
            "run_tablebase_analysis_job",
            max_positions=max_positions,
            batch_size=max(1, min(int(batch_size), 5_000)),
            candidate_count=pending,
            game_links=clean_links if game_links is not None else None,
            ingestion_run_id=ingestion_run_id,
            _queue_name="pipeline_queue",
        )
        if job is None:
            raise RuntimeError("Redis did not create the tablebase job.")
        job_id = str(job.job_id)
        await redis.set(
            TABLEBASE_COORDINATION_KEY,
            job_id,
            ex=COORDINATION_TTL_SECONDS,
        )
        target = min(pending, int(max_positions)) if max_positions is not None else pending
        await _write_progress(
            redis,
            job_id,
            total=target + 1,
            processed=0,
            failed=0,
            phase="queued",
            detail="Waiting to cache exact Syzygy endgames.",
            ingestion_run_id=ingestion_run_id,
        )
        return {"status": "queued", "job_id": job_id, "pending": pending}
    except Exception:
        if _redis_text(await redis.get(TABLEBASE_COORDINATION_KEY)) == "reserving":
            await redis.delete(TABLEBASE_COORDINATION_KEY)
        raise


async def run_tablebase_analysis_job(
    ctx: dict,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    candidate_count: int | None = None,
    game_links: Sequence[int] | None = None,
    ingestion_run_id: str | None = None,
    **kwargs,
) -> dict[str, int]:
    redis: ArqRedis = ctx["redis"]
    job_id = str(ctx.get("job_id") or "tablebase-analysis")
    try:
        await start_ingestion_stage(
            ingestion_run_id,
            TABLEBASE_MARKING,
            detail="Probing exact Syzygy endgames and refreshing projections.",
        )
        result = await analyze_tablebase_positions(
            ctx,
            game_links=game_links,
            candidate_count=candidate_count,
            max_positions=max_positions,
            batch_size=batch_size,
            progress_job_id=job_id,
            ingestion_run_id=ingestion_run_id,
        )
        if game_links is None and await count_tablebase_candidates() <= 0:
            await refresh_fen_pipeline_summary(mark_tablebase_current=True)
        await _write_progress(
            redis,
            job_id,
            total=result["target"] + 1,
            processed=result["processed"] + 1,
            failed=result["unavailable"],
            phase="complete",
            detail=f"Cached {result['solved']} exact Syzygy positions.",
            ingestion_run_id=ingestion_run_id,
        )
        try:
            salience_jobs = await enqueue_stale_player_salience_jobs(redis)
            if salience_jobs:
                print(
                    f"[TABLEBASE {job_id[:6]}] Queued {len(salience_jobs)} "
                    "player salience refreshes.",
                    flush=True,
                )
        except Exception as error:
            print(
                f"[TABLEBASE {job_id[:6]}] Could not queue player salience "
                f"refreshes: {error!r}",
                flush=True,
            )
        await finish_ingestion_stage(
            ingestion_run_id,
            TABLEBASE_MARKING,
            processed=result["processed"] + 1,
            total=result["target"] + 1,
            detail=f"Cached {result['solved']} exact Syzygy positions.",
        )
        await finish_ingestion_run(ingestion_run_id)
        return result
    except Exception as error:
        previous_progress = await _read_progress(redis, job_id)
        await _write_progress(
            redis,
            job_id,
            total=int(previous_progress.get("total") or 0),
            processed=int(previous_progress.get("processed") or 0),
            failed=max(1, int(previous_progress.get("failed") or 0)),
            phase="failed",
            detail=str(error),
            ingestion_run_id=ingestion_run_id,
        )
        await fail_running_ingestion_stages(ingestion_run_id, str(error))
        await finish_ingestion_run(
            ingestion_run_id,
            status="failed",
            error=str(error),
        )
        raise
    finally:
        await _release_coordination(redis, job_id)
