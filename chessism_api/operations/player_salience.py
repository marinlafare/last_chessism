"""Corpus-wide game salience projections for tracked players."""

from __future__ import annotations

import asyncio
import json
import time
import logging
from collections.abc import Iterable
from datetime import date
from typing import Any

from arq.connections import ArqRedis
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup

logger = logging.getLogger(__name__)
from chessism_api.operations.player_salience_calculation import calculate_player_salience
from chessism_api.operations.player_hero_analytics import CHART_MODES, _analytics_scope
from chessism_api.operations.player_timezone import player_local_timestamp_sql


SALIENCE_QUEUE_NAME = "salience_queue"
SALIENCE_PROGRESS_KIND = "player_salience"
SALIENCE_PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
MINIMUM_ACCURACY_PLAYER_MOVES_EXCLUSIVE = 10


def position_depth_weight(ply: int) -> float:
    """Give opening positions some weight and reach full weight at ply 16."""
    safe_ply = max(0, int(ply))
    return 0.25 + 0.75 * min(safe_ply / 16.0, 1.0)


def position_salience(games_with_position: int, occurrence_index: int) -> float:
    """Return corpus and intra-game repetition weight for one occurrence."""
    corpus_frequency = max(1, int(games_with_position))
    intra_game_index = max(1, int(occurrence_index))
    return 1.0 / (corpus_frequency * intra_game_index)


def salience_weighted_accuracy(rows: Iterable[dict[str, Any]]) -> float | None:
    """Return a weighted accuracy without treating repetition as extra evidence."""
    numerator = 0.0
    denominator = 0.0
    for row in rows:
        accuracy = row.get("accuracy")
        salience = row.get("salience")
        if accuracy is None or salience is None:
            continue
        safe_salience = max(0.0, float(salience))
        numerator += float(accuracy) * safe_salience
        denominator += safe_salience
    return numerator / denominator if denominator > 0 else None


def _normalized_player_names(player_names: Iterable[str]) -> list[str]:
    return sorted({str(name).strip().lower() for name in player_names if str(name).strip()})


async def mark_player_salience_stale(
    player_names: Iterable[str],
    *,
    game_links: Iterable[int] = (),
    session: AsyncSession | None = None,
) -> int:
    """Mark tracked players stale in the same transaction as game ingestion."""
    normalized = _normalized_player_names(player_names)
    if not normalized:
        return 0
    links = sorted({int(link) for link in game_links})

    async def execute(working_session):
        rows = (await working_session.execute(text("""
            INSERT INTO player_salience_summary(player_id,status)
            SELECT id,'stale' FROM player
            WHERE player_name=ANY(CAST(:players AS text[]))
              AND joined>0 AND deleted_at IS NULL
            ON CONFLICT(player_id) DO UPDATE SET status='stale',error=NULL
            RETURNING player_id
        """), {'players':normalized})).all()
        if links:
            await working_session.execute(text("""
                INSERT INTO player_salience_pending_game(game_link,is_white)
                SELECT gp.game_link,gp.is_white FROM game_player gp
                JOIN player p ON p.id=gp.player_id JOIN game g ON g.link=gp.game_link
                WHERE gp.game_link=ANY(CAST(:links AS bigint[]))
                  AND p.player_name=ANY(CAST(:players AS text[]))
                  AND p.joined>0 AND p.deleted_at IS NULL AND g.is_standard AND g.n_moves>0
                ON CONFLICT DO NOTHING
            """), {'links':links,'players':normalized})
        return len(rows)

    if session is not None:
        return await execute(session)
    async with write_transaction(AsyncDBSession) as owned_session:
        return await execute(owned_session)


async def seed_player_salience_summaries() -> int:
    """Create stale projection rows for every tracked, non-deleted player."""
    async with write_transaction(AsyncDBSession) as session:
        rows = (await session.execute(text("""
            INSERT INTO player_salience_summary(player_id,status)
            SELECT id,'stale' FROM player WHERE joined>0 AND deleted_at IS NULL
            ON CONFLICT(player_id) DO NOTHING RETURNING player_id
        """))).all()
        return len(rows)


async def _write_progress(
    redis: ArqRedis,
    job_id: str,
    *,
    player_name: str,
    phase: str,
    total: int,
    processed: int,
    detail: str,
    failed: int = 0,
) -> None:
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps({
            "job_id": job_id,
            "kind": SALIENCE_PROGRESS_KIND,
            "player_name": player_name,
            "phase": phase,
            "total": max(0, int(total)),
            "processed": max(0, int(processed)),
            "failed": max(0, int(failed)),
            "detail": detail,
            "updated_at": time.time(),
        }),
        ex=SALIENCE_PROGRESS_TTL_SECONDS,
    )


async def enqueue_player_salience(
    redis: ArqRedis,
    player_name: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Queue one corpus refresh while preventing duplicate player jobs."""
    player = str(player_name).strip().lower()
    async with write_transaction(AsyncDBSession) as session:
        from .cuda_summaries.salience_store import enabled
        if await enabled(session):
            return {'status':'pending_cuda','player_name':player,'job_id':None}
        exists = await session.scalar(text("""
            SELECT EXISTS (
                SELECT 1 FROM player
                WHERE player_name = :player
                  AND COALESCE(joined, 0) > 0
                  AND deleted_at IS NULL
            )
        """), {"player": player})
        if not exists:
            raise ValueError(f"Tracked player {player!r} was not found.")
        target = 'player_salience_summary'
        owner_column = 'player_id'
        owner = '(SELECT id FROM player WHERE player_name=:player)'
        await session.execute(text(f"""
            INSERT INTO {target} (
                {owner_column}, status, source_game_count,
                source_position_count, effective_game_count, error
            ) VALUES ({owner}, 'stale', 0, 0, 0, NULL)
            ON CONFLICT ({owner_column}) DO NOTHING
        """), {"player": player})
        current_status = await session.scalar(text(f"""
            SELECT status
            FROM {target}
            WHERE {owner_column} = {owner}
            FOR UPDATE
        """), {"player": player})
        if current_status in ("queued", "running"):
            return {"status": "already_active", "player_name": player, "job_id": None}
        if current_status == "ready" and not force:
            return {"status": "up_to_date", "player_name": player, "job_id": None}
        await session.execute(text(f"""
            UPDATE {target}
            SET status = 'queued', error = NULL
            WHERE {owner_column} = {owner}
        """), {"player": player})

    try:
        job = await redis.enqueue_job(
            "run_player_salience_job",
            player_name=player,
            _queue_name=SALIENCE_QUEUE_NAME,
        )
        if job is None:
            raise RuntimeError("Redis did not create the player salience job.")
        job_id = str(job.job_id)
        await _write_progress(
            redis,
            job_id,
            player_name=player,
            phase="queued",
            total=1,
            processed=0,
            detail=f"Waiting to calculate corpus-wide salience for {player}.",
        )
        return {"status": "queued", "player_name": player, "job_id": job_id}
    except Exception:
        async with write_transaction(AsyncDBSession) as session:
            await session.execute(text(f"""
                UPDATE {target}
                SET status = 'stale'
                WHERE {owner_column} = {owner} AND status = 'queued'
            """), {"player": player})
        raise


async def enqueue_stale_player_salience_jobs(
    redis: ArqRedis,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Queue bounded stale projections after the FEN/tablebase pipeline drains."""
    async with read_transaction(AsyncDBSession) as session:
        result = await session.execute(text("""
            SELECT p.player_name
            FROM player_salience_summary s JOIN player p ON p.id=s.player_id
            WHERE s.status IN ('stale', 'failed')
            ORDER BY player_name
            LIMIT :limit
        """), {"limit": max(1, min(int(limit), 1_000))})
        players = [str(row[0]) for row in result.all()]
    return [await enqueue_player_salience(redis, player, force=True) for player in players]


async def run_player_salience_job(
    ctx: dict[str, Any],
    player_name: str,
    **_job_options: Any,
) -> dict[str, Any]:
    redis: ArqRedis = ctx["redis"]
    job_id = str(ctx.get("job_id") or f"player-salience-{player_name}")
    player = str(player_name).strip().lower()
    async with write_transaction(AsyncDBSession) as session:
        from .cuda_summaries.salience_store import enabled
        if await enabled(session):
            # Older producers may still send a queue wake-up. It must never
            # become a CPU calculation or an implicit manual CUDA request.
            await session.execute(text("""UPDATE player_salience_summary
                SET status='stale' WHERE player_id=(SELECT id FROM player WHERE player_name=:player)
                AND status IN ('queued','running')"""), {'player':player})
            await _write_progress(redis,job_id,player_name=player,phase='complete',total=1,processed=1,
                detail='Salience is pending the next manual Run CUDA summaries request.')
            return {'status':'pending_cuda','player_name':player,'job_id':None}
    await _write_progress(
        redis,
        job_id,
        player_name=player,
        phase="calculating",
        total=1,
        processed=0,
        detail=f"Calculating position frequencies across every game owned by {player}.",
    )
    try:
        result = await calculate_player_salience(player)
        await _write_progress(
            redis,
            job_id,
            player_name=player,
            phase="complete",
            total=1,
            processed=1,
            detail=(
                f"{result['calculation'].title()} calculation covered "
                f"{result['source_game_count']:,} games as "
                f"{result['effective_game_count']:,.2f} effective games"
                + ("; another refresh is required." if result["status"] != "ready" else ".")
            ),
        )
        return result
    except (Exception, asyncio.CancelledError) as error:
        await _write_progress(
            redis,
            job_id,
            player_name=player,
            phase="failed",
            total=1,
            processed=0,
            failed=1,
            detail=str(error),
        )
        raise


def _summary_payload(row: Any | None, player_name: str) -> dict[str, Any]:
    if row is None:
        return {
            "player_name": player_name,
            "status": "missing",
            "source_game_count": 0,
            "source_position_count": 0,
            "effective_game_count": 0.0,
            "error": None,
        }
    return {
        "player_name": str(row["player_name"]),
        "status": str(row["status"]),
        "source_game_count": int(row["source_game_count"] or 0),
        "source_position_count": int(row["source_position_count"] or 0),
        "effective_game_count": round(float(row["effective_game_count"] or 0), 6),
        "error": row["error"],
    }


async def get_salience_overview(limit: int = 100) -> dict[str, Any]:
    async with read_transaction(AsyncDBSession) as session:
        result = await session.execute(text("""
            SELECT
                player.player_name,
                COALESCE(summary.status, 'missing') AS status,
                COALESCE(summary.source_game_count, 0) AS source_game_count,
                COALESCE(summary.source_position_count, 0) AS source_position_count,
                COALESCE(summary.effective_game_count, 0) AS effective_game_count,
                summary.error
            FROM player
            LEFT JOIN player_salience_summary summary
              ON summary.player_id = player.id
            WHERE COALESCE(player.joined, 0) > 0
              AND player.deleted_at IS NULL
            ORDER BY
                CASE COALESCE(summary.status, 'missing')
                    WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'stale' THEN 2
                    WHEN 'missing' THEN 3 WHEN 'failed' THEN 4 ELSE 5
                END,
                player.player_name
            LIMIT :limit
        """), {"limit": max(1, min(int(limit), 1_000))})
        rows = result.mappings().all()
    players = [_summary_payload(row, str(row["player_name"])) for row in rows]
    status_counts: dict[str, int] = {}
    for item in players:
        status = str(item["status"])
        status_counts[status] = status_counts.get(status, 0) + 1
    return {"players": players, "status_counts": status_counts}


async def get_player_salience_report(
    player_name: str,
    *,
    game_limit: int = 10,
) -> dict[str, Any]:
    player = str(player_name).strip().lower()
    safe_limit = max(1, min(int(game_limit), 50))
    async with read_transaction(AsyncDBSession) as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        player_exists = await session.scalar(
            text("SELECT EXISTS (SELECT 1 FROM player WHERE player_name = :player)"),
            {"player": player},
        )
        if not player_exists:
            raise ValueError(f"Player {player!r} was not found.")
        summary = (await session.execute(text("""
            SELECT
                p.player_name,s.status,s.source_game_count,
                s.source_position_count,s.effective_game_count,s.error
            FROM public.player_salience_summary s JOIN public.player p ON p.id=s.player_id
            WHERE p.player_name = :player
        """), {"player": player})).mappings().first()
        distribution = (await session.execute(text("""
            SELECT
                COUNT(*)::bigint AS games,
                AVG(salience)::double precision AS mean,
                MIN(salience)::double precision AS minimum,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salience)::double precision AS p25,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salience)::double precision AS median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salience)::double precision AS p75,
                MAX(salience)::double precision AS maximum
            FROM public.game_player_salience s JOIN public.game_player gp
                ON gp.game_link=s.game_link AND gp.is_white=s.is_white
            JOIN public.player p ON p.id=gp.player_id WHERE p.player_name=:player
        """), {"player": player})).mappings().one()
        colors = (await session.execute(text("""
            SELECT
                CASE WHEN s.is_white THEN 'white' ELSE 'black' END AS player_color,
                COUNT(*)::bigint AS games,
                SUM(salience)::double precision AS effective_games,
                AVG(salience)::double precision AS mean_salience
            FROM public.game_player_salience s JOIN public.game_player gp
                ON gp.game_link=s.game_link AND gp.is_white=s.is_white
            JOIN public.player p ON p.id=gp.player_id WHERE p.player_name=:player
            GROUP BY player_color
            ORDER BY player_color
        """), {"player": player})).mappings().all()

        async def game_rows(direction: str) -> list[dict[str, Any]]:
            order = "ASC" if direction == "ASC" else "DESC"
            rows = (await session.execute(text(f"""
                SELECT
                    salience.game_link,
                    CASE WHEN salience.is_white THEN 'white' ELSE 'black' END AS player_color,
                    salience.salience,
                    salience.position_occurrence_count,
                    salience.unique_position_count,
                    salience.repeated_position_count,
                    g.played_at,
                    mode.name AS mode,
                    g.n_moves,
                    CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
                    opponent.player_name AS opponent_name
                FROM game_player_salience salience
                JOIN public.game_player gp ON gp.game_link=salience.game_link AND gp.is_white=salience.is_white
                JOIN public.player p ON p.id=gp.player_id
                JOIN public.game g ON g.link=gp.game_link
                LEFT JOIN public.game_mode mode ON mode.id=g.mode_id
                JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
                WHERE p.player_name=:player
                ORDER BY salience.salience {order}, salience.game_link,salience.is_white DESC
                LIMIT :limit
            """), {"player": player, "limit": safe_limit})).mappings().all()
            return [
                {
                    **dict(row),
                    "played_at": row["played_at"].isoformat() if row["played_at"] else None,
                    "salience": round(float(row["salience"]), 8),
                }
                for row in rows
            ]

        most_repetitive = await game_rows("ASC")
        most_distinctive = await game_rows("DESC")

    def optional_float(value: Any) -> float | None:
        return round(float(value), 8) if value is not None else None

    return {
        "summary": _summary_payload(summary, player),
        "distribution": {
            "games": int(distribution["games"] or 0),
            "mean": optional_float(distribution["mean"]),
            "minimum": optional_float(distribution["minimum"]),
            "p25": optional_float(distribution["p25"]),
            "median": optional_float(distribution["median"]),
            "p75": optional_float(distribution["p75"]),
            "maximum": optional_float(distribution["maximum"]),
        },
        "by_color": [
            {
                "player_color": str(row["player_color"]),
                "games": int(row["games"]),
                "effective_games": round(float(row["effective_games"] or 0), 6),
                "mean_salience": round(float(row["mean_salience"] or 0), 8),
            }
            for row in colors
        ],
        "most_repetitive": most_repetitive,
        "most_distinctive": most_distinctive,
    }


def normalize_salience_modes(mode: str) -> tuple[str, ...]:
    requested = {
        item.strip().lower()
        for item in str(mode or "all").split(",")
        if item.strip()
    }
    if not requested or requested == {"all"}:
        return CHART_MODES
    if "all" in requested or not requested.issubset(CHART_MODES):
        raise ValueError("Mode must contain only bullet, blitz and/or rapid.")
    return tuple(name for name in CHART_MODES if name in requested)


async def get_player_daily_salience_accuracy(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    timezone_name: str | None = None,
    salience_min: float | None = None,
    salience_max: float | None = None,
) -> dict[str, Any]:
    """Filter individual games, then aggregate salience-weighted daily accuracy."""
    from chessism_api.operations.player_salience_chart import daily_chart
    return await daily_chart(player_name, normalize_salience_modes(mode),
        date_from, date_to, timezone_name, salience_min, salience_max)
