"""Corpus-wide game salience projections for tracked players."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from datetime import date
from typing import Any

from arq.connections import ArqRedis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.engine import AsyncDBSession
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
    session: AsyncSession | None = None,
) -> int:
    """Mark tracked players stale in the same transaction as game ingestion."""
    normalized = _normalized_player_names(player_names)
    if not normalized:
        return 0
    statement = text("""
        INSERT INTO player_salience_summary (
            player_name, status, source_game_count,
            source_position_count, effective_game_count, error
        )
        SELECT
            player.player_name, 'stale', 0, 0, 0, NULL
        FROM player
        WHERE player.player_name = ANY(CAST(:players AS text[]))
          AND COALESCE(player.joined, 0) > 0
          AND player.deleted_at IS NULL
        ON CONFLICT (player_name) DO UPDATE SET
            status = 'stale',
            error = NULL
        RETURNING player_name
    """)
    if session is not None:
        result = await session.execute(statement, {"players": normalized})
        return len(result.fetchall())
    async with AsyncDBSession() as owned_session:
        result = await owned_session.execute(statement, {"players": normalized})
        rows = result.fetchall()
        await owned_session.commit()
        return len(rows)


async def seed_player_salience_summaries() -> int:
    """Create stale projection rows for every tracked, non-deleted player."""
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            INSERT INTO player_salience_summary (
                player_name, status, source_game_count,
                source_position_count, effective_game_count, error
            )
            SELECT player_name, 'stale', 0, 0, 0, NULL
            FROM player
            WHERE COALESCE(joined, 0) > 0
              AND deleted_at IS NULL
            ON CONFLICT (player_name) DO NOTHING
            RETURNING player_name
        """))
        rows = result.fetchall()
        await session.commit()
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


async def _set_failed(player_name: str, error: Exception) -> None:
    async with AsyncDBSession() as session:
        await session.execute(text("""
            UPDATE player_salience_summary
            SET
                status = CASE WHEN status = 'stale' THEN 'stale' ELSE 'failed' END,
                error = :error
            WHERE player_name = :player
        """), {"player": player_name, "error": str(error)})
        await session.commit()


async def calculate_player_salience(player_name: str) -> dict[str, Any]:
    """Atomically rebuild one player's corpus-wide per-game salience values."""
    player = str(player_name).strip().lower()
    if not player:
        raise ValueError("Player name is required.")

    async with AsyncDBSession() as session:
        tracked = await session.scalar(text("""
            SELECT EXISTS (
                SELECT 1
                FROM player
                WHERE player_name = :player
                  AND COALESCE(joined, 0) > 0
                  AND deleted_at IS NULL
            )
        """), {"player": player})
        if not tracked:
            raise ValueError(f"Tracked player {player!r} was not found.")
        await session.execute(text("""
            INSERT INTO player_salience_summary (
                player_name, status, source_game_count,
                source_position_count, effective_game_count, error
            ) VALUES (:player, 'running', 0, 0, 0, NULL)
            ON CONFLICT (player_name) DO UPDATE SET
                status = 'running',
                error = NULL
        """), {"player": player})
        await session.commit()

    try:
        async with AsyncDBSession() as session:
            async with session.begin():
                await session.execute(text("""
                    CREATE TEMP TABLE game_player_salience_stage
                    ON COMMIT DROP AS
                    WITH corpus_games AS MATERIALIZED (
                        SELECT gp.link AS game_link, gp.color AS player_color
                        FROM game_player gp
                        JOIN game game_row ON game_row.link = gp.link
                        WHERE gp.player_name = :player
                          AND game_row.fens_done
                    ),
                    game_positions AS MATERIALIZED (
                        SELECT
                            corpus.game_link,
                            corpus.player_color,
                            association.fen_fen,
                            MIN(
                                association.n_move * 2
                                - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END
                            )::integer AS first_ply
                        FROM corpus_games corpus
                        JOIN game_fen_association association
                          ON association.game_link = corpus.game_link
                        GROUP BY
                            corpus.game_link,
                            corpus.player_color,
                            association.fen_fen
                    ),
                    position_frequency AS MATERIALIZED (
                        SELECT
                            player_color,
                            fen_fen,
                            COUNT(*)::double precision AS games_with_position
                        FROM game_positions
                        GROUP BY player_color, fen_fen
                    ),
                    weighted_positions AS MATERIALIZED (
                        SELECT
                            game_positions.game_link,
                            game_positions.player_color,
                            (
                                0.25 + 0.75 * LEAST(
                                    game_positions.first_ply::double precision / 16.0,
                                    1.0
                                )
                            ) AS depth_weight,
                            position_frequency.games_with_position
                        FROM game_positions
                        JOIN position_frequency
                          ON position_frequency.player_color = game_positions.player_color
                         AND position_frequency.fen_fen = game_positions.fen_fen
                    )
                    SELECT
                        weighted_positions.game_link,
                        CAST(:player AS text) AS player_name,
                        weighted_positions.player_color,
                        (
                            SUM(depth_weight / games_with_position)
                            / NULLIF(SUM(depth_weight), 0)
                        )::double precision AS salience,
                        COUNT(*)::integer AS position_count
                    FROM weighted_positions
                    GROUP BY
                        weighted_positions.game_link,
                        weighted_positions.player_color
                """), {"player": player})

                stats = (await session.execute(text("""
                    SELECT
                        COUNT(*)::bigint AS source_game_count,
                        COALESCE(SUM(position_count), 0)::bigint AS source_position_count,
                        COALESCE(SUM(salience), 0)::double precision AS effective_game_count
                    FROM game_player_salience_stage
                """))).mappings().one()

                await session.execute(text("""
                    DELETE FROM game_player_salience
                    WHERE player_name = :player
                """), {"player": player})
                await session.execute(text("""
                    INSERT INTO game_player_salience (
                        game_link, player_name, player_color, salience, position_count
                    )
                    SELECT
                        game_link, player_name, player_color, salience, position_count
                    FROM game_player_salience_stage
                """))
                status = await session.scalar(text("""
                    UPDATE player_salience_summary
                    SET
                        status = CASE WHEN status = 'running' THEN 'ready' ELSE status END,
                        source_game_count = :source_game_count,
                        source_position_count = :source_position_count,
                        effective_game_count = :effective_game_count,
                        error = NULL
                    WHERE player_name = :player
                    RETURNING status
                """), {
                    "player": player,
                    "source_game_count": int(stats["source_game_count"] or 0),
                    "source_position_count": int(stats["source_position_count"] or 0),
                    "effective_game_count": float(stats["effective_game_count"] or 0),
                })

        return {
            "player_name": player,
            "status": str(status or "stale"),
            "source_game_count": int(stats["source_game_count"] or 0),
            "source_position_count": int(stats["source_position_count"] or 0),
            "effective_game_count": round(float(stats["effective_game_count"] or 0), 6),
        }
    except Exception as error:
        await _set_failed(player, error)
        raise


async def enqueue_player_salience(
    redis: ArqRedis,
    player_name: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Queue one corpus refresh while preventing duplicate player jobs."""
    player = str(player_name).strip().lower()
    async with AsyncDBSession() as session:
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
        await session.execute(text("""
            INSERT INTO player_salience_summary (
                player_name, status, source_game_count,
                source_position_count, effective_game_count, error
            ) VALUES (:player, 'stale', 0, 0, 0, NULL)
            ON CONFLICT (player_name) DO NOTHING
        """), {"player": player})
        current_status = await session.scalar(text("""
            SELECT status
            FROM player_salience_summary
            WHERE player_name = :player
            FOR UPDATE
        """), {"player": player})
        if current_status in ("queued", "running"):
            await session.commit()
            return {"status": "already_active", "player_name": player, "job_id": None}
        if current_status == "ready" and not force:
            await session.commit()
            return {"status": "up_to_date", "player_name": player, "job_id": None}
        await session.execute(text("""
            UPDATE player_salience_summary
            SET status = 'queued', error = NULL
            WHERE player_name = :player
        """), {"player": player})
        await session.commit()

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
        async with AsyncDBSession() as session:
            await session.execute(text("""
                UPDATE player_salience_summary
                SET status = 'stale'
                WHERE player_name = :player AND status = 'queued'
            """), {"player": player})
            await session.commit()
        raise


async def enqueue_stale_player_salience_jobs(
    redis: ArqRedis,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Queue bounded stale projections after the FEN/tablebase pipeline drains."""
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            SELECT player_name
            FROM player_salience_summary
            WHERE status IN ('stale', 'failed')
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
                f"Calculated {result['source_game_count']:,} games as "
                f"{result['effective_game_count']:,.2f} effective games"
                + ("; another refresh is required." if result["status"] != "ready" else ".")
            ),
        )
        return result
    except Exception as error:
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
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            SELECT
                player_name, status, source_game_count,
                source_position_count, effective_game_count, error
            FROM player_salience_summary
            ORDER BY
                CASE status
                    WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'stale' THEN 2
                    WHEN 'failed' THEN 3 ELSE 4
                END,
                player_name
            LIMIT :limit
        """), {"limit": max(1, min(int(limit), 1_000))})
        rows = result.mappings().all()
    return {"players": [_summary_payload(row, str(row["player_name"])) for row in rows]}


async def get_player_salience_report(
    player_name: str,
    *,
    game_limit: int = 10,
) -> dict[str, Any]:
    player = str(player_name).strip().lower()
    safe_limit = max(1, min(int(game_limit), 50))
    async with AsyncDBSession() as session:
        player_exists = await session.scalar(
            text("SELECT EXISTS (SELECT 1 FROM player WHERE player_name = :player)"),
            {"player": player},
        )
        if not player_exists:
            raise ValueError(f"Player {player!r} was not found.")
        summary = (await session.execute(text("""
            SELECT
                player_name, status, source_game_count,
                source_position_count, effective_game_count, error
            FROM player_salience_summary
            WHERE player_name = :player
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
            FROM game_player_salience
            WHERE player_name = :player
        """), {"player": player})).mappings().one()
        colors = (await session.execute(text("""
            SELECT
                player_color,
                COUNT(*)::bigint AS games,
                SUM(salience)::double precision AS effective_games,
                AVG(salience)::double precision AS mean_salience
            FROM game_player_salience
            WHERE player_name = :player
            GROUP BY player_color
            ORDER BY player_color
        """), {"player": player})).mappings().all()

        async def game_rows(direction: str) -> list[dict[str, Any]]:
            order = "ASC" if direction == "ASC" else "DESC"
            rows = (await session.execute(text(f"""
                SELECT
                    salience.game_link,
                    salience.player_color,
                    salience.salience,
                    salience.position_count,
                    gp.played_at,
                    gp.mode,
                    gp.n_moves,
                    gp.result,
                    gp.opponent_name
                FROM game_player_salience salience
                JOIN game_player gp
                  ON gp.link = salience.game_link
                 AND gp.color = salience.player_color
                WHERE salience.player_name = :player
                ORDER BY salience.salience {order}, salience.game_link
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
) -> dict[str, Any]:
    """Return chart-ready weighted accuracy and effective games per local day."""
    player = str(player_name).strip().lower()
    selected_modes = normalize_salience_modes(mode)
    local_timestamp = player_local_timestamp_sql("gp")
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player, "all", date_from, date_to, timezone_name
        )
        params = {**scope.params(), "selected_modes": list(selected_modes)}
        result = await session.execute(text(f"""
            SELECT
                ({local_timestamp})::date AS date_game_init,
                (
                    SUM(engine.game_efficiency * salience.salience)
                    / NULLIF(SUM(salience.salience), 0)
                )::double precision AS weighted_accuracy,
                AVG(engine.game_efficiency)::double precision AS unweighted_accuracy,
                SUM(salience.salience)::double precision AS daily_salience,
                COUNT(*)::integer AS games
            FROM game_player_engine_summary engine
            JOIN game_player_salience salience
              ON salience.game_link = engine.game_link
             AND salience.player_color = engine.player_color
             AND salience.player_name = engine.player_name
            JOIN game_player gp
              ON gp.link = engine.game_link
             AND gp.color = engine.player_color
             AND gp.player_name = engine.player_name
            WHERE engine.player_name = :player
              AND engine.analyzed_player_moves > {MINIMUM_ACCURACY_PLAYER_MOVES_EXCLUSIVE}
              AND engine.game_efficiency IS NOT NULL
              AND gp.mode = ANY(CAST(:selected_modes AS text[]))
              AND (
                  CAST(:date_from_utc AS timestamptz) IS NULL
                  OR gp.played_at >= CAST(:date_from_utc AS timestamptz)
              )
              AND (
                  CAST(:date_to_utc AS timestamptz) IS NULL
                  OR gp.played_at < CAST(:date_to_utc AS timestamptz)
              )
            GROUP BY date_game_init
            ORDER BY date_game_init
        """), params)
        rows = result.mappings().all()
        summary = (await session.execute(text("""
            SELECT status, source_game_count, effective_game_count
            FROM player_salience_summary
            WHERE player_name = :player
        """), {"player": player})).mappings().first()

    payload = scope.response_base()
    payload["filters"]["mode"] = (
        "all" if selected_modes == CHART_MODES else ",".join(selected_modes)
    )
    payload["filters"]["modes"] = list(selected_modes)
    payload["filters"]["minimum_analyzed_player_moves_exclusive"] = (
        MINIMUM_ACCURACY_PLAYER_MOVES_EXCLUSIVE
    )
    return {
        **payload,
        "salience": {
            "status": str(summary["status"]) if summary else "missing",
            "source_game_count": int(summary["source_game_count"] or 0) if summary else 0,
            "effective_game_count": (
                round(float(summary["effective_game_count"] or 0), 6) if summary else 0.0
            ),
        },
        "columns": [
            "date_game_init",
            "weighted_accuracy",
            "unweighted_accuracy",
            "daily_salience",
            "games",
        ],
        "points": [
            [
                row["date_game_init"].isoformat(),
                round(float(row["weighted_accuracy"]), 2),
                round(float(row["unweighted_accuracy"]), 2),
                round(float(row["daily_salience"]), 6),
                int(row["games"]),
            ]
            for row in rows
        ],
    }
