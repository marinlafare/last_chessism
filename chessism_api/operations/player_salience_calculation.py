"""Exact full and incremental calculations for player corpus salience."""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.salience_staging import stage_game_positions


INCREMENTAL_MAX_GAMES = 5_000
INCREMENTAL_MAX_CORPUS_RATIO = 0.05


async def _set_failed(player_name: str, error: BaseException) -> None:
    async with AsyncDBSession() as session:
        await session.execute(text("""
            UPDATE player_salience_summary
            SET
                status = CASE WHEN status = 'stale' THEN 'stale' ELSE 'failed' END,
                error = :error
            WHERE player_name = :player
        """), {"player": player_name, "error": str(error) or "Salience calculation was interrupted."})
        await session.commit()


async def _prepare_session_memory(session) -> None:
    """Prevent large salience sorts from using PostgreSQL's 4 MB default."""
    await session.execute(text("SET LOCAL work_mem = '256MB'"))
    await session.execute(text("SET LOCAL maintenance_work_mem = '512MB'"))


async def _full_rebuild(session, player: str) -> dict[str, Any]:
    """Build a canonical baseline from every FEN-ready game for one player."""
    await session.execute(text("""
        CREATE TEMP TABLE salience_snapshot_games
        ON COMMIT DROP AS
        SELECT gp.link AS game_link, gp.color AS player_color
        FROM game_player gp
        JOIN game game_row ON game_row.link = gp.link
        WHERE gp.player_name = :player
          AND game_row.fens_done
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_snapshot_games_key
        ON salience_snapshot_games (game_link, player_color)
    """))

    await stage_game_positions(session, incremental=False)

    await session.execute(text("""
        CREATE TEMP TABLE salience_frequency_stage
        ON COMMIT DROP AS
        SELECT
            CAST(:player AS text) AS player_name,
            player_color,
            fen_fen,
            COUNT(*)::bigint AS games_with_position,
            SUM(occurrence_count)::bigint AS total_occurrences
        FROM salience_game_position_stage
        GROUP BY player_color, fen_fen
        HAVING COUNT(*) > 1
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_frequency_stage_key
        ON salience_frequency_stage (player_color, fen_fen)
    """))
    await session.execute(text("ANALYZE salience_frequency_stage"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_game_stage
        ON COMMIT DROP AS
        SELECT
            game_position.game_link,
            CAST(:player AS text) AS player_name,
            game_position.player_color,
            SUM(
                game_position.weighted_occurrence_mass
                / COALESCE(frequency.games_with_position, 1)
            )::double precision AS weighted_numerator,
            SUM(game_position.depth_weight_sum)::double precision AS depth_weight_sum,
            SUM(
                game_position.weighted_occurrence_mass
                / COALESCE(frequency.games_with_position, 1)
            )::double precision
                / NULLIF(SUM(game_position.depth_weight_sum), 0) AS salience,
            SUM(game_position.occurrence_count)::integer
                AS position_occurrence_count,
            COUNT(*)::integer AS unique_position_count,
            SUM(game_position.occurrence_count - 1)::integer
                AS repeated_position_count
        FROM salience_game_position_stage game_position
        LEFT JOIN salience_frequency_stage frequency
          ON frequency.player_color = game_position.player_color
         AND frequency.fen_fen = game_position.fen_fen
        GROUP BY game_position.game_link, game_position.player_color
    """), {"player": player})

    stats = (await session.execute(text("""
        SELECT
            COUNT(*)::bigint AS source_game_count,
            COALESCE(SUM(position_occurrence_count), 0)::bigint
                AS source_position_count,
            COALESCE(SUM(salience), 0)::double precision AS effective_game_count
        FROM salience_game_stage
    """))).mappings().one()

    await session.execute(
        text("DELETE FROM player_position_frequency WHERE player_name = :player"),
        {"player": player},
    )
    await session.execute(text("""
        INSERT INTO player_position_frequency (
            player_name, player_color, fen_fen,
            games_with_position, total_occurrences
        )
        SELECT
            player_name, player_color, fen_fen,
            games_with_position, total_occurrences
        FROM salience_frequency_stage
        WHERE games_with_position > 1
    """))
    await session.execute(
        text("DELETE FROM game_player_salience WHERE player_name = :player"),
        {"player": player},
    )
    await session.execute(text("""
        INSERT INTO game_player_salience (
            game_link, player_name, player_color, salience,
            weighted_numerator, depth_weight_sum,
            position_occurrence_count, unique_position_count,
            repeated_position_count
        )
        SELECT
            game_link, player_name, player_color, salience,
            weighted_numerator, depth_weight_sum,
            position_occurrence_count, unique_position_count,
            repeated_position_count
        FROM salience_game_stage
    """))
    await session.execute(text("""
        DELETE FROM player_salience_pending_game pending
        USING salience_snapshot_games snapshot
        WHERE pending.player_name = :player
          AND pending.game_link = snapshot.game_link
          AND pending.player_color = snapshot.player_color
    """), {"player": player})
    return {**dict(stats), "calculation": "full"}


async def _incremental_update(session, player: str) -> dict[str, Any]:
    """Apply exact frequency deltas for newly ingested FEN-ready games."""
    await session.execute(text("""
        CREATE TEMP TABLE salience_new_games
        ON COMMIT DROP AS
        SELECT pending.game_link, pending.player_color
        FROM player_salience_pending_game pending
        JOIN game game_row ON game_row.link = pending.game_link
        LEFT JOIN game_player_salience existing
          ON existing.game_link = pending.game_link
         AND existing.player_color = pending.player_color
        WHERE pending.player_name = :player
          AND game_row.fens_done
          AND existing.game_link IS NULL
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_new_games_key
        ON salience_new_games (game_link, player_color)
    """))

    await stage_game_positions(session, incremental=True)

    # Materializing the player's existing game keys first is important: joining
    # changed opening FENs directly against the global association table makes
    # PostgreSQL visit millions of other players' opening occurrences.
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_game
        ON COMMIT DROP AS
        SELECT game_link, player_color
        FROM game_player_salience
        WHERE player_name = :player
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_existing_game_key
        ON salience_existing_game (game_link, player_color)
    """))
    await session.execute(text("ANALYZE salience_existing_game"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_changed_fen
        ON COMMIT DROP AS
        SELECT DISTINCT player_color, fen_fen
        FROM salience_new_game_position
    """))
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_changed_fen_key
        ON salience_changed_fen (player_color, fen_fen)
    """))
    await session.execute(text("ANALYZE salience_changed_fen"))

    # This compact stage serves both frequency discovery and historical score
    # adjustment. Persisting singleton frequencies would duplicate almost every
    # game position, so an absent persistent row deliberately means frequency 1.
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_game_position
        ON COMMIT DROP AS
        WITH ordered_occurrences AS (
            SELECT
                existing.game_link,
                existing.player_color,
                association.fen_fen,
                (
                    association.n_move * 2
                    - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END
                )::integer AS ply,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        existing.game_link,
                        existing.player_color,
                        association.fen_fen
                    ORDER BY
                        association.n_move,
                        CASE WHEN association.move_color = 'white' THEN 1 ELSE 2 END
                )::integer AS occurrence_index
            FROM salience_existing_game existing
            CROSS JOIN LATERAL (
                SELECT
                    game_fen_association.fen_fen,
                    game_fen_association.n_move,
                    game_fen_association.move_color
                FROM game_fen_association
                WHERE game_fen_association.game_link = existing.game_link
                OFFSET 0
            ) association
            JOIN salience_changed_fen changed
              ON changed.player_color = existing.player_color
             AND changed.fen_fen = association.fen_fen
        )
        SELECT
            game_link,
            player_color,
            fen_fen,
            COUNT(*)::integer AS occurrence_count,
            SUM(
                (
                    0.25 + 0.75 * LEAST(ply::double precision / 16.0, 1.0)
                ) / occurrence_index
            )::double precision AS weighted_occurrence_mass
        FROM ordered_occurrences
        GROUP BY game_link, player_color, fen_fen
    """))
    await session.execute(text("""
        CREATE INDEX salience_existing_game_position_frequency_idx
        ON salience_existing_game_position (player_color, fen_fen)
    """))
    await session.execute(text("ANALYZE salience_existing_game_position"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_frequency_change
        ON COMMIT DROP AS
        WITH existing_frequency AS (
            SELECT
                player_color,
                fen_fen,
                COUNT(*)::bigint AS game_frequency,
                SUM(occurrence_count)::bigint AS total_occurrences
            FROM salience_existing_game_position
            GROUP BY player_color, fen_fen
        ),
        new_frequency AS (
            SELECT
                player_color,
                fen_fen,
                COUNT(*)::bigint AS game_frequency,
                SUM(occurrence_count)::bigint AS total_occurrences
            FROM salience_new_game_position
            GROUP BY player_color, fen_fen
        )
        SELECT
            new_frequency.player_color,
            new_frequency.fen_fen,
            COALESCE(existing_frequency.game_frequency, 0)::bigint
                AS old_game_frequency,
            (
                COALESCE(existing_frequency.game_frequency, 0)
                + new_frequency.game_frequency
            )::bigint AS new_game_frequency,
            (
                COALESCE(existing_frequency.total_occurrences, 0)
                + new_frequency.total_occurrences
            )::bigint AS new_total_occurrences
        FROM new_frequency
        LEFT JOIN existing_frequency
          ON existing_frequency.player_color = new_frequency.player_color
         AND existing_frequency.fen_fen = new_frequency.fen_fen
    """))
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_frequency_change_key
        ON salience_frequency_change (player_color, fen_fen)
    """))
    await session.execute(text("ANALYZE salience_frequency_change"))

    # Only historical games containing a changed FEN need a score adjustment.
    # The exact delta is A(game,FEN) * (1/new_frequency - 1/old_frequency).
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_delta
        ON COMMIT DROP AS
        SELECT
            existing.game_link,
            existing.player_color,
            SUM(
                existing.weighted_occurrence_mass
                * (
                    1.0 / frequency.new_game_frequency
                    - 1.0 / frequency.old_game_frequency
                )
            )::double precision AS numerator_delta
        FROM salience_existing_game_position existing
        JOIN salience_frequency_change frequency
          ON frequency.player_color = existing.player_color
         AND frequency.fen_fen = existing.fen_fen
        WHERE frequency.old_game_frequency > 0
        GROUP BY existing.game_link, existing.player_color
    """))
    await session.execute(text("""
        UPDATE game_player_salience target
        SET
            weighted_numerator = target.weighted_numerator + delta.numerator_delta,
            salience = (
                target.weighted_numerator + delta.numerator_delta
            ) / target.depth_weight_sum
        FROM salience_existing_delta delta
        WHERE target.player_name = :player
          AND target.game_link = delta.game_link
          AND target.player_color = delta.player_color
    """), {"player": player})

    await session.execute(text("""
        INSERT INTO game_player_salience (
            game_link, player_name, player_color, salience,
            weighted_numerator, depth_weight_sum,
            position_occurrence_count, unique_position_count,
            repeated_position_count
        )
        SELECT
            new_position.game_link,
            CAST(:player AS text),
            new_position.player_color,
            SUM(
                new_position.weighted_occurrence_mass
                / frequency.new_game_frequency
            ) / NULLIF(SUM(new_position.depth_weight_sum), 0) AS salience,
            SUM(
                new_position.weighted_occurrence_mass
                / frequency.new_game_frequency
            )::double precision AS weighted_numerator,
            SUM(new_position.depth_weight_sum)::double precision AS depth_weight_sum,
            SUM(new_position.occurrence_count)::integer AS position_occurrence_count,
            COUNT(*)::integer AS unique_position_count,
            SUM(new_position.occurrence_count - 1)::integer AS repeated_position_count
        FROM salience_new_game_position new_position
        JOIN salience_frequency_change frequency
          ON frequency.player_color = new_position.player_color
         AND frequency.fen_fen = new_position.fen_fen
        GROUP BY new_position.game_link, new_position.player_color
    """), {"player": player})

    await session.execute(text("""
        INSERT INTO player_position_frequency (
            player_name, player_color, fen_fen,
            games_with_position, total_occurrences
        )
        SELECT
            CAST(:player AS text),
            player_color,
            fen_fen,
            new_game_frequency,
            new_total_occurrences
        FROM salience_frequency_change
        WHERE new_game_frequency > 1
        ON CONFLICT (player_name, player_color, fen_fen) DO UPDATE SET
            games_with_position = EXCLUDED.games_with_position,
            total_occurrences = EXCLUDED.total_occurrences
    """), {"player": player})
    await session.execute(text("""
        DELETE FROM player_salience_pending_game pending
        USING salience_new_games new_game
        WHERE pending.player_name = :player
          AND pending.game_link = new_game.game_link
          AND pending.player_color = new_game.player_color
    """), {"player": player})

    stats = (await session.execute(text("""
        SELECT
            COUNT(*)::bigint AS source_game_count,
            COALESCE(SUM(position_occurrence_count), 0)::bigint
                AS source_position_count,
            COALESCE(SUM(salience), 0)::double precision AS effective_game_count
        FROM game_player_salience
        WHERE player_name = :player
    """), {"player": player})).mappings().one()
    return {**dict(stats), "calculation": "incremental"}


async def _calculation_strategy(player: str) -> tuple[str, int]:
    async with AsyncDBSession() as session:
        state = (await session.execute(text("""
            SELECT
                summary.source_game_count,
                COUNT(*) FILTER (
                    WHERE game_row.fens_done AND existing.game_link IS NULL
                )::integer AS pending_games
            FROM player_salience_summary summary
            LEFT JOIN player_salience_pending_game pending
              ON pending.player_name = summary.player_name
            LEFT JOIN game game_row ON game_row.link = pending.game_link
            LEFT JOIN game_player_salience existing
              ON existing.game_link = pending.game_link
             AND existing.player_color = pending.player_color
            WHERE summary.player_name = :player
            GROUP BY summary.source_game_count
        """), {"player": player})).mappings().first()
    if not state:
        return "full", 0
    baseline_games = int(state["source_game_count"] or 0)
    pending_games = int(state["pending_games"] or 0)
    threshold = min(
        INCREMENTAL_MAX_GAMES,
        max(1, int(baseline_games * INCREMENTAL_MAX_CORPUS_RATIO)),
    )
    if (
        baseline_games > 0
        and pending_games > 0
        and pending_games <= threshold
    ):
        return "incremental", pending_games
    return "full", pending_games


async def calculate_player_salience(player_name: str) -> dict[str, Any]:
    """Refresh one player with an exact incremental or canonical full pass."""
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
        strategy, pending_games = await _calculation_strategy(player)
        async with AsyncDBSession() as session:
            async with session.begin():
                await _prepare_session_memory(session)
                stats = (
                    await _incremental_update(session, player)
                    if strategy == "incremental"
                    else await _full_rebuild(session, player)
                )
                status = await session.scalar(text("""
                    UPDATE player_salience_summary
                    SET
                        status = CASE
                            WHEN status = 'running' THEN 'ready'
                            ELSE status
                        END,
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
            "calculation": str(stats["calculation"]),
            "pending_games": pending_games,
            "source_game_count": int(stats["source_game_count"] or 0),
            "source_position_count": int(stats["source_position_count"] or 0),
            "effective_game_count": round(float(stats["effective_game_count"] or 0), 6),
        }
    except (Exception, asyncio.CancelledError) as error:
        await _set_failed(player, error)
        raise
