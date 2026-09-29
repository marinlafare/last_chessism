"""Game-efficiency series used only by the player hero Stockfish workspace."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.player_game_scores import (
    refresh_game_player_engine_summaries,
)
from chessism_api.operations.player_hero_analytics import (
    CHART_MODES,
    _analytics_scope,
    _missing_player_engine_summary_links,
)
from chessism_api.operations.player_timezone import player_local_timestamp_sql


def daily_efficiency_points(rows: list[Any]) -> list[list[str | float]]:
    return [
        [
            row["date_game_init"].isoformat(),
            round(float(row["game_efficiency"]), 2),
        ]
        for row in rows
        if row.get("game_efficiency") is not None
    ]


def normalize_efficiency_modes(mode: str) -> tuple[str, ...]:
    requested = {
        item.strip().lower()
        for item in str(mode or "all").split(",")
        if item.strip()
    }
    if not requested or requested == {"all"}:
        return CHART_MODES
    if "all" in requested or not requested.issubset(CHART_MODES):
        raise ValueError("Mode must contain only bullet, blitz and/or rapid.")
    return tuple(mode_name for mode_name in CHART_MODES if mode_name in requested)


async def get_player_daily_efficiency(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    """Return mean game efficiency for every active local calendar day."""
    selected_modes = normalize_efficiency_modes(mode)
    local_timestamp = player_local_timestamp_sql("gp")
    daily_sql = text(f"""
        SELECT
            ({local_timestamp})::date AS date_game_init,
            AVG(engine.game_efficiency)::double precision AS game_efficiency
        FROM game_player_engine_summary engine
        JOIN game_player gp
          ON gp.link = engine.game_link
         AND gp.color = engine.player_color
         AND gp.player_name = engine.player_name
        WHERE engine.player_name = :player
          AND gp.mode = ANY(CAST(:selected_modes AS text[]))
          AND engine.game_efficiency IS NOT NULL
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
        GROUP BY date_game_init
        ORDER BY date_game_init
    """)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, "all", date_from, date_to, timezone_name
        )
        params = {**scope.params(), "selected_modes": list(selected_modes)}
        missing_links = await _missing_player_engine_summary_links(
            session,
            params,
            selected_modes,
            require_efficiency=True,
        )

    if missing_links:
        await refresh_game_player_engine_summaries(missing_links)

    async with AsyncDBSession() as session:
        result = await session.execute(daily_sql, params)
        rows = result.mappings().all()

    payload = scope.response_base()
    payload["filters"]["mode"] = (
        "all" if selected_modes == CHART_MODES else ",".join(selected_modes)
    )
    payload["filters"]["modes"] = list(selected_modes)
    return {
        **payload,
        "columns": ["date_game_init", "game_efficiency"],
        "points": daily_efficiency_points(rows),
    }
