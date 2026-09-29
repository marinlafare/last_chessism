"""Queries that exclusively power the player hero analytics workspace."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.player_game_scores import (
    is_lichess_blunder,
    lichess_winning_chances,
    refresh_game_player_engine_summaries,
)
from chessism_api.operations.player_timezone import (
    ResolvedPlayerTimezone,
    player_local_timestamp_sql,
    resolve_player_timezone,
)


VALID_MODES = {"all", "bullet", "blitz", "rapid"}
CHART_MODES = ("bullet", "blitz", "rapid")
WEEKDAY_NAMES = (
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
)
PLAYER_LOCAL_TIMESTAMP_SQL = player_local_timestamp_sql("gp")


@dataclass(frozen=True)
class AnalyticsScope:
    player: str
    mode: str
    date_from: date | None
    date_to: date | None
    timezone: ResolvedPlayerTimezone

    def params(self) -> dict[str, Any]:
        start = (
            self.timezone.local_boundary_to_utc(datetime.combine(self.date_from, time.min))
            if self.date_from else None
        )
        end = (
            self.timezone.local_boundary_to_utc(
                datetime.combine(self.date_to + timedelta(days=1), time.min)
            )
            if self.date_to else None
        )
        return {
            "player": self.player,
            "mode": self.mode,
            "date_from_utc": start,
            "date_to_utc": end,
            **self.timezone.sql_params(),
        }

    def response_base(self) -> dict[str, Any]:
        return {
            "player_name": self.player,
            "filters": {
                "mode": self.mode,
                "date_from": self.date_from.isoformat() if self.date_from else None,
                "date_to": self.date_to.isoformat() if self.date_to else None,
            },
            "time_context": self.timezone.payload(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


async def _analytics_scope(
    session: AsyncSession,
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    timezone_name: str | None = None,
) -> AnalyticsScope:
    player = str(player_name or "").strip().lower()
    normalized_mode = str(mode or "all").strip().lower()
    if not player:
        raise ValueError("A player name is required.")
    if normalized_mode not in VALID_MODES:
        raise ValueError("Mode must be all, bullet, blitz or rapid.")
    if date_from and date_to and date_from > date_to:
        raise ValueError("Start date must not be after end date.")

    result = await session.execute(text("""
        SELECT player_name, country, location, timezone, timezone_source
        FROM player
        WHERE player_name = :player
          AND deleted_at IS NULL
    """), {"player": player})
    player_row = result.mappings().first()
    if not player_row:
        raise LookupError(f"Player {player!r} was not found.")
    resolved = resolve_player_timezone(dict(player_row), timezone_name)
    return AnalyticsScope(player, normalized_mode, date_from, date_to, resolved)


def _result_counts(result: float | None) -> tuple[int, int, int]:
    if result == 1:
        return 1, 0, 0
    if result == 0.5:
        return 0, 1, 0
    return 0, 0, 1


def _activity_bucket(**identity: Any) -> dict[str, Any]:
    return {**identity, "games": 0, "proportion": 0.0, "wins": 0, "draws": 0, "losses": 0}


def _summarize_activity_rows(rows: list[Any]) -> dict[str, Any]:
    weekdays = [
        _activity_bucket(weekday=index, label=WEEKDAY_NAMES[index - 1])
        for index in range(1, 8)
    ]
    hours = [_activity_bucket(hour=hour) for hour in range(24)]
    cells = {
        (weekday, hour): _activity_bucket(weekday=weekday, hour=hour)
        for weekday in range(1, 8) for hour in range(24)
    }
    total_games = sum(int(row["games"]) for row in rows)
    for row in rows:
        weekday = int(row["weekday"])
        hour = int(row["hour"])
        for target in (weekdays[weekday - 1], hours[hour], cells[(weekday, hour)]):
            for key in ("games", "wins", "draws", "losses"):
                target[key] += int(row[key] or 0)
    for bucket in [*weekdays, *hours, *cells.values()]:
        bucket["proportion"] = round(bucket["games"] / total_games, 8) if total_games else 0.0
    return {
        "total_games": total_games,
        "weekdays": weekdays,
        "hours": hours,
        "weekday_hours": list(cells.values()),
    }


async def get_player_behavioural_activity(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    query = text(f"""
        SELECT
            gp.mode,
            EXTRACT(ISODOW FROM {PLAYER_LOCAL_TIMESTAMP_SQL})::int AS weekday,
            EXTRACT(HOUR FROM {PLAYER_LOCAL_TIMESTAMP_SQL})::int AS hour,
            COUNT(*)::bigint AS games,
            COUNT(*) FILTER (WHERE gp.result = 1)::bigint AS wins,
            COUNT(*) FILTER (WHERE gp.result = 0.5)::bigint AS draws,
            COUNT(*) FILTER (WHERE gp.result = 0)::bigint AS losses
        FROM game_player gp
        WHERE gp.player_name = :player
          AND (:mode = 'all' OR gp.mode = :mode)
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
          AND gp.played_at IS NOT NULL
        GROUP BY gp.mode, weekday, hour
        ORDER BY gp.mode, weekday, hour
    """)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, mode, date_from, date_to
        )
        result = await session.execute(query, scope.params())
        rows = result.mappings().all()

    aggregate = _summarize_activity_rows(rows)
    by_mode = {
        chart_mode: _summarize_activity_rows([
            row for row in rows if str(row["mode"] or "").lower() == chart_mode
        ])
        for chart_mode in CHART_MODES
    }

    return {
        **scope.response_base(),
        **aggregate,
        "by_mode": by_mode,
    }


async def get_player_behavioural_ratings(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    query = text(f"""
        WITH ranked AS (
            SELECT
                gp.mode,
                gp.rating,
                gp.link,
                gp.played_at,
                ({PLAYER_LOCAL_TIMESTAMP_SQL})::date AS local_date,
                ROW_NUMBER() OVER (
                    PARTITION BY gp.mode, ({PLAYER_LOCAL_TIMESTAMP_SQL})::date
                    ORDER BY gp.played_at DESC, gp.link DESC
                ) AS day_rank
            FROM game_player gp
            WHERE gp.player_name = :player
              AND (:mode = 'all' OR gp.mode = :mode)
              AND gp.mode IN ('bullet', 'blitz', 'rapid')
              AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
              AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
              AND gp.played_at IS NOT NULL
        )
        SELECT mode, rating, link, played_at, local_date
        FROM ranked
        WHERE day_rank = 1
        ORDER BY mode, local_date
    """)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, mode, date_from, date_to
        )
        result = await session.execute(query, scope.params())
        rows = result.mappings().all()

    modes = [mode] if mode != "all" else list(CHART_MODES)
    observed_dates = [row["local_date"] for row in rows]
    start_date = date_from or (min(observed_dates) if observed_dates else None)
    end_date = date_to or (max(observed_dates) if observed_dates else None)
    by_mode_date = {(str(row["mode"]), row["local_date"]): row for row in rows}
    series = []
    for rating_mode in modes:
        points = []
        current = start_date
        while current and end_date and current <= end_date:
            row = by_mode_date.get((rating_mode, current))
            local_played = scope.timezone.local_datetime(row["played_at"]) if row else None
            points.append({
                "date": current.isoformat(),
                "last_rating": int(row["rating"]) if row else None,
                "game_id": int(row["link"]) if row else None,
                "played_at_utc": row["played_at"].isoformat() if row else None,
                "played_at_local": local_played.isoformat() if local_played else None,
            })
            current += timedelta(days=1)
        series.append({"mode": rating_mode, "points": points})

    return {
        **scope.response_base(),
        "date_from": start_date.isoformat() if start_date else None,
        "date_to": end_date.isoformat() if end_date else None,
        "series": series,
    }


async def get_player_behavioural_day(
    player_name: str,
    target_date: date,
    mode: str = "all",
) -> dict[str, Any]:
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, mode, target_date, target_date
        )
        result = await session.execute(text("""
            SELECT gp.link, gp.mode, gp.rating, gp.result, gp.played_at
            FROM game_player gp
            WHERE gp.player_name = :player
              AND (:mode = 'all' OR gp.mode = :mode)
              AND gp.played_at >= CAST(:date_from_utc AS timestamptz)
              AND gp.played_at < CAST(:date_to_utc AS timestamptz)
            ORDER BY gp.played_at, gp.link
        """), scope.params())
        rows = result.mappings().all()

    hours = [_activity_bucket(hour=hour) | {"ratings": [], "last_rating": None} for hour in range(24)]
    for row in rows:
        played_at_local = scope.timezone.local_datetime(row["played_at"])
        if played_at_local is None or played_at_local.date() != target_date:
            continue
        played_at_utc = row["played_at"].astimezone(timezone.utc)
        bucket = hours[played_at_local.hour]
        wins, draws, losses = _result_counts(float(row["result"]))
        bucket["games"] += 1
        bucket["wins"] += wins
        bucket["draws"] += draws
        bucket["losses"] += losses
        observation = {
            "game_id": int(row["link"]),
            "mode": str(row["mode"] or "unknown"),
            "rating": int(row["rating"]),
            "played_at_utc": played_at_utc.isoformat(),
            "played_at_local": played_at_local.isoformat(),
        }
        bucket["ratings"].append(observation)
        bucket["last_rating"] = observation["rating"]
    total = sum(int(bucket["games"]) for bucket in hours)
    for bucket in hours:
        bucket["proportion"] = round(bucket["games"] / total, 8) if total else 0.0
    return {**scope.response_base(), "date": target_date.isoformat(), "games": total, "hours": hours}


MEASURE_KEYS = (
    "positions", "positive_positions", "negative_positions", "equal_positions",
    "transitions", "cp_gain_events", "cp_loss_events", "player_cp_sum",
    "total_cp_gain", "total_cp_loss", "own_move_cp_gain", "own_move_cp_loss",
    "opponent_move_cp_gain", "opponent_move_cp_loss", "mate_for", "mate_against",
    "tablebase_winning", "tablebase_drawing", "tablebase_losing",
)


def _measure_bucket(**identity: Any) -> dict[str, Any]:
    return {**identity, **{key: 0 for key in MEASURE_KEYS}}


def _finalize_measure(bucket: dict[str, Any]) -> dict[str, Any]:
    positions = int(bucket["positions"] or 0)
    transitions = int(bucket["transitions"] or 0)
    player_cp_sum = round(float(bucket["player_cp_sum"]), 2) if positions else None
    total_cp_gain = round(float(bucket["total_cp_gain"]), 2) if transitions else None
    total_cp_loss = round(float(bucket["total_cp_loss"]), 2) if transitions else None

    bucket["has_cp_data"] = positions > 0
    bucket["has_cp_transitions"] = transitions > 0
    bucket["player_cp_sum"] = player_cp_sum
    bucket["total_cp_gain"] = total_cp_gain
    bucket["total_cp_loss"] = total_cp_loss
    bucket["net_cp_change"] = (
        round(total_cp_gain - total_cp_loss, 2)
        if total_cp_gain is not None and total_cp_loss is not None
        else None
    )
    bucket["player_cp_average"] = (
        round(player_cp_sum / positions, 2) if player_cp_sum is not None else None
    )
    bucket["average_cp_gain"] = (
        round(total_cp_gain / bucket["cp_gain_events"], 2)
        if total_cp_gain is not None and bucket["cp_gain_events"] else None
    )
    bucket["average_cp_loss"] = (
        round(total_cp_loss / bucket["cp_loss_events"], 2)
        if total_cp_loss is not None and bucket["cp_loss_events"] else None
    )
    return bucket


def _summarize_measure_rows(rows: list[Any]) -> dict[str, Any]:
    weekdays = [_measure_bucket(weekday=i, label=WEEKDAY_NAMES[i - 1]) for i in range(1, 8)]
    hours = [_measure_bucket(hour=i) for i in range(24)]
    cells = {
        (day, hour): _measure_bucket(weekday=day, hour=hour)
        for day in range(1, 8) for hour in range(24)
    }
    for row in rows:
        day, hour = int(row["weekday"]), int(row["hour"])
        for target in (weekdays[day - 1], hours[hour], cells[(day, hour)]):
            for key in MEASURE_KEYS:
                target[key] += row[key] or 0
    return {
        "weekdays": [_finalize_measure(bucket) for bucket in weekdays],
        "hours": [_finalize_measure(bucket) for bucket in hours],
        "weekday_hours": [_finalize_measure(bucket) for bucket in cells.values()],
    }


def _summarize_measure_coverage(rows: list[Any]) -> dict[str, int]:
    total_games = sum(int(row["total_games"] or 0) for row in rows)
    eligible_games = sum(int(row["eligible_games"] or 0) for row in rows)
    return {
        "total_games": total_games,
        "eligible_games": eligible_games,
        "excluded_games": total_games - eligible_games,
        "scored_positions": sum(int(row["scored_positions"] or 0) for row in rows),
    }


async def _missing_player_engine_summary_links(
    session: AsyncSession,
    params: dict[str, Any],
    modes: tuple[str, ...] | None = None,
    require_efficiency: bool = False,
) -> tuple[int, ...]:
    mode_filter = (
        "gp.mode = ANY(CAST(:selected_modes AS text[]))"
        if modes else "(:mode = 'all' OR gp.mode = :mode)"
    )
    query_params = {
        **params,
        **({"selected_modes": list(modes)} if modes else {}),
    }
    result = await session.execute(text(f"""
        SELECT gp.link
        FROM game_player gp
        JOIN game_analysis_summary coverage ON coverage.link = gp.link
        LEFT JOIN game_player_engine_summary engine
          ON engine.game_link = gp.link AND engine.player_color = gp.color
        WHERE gp.player_name = :player
          AND {mode_filter}
          AND coverage.is_fully_analyzed
          AND coverage.total_positions > 0
          AND (
            engine.game_link IS NULL
            OR (:require_efficiency AND engine.game_efficiency IS NULL)
          )
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
    """), {**query_params, "require_efficiency": require_efficiency})
    return tuple(int(link) for link in result.scalars().all())


def _daily_cp_points(rows: list[Any]) -> list[list[str | float]]:
    return [
        [row["date_game_init"].isoformat(), round(float(row["game_cp"] or 0), 2)]
        for row in rows
    ]


async def get_player_range_game_scores(
    player_name: str,
    *,
    game_ids: list[int] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    mode: str = "all",
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    """Return one compact, player-perspective engine score per eligible game."""
    clean_ids = sorted({int(game_id) for game_id in game_ids or []})
    has_date_selection = date_from is not None or date_to is not None
    if bool(clean_ids) == has_date_selection:
        raise ValueError("Provide either game_ids or a complete date range, not both.")
    if has_date_selection and (date_from is None or date_to is None):
        raise ValueError("Both date_from and date_to are required for a date range.")
    if len(clean_ids) > 5_000:
        raise ValueError("At most 5,000 game IDs can be requested at once.")
    if page < 1 or not 1 <= page_size <= 500:
        raise ValueError("Page must be positive and page_size must be between 1 and 500.")

    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session,
            player_name,
            mode,
            date_from if has_date_selection else None,
            date_to if has_date_selection else None,
        )
        params = {
            **scope.params(),
            "game_ids": clean_ids,
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }
        selection_sql = (
            "gp.link = ANY(CAST(:game_ids AS bigint[]))"
            if clean_ids
            else """
                gp.played_at >= CAST(:date_from_utc AS timestamptz)
                AND gp.played_at < CAST(:date_to_utc AS timestamptz)
            """
        )
        count_result = await session.execute(text(f"""
            SELECT
                COUNT(*)::bigint AS selected_games,
                COUNT(*) FILTER (
                    WHERE coverage.is_fully_analyzed
                      AND coverage.total_positions > 0
                )::bigint AS eligible_games
            FROM game_player gp
            LEFT JOIN game_analysis_summary coverage ON coverage.link = gp.link
            WHERE gp.player_name = :player
              AND (:mode = 'all' OR gp.mode = :mode)
              AND {selection_sql}
        """), params)
        coverage = dict(count_result.mappings().first() or {})
        page_result = await session.execute(text(f"""
            SELECT gp.link
            FROM game_player gp
            JOIN game_analysis_summary coverage ON coverage.link = gp.link
            WHERE gp.player_name = :player
              AND (:mode = 'all' OR gp.mode = :mode)
              AND coverage.is_fully_analyzed
              AND coverage.total_positions > 0
              AND {selection_sql}
            ORDER BY gp.played_at DESC NULLS LAST, gp.link DESC
            LIMIT :limit OFFSET :offset
        """), params)
        page_game_ids = tuple(int(link) for link in page_result.scalars().all())
        if page_game_ids:
            existing_result = await session.execute(text("""
                SELECT game_link
                FROM game_player_engine_summary
                WHERE player_name = :player
                  AND game_link = ANY(CAST(:page_game_ids AS bigint[]))
                  AND game_efficiency IS NOT NULL
            """), {"player": scope.player, "page_game_ids": list(page_game_ids)})
            existing_ids = {int(link) for link in existing_result.scalars().all()}
        else:
            existing_ids = set()

    missing_ids = tuple(link for link in page_game_ids if link not in existing_ids)
    if missing_ids:
        await refresh_game_player_engine_summaries(missing_ids)

    rows: list[Any] = []
    if page_game_ids:
        async with AsyncDBSession() as session:
            score_result = await session.execute(text("""
                SELECT
                    summary.game_link,
                    summary.player_name,
                    summary.player_color,
                    gp.opponent_name,
                    gp.played_at,
                    gp.mode,
                    summary.analyzed_player_moves,
                    summary.own_move_cp_gain,
                    summary.own_move_cp_loss,
                    summary.game_efficiency,
                    summary.mean_win_percent_loss,
                    summary.median_win_percent_loss,
                    summary.blunder_count,
                    summary.mate_for_positions,
                    summary.mate_against_positions,
                    summary.final_player_cp,
                    summary.result,
                    summary.end_by
                FROM game_player_engine_summary summary
                JOIN game_player gp
                  ON gp.link = summary.game_link
                 AND gp.color = summary.player_color
                WHERE summary.player_name = :player
                  AND summary.game_link = ANY(CAST(:page_game_ids AS bigint[]))
                ORDER BY gp.played_at DESC NULLS LAST, summary.game_link DESC
            """), {"player": scope.player, "page_game_ids": list(page_game_ids)})
            rows = score_result.mappings().all()

    games = []
    for row in rows:
        gain = float(row["own_move_cp_gain"] or 0)
        loss = float(row["own_move_cp_loss"] or 0)
        played_at = row.get("played_at")
        games.append({
            "game_id": int(row["game_link"]),
            "player_name": str(row["player_name"]),
            "player_color": str(row["player_color"]),
            "opponent_name": str(row["opponent_name"]),
            "played_at": (
                played_at.astimezone(timezone.utc).isoformat() if played_at else None
            ),
            "mode": str(row["mode"] or "unknown"),
            "result": str(row["result"]),
            "end_by": str(row["end_by"]),
            "analyzed_player_moves": int(row["analyzed_player_moves"] or 0),
            "cp_gain": round(gain, 2),
            "cp_loss": round(loss, 2),
            "cp_net": round(gain - loss, 2),
            "game_efficiency": (
                round(float(row["game_efficiency"]), 2)
                if row.get("game_efficiency") is not None else None
            ),
            "mean_win_percent_loss": (
                round(float(row["mean_win_percent_loss"]), 4)
                if row.get("mean_win_percent_loss") is not None else None
            ),
            "median_win_percent_loss": (
                round(float(row["median_win_percent_loss"]), 4)
                if row.get("median_win_percent_loss") is not None else None
            ),
            "blunder_count": int(row["blunder_count"] or 0),
            "mate_for_positions": int(row["mate_for_positions"] or 0),
            "mate_against_positions": int(row["mate_against_positions"] or 0),
            "final_player_cp": (
                round(float(row["final_player_cp"]), 2)
                if row.get("final_player_cp") is not None else None
            ),
        })

    selected_games = int(coverage.get("selected_games") or 0)
    eligible_games = int(coverage.get("eligible_games") or 0)
    return {
        **scope.response_base(),
        "selection": {
            "kind": "game_ids" if clean_ids else "date_range",
            "requested_game_ids": len(clean_ids) if clean_ids else None,
        },
        "coverage": {
            "selected_games": selected_games,
            "eligible_games": eligible_games,
            "excluded_games": selected_games - eligible_games,
        },
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": eligible_games,
            "total_pages": (eligible_games + page_size - 1) // page_size,
            "has_more": page * page_size < eligible_games,
        },
        "games": games,
    }


async def get_player_daily_game_cp(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    """Return one player-oriented total CP point per active local day."""
    local_timestamp = player_local_timestamp_sql("gp")
    daily_sql = text(f"""
        SELECT
            ({local_timestamp})::date AS date_game_init,
            SUM(
                engine.own_move_cp_gain - engine.own_move_cp_loss
            )::double precision AS game_cp
        FROM game_player_engine_summary engine
        JOIN game_player gp
          ON gp.link = engine.game_link
         AND gp.color = engine.player_color
         AND gp.player_name = engine.player_name
        WHERE engine.player_name = :player
          AND (:mode = 'all' OR gp.mode = :mode)
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
        GROUP BY date_game_init
        ORDER BY date_game_init
    """)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, mode, date_from, date_to, timezone_name
        )
        params = scope.params()
        missing_links = await _missing_player_engine_summary_links(session, params)

    if missing_links:
        await refresh_game_player_engine_summaries(missing_links)

    async with AsyncDBSession() as session:
        result = await session.execute(daily_sql, params)
        rows = result.mappings().all()

    return {
        **scope.response_base(),
        "columns": ["date_game_init", "game_cp"],
        "points": _daily_cp_points(rows),
    }


async def get_player_quality_calendar(
    player_name: str,
    mode: str = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    local_timestamp = player_local_timestamp_sql("gp")
    metrics_sql = text(f"""
        SELECT
            gp.mode,
            EXTRACT(ISODOW FROM {local_timestamp})::int AS weekday,
            EXTRACT(HOUR FROM {local_timestamp})::int AS hour,
            SUM(engine.analyzed_player_moves)::bigint AS positions,
            0::bigint AS positive_positions,
            0::bigint AS negative_positions,
            0::bigint AS equal_positions,
            0::double precision AS player_cp_sum,
            SUM(engine.analyzed_player_moves)::bigint AS transitions,
            0::bigint AS cp_gain_events,
            0::bigint AS cp_loss_events,
            SUM(engine.own_move_cp_gain)::double precision AS total_cp_gain,
            SUM(engine.own_move_cp_loss)::double precision AS total_cp_loss,
            SUM(engine.own_move_cp_gain)::double precision AS own_move_cp_gain,
            SUM(engine.own_move_cp_loss)::double precision AS own_move_cp_loss,
            0::double precision AS opponent_move_cp_gain,
            0::double precision AS opponent_move_cp_loss,
            SUM(engine.mate_for_positions)::bigint AS mate_for,
            SUM(engine.mate_against_positions)::bigint AS mate_against,
            0::bigint AS tablebase_winning,
            0::bigint AS tablebase_drawing,
            0::bigint AS tablebase_losing
        FROM game_player gp
        JOIN game_player_engine_summary engine
          ON engine.game_link = gp.link AND engine.player_color = gp.color
        WHERE gp.player_name = :player
          AND (:mode = 'all' OR gp.mode = :mode)
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
        GROUP BY gp.mode, weekday, hour
        ORDER BY gp.mode, weekday, hour
    """)
    coverage_sql = text("""
        SELECT
            gp.mode,
            COUNT(*)::bigint AS total_games,
            COUNT(*) FILTER (WHERE summary.is_fully_analyzed AND summary.total_positions > 0)::bigint AS eligible_games,
            COALESCE(SUM(summary.total_positions) FILTER (WHERE summary.is_fully_analyzed), 0)::bigint AS scored_positions
        FROM game_player gp
        LEFT JOIN game_analysis_summary summary ON summary.link = gp.link
        WHERE gp.player_name = :player
          AND (:mode = 'all' OR gp.mode = :mode)
          AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
          AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
        GROUP BY gp.mode
    """)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(
            session, player_name, mode, date_from, date_to, timezone_name
        )
        params = scope.params()
        missing_links = await _missing_player_engine_summary_links(session, params)

    if missing_links:
        await refresh_game_player_engine_summaries(missing_links)

    async with AsyncDBSession() as session:
        result = await session.execute(metrics_sql, params)
        rows = result.mappings().all()
        coverage_result = await session.execute(coverage_sql, params)
        coverage_rows = coverage_result.mappings().all()

    aggregate = _summarize_measure_rows(rows)
    coverage = _summarize_measure_coverage(coverage_rows)
    by_mode = {}
    for chart_mode in CHART_MODES:
        mode_rows = [row for row in rows if str(row["mode"] or "").lower() == chart_mode]
        mode_coverage = [
            row for row in coverage_rows if str(row["mode"] or "").lower() == chart_mode
        ]
        by_mode[chart_mode] = {
            "coverage": _summarize_measure_coverage(mode_coverage),
            **_summarize_measure_rows(mode_rows),
        }
    return {
        **scope.response_base(),
        "coverage": coverage,
        **aggregate,
        "by_mode": by_mode,
    }


def _score_kind(raw_score: float, source: str | None) -> str:
    if source == "tablebase":
        return "tablebase"
    if abs(raw_score) >= 9000:
        return "mate"
    return "cp"


def _format_game_positions(
    rows: list[dict[str, Any]],
    player_color: str,
) -> list[dict[str, Any]]:
    positions = []
    previous_cp: float | None = None
    previous_kind: str | None = None
    for row in rows:
        raw_score = float(row["score"])
        player_score = raw_score if player_color == "white" else -raw_score
        kind = _score_kind(raw_score, row.get("analysis_source"))
        cp_change = (
            player_score - previous_cp
            if kind == "cp" and previous_kind == "cp" and previous_cp is not None
            else None
        )
        positions.append({
            "move_number": int(row["n_move"]),
            "ply": int(row["ply"]),
            "move_color": str(row["move_color"]),
            "mover_is_player": str(row["move_color"]) == player_color,
            "move": row.get("move"),
            "reaction_time": float(row["reaction_time"]) if row.get("reaction_time") is not None else None,
            "time_left": float(row["time_left"]) if row.get("time_left") is not None else None,
            "fen": row["fen"],
            "raw_white_score": raw_score,
            "player_score": player_score,
            "score_kind": kind,
            "cp_change": round(cp_change, 2) if cp_change is not None else None,
            "cp_gain": round(max(cp_change, 0), 2) if cp_change is not None else None,
            "cp_loss": round(max(-cp_change, 0), 2) if cp_change is not None else None,
            "wdl_win": row.get("wdl_win") if player_color == "white" else row.get("wdl_loss"),
            "wdl_draw": row.get("wdl_draw"),
            "wdl_loss": row.get("wdl_loss") if player_color == "white" else row.get("wdl_win"),
            "analysis_source": row.get("analysis_source"),
        })
        previous_cp = player_score
        previous_kind = kind
    return positions


GAME_POSITIONS_SQL = text("""
    SELECT
        association.game_link,
        association.n_move,
        association.move_color,
        association.n_move * 2 - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END AS ply,
        fen.fen,
        fen.score,
        fen.wdl_win,
        fen.wdl_draw,
        fen.wdl_loss,
        fen.analysis_source,
        CASE WHEN association.move_color = 'white' THEN moves.white_move ELSE moves.black_move END AS move,
        CASE WHEN association.move_color = 'white' THEN moves.white_reaction_time ELSE moves.black_reaction_time END AS reaction_time,
        CASE WHEN association.move_color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END AS time_left
    FROM game_fen_association association
    JOIN fen ON fen.fen = association.fen_fen
    LEFT JOIN moves ON moves.link = association.game_link AND moves.n_move = association.n_move
    WHERE association.game_link = ANY(CAST(:game_ids AS bigint[]))
      AND fen.score IS NOT NULL
    ORDER BY association.game_link, association.n_move,
             CASE WHEN association.move_color = 'white' THEN 0 ELSE 1 END
""")


async def get_player_game_measures(
    player_name: str,
    game_id: int,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(session, player_name, timezone_name=timezone_name)
        game_result = await session.execute(text("""
            SELECT gp.link, gp.color, gp.mode, gp.result, gp.rating,
                   gp.opponent_name, gp.opponent_rating, gp.played_at,
                   summary.total_positions, summary.analyzed_positions,
                   summary.is_fully_analyzed
            FROM game_player gp
            LEFT JOIN game_analysis_summary summary ON summary.link = gp.link
            WHERE gp.player_name = :player AND gp.link = :game_id
        """), {"player": scope.player, "game_id": game_id})
        game = game_result.mappings().first()
        if not game:
            raise LookupError(f"Game {game_id} does not belong to {scope.player}.")
        position_result = await session.execute(GAME_POSITIONS_SQL, {"game_ids": [game_id]})
        rows = [dict(row) for row in position_result.mappings().all()]

    local_played = scope.timezone.local_datetime(game["played_at"])
    return {
        **scope.response_base(),
        "game": {
            "game_id": int(game["link"]),
            "color": str(game["color"]),
            "mode": str(game["mode"] or "unknown"),
            "result": float(game["result"]),
            "rating": int(game["rating"]),
            "opponent": str(game["opponent_name"]),
            "opponent_rating": int(game["opponent_rating"]),
            "played_at_utc": game["played_at"].isoformat() if game["played_at"] else None,
            "played_at_local": local_played.isoformat() if local_played else None,
            "total_positions": int(game["total_positions"] or 0),
            "analyzed_positions": int(game["analyzed_positions"] or 0),
            "is_fully_analyzed": bool(game["is_fully_analyzed"]),
            "positions": _format_game_positions(rows, str(game["color"])),
        },
    }


def _encode_cursor(played_at: datetime, game_id: int) -> str:
    raw = json.dumps([played_at.isoformat(), int(game_id)]).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[datetime | None, int | None]:
    if not cursor:
        return None, None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        played_at, game_id = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        parsed = datetime.fromisoformat(played_at)
        return parsed, int(game_id)
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid pagination cursor.") from error


async def get_player_hour_measures(
    player_name: str,
    target_date: date,
    hour: int,
    mode: str = "all",
    timezone_name: str | None = None,
    limit_games: int = 20,
    cursor: str | None = None,
) -> dict[str, Any]:
    if hour < 0 or hour > 23:
        raise ValueError("hour must be between 0 and 23")
    cursor_time, cursor_game_id = _decode_cursor(cursor)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(session, player_name, mode, timezone_name=timezone_name)
        local_start = datetime.combine(target_date, time(hour=hour))
        local_end = local_start + timedelta(hours=1)
        params = {
            "player": scope.player,
            "mode": scope.mode,
            "hour_start": scope.timezone.local_boundary_to_utc(local_start),
            "hour_end": scope.timezone.local_boundary_to_utc(local_end),
            "cursor_time": cursor_time,
            "cursor_game_id": cursor_game_id,
            "limit": limit_games + 1,
        }
        game_result = await session.execute(text("""
            SELECT gp.link, gp.color, gp.mode, gp.result, gp.rating,
                   gp.opponent_name, gp.opponent_rating, gp.played_at,
                   summary.total_positions, summary.analyzed_positions,
                   summary.is_fully_analyzed
            FROM game_player gp
            LEFT JOIN game_analysis_summary summary ON summary.link = gp.link
            WHERE gp.player_name = :player
              AND (:mode = 'all' OR gp.mode = :mode)
              AND gp.played_at >= CAST(:hour_start AS timestamptz)
              AND gp.played_at < CAST(:hour_end AS timestamptz)
              AND (
                    CAST(:cursor_time AS timestamptz) IS NULL
                    OR (gp.played_at, gp.link) > (CAST(:cursor_time AS timestamptz), :cursor_game_id)
                  )
            ORDER BY gp.played_at, gp.link
            LIMIT :limit
        """), params)
        game_rows = [dict(row) for row in game_result.mappings().all()]
        has_more = len(game_rows) > limit_games
        selected_games = game_rows[:limit_games]
        game_ids = [int(row["link"]) for row in selected_games]
        positions_by_game: dict[int, list[dict[str, Any]]] = {game_id: [] for game_id in game_ids}
        if game_ids:
            position_result = await session.execute(GAME_POSITIONS_SQL, {"game_ids": game_ids})
            for row in position_result.mappings().all():
                positions_by_game[int(row["game_link"])].append(dict(row))

    games = []
    for game in selected_games:
        game_id = int(game["link"])
        local_played = scope.timezone.local_datetime(game["played_at"])
        games.append({
            "game_id": game_id,
            "color": str(game["color"]),
            "mode": str(game["mode"] or "unknown"),
            "result": float(game["result"]),
            "rating": int(game["rating"]),
            "opponent": str(game["opponent_name"]),
            "opponent_rating": int(game["opponent_rating"]),
            "played_at_utc": game["played_at"].isoformat(),
            "played_at_local": local_played.isoformat() if local_played else None,
            "total_positions": int(game["total_positions"] or 0),
            "analyzed_positions": int(game["analyzed_positions"] or 0),
            "is_fully_analyzed": bool(game["is_fully_analyzed"]),
            "positions": _format_game_positions(
                positions_by_game.get(game_id, []), str(game["color"])
            ),
        })
    next_cursor = None
    if has_more and selected_games:
        last = selected_games[-1]
        next_cursor = _encode_cursor(last["played_at"], int(last["link"]))
    return {
        **scope.response_base(),
        "date": target_date.isoformat(),
        "hour": hour,
        "games": games,
        "pagination": {
            "limit_games": limit_games,
            "returned_games": len(games),
            "has_more": has_more,
            "next_cursor": next_cursor,
        },
    }
