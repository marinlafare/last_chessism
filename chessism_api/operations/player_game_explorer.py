"""Fast chart-bin exploration and canonical move-by-move game scores."""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, timezone
from typing import Any

import chess
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.player_game_scores import (
    is_lichess_blunder,
    lichess_move_accuracy,
    lichess_win_percent,
)
from chessism_api.operations.player_hero_analytics import (
    CHART_MODES,
    WEEKDAY_NAMES,
    _analytics_scope,
)
from chessism_api.operations.player_timezone import player_local_timestamp_sql


LOCAL_PLAYED_AT_SQL = player_local_timestamp_sql("gp")
STARTING_WHITE_CP = 15.0


def _encode_cursor(played_at: datetime, game_id: int) -> str:
    value = json.dumps([played_at.isoformat(), int(game_id)]).encode("utf-8")
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[datetime | None, int | None]:
    if not cursor:
        return None, None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        played_at, game_id = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        return datetime.fromisoformat(played_at), int(game_id)
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid pagination cursor.") from error


def _normalize_modes(modes: list[str] | None) -> list[str]:
    normalized = list(dict.fromkeys(str(mode).strip().lower() for mode in modes or []))
    invalid = set(normalized) - set(CHART_MODES)
    if invalid:
        raise ValueError("Modes may contain only bullet, blitz and rapid.")
    if not normalized:
        raise ValueError("Select at least one game type.")
    return normalized


def _selection_clause(
    scope_kind: str,
    target_date: str | None,
    weekday: int | None,
    hour: int | None,
) -> tuple[str, dict[str, Any], str]:
    if scope_kind == "date":
        if not target_date:
            raise ValueError("A date is required for a date selection.")
        try:
            parsed_date = date.fromisoformat(target_date)
        except ValueError as error:
            raise ValueError("date must use YYYY-MM-DD.") from error
        return (
            f"({LOCAL_PLAYED_AT_SQL})::date = CAST(:target_date AS date)",
            {"target_date": parsed_date},
            target_date,
        )
    if scope_kind == "hour":
        if hour is None or not 0 <= hour <= 23:
            raise ValueError("hour must be between 0 and 23.")
        return (
            f"EXTRACT(HOUR FROM {LOCAL_PLAYED_AT_SQL})::int = :hour",
            {"hour": hour},
            f"{hour:02d}:00",
        )
    if scope_kind in {"weekday", "weekday_hour"}:
        if weekday is None or not 1 <= weekday <= 7:
            raise ValueError("weekday must be between 1 (Monday) and 7 (Sunday).")
        params: dict[str, Any] = {"weekday": weekday}
        clause = f"EXTRACT(ISODOW FROM {LOCAL_PLAYED_AT_SQL})::int = :weekday"
        label = WEEKDAY_NAMES[weekday - 1]
        if scope_kind == "weekday_hour":
            if hour is None or not 0 <= hour <= 23:
                raise ValueError("hour must be between 0 and 23.")
            params["hour"] = hour
            clause += f" AND EXTRACT(HOUR FROM {LOCAL_PLAYED_AT_SQL})::int = :hour"
            label = f"{label} · {hour:02d}:00"
        return clause, params, label
    raise ValueError("scope must be date, hour, weekday or weekday_hour.")


def _compact_game(row: Any, local_played_at: datetime | None) -> dict[str, Any]:
    gain = float(row["own_move_cp_gain"] or 0)
    loss = float(row["own_move_cp_loss"] or 0)
    return {
        "game_id": int(row["game_link"]),
        "played_at_utc": row["played_at"].astimezone(timezone.utc).isoformat(),
        "played_at_local": local_played_at.isoformat() if local_played_at else None,
        "mode": str(row["mode"] or "unknown"),
        "player_color": str(row["player_color"]),
        "opponent_name": str(row["opponent_name"]),
        "player_rating": int(row["rating"]),
        "opponent_rating": int(row["opponent_rating"]),
        "result": str(row["result"]),
        "end_by": str(row["end_by"]) if row["end_by"] is not None else None,
        "accuracy": (
            round(float(row["game_efficiency"]), 2)
            if row["game_efficiency"] is not None else None
        ),
        "analyzed_player_moves": int(row["analyzed_player_moves"] or 0),
        "cp_gain": round(gain, 2),
        "cp_loss": round(loss, 2),
        "cp_net": round(gain - loss, 2),
        "mean_win_percent_loss": (
            round(float(row["mean_win_percent_loss"]), 4)
            if row["mean_win_percent_loss"] is not None else None
        ),
        "median_win_percent_loss": (
            round(float(row["median_win_percent_loss"]), 4)
            if row["median_win_percent_loss"] is not None else None
        ),
        "blunder_count": (
            int(row["blunder_count"])
            if row["blunder_count"] is not None else None
        ),
        "final_player_cp": (
            round(float(row["final_player_cp"]), 2)
            if row["final_player_cp"] is not None else None
        ),
    }


async def explore_player_games(
    player_name: str,
    *,
    scope_kind: str,
    modes: list[str],
    target_date: str | None = None,
    weekday: int | None = None,
    hour: int | None = None,
    analyzed_only: bool = True,
    minimum_game_moves_exclusive: int | None = None,
    limit: int = 30,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Return compact analyzed games belonging to one visual chart bin."""
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100.")
    selected_modes = _normalize_modes(modes)
    selection_sql, selection_params, label = _selection_clause(
        scope_kind, target_date, weekday, hour
    )
    cursor_time, cursor_game_id = _decode_cursor(cursor)
    async with AsyncDBSession() as session:
        scope = await _analytics_scope(session, player_name)
        params = {
            **scope.timezone.sql_params(),
            **selection_params,
            "player": scope.player,
            "modes": selected_modes,
            "cursor_time": cursor_time,
            "cursor_game_id": cursor_game_id,
            "analyzed_only": analyzed_only,
            "minimum_game_moves_exclusive": minimum_game_moves_exclusive,
            "limit": limit + 1,
        }
        minimum_moves_filter = (
            "AND gp.n_moves > :minimum_game_moves_exclusive"
            if minimum_game_moves_exclusive is not None else ""
        )
        base_where = f"""
            gp.player_name = :player
            AND gp.mode = ANY(CAST(:modes AS text[]))
            AND gp.played_at IS NOT NULL
            AND (NOT :analyzed_only OR engine.game_efficiency IS NOT NULL)
            {minimum_moves_filter}
            AND {selection_sql}
        """
        summary_result = await session.execute(text(f"""
            SELECT
                COUNT(*)::bigint AS games,
                COUNT(engine.game_efficiency)::bigint AS analyzed_games,
                AVG(engine.game_efficiency)::double precision AS accuracy,
                COUNT(*) FILTER (WHERE gp.result = 1)::bigint AS wins,
                COUNT(*) FILTER (WHERE gp.result = 0.5)::bigint AS draws,
                COUNT(*) FILTER (WHERE gp.result = 0)::bigint AS losses,
                SUM(engine.blunder_count)::bigint AS blunders
            FROM game_player gp
            LEFT JOIN game_player_engine_summary engine
              ON engine.game_link = gp.link AND engine.player_color = gp.color
            WHERE {base_where}
        """), params)
        summary = summary_result.mappings().one()
        result = await session.execute(text(f"""
            SELECT
                gp.link AS game_link, gp.color AS player_color,
                engine.analyzed_player_moves,
                engine.own_move_cp_gain, engine.own_move_cp_loss,
                engine.game_efficiency, engine.mean_win_percent_loss,
                engine.median_win_percent_loss, engine.blunder_count,
                engine.final_player_cp,
                CASE WHEN gp.result = 1 THEN 'win'
                     WHEN gp.result = 0.5 THEN 'draw' ELSE 'loss' END AS result,
                engine.end_by,
                gp.played_at, gp.mode, gp.opponent_name, gp.rating, gp.opponent_rating
            FROM game_player gp
            LEFT JOIN game_player_engine_summary engine
              ON engine.game_link = gp.link AND engine.player_color = gp.color
            WHERE {base_where}
              AND (
                CAST(:cursor_time AS timestamptz) IS NULL
                OR (gp.played_at, gp.link) <
                   (CAST(:cursor_time AS timestamptz), :cursor_game_id)
              )
            ORDER BY gp.played_at DESC, gp.link DESC
            LIMIT :limit
        """), params)
        rows = result.mappings().all()

    has_more = len(rows) > limit
    selected = rows[:limit]
    games = [
        _compact_game(row, scope.timezone.local_datetime(row["played_at"]))
        for row in selected
    ]
    next_cursor = None
    if has_more and selected:
        next_cursor = _encode_cursor(selected[-1]["played_at"], selected[-1]["game_link"])
    return {
        **scope.response_base(),
        "selection": {
            "scope": scope_kind,
            "date": target_date,
            "weekday": weekday,
            "hour": hour,
            "label": label,
            "modes": selected_modes,
            "analyzed_only": analyzed_only,
            "minimum_game_moves_exclusive": minimum_game_moves_exclusive,
        },
        "summary": {
            "games": int(summary["games"] or 0),
            "analyzed_games": int(summary["analyzed_games"] or 0),
            "accuracy": (
                round(float(summary["accuracy"]), 2)
                if summary["accuracy"] is not None else None
            ),
            "wins": int(summary["wins"] or 0),
            "draws": int(summary["draws"] or 0),
            "losses": int(summary["losses"] or 0),
            "blunders": int(summary["blunders"] or 0),
        },
        "games": games,
        "pagination": {
            "limit": limit,
            "returned_games": len(games),
            "has_more": has_more,
            "next_cursor": next_cursor,
        },
    }


def _score_kind(score: float | None, source: str | None) -> str | None:
    if score is None:
        return None
    if source == "tablebase":
        return "tablebase"
    return "mate" if abs(float(score)) >= 9_000 else "cp"


def _evaluation(
    score: float | None,
    source: str | None,
    wdl: tuple[float | None, float | None, float | None] | None = None,
) -> dict[str, Any] | None:
    if score is None:
        return None
    win, draw, loss = wdl or (None, None, None)
    return {
        "kind": _score_kind(score, source),
        "white_cp": round(float(score), 2),
        "white_win_percent": round(lichess_win_percent(float(score)), 4),
        "white_wdl": {
            "win": float(win) if win is not None else None,
            "draw": float(draw) if draw is not None else None,
            "loss": float(loss) if loss is not None else None,
        },
        "source": source or "stockfish",
    }


def _classification(
    before: float | None,
    after: float | None,
    before_kind: str | None,
    after_kind: str | None,
) -> tuple[str | None, float | None, float | None]:
    if before is None or after is None or before_kind is None or after_kind is None:
        return None, None, None
    loss = max(0.0, lichess_win_percent(before) - lichess_win_percent(after))
    accuracy = lichess_move_accuracy(before, after)
    if is_lichess_blunder(before, after, before_kind, after_kind):
        label = "blunder"
    elif loss >= 10:
        label = "mistake"
    elif loss >= 5:
        label = "inaccuracy"
    elif loss <= 0.5:
        label = "best"
    else:
        label = "good"
    return label, round(loss, 4), round(accuracy, 2)


def _uci_moves(rows: list[Any]) -> list[str | None]:
    board = chess.Board()
    uci_moves: list[str | None] = []
    for row in rows:
        try:
            move = board.parse_san(str(row["move"]))
            uci_moves.append(move.uci())
            board.push(move)
        except (ValueError, chess.IllegalMoveError, chess.InvalidMoveError, chess.AmbiguousMoveError):
            uci_moves.append(None)
    return uci_moves


def _complete_fen(fen: str, ply: int) -> str:
    """Expand this project's four-field position key into display-ready FEN."""
    fields = str(fen).split()
    if len(fields) == 4:
        fields.extend(["0", str(max(1, (int(ply) + 1) // 2))])
    elif len(fields) == 5:
        fields.append(str(max(1, (int(ply) + 1) // 2)))
    return " ".join(fields)


def _format_moves(rows: list[Any]) -> list[dict[str, Any]]:
    moves: list[dict[str, Any]] = []
    uci_moves = _uci_moves(rows)
    previous_fen = chess.STARTING_FEN
    previous_score: float | None = STARTING_WHITE_CP
    previous_kind: str | None = "cp"
    previous_source: str | None = "initial"
    previous_wdl: tuple[float | None, float | None, float | None] | None = None
    previous_line: str | None = None
    for index, row in enumerate(rows):
        move_color = str(row["move_color"])
        score = float(row["score"]) if row["score"] is not None else None
        kind = _score_kind(score, row["analysis_source"])
        current_wdl = (row.get("wdl_win"), row.get("wdl_draw"), row.get("wdl_loss"))
        multiplier = 1 if move_color == "white" else -1
        mover_before = previous_score * multiplier if previous_score is not None else None
        mover_after = score * multiplier if score is not None else None
        label, loss, accuracy = _classification(
            mover_before, mover_after, previous_kind, kind
        )
        fen_after = _complete_fen(row["fen"], int(row["ply"]))
        moves.append({
            "ply": int(row["ply"]),
            "move_number": int(row["n_move"]),
            "move_color": move_color,
            "san": row["move"],
            "uci": uci_moves[index],
            "fen_before": previous_fen,
            "fen_after": fen_after,
            "evaluation_before": _evaluation(previous_score, previous_source, previous_wdl),
            "evaluation_after": _evaluation(score, row["analysis_source"], current_wdl),
            "mover_win_percent_before": (
                round(lichess_win_percent(mover_before), 4)
                if mover_before is not None else None
            ),
            "mover_win_percent_after": (
                round(lichess_win_percent(mover_after), 4)
                if mover_after is not None else None
            ),
            "mover_win_percent_loss": loss,
            "move_accuracy": accuracy,
            "classification": label,
            "clock": {
                "white_time_left": row["white_time_left"],
                "black_time_left": row["black_time_left"],
            },
            "reaction_time": {
                "white": row["white_reaction_time"],
                "black": row["black_reaction_time"],
            },
            "engine_line_before": previous_line.split() if previous_line else [],
            "tablebase": {
                "wdl": row["tablebase_wdl"],
                "dtz": row["tablebase_dtz"],
            } if row["analysis_source"] == "tablebase" else None,
        })
        previous_fen = fen_after
        previous_score = score
        previous_kind = kind
        previous_source = row["analysis_source"]
        previous_wdl = current_wdl
        previous_line = row["next_moves"]
    return moves


async def get_game_score(game_id: int) -> dict[str, Any]:
    """Return a neutral, complete game record for the interactive board viewer."""
    async with AsyncDBSession() as session:
        game_result = await session.execute(text("""
            SELECT game.*, coverage.total_positions, coverage.analyzed_positions,
                   coverage.is_fully_analyzed
            FROM game
            LEFT JOIN game_analysis_summary coverage ON coverage.link = game.link
            WHERE game.link = :game_id
        """), {"game_id": game_id})
        game = game_result.mappings().first()
        if not game:
            raise LookupError(f"Game {game_id} was not found.")
        summary_result = await session.execute(text("""
            SELECT * FROM game_player_engine_summary
            WHERE game_link = :game_id
            ORDER BY CASE WHEN player_color = 'white' THEN 0 ELSE 1 END
        """), {"game_id": game_id})
        summaries = summary_result.mappings().all()
        move_result = await session.execute(text("""
            SELECT
                association.n_move,
                association.move_color,
                association.n_move * 2
                  - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END AS ply,
                fen.fen, fen.score, fen.next_moves, fen.analysis_source,
                fen.wdl_win, fen.wdl_draw, fen.wdl_loss,
                fen.tablebase_wdl, fen.tablebase_dtz,
                CASE WHEN association.move_color = 'white'
                     THEN moves.white_move ELSE moves.black_move END AS move,
                moves.white_reaction_time, moves.black_reaction_time,
                moves.white_time_left, moves.black_time_left
            FROM game_fen_association association
            JOIN fen ON fen.fen = association.fen_fen
            LEFT JOIN moves
              ON moves.link = association.game_link AND moves.n_move = association.n_move
            WHERE association.game_link = :game_id
            ORDER BY association.n_move,
                     CASE WHEN association.move_color = 'white' THEN 0 ELSE 1 END
        """), {"game_id": game_id})
        rows = move_result.mappings().all()

    moves = _format_moves(rows)
    return {
        "game": {
            "game_id": int(game["link"]),
            "played_at_utc": (
                game["played_at"].astimezone(timezone.utc).isoformat()
                if game["played_at"] else None
            ),
            "mode": str(game["mode"] or "unknown"),
            "time_control": str(game["time_control"]),
            "eco": str(game["eco"]),
            "white": {"name": str(game["white"]), "rating": int(game["white_elo"])},
            "black": {"name": str(game["black"]), "rating": int(game["black_elo"])},
            "result": {
                "white": float(game["white_result"]),
                "black": float(game["black_result"]),
                "white_reason": str(game["white_str_result"]),
                "black_reason": str(game["black_str_result"]),
            },
            "initial_fen": chess.STARTING_FEN,
            "total_plies": len(moves),
            "expected_positions": int(game["total_positions"] or game["n_moves"] or 0),
            "analyzed_positions": int(game["analyzed_positions"] or 0),
            "is_fully_analyzed": bool(game["is_fully_analyzed"]),
        },
        "player_summaries": [
            {
                "player_name": str(row["player_name"]),
                "player_color": str(row["player_color"]),
                "accuracy": (
                    round(float(row["game_efficiency"]), 2)
                    if row["game_efficiency"] is not None else None
                ),
                "analyzed_player_moves": int(row["analyzed_player_moves"] or 0),
                "cp_gain": round(float(row["own_move_cp_gain"] or 0), 2),
                "cp_loss": round(float(row["own_move_cp_loss"] or 0), 2),
                "blunder_count": int(row["blunder_count"] or 0),
                "final_player_cp": (
                    round(float(row["final_player_cp"]), 2)
                    if row["final_player_cp"] is not None else None
                ),
                "result": str(row["result"]),
                "end_by": str(row["end_by"]),
            }
            for row in summaries
        ],
        "moves": moves,
    }
