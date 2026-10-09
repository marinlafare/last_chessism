"""Pure, clock-aware conversion from Chess.com PGNs to database-shaped rows."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT
from chessism_api.operations.models import MoveCreateData


def normalize_time_control_mode(time_control: str | None) -> str:
    value = str(time_control or "").strip()
    if not value:
        return "unknown"
    if "/" in value:
        return "daily"
    try:
        seconds = int(value.split("+", 1)[0])
    except (TypeError, ValueError):
        return "unknown"
    if seconds < 180:
        return "bullet"
    if seconds < 600:
        return "blitz"
    if seconds <= 1_800:
        return "rapid"
    return "classical"


def get_pgn_item(game_pgn: str, item: str) -> str:
    """Return one exact PGN header without confusing Date with UTCDate."""
    match = re.search(
        rf'^\[{re.escape(item)}\s+"(.*)"\]\s*$',
        game_pgn,
        flags=re.MULTILINE,
    )
    if match:
        value = match.group(1).lower()
        return value if item == "Termination" else value.replace(" ", "")
    if item in {"StartTime", "EndTime"}:
        return "00:00:00"
    if item in {"Date", "EndDate"}:
        return "0.0.0"
    raise ValueError(f"PGN item {item!r} is missing")


def get_start_and_end_date(
    game: dict[str, Any],
    game_row: dict[str, Any],
) -> dict[str, Any]:
    """Attach UTC calendar fields and elapsed seconds to a game row."""
    try:
        year, month, day = (
            int(part) for part in get_pgn_item(game["pgn"], "Date").split(".")
        )
        datetime(year, month, day)
    except (KeyError, TypeError, ValueError) as error:
        print(f"Invalid date for {game.get('url', 'N/A')}: {error}")
        game_row["year"] = 0
        return game_row

    game_row.update({"year": year, "month": month, "day": day})
    try:
        hour, minute, second = (
            int(part)
            for part in get_pgn_item(game["pgn"], "StartTime").split(":")
        )
    except (TypeError, ValueError):
        hour = minute = second = 0
    game_row.update({"hour": hour, "minute": minute, "second": second})
    game_start = datetime(year, month, day, hour, minute, second)

    try:
        end_year, end_month, end_day = (
            int(part) for part in get_pgn_item(game["pgn"], "EndDate").split(".")
        )
        end_hour, end_minute, end_second = (
            int(part)
            for part in get_pgn_item(game["pgn"], "EndTime").split(":")
        )
        game_end = datetime(
            end_year,
            end_month,
            end_day,
            end_hour,
            end_minute,
            end_second,
        )
        elapsed = (game_end - game_start).total_seconds()
    except (TypeError, ValueError):
        end_year = end_month = end_day = 0
        end_hour = end_minute = end_second = 0
        elapsed = 0
    game_row.update({
        "end_year": end_year,
        "end_month": end_month,
        "end_day": end_day,
        "end_hour": end_hour,
        "end_minute": end_minute,
        "end_second": end_second,
        "time_elapsed": elapsed,
    })
    return game_row


def translate_result_to_float(result: str) -> float | None:
    if result in WINING_RESULT:
        return 1.0
    if result in DRAW_RESULTS:
        return 0.5
    if result in LOSE_RESULTS:
        return 0.0
    print(f"Unknown natural-language game result: {result}")
    return None


def get_black_and_white_data(
    game: dict[str, Any],
    game_row: dict[str, Any],
) -> dict[str, Any]:
    for color in ("black", "white"):
        side = game[color]
        result = str(side["result"]).lower()
        game_row[color] = str(side["username"]).lower()
        game_row[f"{color}_elo"] = int(side["rating"])
        game_row[f"{color}_str_result"] = result
        game_row[f"{color}_result"] = translate_result_to_float(result)
    return game_row


def get_time_bonus(game: dict[str, Any]) -> int:
    time_control = str(game["time_control"])
    return int(time_control.rsplit("+", 1)[-1]) if "+" in time_control else 0


def get_n_moves(raw_moves: str) -> int:
    move_numbers = [
        int(token.replace(".", ""))
        for token in raw_moves.split()
        if token.replace(".", "").isnumeric()
    ]
    return max(move_numbers, default=0)


def _parse_time_to_seconds(value: str) -> float:
    if value == "--":
        return 0.0
    try:
        parts = value.split(":")
        if len(parts) == 3:
            seconds = int(parts[0]) * 3_600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            seconds = int(parts[0]) * 60 + float(parts[1])
        else:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid %clk value: {value!r}") from error
    if seconds < 0:
        raise ValueError(f"Invalid negative %clk value: {value!r}")
    return round(seconds, 3)


def _calculate_reaction_times(values: list[float], time_bonus: int) -> list[float]:
    return [
        round(
            abs(values[index] - values[index + 1]) + time_bonus
            if index < len(values) - 1
            else time_bonus,
            3,
        )
        for index in range(len(values))
    ]


def create_moves_table(
    game_url: str,
    times: list[str],
    clean_moves: list[str],
    time_bonus: int,
) -> dict[str, Any]:
    """Pair SAN moves with clocks, rejecting any incomplete clock stream."""
    if not clean_moves:
        raise ValueError("PGN contains no moves")
    if len(times) != len(clean_moves):
        raise ValueError(
            "Every played half-move must have a %clk annotation "
            f"({len(times)} clocks for {len(clean_moves)} moves)"
        )

    padded_moves = list(clean_moves)
    padded_times = list(times)
    if len(padded_moves) % 2:
        padded_moves.append("--")
        padded_times.append("--")

    white_moves = [str(move) for move in padded_moves[::2]]
    black_moves = [str(move) for move in padded_moves[1::2]]
    white_times = [_parse_time_to_seconds(value) for value in padded_times[::2]]
    black_times = [_parse_time_to_seconds(value) for value in padded_times[1::2]]
    return {
        "link": int(game_url.rsplit("/", 1)[-1]),
        "white_moves": white_moves,
        "white_reaction_times": _calculate_reaction_times(white_times, time_bonus),
        "white_time_left": white_times,
        "black_moves": black_moves,
        "black_reaction_times": _calculate_reaction_times(black_times, time_bonus),
        "black_time_left": black_times,
    }


def get_moves_data(game: dict[str, Any]) -> tuple[int, dict[str, Any] | None]:
    normalized_pgn = str(game["pgn"]).replace("\r\n", "\n")
    _, separator, move_text = normalized_pgn.partition("\n\n")
    if not separator:
        raise ValueError("PGN is missing a move section")

    raw_moves = (
        move_text
        .replace("1/2-1/2", "")
        .replace("1-0", "")
        .replace("0-1", "")
    )
    n_moves = get_n_moves(raw_moves)
    times = re.findall(r"\[%clk\s+([^\]]+)\]", raw_moves)
    move_text_without_comments = re.sub(r"{[^}]*}*", "", raw_moves)
    clean_moves = [
        token
        for token in move_text_without_comments.split()
        if token and "." not in token
    ]
    if n_moves == 0 and not clean_moves:
        return 0, None
    return n_moves, create_moves_table(
        str(game["url"]),
        times,
        clean_moves,
        get_time_bonus(game),
    )


def create_game_dict(game: dict[str, Any]) -> dict[str, Any] | str | bool:
    if "pgn" not in game:
        return "NO PGN"

    rules = str(game.get("rules") or "chess").strip().lower()
    game_row: dict[str, Any] = {
        "rules": rules,
        "initial_setup": game.get("initial_setup"),
        "fens_done": rules != "chess",
        "link": int(str(game["url"]).rsplit("/", 1)[-1]),
        "time_control": game["time_control"],
        "mode": normalize_time_control_mode(game["time_control"]),
    }
    get_start_and_end_date(game, game_row)
    if game_row["year"] == 0:
        return False

    get_black_and_white_data(game, game_row)
    game_row["played_at"] = datetime(
        game_row["year"],
        game_row["month"],
        game_row["day"],
        game_row["hour"],
        game_row["minute"],
        game_row["second"],
        tzinfo=timezone.utc,
    )
    game_row["avg_elo"] = (game_row["white_elo"] + game_row["black_elo"]) / 2
    if game_row["white_result"] is None or game_row["black_result"] is None:
        return False
    try:
        game_row["n_moves"], game_row["moves_data"] = get_moves_data(game)
    except (KeyError, TypeError, ValueError) as error:
        print(
            f"Skipping game {game.get('url', 'N/A')} due to invalid move/clock "
            f"data: {error}"
        )
        return False
    game_row["eco"] = game.get("eco") or "no_eco"
    return game_row


def format_one_game_moves(moves: dict[str, Any]) -> list[dict[str, Any]]:
    fields = (
        "white_moves",
        "white_reaction_times",
        "white_time_left",
        "black_moves",
        "black_reaction_times",
        "black_time_left",
    )
    if not all(isinstance(moves.get(field), list) for field in fields):
        return []
    row_count = len(moves["white_moves"])
    if any(len(moves[field]) != row_count for field in fields):
        return []

    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        row = {
            "n_move": index + 1,
            "link": moves["link"],
            "white_move": str(moves["white_moves"][index]),
            "white_reaction_time": round(moves["white_reaction_times"][index], 3),
            "white_time_left": round(moves["white_time_left"][index], 3),
            "black_move": str(moves["black_moves"][index]),
            "black_reaction_time": round(moves["black_reaction_times"][index], 3),
            "black_time_left": round(moves["black_time_left"][index], 3),
        }
        try:
            rows.append(MoveCreateData(**row).model_dump())
        except Exception as error:
            print(f"Invalid move row {index + 1} for game {moves.get('link')}: {error}")
            return []
    return rows


def create_game_player_rows(game: dict[str, Any]) -> list[dict[str, Any]]:
    return [{
        "link": game["link"],
        "color": color,
        "player_name": game[color],
        "opponent_name": game[opponent],
        "result": game[f"{color}_result"],
        "rating": game[f"{color}_elo"],
        "opponent_rating": game[f"{opponent}_elo"],
        "mode": game.get("mode"),
        "played_at": game.get("played_at"),
        "eco": game["eco"],
        "n_moves": game["n_moves"],
        "time_elapsed": game["time_elapsed"],
        "avg_elo": game.get("avg_elo"),
    } for color, opponent in (("white", "black"), ("black", "white"))]


def create_game_opening_rows(
    game: dict[str, Any],
    moves: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not moves:
        return []
    white_moves = moves.get("white_moves") or []
    black_moves = moves.get("black_moves") or []
    rows: list[dict[str, Any]] = []
    for n_moves in range(3, min(10, len(white_moves), len(black_moves)) + 1):
        opening: list[str] = []
        for index in range(n_moves):
            white = str(white_moves[index] or "").strip()
            black = str(black_moves[index] or "").strip()
            if not white or not black or "--" in (white, black):
                opening = []
                break
            opening.extend((white, black))
        if opening:
            rows.append({
                "link": game["link"],
                "n_moves": n_moves,
                "opening": " ".join(opening),
                "mode": game.get("mode"),
                "avg_elo": game.get("avg_elo"),
                "played_at": game.get("played_at"),
            })
    return rows
