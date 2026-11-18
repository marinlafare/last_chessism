"""Public time-control analytics; each operation owns one read-only UTC snapshot."""
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.database.postgresql import time_control_reads as reads
from chessism_api.rating_statistics import weighted_jenks_breaks


def _player_result_to_score(result_filter: str) -> Optional[float]:
    normalized = (result_filter or "").strip().lower()
    if normalized == "win":
        return 1.0
    if normalized == "loss":
        return 0.0
    if normalized == "draw":
        return 0.5
    return None


def _to_float_safe(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_y_ticks(min_rating: int, max_rating: int, count: int = 5) -> List[int]:
    if min_rating == max_rating:
        center = min_rating
        return [center - 20, center - 10, center, center + 10, center + 20]

    step = (max_rating - min_rating) / max(1, count - 1)
    ticks = [round(min_rating + i * step) for i in range(count)]
    return sorted(set(ticks))


def _resolve_chart_cutoff(range_type: str, years: Optional[int], current_dt: datetime) -> Tuple[Optional[datetime], str, Optional[int]]:
    rt = (range_type or "all").strip().lower()
    if rt == "six_months":
        return current_dt - timedelta(days=183), "six_months", None
    if rt == "one_year":
        return current_dt - timedelta(days=365), "one_year", None
    if rt == "years":
        safe_years = max(1, years or 1)
        return current_dt - timedelta(days=365 * safe_years), "years", safe_years
    return None, "all", None


def _chart_history_payload(first_dt: datetime, latest_dt: datetime) -> Dict[str, Any]:
    span_days = max(0, (latest_dt - first_dt).days)
    complete_years = min(20, int(span_days // 365))
    return {
        "start": f"{first_dt.year:04d}-{first_dt.month:02d}-{first_dt.day:02d}",
        "end": f"{latest_dt.year:04d}-{latest_dt.month:02d}-{latest_dt.day:02d}",
        "span_days": span_days,
        "year_ticks": list(range(complete_years, 0, -1))
    }


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


async def get_rating_time_control_chart(time_control: str, *, factory=None) -> Dict[str, Any]:
    """
    Returns histogram-ready rating data for a normalized time control mode.
    x: sorted ratings, y: frequency per rating.
    """
    target_mode = (time_control or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"x": [], "y": [], "time_control": target_mode}


    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.rating_histogram(session, target_mode)

    x_values = [int(row.get("rating") or 0) for row in rows]
    y_values = [int(row.get("appearances") or 0) for row in rows]
    jenks_ranges = weighted_jenks_breaks(x_values, y_values, n_classes=3)

    bins = {
        "jenks_1": {"min_rating": 0, "max_rating": 0},
        "jenks_2": {"min_rating": 0, "max_rating": 0},
        "jenks_3": {"min_rating": 0, "max_rating": 0}
    }
    for idx, rating_range in enumerate(jenks_ranges[:3], start=1):
        bins[f"jenks_{idx}"] = {
            "min_rating": int(rating_range[0]),
            "max_rating": int(rating_range[1])
        }

    return {
        "x": x_values,
        "y": y_values,
        "time_control": target_mode,
        "bins": bins
    }


async def get_time_control_top_moves(
    mode: str,
    move_color: str = "white",
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    page: int = 1,
    page_size: int = 5,
    max_move: int = 10, *, factory=None) -> Dict[str, Any]:
    """
    Returns paginated most played moves by move number for a mode.
    The backend computes one top move per move number and paginates those rows.
    """
    target_mode = (mode or "").strip().lower()
    target_color = (move_color or "white").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "move_color": target_color,
            "page": 1,
            "page_size": 0,
            "total": 0,
            "total_pages": 0,
            "rows": []
        }
    if target_color not in {"white", "black"}:
        target_color = "white"
    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    safe_max_move = max(1, min(int(max_move), 30))
    safe_page_size = max(1, min(int(page_size), 10))
    safe_page = max(1, int(page))
    total_rows = safe_max_move
    total_pages = (total_rows + safe_page_size - 1) // safe_page_size if total_rows else 0
    if total_pages > 0 and safe_page > total_pages:
        safe_page = total_pages
    offset = (safe_page - 1) * safe_page_size


    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.top_moves(session, mode=target_mode, move_color=target_color,
            max_move=safe_max_move, offset=offset, limit=safe_page_size, min_rating=safe_min_rating, max_rating=safe_max_rating)

    output_rows = [
        {
            "move_number": int(row.get("move_number") or 0),
            "move": str(row.get("move") or "-"),
            "times_played": int(row.get("times_played") or 0)
        }
        for row in rows
    ]

    return {
        "mode": target_mode,
        "move_color": target_color,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total_rows,
        "total_pages": total_pages,
        "rows": output_rows
    }


async def get_time_control_top_openings(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    page: int = 1,
    page_size: int = 5,
    n_moves: int = 3, *, factory=None) -> Dict[str, Any]:
    """
    Returns paginated top openings for a normalized mode using move sequences.
    Opening sequence is built from exactly the first n_moves full moves in each game.
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "page": 1,
            "page_size": 0,
            "total": 0,
            "total_pages": 0,
            "rows": []
        }

    safe_page_size = max(1, min(int(page_size), 25))
    safe_page = max(1, int(page))
    safe_n_moves = max(3, min(int(n_moves), 10))
    required_half_moves = safe_n_moves * 2
    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating


    async with read_transaction(factory or AsyncDBSession) as session:
        params = dict(mode=target_mode, n_moves=safe_n_moves, min_rating=safe_min_rating, max_rating=safe_max_rating)
        total = await reads.opening_count(session, **params)
        total_pages = (total + safe_page_size - 1) // safe_page_size if total else 0
        if total_pages and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size
        rows = await reads.opening_page(session, **params, offset=offset, limit=safe_page_size)

    total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0

    move_words = {
        1: "one",
        2: "two",
        3: "three",
        4: "four",
        5: "five",
        6: "six",
        7: "seven",
        8: "eight",
        9: "nine",
        10: "ten",
        11: "eleven",
        12: "twelve",
        13: "thirteen",
        14: "fourteen",
        15: "fifteen",
        16: "sixteen",
        17: "seventeen",
        18: "eighteen",
        19: "nineteen",
        20: "twenty"
    }

    openings_map: Dict[str, Dict[str, str]] = {}
    output_rows = []

    for idx, row in enumerate(rows, start=1):
        opening_text = str(row.get("opening") or "").strip()
        half_moves = [token for token in opening_text.split() if token]
        half_moves = half_moves[:required_half_moves]
        total_full_moves = safe_n_moves
        moves_payload: Dict[str, str] = {}
        mean_rating_for_this_opening = int(row.get("mean_rating_for_this_opening") or 0)

        for move_number in range(1, total_full_moves + 1):
            key_suffix = move_words.get(move_number, str(move_number))
            move_key = f"move_{key_suffix}"
            white_idx = (move_number - 1) * 2
            black_idx = white_idx + 1
            white_move = half_moves[white_idx] if white_idx < len(half_moves) else "--"
            black_move = half_moves[black_idx] if black_idx < len(half_moves) else "--"
            moves_payload[move_key] = f"{white_move},{black_move}"

        opening_key = f"most_common_opening_for_this_time_control_{idx}"
        n_games_for_this_opening = int(row.get("times_played") or 0)
        openings_map[opening_key] = {
            "mean_rating_for_this_opening": mean_rating_for_this_opening,
            "n_games_for_this_opening": n_games_for_this_opening,
            **moves_payload
        }
        output_rows.append({
            "top": idx,
            "key": opening_key,
            "moves": moves_payload,
            "half_moves": half_moves,
            "times_played": n_games_for_this_opening,
            "n_games_for_this_opening": n_games_for_this_opening,
            "mean_rating_for_this_opening": mean_rating_for_this_opening
        })

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "n_moves": safe_n_moves,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "openings": openings_map,
        "rows": output_rows
    }


async def get_time_control_result_color_matrix(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None, *, factory=None) -> Dict[str, Any]:
    """
    Returns global result matrix split by color (white/black) for a mode and rating range.
    Rating filter uses game average rating.
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "total_games": 0,
            "white": {"wins": 0, "draws": 0, "losses": 0, "total": 0, "score_rate": 0.0},
            "black": {"wins": 0, "draws": 0, "losses": 0, "total": 0, "score_rate": 0.0}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating


    async with read_transaction(factory or AsyncDBSession) as session:
        row = await reads.results(session, mode=target_mode,
            min_rating=safe_min_rating, max_rating=safe_max_rating)

    total_games = int(row.get("total_games") or 0)
    white_wins = int(row.get("white_wins") or 0)
    white_draws = int(row.get("white_draws") or 0)
    white_losses = int(row.get("white_losses") or 0)
    black_wins = int(row.get("black_wins") or 0)
    black_draws = int(row.get("black_draws") or 0)
    black_losses = int(row.get("black_losses") or 0)

    white_total = white_wins + white_draws + white_losses
    black_total = black_wins + black_draws + black_losses
    white_score_rate = round(((white_wins + 0.5 * white_draws) / white_total), 4) if white_total > 0 else 0.0
    black_score_rate = round(((black_wins + 0.5 * black_draws) / black_total), 4) if black_total > 0 else 0.0

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "total_games": total_games,
        "white": {
            "wins": white_wins,
            "draws": white_draws,
            "losses": white_losses,
            "total": white_total,
            "score_rate": white_score_rate
        },
        "black": {
            "wins": black_wins,
            "draws": black_draws,
            "losses": black_losses,
            "total": black_total,
            "score_rate": black_score_rate
        }
    }


async def get_time_control_game_length_analytics(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None, *, factory=None) -> Dict[str, Any]:
    """
    Returns game length analytics for a mode and rating range:
    - summary stats for n_moves and time_elapsed
    - histogram for moves and elapsed minutes
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "total_games": 0,
            "summary": {
                "avg_n_moves": None,
                "median_n_moves": None,
                "p90_n_moves": None,
                "avg_time_elapsed_sec": None,
                "median_time_elapsed_sec": None,
                "p90_time_elapsed_sec": None
            },
            "n_moves_hist": {"x": [], "y": []},
            "time_elapsed_hist": {"x": [], "y": [], "unit": "minutes"}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating


    def _to_float(val: Any) -> Optional[float]:
        return float(val) if val is not None else None

    async with read_transaction(factory or AsyncDBSession) as session:
        summary_row, moves_rows, elapsed_rows = await reads.lengths(session, mode=target_mode,
            min_rating=safe_min_rating, max_rating=safe_max_rating)

    moves_x = [f"{int(row['bucket_start'])}-{int(row['bucket_end'])}" for row in moves_rows]
    moves_y = [int(row.get("total") or 0) for row in moves_rows]
    elapsed_x = [str(row.get("bucket_label") or "") for row in elapsed_rows]
    elapsed_y = [int(row.get("total") or 0) for row in elapsed_rows]

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "total_games": int(summary_row.get("total_games") or 0),
        "summary": {
            "avg_n_moves": _to_float(summary_row.get("avg_n_moves")),
            "median_n_moves": _to_float(summary_row.get("median_n_moves")),
            "p90_n_moves": _to_float(summary_row.get("p90_n_moves")),
            "avg_time_elapsed_sec": _to_float(summary_row.get("avg_time_elapsed_sec")),
            "median_time_elapsed_sec": _to_float(summary_row.get("median_time_elapsed_sec")),
            "p90_time_elapsed_sec": _to_float(summary_row.get("p90_time_elapsed_sec"))
        },
        "n_moves_hist": {"x": moves_x, "y": moves_y},
        "time_elapsed_hist": {"x": elapsed_x, "y": elapsed_y, "unit": "minutes"}
    }


async def get_time_control_activity_trend(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None, *, factory=None) -> Dict[str, Any]:
    """
    Returns activity heat data for a mode and rating range:
    - month of year (12 buckets)
    - day of week (7 buckets)
    - hour of day (24 buckets)
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "month_heat": {"labels": [], "values": []},
            "weekday_heat": {"labels": [], "values": []},
            "hour_heat": {"labels": [], "values": []}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating


    async with read_transaction(factory or AsyncDBSession) as session:
        month_rows, weekday_rows, hour_rows = await reads.activity(session, mode=target_mode,
            min_rating=safe_min_rating, max_rating=safe_max_rating)

    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_values = [0] * 12
    for row in month_rows:
        month_idx = int(row.get("idx") or 0)
        if 1 <= month_idx <= 12:
            month_values[month_idx - 1] = int(row.get("total") or 0)

    weekday_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    weekday_values = [0] * 7
    for row in weekday_rows:
        dow_idx = int(row.get("idx") or 0)
        if 0 <= dow_idx <= 6:
            weekday_values[dow_idx] = int(row.get("total") or 0)

    hour_labels = [f"{hour:02d}" for hour in range(24)]
    hour_values = [0] * 24
    for row in hour_rows:
        hour_idx = int(row.get("idx") or 0)
        if 0 <= hour_idx <= 23:
            hour_values[hour_idx] = int(row.get("total") or 0)

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "month_heat": {"labels": month_labels, "values": month_values},
        "weekday_heat": {"labels": weekday_labels, "values": weekday_values},
        "hour_heat": {"labels": hour_labels, "values": hour_values}
    }


async def get_player_time_control_top_moves(
    player_name: str,
    mode: str,
    move_color: str = "white",
    page: int = 1,
    page_size: int = 5,
    max_move: int = 10, *, factory=None) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    target_color = (move_color or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "move_color": target_color, "rows": [], "total": 0, "page": 1, "page_size": 0, "total_pages": 0}
    if target_color not in {"white", "black"}:
        target_color = "white"

    safe_page_size = max(1, min(int(page_size), 10))
    safe_page = max(1, int(page))
    safe_max_move = max(1, min(int(max_move), 30))
    total_rows = safe_max_move
    total_pages = (total_rows + safe_page_size - 1) // safe_page_size if total_rows > 0 else 0
    if total_pages > 0 and safe_page > total_pages:
        safe_page = total_pages
    offset = (safe_page - 1) * safe_page_size

    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.top_moves(session, mode=target_mode, move_color=target_color,
            max_move=safe_max_move, offset=offset, limit=safe_page_size, player=player_name)

    return {
        "mode": target_mode,
        "move_color": target_color,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total_rows,
        "total_pages": total_pages,
        "rows": [
            {
                "move_number": int(row.get("move_number") or 0),
                "move": str(row.get("move") or "-"),
                "times_played": int(row.get("times_played") or 0)
            }
            for row in rows
        ]
    }


async def get_player_time_control_top_openings(
    player_name: str,
    mode: str,
    result_filter: str,
    page: int = 1,
    page_size: int = 5,
    n_moves: int = 3, *, factory=None) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "rows": [], "total": 0, "page": 1, "page_size": 0, "total_pages": 0}

    target_result = (result_filter or "").strip().lower()
    target_score = _player_result_to_score(target_result)
    if target_score is None:
        target_result = "win"
        target_score = 1.0

    safe_page_size = max(1, min(int(page_size), 25))
    safe_page = max(1, int(page))
    safe_n_moves = max(3, min(int(n_moves), 10))
    required_half_moves = safe_n_moves * 2


    async with read_transaction(factory or AsyncDBSession) as session:
        params = dict(mode=target_mode, n_moves=safe_n_moves, player=player_name, target_score=target_score)
        total = await reads.opening_count(session, **params)
        total_pages = (total + safe_page_size - 1) // safe_page_size if total else 0
        if total_pages and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size
        rows = await reads.opening_page(session, **params, offset=offset, limit=safe_page_size)

    output_rows = []
    for idx, row in enumerate(rows, start=1):
        opening_text = str(row.get("opening") or "").strip()
        half_moves = [token for token in opening_text.split() if token][:required_half_moves]
        output_rows.append({
            "top": idx,
            "key": f"player_opening_{target_result}_{idx}",
            "opening": opening_text,
            "half_moves": half_moves,
            "times_played": int(row.get("times_played") or 0)
        })

    return {
        "mode": target_mode,
        "result_filter": target_result,
        "n_moves": safe_n_moves,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": (total + safe_page_size - 1) // safe_page_size if total > 0 else 0,
        "rows": output_rows
    }


async def get_player_time_control_lengths(player_name: str, mode: str, *, factory=None) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "total_games": 0, "summary": {}, "n_moves_hist": {"x": [], "y": []}, "time_elapsed_hist": {"x": [], "y": [], "unit": "minutes"}}

    async with read_transaction(factory or AsyncDBSession) as session:
        summary_row, moves_hist_rows, elapsed_hist_rows = await reads.lengths(
            session, mode=target_mode, player=player_name)

    return {
        "mode": target_mode,
        "total_games": int(summary_row.get("total_games") or 0),
        "summary": {
            "avg_n_moves": _to_float_safe(summary_row.get("avg_n_moves")),
            "median_n_moves": _to_float_safe(summary_row.get("median_n_moves")),
            "p90_n_moves": _to_float_safe(summary_row.get("p90_n_moves")),
            "avg_time_elapsed_sec": _to_float_safe(summary_row.get("avg_time_elapsed_sec"))
        },
        "n_moves_hist": {
            "x": [f"{int(r.get('bucket_start') or 0)}-{int(r.get('bucket_end') or 0)}" for r in moves_hist_rows],
            "y": [int(r.get("total") or 0) for r in moves_hist_rows]
        },
        "time_elapsed_hist": {
            "x": [f"{int(r.get('bucket_start_min') or 0)}-{int(r.get('bucket_end_min') or 0)}" for r in elapsed_hist_rows],
            "y": [int(r.get("total") or 0) for r in elapsed_hist_rows],
            "unit": "minutes"
        }
    }


async def get_player_time_control_activity_trend(player_name: str, mode: str, *, factory=None) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "month_heat": {"labels": [], "values": []}, "weekday_heat": {"labels": [], "values": []}, "hour_heat": {"labels": [], "values": []}}
    async with read_transaction(factory or AsyncDBSession) as session:
        months, weekdays, hours = await reads.activity(session, mode=target_mode, player=player_name)

    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    weekday_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    return {
        "mode": target_mode,
        "month_heat": {
            "labels": [month_names[max(0, min(11, int(r.get('idx') or 1) - 1))] for r in months],
            "values": [int(r.get("total") or 0) for r in months]
        },
        "weekday_heat": {
            "labels": [weekday_names[max(0, min(6, int(r.get('idx') or 0)))] for r in weekdays],
            "values": [int(r.get("total") or 0) for r in weekdays]
        },
        "hour_heat": {
            "labels": [f"{int(r.get('idx') or 0):02d}" for r in hours],
            "values": [int(r.get("total") or 0) for r in hours]
        }
    }


async def get_player_mode_chart(
    player_name: str,
    mode: str,
    range_type: str = "all",
    years: Optional[int] = None, *, factory=None) -> Dict[str, Any]:
    """
    Returns chart data for a single normalized mode (oldest to newest),
    including backend-computed color buckets from mean/std.
    """
    target_mode = (mode or "").strip().lower()

    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.rating_history(session, player_name, target_mode)

    mode_points: List[Tuple[datetime, str, int]] = []
    for row in rows:
        dt = row.get("played_at")
        if not dt:
            continue
        dt = _to_naive_utc(dt)

        rating = int(row.get("rating") or 0)
        date_value = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
        mode_points.append((dt, date_value, rating))

    if not mode_points:
        return {
            "points": [],
            "chart_title": f"rating of {target_mode}",
            "mode": target_mode,
            "range": {"type": "all", "years": None, "history": None},
            "stats": {"mean": None, "std": None, "lower": None, "upper": None, "count": 0},
            "y_axis": {"min": None, "max": None, "ticks": []}
        }

    first_dt = mode_points[0][0]
    latest_dt = mode_points[-1][0]
    current_dt = datetime.utcnow()
    history_end_dt = max(latest_dt, current_dt)
    history = _chart_history_payload(first_dt, history_end_dt)
    cutoff, resolved_range, resolved_years = _resolve_chart_cutoff(range_type, years, history_end_dt)

    filtered = [
        point for point in mode_points
        if cutoff is None or point[0] >= cutoff
    ]

    if not filtered:
        return {
            "points": [],
            "chart_title": f"rating of {target_mode}",
            "mode": target_mode,
            "range": {"type": resolved_range, "years": resolved_years, "history": history},
            "stats": {"mean": None, "std": None, "lower": None, "upper": None, "count": 0},
            "y_axis": {"min": None, "max": None, "ticks": []}
        }

    y_values = [point[2] for point in filtered]

    mean_value = sum(y_values) / len(y_values)
    variance = sum((value - mean_value) ** 2 for value in y_values) / len(y_values)
    std_value = math.sqrt(variance)
    lower = mean_value - std_value
    upper = mean_value + std_value

    points = []
    for _, date_value, rating in filtered:
        if rating <= lower:
            bucket = "std_behind"
            color = "#f07167"
        elif rating >= upper:
            bucket = "std_ahead"
            color = "#3fd089"
        else:
            bucket = "mean_band"
            color = "#8be9fd"

        points.append({
            "x": date_value,
            "y": rating,
            "bucket": bucket,
            "color": color
        })

    y_min = min(y_values)
    y_max = max(y_values)
    y_ticks = _build_y_ticks(y_min, y_max, count=5)

    return {
        "points": points,
        "chart_title": f"rating of {target_mode}",
        "mode": target_mode,
        "range": {"type": resolved_range, "years": resolved_years, "history": history},
        "stats": {
            "mean": round(mean_value, 2),
            "std": round(std_value, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
            "count": len(y_values)
        },
        "y_axis": {"min": y_min, "max": y_max, "ticks": y_ticks}
    }

