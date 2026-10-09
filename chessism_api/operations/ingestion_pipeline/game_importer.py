"""Application service that turns downloaded archives into durable game rows."""

from __future__ import annotations

import time
from typing import Any

from chessism_api.database.ask_db import (
    refresh_database_summary_game_counts,
    refresh_fen_pipeline_summary,
    refresh_main_character_mode_summary_for_players,
)
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player
from chessism_api.operations.ingestion_pipeline.game_repository import (
    GameArchive,
    filter_new_games,
    insert_game_bundle,
    missing_player_names,
)
from chessism_api.operations.ingestion_pipeline.pgn import (
    create_game_dict,
    create_game_opening_rows,
    create_game_player_rows,
    format_one_game_moves,
)
from chessism_api.operations.models import GameCreateData


async def format_games(
    games: GameArchive,
    player_name: str,
) -> list[dict[str, Any]] | str:
    """Deduplicate an archive, create player shells, and parse valid PGNs."""
    started = time.monotonic()
    games_to_process = await filter_new_games(games)
    if not games_to_process:
        return "All games already at DB"

    raw_games = [
        game
        for year_games in games_to_process.values()
        for month_games in year_games.values()
        for game in month_games
    ]
    player_names = {
        str(side.get("username") or "").strip().lower()
        for game in raw_games
        for side in (game.get("white") or {}, game.get("black") or {})
        if str(side.get("username") or "").strip()
    }
    missing_players = await missing_player_names(player_names)
    if missing_players:
        await DBInterface(Player).create_all([
            {"player_name": name, "joined": 0}
            for name in missing_players
        ])

    parsed: list[dict[str, Any] | str | bool] = []
    for game in raw_games:
        try:
            parsed.append(create_game_dict(game))
        except Exception as error:
            print(
                f"Skipping malformed Chess.com game {game.get('url', 'N/A')}: "
                f"{error}",
                flush=True,
            )
    valid = [game for game in parsed if isinstance(game, dict)]
    print(
        f"Parsed {len(valid)}/{len(raw_games)} new games for {player_name} "
        f"in {time.monotonic() - started:.2f}s.",
        flush=True,
    )
    return valid


def _prepare_game_bundle(
    formatted_games: list[dict[str, Any]],
) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "games": [],
        "moves": [],
        "game_players": [],
        "game_openings": [],
        "no_move_games": [],
        "affected_months": set(),
        "affected_players": set(),
    }
    for formatted_game in formatted_games:
        moves_data = formatted_game.get("moves_data")
        game_data = {
            key: value
            for key, value in formatted_game.items()
            if key != "moves_data"
        }
        try:
            game = GameCreateData(**game_data).model_dump()
        except Exception as error:
            print(
                f"Skipping malformed formatted game {formatted_game.get('link')}: "
                f"{error}",
                flush=True,
            )
            continue

        if int(game["n_moves"]) == 0:
            bundle["no_move_games"].append({
                "game_id": game["link"],
                "played_at": game["played_at"],
            })
            continue

        game_moves = format_one_game_moves(moves_data) if moves_data else []
        if not game_moves:
            print(f"Skipping game {game['link']}: no valid move rows.", flush=True)
            continue

        bundle["games"].append(game)
        bundle["moves"].extend(game_moves)
        bundle["game_players"].extend(create_game_player_rows(game))
        bundle["game_openings"].extend(create_game_opening_rows(game, moves_data))
        bundle["affected_players"].update((game["white"], game["black"]))
        if game.get("year") and game.get("month"):
            bundle["affected_months"].add((int(game["year"]), int(game["month"])))
    return bundle


async def insert_games_months_moves_and_players(
    formatted_games: list[dict[str, Any]],
    player_name: str,
) -> str:
    """Validate and atomically persist a parsed game import."""
    started = time.monotonic()
    bundle = _prepare_game_bundle(formatted_games)
    durable_row_count = sum(
        len(bundle[key])
        for key in (
            "games",
            "moves",
            "game_players",
            "game_openings",
            "no_move_games",
        )
    )
    if durable_row_count == 0:
        return f"No new data to insert for {player_name}."

    await insert_game_bundle(
        bundle["games"],
        bundle["moves"],
        bundle["game_players"],
        bundle["game_openings"],
        bundle["no_move_games"],
        player_name=player_name,
        affected_months=bundle["affected_months"],
        affected_players=bundle["affected_players"],
    )
    if bundle["games"]:
        await refresh_main_character_mode_summary_for_players(
            bundle["affected_players"]
        )
        await refresh_database_summary_game_counts()
        await refresh_fen_pipeline_summary()

    print(
        f"Persisted {len(bundle['games'])} games, {len(bundle['moves'])} moves, "
        f"and {len(bundle['no_move_games'])} zero-move tombstones in "
        f"{time.monotonic() - started:.2f}s.",
        flush=True,
    )
    return (
        f"Successfully processed and inserted {len(bundle['games'])} games "
        f"and recorded {len(bundle['no_move_games'])} zero-move games for "
        f"{player_name}."
    )
