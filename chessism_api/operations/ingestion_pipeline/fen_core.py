"""Pure position extraction and in-memory aggregation helpers."""

from __future__ import annotations

from typing import Any

import chess


MoveRow = dict[str, Any]
AssociationRow = dict[str, Any]
FailureRow = dict[str, Any]


def count_fen_pieces(fen: str) -> int:
    """Count chessmen from the board field without constructing a board."""
    return sum(character.isalpha() for character in str(fen or "").split(" ", 1)[0])


def create_association_data(
    raw_fen: str,
    n_move: int,
    move_color: str,
    link: int,
) -> AssociationRow:
    """Create the shared row used by aggregation and association persistence."""
    fen_parts = raw_fen.split(" ")
    return {
        "game_link": int(link),
        "fen_fen": " ".join(fen_parts[:4]),
        "n_move": int(n_move),
        "move_color": move_color,
        "move_counter_string": f"#{fen_parts[4]}_{fen_parts[5]}",
    }


def process_single_game_sync(
    game_data: tuple[int, list[MoveRow]],
) -> tuple[list[AssociationRow], list[FailureRow]]:
    """Replay one game and return one canonical position per played half-move."""
    link, one_game_moves = game_data
    try:
        moves = sorted(one_game_moves, key=lambda row: row["n_move"])
    except KeyError:
        return [], [{
            "link": link,
            "move_num": -1,
            "san": "N/A",
            "error": "KeyError on move sort",
        }]

    board = chess.Board()
    associations: list[AssociationRow] = []
    failures: list[FailureRow] = []
    for index, move_data in enumerate(moves, start=1):
        move_number = move_data.get("n_move")
        if move_number != index:
            failures.append({
                "link": link,
                "move_num": move_number,
                "san": "N/A",
                "error": "Move sequence mismatch",
            })
            break

        for color, field in (("white", "white_move"), ("black", "black_move")):
            san = move_data.get(field)
            if not san or san == "--":
                continue
            try:
                board.push(board.parse_san(san))
                associations.append(
                    create_association_data(board.fen(), move_number, color, link)
                )
            except ValueError as error:
                failures.append({
                    "link": link,
                    "move_num": move_number,
                    "color": color,
                    "san": san,
                    "error": str(error),
                })
                return associations, failures

    return associations, failures


def count_expected_fen_positions(moves: list[MoveRow]) -> int:
    return sum(
        bool(move.get(field) and move.get(field) != "--")
        for move in moves
        for field in ("white_move", "black_move")
    )


def aggregate_fen_data(
    associations: list[AssociationRow],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Aggregate unique FENs and de-duplicate durable game-position links."""
    fen_map: dict[str, dict[str, Any]] = {}
    counter_sets: dict[str, set[str]] = {}
    durable_associations: list[dict[str, Any]] = []
    seen_associations: set[tuple[Any, ...]] = set()

    for association in associations:
        fen = association["fen_fen"]
        counter = association.get("move_counter_string", "#0_0")
        fen_row = fen_map.get(fen)
        if fen_row is None:
            fen_row = {
                "fen": fen,
                "piece_count": count_fen_pieces(fen),
                "n_games": 0,
                "moves_counter": "",
                "score": None,
                "next_moves": None,
            }
            fen_map[fen] = fen_row
            counter_sets[fen] = set()
        fen_row["n_games"] += 1
        if counter not in counter_sets[fen]:
            counter_sets[fen].add(counter)
            fen_row["moves_counter"] += counter

        key = (
            association["game_link"],
            fen,
            association["n_move"],
            association["move_color"],
        )
        if key in seen_associations:
            continue
        seen_associations.add(key)
        durable_associations.append({
            "game_link": association["game_link"],
            "fen_fen": fen,
            "n_move": association["n_move"],
            "move_color": association["move_color"],
        })

    return list(fen_map.values()), durable_associations


def split_balanced(data: list[Any], chunk_count: int) -> list[list[Any]]:
    """Split data into exactly ``chunk_count`` balanced chunks."""
    if chunk_count <= 0:
        return [data]
    base_size, remainder = divmod(len(data), chunk_count)
    chunks: list[list[Any]] = []
    start = 0
    for index in range(chunk_count):
        size = base_size + (1 if index < remainder else 0)
        chunks.append(data[start:start + size])
        start += size
    return chunks
