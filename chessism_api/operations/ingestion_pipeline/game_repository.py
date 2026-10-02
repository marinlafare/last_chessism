"""Database persistence and deduplication for downloaded Chess.com games."""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import text

from chessism_api.database.db_interface import DBInterface
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    Game,
    GameOpening,
    GamePlayer,
    Move,
    NoMovesGame,
)
from chessism_api.operations.player_salience import mark_player_salience_stale


GameArchive = dict[str, dict[str, list[dict[str, Any]]]]


async def _sync_player_month_counts(
    session,
    player_name: str,
    affected_months: set[tuple[int, int]],
) -> None:
    if not affected_months:
        return

    await session.execute(text("""
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_ingested_months (
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            PRIMARY KEY (year, month)
        ) ON COMMIT DROP;
    """))
    month_rows = tuple(sorted(affected_months))
    for start in range(0, len(month_rows), 1_000):
        chunk = month_rows[start:start + 1_000]
        values = ", ".join(
            f"(:year_{index}, :month_{index})"
            for index in range(len(chunk))
        )
        parameters = {
            key: value
            for index, (year, month) in enumerate(chunk)
            for key, value in (
                (f"year_{index}", int(year)),
                (f"month_{index}", int(month)),
            )
        }
        await session.execute(text(f"""
            INSERT INTO temp_ingested_months (year, month)
            VALUES {values}
            ON CONFLICT DO NOTHING;
        """), parameters)

    await session.execute(text("""
        INSERT INTO months (player_name, year, month, n_games)
        SELECT
            :player_name,
            ingested.year,
            ingested.month,
            COUNT(game.link)::int
        FROM temp_ingested_months AS ingested
        LEFT JOIN game
          ON game.year = ingested.year
         AND game.month = ingested.month
         AND (game.white = :player_name OR game.black = :player_name)
        GROUP BY ingested.year, ingested.month
        ON CONFLICT (player_name, year, month) DO UPDATE SET
            n_games = EXCLUDED.n_games;
    """), {"player_name": player_name})


async def sync_player_months(
    player_name: str,
    months: set[tuple[int, int]],
) -> None:
    """Record successfully fetched archives, including zero-game months."""
    if not months:
        return
    async with AsyncDBSession() as session:
        try:
            await _sync_player_month_counts(session, player_name, months)
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def insert_game_bundle(
    games: list[dict[str, Any]],
    moves: list[dict[str, Any]],
    game_players: list[dict[str, Any]],
    game_openings: list[dict[str, Any]],
    no_move_games: list[dict[str, Any]],
    *,
    player_name: str,
    affected_months: set[tuple[int, int]],
    affected_players: set[str],
) -> None:
    """Commit one internally consistent import bundle in dependency order."""
    model_rows = (
        (Game, games),
        (Move, moves),
        (GamePlayer, game_players),
        (GameOpening, game_openings),
        (NoMovesGame, no_move_games),
    )
    async with AsyncDBSession() as session:
        try:
            for model, rows in model_rows:
                if rows:
                    await DBInterface(model).create_all_with_session(session, rows)
            await _sync_player_month_counts(session, player_name, affected_months)
            if games:
                await mark_player_salience_stale(affected_players, session=session)
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def _existing_game_links(links: set[int]) -> set[int]:
    """Check playable games and zero-move tombstones with one temp-table join."""
    if not links:
        return set()

    started = time.monotonic()
    ordered_links = tuple(sorted(links))
    async with AsyncDBSession() as session:
        await session.execute(text("""
            CREATE TEMPORARY TABLE temp_game_links_check (
                link BIGINT PRIMARY KEY
            ) ON COMMIT DROP;
        """))
        for start in range(0, len(ordered_links), 1_000):
            chunk = ordered_links[start:start + 1_000]
            values = ", ".join(f"(:link_{index})" for index in range(len(chunk)))
            await session.execute(
                text(f"""
                    INSERT INTO temp_game_links_check (link)
                    VALUES {values}
                    ON CONFLICT DO NOTHING;
                """),
                {f"link_{index}": link for index, link in enumerate(chunk)},
            )
        result = await session.execute(text("""
            SELECT game.link
            FROM game
            JOIN temp_game_links_check candidate ON candidate.link = game.link
            UNION
            SELECT empty_game.game_id
            FROM no_moves_games AS empty_game
            JOIN temp_game_links_check candidate
              ON candidate.link = empty_game.game_id;
        """))
        existing = {int(link) for link in result.scalars().all()}

    print(
        f"Checked {len(ordered_links)} downloaded IDs in "
        f"{time.monotonic() - started:.2f}s; {len(existing)} already exist.",
        flush=True,
    )
    return existing


async def missing_player_names(player_names: set[str]) -> set[str]:
    """Return names without a player row, using bound parameters throughout."""
    clean_names = tuple(sorted({name for name in player_names if name}))
    if not clean_names:
        return set()

    async with AsyncDBSession() as session:
        await session.execute(text("""
            CREATE TEMPORARY TABLE temp_player_names_check (
                player_name VARCHAR PRIMARY KEY
            ) ON COMMIT DROP;
        """))
        for start in range(0, len(clean_names), 1_000):
            chunk = clean_names[start:start + 1_000]
            values = ", ".join(f"(:name_{index})" for index in range(len(chunk)))
            await session.execute(
                text(f"""
                    INSERT INTO temp_player_names_check (player_name)
                    VALUES {values}
                    ON CONFLICT DO NOTHING;
                """),
                {f"name_{index}": name for index, name in enumerate(chunk)},
            )
        result = await session.execute(text("""
            SELECT candidate.player_name
            FROM temp_player_names_check AS candidate
            LEFT JOIN player
              ON player.player_name = candidate.player_name
            WHERE player.player_name IS NULL;
        """))
        return {str(name) for name in result.scalars().all()}


async def filter_new_games(games: GameArchive) -> GameArchive | None:
    """Return the archive subset whose IDs are absent from both game ledgers."""
    game_map: dict[int, tuple[dict[str, Any], str, str]] = {}
    for year, month_data in games.items():
        for month, month_games in month_data.items():
            for game in month_games:
                url = str(game.get("url") or "")
                try:
                    link = int(url.rsplit("/", 1)[-1])
                except (TypeError, ValueError):
                    print(f"Skipping downloaded game with invalid URL: {url!r}")
                    continue
                game_map[link] = (game, year, month)

    if not game_map:
        return None

    existing = await _existing_game_links(set(game_map))
    archive: GameArchive = {}
    for link in sorted(set(game_map) - existing):
        game, year, month = game_map[link]
        archive.setdefault(year, {}).setdefault(month, []).append(game)
    return archive or None
