"""Database access used by FEN extraction and persistence workers."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Game, Move


async def claim_games_needing_fens(
    session: AsyncSession,
    batch_size: int,
) -> list[int]:
    result = await session.execute(
        select(Game.link)
        .where(
            Game.fens_done.is_(False),
            Game.fens_processing.is_(False),
            Game.rules == "chess",
        )
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    return [int(link) for link in result.scalars().all()]


async def read_moves_for_games(
    session: AsyncSession,
    game_links: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not game_links:
        return {}
    result = await session.execute(
        select(Move.link, Move.n_move, Move.white_move, Move.black_move)
        .where(Move.link.in_(game_links))
        .order_by(Move.link, Move.n_move)
    )
    moves_by_link: dict[int, list[dict[str, Any]]] = {}
    for row in result.mappings():
        moves_by_link.setdefault(int(row["link"]), []).append(dict(row))
    return moves_by_link


async def set_game_fen_state(
    session: AsyncSession,
    game_links: list[int],
    *,
    done: bool | None = None,
    processing: bool | None = None,
) -> None:
    if not game_links:
        return
    values: dict[str, bool] = {}
    if done is not None:
        values["fens_done"] = done
    if processing is not None:
        values["fens_processing"] = processing
    if values:
        await session.execute(
            update(Game).where(Game.link.in_(game_links)).values(**values)
        )


async def release_fen_processing_claims(
    game_links: list[int] | None = None,
) -> int:
    async with AsyncDBSession() as session:
        statement = update(Game).where(Game.fens_processing.is_(True))
        if game_links is not None:
            unique_links = list(dict.fromkeys(int(link) for link in game_links))
            if not unique_links:
                return 0
            statement = statement.where(Game.link.in_(unique_links))
        result = await session.execute(statement.values(fens_processing=False))
        await session.commit()
        return int(result.rowcount or 0)


async def finalize_fen_game_states(
    successful_game_links: list[int],
    failed_game_links: list[int],
) -> None:
    async with AsyncDBSession() as session:
        try:
            await set_game_fen_state(
                session,
                successful_game_links,
                done=True,
                processing=False,
            )
            await set_game_fen_state(
                session,
                failed_game_links,
                done=False,
                processing=False,
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise

async def refresh_fen_occurrence_counts(fen_values: list[str]) -> None:
    """Recount only affected positions, keeping retries idempotent."""
    unique_fens = list(dict.fromkeys(str(fen) for fen in fen_values if fen))
    if not unique_fens:
        return
    query = text("""
        WITH target(fen) AS (
            SELECT unnest(CAST(:fen_values AS VARCHAR[]))
        ), counts AS (
            SELECT target.fen, COUNT(association.id)::bigint AS occurrences
            FROM target
            LEFT JOIN game_fen_association association
              ON association.fen_fen = target.fen
            GROUP BY target.fen
        )
        UPDATE fen
        SET n_games = counts.occurrences
        FROM counts
        WHERE fen.fen = counts.fen;
    """)
    async with AsyncDBSession() as session:
        try:
            for start in range(0, len(unique_fens), 5_000):
                await session.execute(
                    query,
                    {"fen_values": unique_fens[start:start + 5_000]},
                )
            await session.commit()
        except Exception:
            await session.rollback()
            raise
