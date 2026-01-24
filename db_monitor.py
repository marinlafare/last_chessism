# db_monitor.py
import asyncio
from typing import Optional

from sqlalchemy import func, select, update

from chessism_api.database.engine import AsyncDBSession, init_db
from chessism_api.database.models import Player, Game, Fen, GameFenAssociation
import constants


async def count_players_with_joined() -> int:
    """
    Returns the number of players where Player.joined is not NULL.
    """
    async with AsyncDBSession() as session:
        stmt = select(func.count(Player.player_name)).where(Player.joined.is_not(None))
        result = await session.execute(stmt)
        return int(result.scalar() or 0)


async def count_games_for_player(player_name: str) -> int:
    """
    Returns the number of games where the player appears as white or black.
    """
    if not player_name:
        return 0
    async with AsyncDBSession() as session:
        stmt = select(func.count(Game.link)).where(
            (Game.white == player_name) | (Game.black == player_name)
        )
        result = await session.execute(stmt)
        return int(result.scalar() or 0)


async def count_fens_for_player(player_name: str) -> int:
    """
    Returns the number of distinct FENs associated with a player.
    """
    if not player_name:
        return 0
    async with AsyncDBSession() as session:
        stmt = (
            select(func.count(func.distinct(Fen.fen)))
            .join(GameFenAssociation, Fen.fen == GameFenAssociation.fen_fen)
            .join(Game, GameFenAssociation.game_link == Game.link)
            .where((Game.white == player_name) | (Game.black == player_name))
        )
        result = await session.execute(stmt)
        return int(result.scalar() or 0)


async def clear_fen_analysis() -> int:
    """
    Sets Fen.score and Fen.next_moves to NULL for all rows.
    Returns number of rows updated.
    """
    async with AsyncDBSession() as session:
        stmt = (
            update(Fen)
            .where(Fen.score.is_not(None) | Fen.next_moves.is_not(None))
            .values(score=None, next_moves=None)
        )
        result = await session.execute(stmt)
        await session.commit()
        return int(result.rowcount or 0)


async def clear_fen_analysis_chunked(batch_size: int = 10000, sleep_seconds: float = 0.0) -> int:
    """
    Clears Fen.score and Fen.next_moves in batches to reduce lock time.
    Returns total rows updated.
    """
    if batch_size <= 0:
        return 0

    total_updated = 0
    while True:
        async with AsyncDBSession() as session:
            subquery = (
                select(Fen.fen)
                .where(Fen.score.is_not(None) | Fen.next_moves.is_not(None))
                .limit(batch_size)
                .scalar_subquery()
            )
            stmt = (
                update(Fen)
                .where(Fen.fen.in_(subquery))
                .values(score=None, next_moves=None)
            )
            result = await session.execute(stmt)
            await session.commit()

        updated = int(result.rowcount or 0)
        total_updated += updated
        print(f"Cleared {updated} rows this batch (total {total_updated}).", flush=True)
        if updated == 0:
            break
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)

    return total_updated


async def _demo(player_name: Optional[str] = None) -> None:
    """
    Optional helper for quick manual checks.
    """
    await init_db(constants.CONN_STRING)
    print("players_with_joined =", await count_players_with_joined())
    if player_name:
        print("games_for_player =", await count_games_for_player(player_name))
        print("fens_for_player  =", await count_fens_for_player(player_name))


if __name__ == "__main__":
    # Example: python db_monitor.py hikaru
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(_demo(name))
