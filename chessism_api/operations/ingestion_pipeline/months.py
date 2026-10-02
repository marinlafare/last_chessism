"""Selection of Chess.com archive months for initial and incremental imports."""

from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Month
from chessism_api.operations import players as player_operations


def _months_between(start: datetime, end: datetime) -> list[str]:
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    result: list[str] = []
    while current <= last:
        result.append(f"{current.year}-{current.month}")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return result


async def _player_joined_at(player_name: str) -> datetime:
    player = await player_operations.read_player(player_name)
    joined = player.get("joined") if player else None
    if not joined:
        profile = await player_operations.insert_player({"player_name": player_name})
        if not profile:
            raise ValueError(f"Could not fetch or create profile for {player_name}.")
        joined = getattr(profile, "joined", None)
    if not joined:
        raise ValueError(f"Chess.com did not provide a joined date for {player_name}.")
    try:
        return datetime.fromtimestamp(int(joined), timezone.utc)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"Invalid joined date for {player_name}: {joined!r}") from error


async def full_archive_months(player_name: str) -> list[str]:
    joined = await _player_joined_at(player_name)
    return _months_between(joined, datetime.now(timezone.utc))


async def missing_archive_months(player_name: str) -> list[str]:
    """Months never recorded in the player's month ledger."""
    possible = await full_archive_months(player_name)
    async with AsyncDBSession() as session:
        result = await session.execute(
            select(Month.year, Month.month)
            .where(Month.player_name == player_name)
        )
        existing = {f"{row.year}-{row.month}" for row in result}
    return [month for month in possible if month not in existing]


async def update_archive_months(player_name: str) -> list[str]:
    """Latest recorded month through the current UTC month, inclusive."""
    async with AsyncDBSession() as session:
        result = await session.execute(
            select(Month.year, Month.month)
            .where(Month.player_name == player_name)
            .order_by(Month.year.desc(), Month.month.desc())
            .limit(1)
        )
        latest = result.first()
    if latest is None:
        return await full_archive_months(player_name)
    start = datetime(int(latest.year), int(latest.month), 1, tzinfo=timezone.utc)
    return _months_between(start, datetime.now(timezone.utc))
