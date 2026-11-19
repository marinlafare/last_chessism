import json
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.database.postgresql.player_insights_reads import ENGINE_INSIGHTS_SQL, PLAYING_PATTERNS_SQL


PLAYER_ANALYTICS_CACHE_VERSION = "v1"
PLAYER_ANALYTICS_CACHE_SECONDS = 300
VALID_MODES = {"all", "bullet", "blitz", "rapid"}


def normalize_player_analytics_filters(
    player_name: str,
    mode: str,
    date_from: date | None,
    date_to: date | None,
) -> dict[str, Any]:
    normalized_player = str(player_name or "").strip().lower()
    normalized_mode = str(mode or "all").strip().lower()
    if not normalized_player:
        raise ValueError("A player name is required.")
    if normalized_mode not in VALID_MODES:
        raise ValueError("Mode must be all, bullet, blitz or rapid.")
    if date_from and date_to and date_from > date_to:
        raise ValueError("Start date must not be after end date.")
    return {
        "player": normalized_player,
        "mode": normalized_mode,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
    }


def player_analytics_cache_key(kind: str, filters: dict[str, Any]) -> str:
    return ":".join([
        "chessism",
        "player_analytics",
        PLAYER_ANALYTICS_CACHE_VERSION,
        kind,
        filters["player"],
        filters["mode"],
        filters["date_from"] or "first",
        filters["date_to"] or "latest",
    ])


def _decode_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


async def _run_payload_query(query: str, filters: dict[str, Any]) -> dict[str, Any]:
    async with read_transaction(AsyncDBSession) as session:
        params = {key:date.fromisoformat(value) if key in ('date_from','date_to') and isinstance(value,str) else value
                  for key,value in filters.items()}
        result = await session.execute(text(query), params)
        payload = _decode_payload(result.scalar())
    payload["player_name"] = filters["player"]
    payload["filters"] = {
        "mode": filters["mode"],
        "date_from": filters["date_from"],
        "date_to": filters["date_to"],
    }
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    return payload


async def get_player_engine_insights(filters: dict[str, Any]) -> dict[str, Any]:
    return await _run_payload_query(ENGINE_INSIGHTS_SQL, filters)


async def get_player_playing_patterns(filters: dict[str, Any]) -> dict[str, Any]:
    return await _run_payload_query(PLAYING_PATTERNS_SQL, filters)
