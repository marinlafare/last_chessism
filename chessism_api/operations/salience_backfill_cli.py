"""Queue the persistent salience projection from an administrative shell."""

from __future__ import annotations

import argparse
import asyncio
import json

import constants
from chessism_api.database.engine import init_db
from chessism_api.operations.player_salience import (
    enqueue_player_salience,
    enqueue_stale_player_salience_jobs,
    seed_player_salience_summaries,
)
from chessism_api.redis_client import close_redis_pool, get_redis_pool


async def queue_backfill(player_name: str | None, limit: int) -> dict:
    await init_db(constants.CONN_STRING)
    redis = await get_redis_pool()
    try:
        seeded = await seed_player_salience_summaries()
        if player_name:
            jobs = [await enqueue_player_salience(redis, player_name, force=True)]
        else:
            jobs = await enqueue_stale_player_salience_jobs(redis, limit=limit)
        return {"seeded_players": seeded, "queued": jobs}
    finally:
        await close_redis_pool()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--player", help="Queue only one tracked player.")
    parser.add_argument("--limit", type=int, default=1_000)
    args = parser.parse_args()
    payload = asyncio.run(queue_backfill(args.player, max(1, min(args.limit, 1_000))))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
