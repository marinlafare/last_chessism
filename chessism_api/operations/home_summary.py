"""Homepage cache policy and presentation; structural snapshots rebuild off HTTP."""
import logging
from datetime import datetime, timezone

from chessism_api.database.postgresql import home_summary as storage
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.player_titles import OFFICIAL_PLAYER_TITLES as TITLES

LOG = logging.getLogger(__name__)
JOB_ID = "home-titled-summary-v1"
LOCK_ID = 731_946_229
REFRESH_SECONDS = 300


def needs_refresh(meta, now=None):
    now = now or datetime.now(timezone.utc)
    previous = meta.get("attempted_at")
    if previous and (now - previous).total_seconds() < REFRESH_SECONDS:
        return False
    return (meta.get("refreshed_at") is None or meta.get("last_error") is not None
            or meta.get("source_revision") != meta.get("current_revision"))


async def get_home_summary(redis, *, session_factory=None):
    async with read_transaction(session_factory or AsyncDBSession) as session:
        meta = await storage.read_snapshot(session)
        untitled = await storage.untitled_players(session)
        roster = await storage.game_players(session)
    game_players = {"titled": [], "untitled": []}
    for row in roster:
        group = "titled" if row["title"] in TITLES else "untitled"
        game_players[group].append(dict(row))
    error = meta["last_error"]
    if needs_refresh(meta):
        try:
            await redis.enqueue_job("run_home_summary_refresh", _job_id=JOB_ID,
                                    _queue_name="analysis_queue")
        except Exception:
            LOG.exception("Could not enqueue homepage summary")
            error = "Summary refresh unavailable; try again later."
    refreshed = meta["refreshed_at"]
    return {
        **{key: int(meta[key]) if refreshed else None
           for key in ("total_fens", "titled_players", "titled_fens", "titled_games")},
        "refreshed_at": refreshed.isoformat() if refreshed else None,
        "stale": not refreshed or meta["source_revision"] != meta["current_revision"],
        "refresh_error": error,
        "game_players": game_players,
        "players": meta["players"],
        "untitled_players": [{"player_name": row["player_name"], "title": "",
                              "games": int(row["games"]), "avatar": row["avatar"]}
                             for row in untitled],
    }


async def run_home_summary_refresh(ctx):
    return await refresh_home_summary()


async def refresh_home_summary(*, session_factory=None):
    factory = session_factory or AsyncDBSession
    attempted_at = None
    try:
        async with write_transaction(factory) as session:
            attempted_at = await storage.begin_refresh(session, LOCK_ID)
            if attempted_at is None:
                return {"status": "already_refreshing"}
            snapshot = await storage.build_snapshot(session)
            if sum(row["game_half_units"] for row in snapshot["players"]) != 2*snapshot["titled_games"]:
                raise ValueError("Titled game contributions do not match distinct games")
            await storage.publish(session, snapshot, attempted_at)
    except Exception:
        LOG.exception("Homepage summary rebuild failed; retaining previous snapshot")
        if attempted_at is not None:
            try:
                async with write_transaction(factory) as session:
                    await storage.record_failure(session, attempted_at)
            except Exception:
                LOG.exception("Could not record homepage refresh failure; preserving original error")
        raise
    return {"status": "complete", **{key: snapshot[key]
        for key in ("total_fens", "titled_players", "titled_games", "titled_fens")}}
