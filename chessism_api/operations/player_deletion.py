import gzip
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.ask_db import (
    refresh_database_summary_game_counts,
    refresh_main_character_mode_summary_for_players,
    refresh_scored_position_summary,
    refresh_scored_rating_summary,
)


PLAYER_DELETION_KIND = "player_deletion"
PLAYER_DELETION_BATCH_SIZE = 250
PROGRESS_TTL_SECONDS = 60 * 60 * 24
BACKUP_DIR = Path(os.environ.get("FEN_ANALYSIS_BACKUP_DIR", "/backups"))
BACKUP_DISPLAY_DIR = os.environ.get(
    "FEN_ANALYSIS_BACKUP_DISPLAY_DIR",
    "/home/jon/Desktop/workshop/db_backups/chessism",
)
ACTIVE_JOB_STATUSES = {
    JobStatus.queued,
    JobStatus.deferred,
    JobStatus.in_progress,
}
PLAYER_ANALYSIS_FUNCTIONS = {
    "run_player_analysis_job",
    "run_player_games_analysis_job",
}


def _normalized_player_name(player_name: str) -> str:
    return str(player_name or "").strip().lower()


def _decoded(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value or "")


async def _read_progress(redis: ArqRedis, key: Any) -> dict[str, Any] | None:
    raw = await redis.get(key)
    if not raw:
        return None
    try:
        return json.loads(_decoded(raw))
    except (TypeError, ValueError):
        return None


async def find_player_deletion_conflict(
    redis: ArqRedis,
    player_name: str,
    *,
    ignore_job_id: str | None = None,
) -> str | None:
    """Return a human-readable reason why a player cannot be deleted now."""
    normalized_player = _normalized_player_name(player_name)
    for key in await redis.keys("chessism:job_progress:*"):
        progress = await _read_progress(redis, key)
        if not progress:
            continue
        if str(progress.get("job_id") or "") == str(ignore_job_id or ""):
            continue
        if str(progress.get("phase") or "").lower() in ("complete", "failed"):
            continue

        kind = str(progress.get("kind") or "")
        progress_player = _normalized_player_name(progress.get("player_name") or "")
        if kind == "fen_extraction":
            return "Automatic FEN extraction is running. Wait until it finishes."
        if kind == PLAYER_DELETION_KIND:
            return "Another player deletion is already queued or running."
        if kind == "game_update" and progress_player == normalized_player:
            return f"A game download or update for {normalized_player} is running."

    queued_rows = await redis.zrange("analysis_queue", 0, -1, withscores=True)
    for raw_job_id, _ in queued_rows:
        job_id = _decoded(raw_job_id)
        if job_id == str(ignore_job_id or ""):
            continue
        job = Job(job_id, redis, _queue_name="analysis_queue")
        if await job.status() not in ACTIVE_JOB_STATUSES:
            continue
        info = await job.info()
        if not info:
            continue
        kwargs = info.kwargs or {}
        function = str(info.function or "")
        is_player_loop = (
            function == "run_analysis_loop_job"
            and str(kwargs.get("scope") or "") == "player"
        )
        if function not in PLAYER_ANALYSIS_FUNCTIONS and not is_player_loop:
            continue
        if _normalized_player_name(kwargs.get("player_name") or "") == normalized_player:
            return f"A player-specific Stockfish analysis for {normalized_player} is active."
    return None


async def get_player_deletion_preview(player_name: str) -> dict[str, Any]:
    """Classify a main player's games by the opponent's current main/shell state."""
    normalized_player = _normalized_player_name(player_name)
    async with AsyncDBSession() as session:
        player_result = await session.execute(text("""
            SELECT player_name, joined, deleted_at
            FROM player
            WHERE player_name = :player;
        """), {"player": normalized_player})
        player = player_result.mappings().first()
        if not player:
            raise LookupError(f"Player {normalized_player} was not found.")

        counts_result = await session.execute(text("""
            SELECT
                COUNT(*)::bigint AS total_games,
                COUNT(*) FILTER (
                    WHERE COALESCE(opponent.joined, 0) = 0
                )::bigint AS exclusive_games,
                COUNT(*) FILTER (
                    WHERE COALESCE(opponent.joined, 0) <> 0
                )::bigint AS shared_games,
                COALESCE(SUM(gas.total_positions) FILTER (
                    WHERE COALESCE(opponent.joined, 0) = 0
                ), 0)::bigint AS fen_associations,
                COALESCE(SUM(gas.analyzed_positions) FILTER (
                    WHERE COALESCE(opponent.joined, 0) = 0
                ), 0)::bigint AS analyzed_fen_associations
            FROM game_player target
            JOIN player opponent ON opponent.player_name = target.opponent_name
            LEFT JOIN LATERAL (
                SELECT total_positions, analyzed_positions
                FROM game_analysis_summary
                WHERE link = target.link
                OFFSET 0
            ) gas ON TRUE
            WHERE target.player_name = :player;
        """), {"player": normalized_player})
        counts = counts_result.mappings().first() or {}

    joined = int(player.get("joined") or 0)
    deleted_at = player.get("deleted_at")
    return {
        "player_name": normalized_player,
        "is_main_player": joined != 0 and deleted_at is None,
        "already_deleted": deleted_at is not None,
        "total_games": int(counts.get("total_games") or 0),
        "exclusive_games": int(counts.get("exclusive_games") or 0),
        "shared_games": int(counts.get("shared_games") or 0),
        "fen_associations": int(counts.get("fen_associations") or 0),
        "analyzed_fen_associations": int(counts.get("analyzed_fen_associations") or 0),
        "fen_rows_deleted": 0,
        "analysis_rows_deleted": 0,
        "backup_location": BACKUP_DISPLAY_DIR,
    }


async def write_player_deletion_progress(
    redis: ArqRedis,
    job_id: str,
    *,
    player_name: str,
    total: int,
    processed: int,
    phase: str,
    detail: str,
    failed: int = 0,
    result: Any = None,
) -> None:
    payload = {
        "job_id": job_id,
        "kind": PLAYER_DELETION_KIND,
        "player_name": _normalized_player_name(player_name),
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "failed": max(0, int(failed)),
        "phase": phase,
        "detail": detail,
        "result": result,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )


async def _write_deletion_manifest(
    player_name: str,
    preview: dict[str, Any],
) -> tuple[str, str]:
    safe_player = re.sub(r"[^a-z0-9_-]+", "-", player_name).strip("-") or "player"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    filename = f"player-delete-{safe_player}-{timestamp}.jsonl.gz"
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    target = BACKUP_DIR / filename
    temporary = BACKUP_DIR / f".{filename}.{uuid4().hex}.tmp"

    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as output:
            output.write(json.dumps({
                "type": "player_deletion_manifest",
                "version": 1,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "player_name": player_name,
                "preview": preview,
                "preserves_fen_rows": True,
                "preserves_fen_analysis": True,
            }, sort_keys=True) + "\n")
            async with AsyncDBSession() as session:
                rows = await session.stream(text("""
                    SELECT
                        target.link,
                        target.opponent_name,
                        CASE
                            WHEN COALESCE(opponent.joined, 0) = 0 THEN 'delete'
                            ELSE 'preserve_shared'
                        END AS disposition
                    FROM game_player target
                    JOIN player opponent ON opponent.player_name = target.opponent_name
                    WHERE target.player_name = :player
                    ORDER BY target.link;
                """), {"player": player_name})
                async for row in rows.mappings():
                    output.write(json.dumps({
                        "type": "game",
                        "link": int(row["link"]),
                        "opponent_name": str(row["opponent_name"]),
                        "disposition": str(row["disposition"]),
                    }, sort_keys=True) + "\n")
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    display_path = f"{BACKUP_DISPLAY_DIR.rstrip('/')}/{filename}"
    return filename, display_path


async def _get_exclusive_game_links(player_name: str) -> list[int]:
    """Snapshot the current exclusive set once, avoiding a rescan per batch."""
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            SELECT target.link
            FROM game_player target
            JOIN player opponent ON opponent.player_name = target.opponent_name
            WHERE target.player_name = :player
              AND COALESCE(opponent.joined, 0) = 0
            ORDER BY target.link;
        """), {"player": player_name})
        return [int(link) for link in result.scalars().all()]


async def _delete_exclusive_game_batch(
    player_name: str,
    candidate_links: list[int],
) -> int:
    """Delete one revalidated batch while retaining every FEN and analysis row."""
    async with AsyncDBSession() as session:
        try:
            clean_candidates = [int(link) for link in candidate_links if link is not None]
            if not clean_candidates:
                return 0
            links_result = await session.execute(text("""
                SELECT target.link
                FROM game_player target
                JOIN player opponent ON opponent.player_name = target.opponent_name
                WHERE target.player_name = :player
                  AND target.link = ANY(CAST(:candidate_links AS BIGINT[]))
                  AND COALESCE(opponent.joined, 0) = 0
                ORDER BY target.link
                FOR UPDATE OF target, opponent SKIP LOCKED;
            """), {
                "player": player_name,
                "candidate_links": clean_candidates,
            })
            game_links = [int(link) for link in links_result.scalars().all()]
            if not game_links:
                await session.rollback()
                return 0

            # n_games is corpus metadata, not engine analysis. Decrement it in
            # the same transaction that removes the associations so a retry can
            # never double-decrement. Scores, WDL, PVs and continuations remain.
            await session.execute(text("""
                WITH removed AS (
                    SELECT fen_fen, COUNT(*)::bigint AS occurrences
                    FROM game_fen_association
                    WHERE game_link = ANY(CAST(:game_links AS BIGINT[]))
                    GROUP BY fen_fen
                )
                UPDATE fen current_fen
                SET n_games = GREATEST(0, current_fen.n_games - removed.occurrences)
                FROM removed
                WHERE current_fen.fen = removed.fen_fen;
            """), {"game_links": game_links})
            await session.execute(text("""
                DELETE FROM game_fen_association
                WHERE game_link = ANY(CAST(:game_links AS BIGINT[]));
            """), {"game_links": game_links})
            await session.execute(text("""
                DELETE FROM moves
                WHERE link = ANY(CAST(:game_links AS BIGINT[]));
            """), {"game_links": game_links})
            deleted = await session.execute(text("""
                DELETE FROM game
                WHERE link = ANY(CAST(:game_links AS BIGINT[]));
            """), {"game_links": game_links})
            await session.commit()
            return int(deleted.rowcount or len(game_links))
        except Exception:
            await session.rollback()
            raise


async def _demote_player(player_name: str) -> None:
    """Remove profile-owned data while retaining the username for shared games."""
    async with AsyncDBSession() as session:
        try:
            remaining_result = await session.execute(text("""
                SELECT COUNT(*)
                FROM game_player target
                JOIN player opponent ON opponent.player_name = target.opponent_name
                WHERE target.player_name = :player
                  AND COALESCE(opponent.joined, 0) = 0;
            """), {"player": player_name})
            if int(remaining_result.scalar() or 0) != 0:
                raise RuntimeError("Exclusive games remain; refusing to demote the player.")

            await session.execute(text(
                "DELETE FROM months WHERE player_name = :player;"
            ), {"player": player_name})
            await session.execute(text(
                "DELETE FROM player_stats WHERE player_name = :player;"
            ), {"player": player_name})
            await session.execute(text(
                "DELETE FROM main_character_mode_summary WHERE player_name = :player;"
            ), {"player": player_name})
            updated = await session.execute(text("""
                UPDATE player
                SET
                    name = NULL,
                    url = NULL,
                    title = NULL,
                    avatar = NULL,
                    followers = NULL,
                    country = NULL,
                    location = NULL,
                    joined = 0,
                    status = NULL,
                    is_streamer = NULL,
                    twitch_url = NULL,
                    verified = NULL,
                    league = NULL,
                    deleted_at = CURRENT_TIMESTAMP
                WHERE player_name = :player;
            """), {"player": player_name})
            if int(updated.rowcount or 0) != 1:
                raise LookupError(f"Player {player_name} was not found.")
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def _refresh_after_player_deletion(player_name: str) -> list[str]:
    warnings: list[str] = []
    refreshes = (
        ("main-player summary", lambda: refresh_main_character_mode_summary_for_players({player_name})),
        ("database game summary", refresh_database_summary_game_counts),
        ("scored-position summary", refresh_scored_position_summary),
        ("scored-rating summary", refresh_scored_rating_summary),
    )
    for label, refresh in refreshes:
        try:
            await refresh()
        except Exception as error:
            warnings.append(f"Could not refresh {label}: {error}")
    return warnings


async def run_delete_player_job(
    ctx: dict,
    player_name: str,
    expected_exclusive_games: int,
    expected_shared_games: int,
    **kwargs,
) -> dict[str, Any]:
    """Safely remove one main player's exclusive games and demote its profile."""
    normalized_player = _normalized_player_name(player_name)
    job_id = str(ctx.get("job_id") or f"delete-player-{normalized_player}")
    redis: ArqRedis = ctx["redis"]
    expected_exclusive = max(0, int(expected_exclusive_games))
    expected_shared = max(0, int(expected_shared_games))
    deleted_games = 0

    try:
        conflict = await find_player_deletion_conflict(
            redis,
            normalized_player,
            ignore_job_id=job_id,
        )
        if conflict:
            raise RuntimeError(conflict)

        preview = await get_player_deletion_preview(normalized_player)
        if not preview["is_main_player"]:
            raise RuntimeError("Only an active main player can be deleted.")
        if (
            preview["exclusive_games"] != expected_exclusive
            or preview["shared_games"] != expected_shared
        ):
            raise RuntimeError("Player game counts changed; create a new deletion preview.")

        await write_player_deletion_progress(
            redis,
            job_id,
            player_name=normalized_player,
            total=expected_exclusive,
            processed=0,
            phase="backing_up",
            detail="Writing the deletion manifest before changing the database.",
        )
        manifest_filename, manifest_path = await _write_deletion_manifest(
            normalized_player,
            preview,
        )

        remaining_links = await _get_exclusive_game_links(normalized_player)
        while remaining_links:
            pass_deleted = 0
            for start in range(0, len(remaining_links), PLAYER_DELETION_BATCH_SIZE):
                batch_deleted = await _delete_exclusive_game_batch(
                    normalized_player,
                    remaining_links[start:start + PLAYER_DELETION_BATCH_SIZE],
                )
                deleted_games += batch_deleted
                pass_deleted += batch_deleted
                await write_player_deletion_progress(
                    redis,
                    job_id,
                    player_name=normalized_player,
                    total=expected_exclusive,
                    processed=min(expected_exclusive, deleted_games),
                    phase="deleting_games",
                    detail=f"Deleted {deleted_games:,} exclusive games; shared games remain intact.",
                )
            if pass_deleted <= 0:
                raise RuntimeError("Exclusive games are locked; retry deletion after active database work finishes.")
            remaining_links = await _get_exclusive_game_links(normalized_player)

        await write_player_deletion_progress(
            redis,
            job_id,
            player_name=normalized_player,
            total=expected_exclusive,
            processed=min(expected_exclusive, deleted_games),
            phase="demoting_player",
            detail="Removing player metadata and retaining a shared-game shell.",
        )
        await _demote_player(normalized_player)
        warnings = await _refresh_after_player_deletion(normalized_player)
        result = {
            "player_name": normalized_player,
            "deleted_games": deleted_games,
            "preserved_shared_games": expected_shared + max(0, expected_exclusive - deleted_games),
            "fen_rows_deleted": 0,
            "analysis_rows_deleted": 0,
            "manifest_filename": manifest_filename,
            "manifest_path": manifest_path,
            "warnings": warnings,
        }
        detail = (
            f"Deleted {deleted_games:,} exclusive games and demoted {normalized_player}; "
            f"all FEN analysis was preserved."
        )
        if warnings:
            detail += f" {len(warnings)} summary refresh warning(s) were recorded."
        await write_player_deletion_progress(
            redis,
            job_id,
            player_name=normalized_player,
            total=expected_exclusive,
            processed=expected_exclusive,
            phase="complete",
            detail=detail,
            result=result,
        )
        return result
    except Exception as error:
        await write_player_deletion_progress(
            redis,
            job_id,
            player_name=normalized_player,
            total=expected_exclusive,
            processed=min(expected_exclusive, deleted_games),
            failed=1,
            phase="failed",
            detail=str(error),
        )
        raise
