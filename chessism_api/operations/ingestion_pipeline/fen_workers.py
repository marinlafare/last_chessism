"""ARQ child jobs for position extraction and bulk persistence."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chessism_api.database.db_interface import DBInterface
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, GameFenAssociation
from chessism_api.operations.ingestion_pipeline.fen_core import (
    count_expected_fen_positions,
    process_single_game_sync,
)
from chessism_api.operations.ingestion_pipeline.fen_repository import (
    claim_games_needing_fens,
    read_moves_for_games,
    set_game_fen_state,
)
from chessism_api.operations.ingestion_pipeline.progress import increment_stage_progress


INSERT_BATCH_SIZE = 5_000
FAILURE_LOG_PATH = Path("/app/illegal_fen.txt")


def _write_failures(worker_id: str, failures: list[dict[str, Any]]) -> None:
    if not failures:
        return
    try:
        FAILURE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with FAILURE_LOG_PATH.open("a", encoding="utf-8") as output:
            output.write(
                f"\n--- FEN Job {worker_id} Run: "
                f"{datetime.now(timezone.utc).isoformat()} ---\n"
            )
            output.writelines(f"{failure}\n" for failure in failures)
            output.write("--- End of Job Run ---\n")
    except OSError as error:
        print(f"[FEN GEN {worker_id}] Could not write failures: {error}", flush=True)


async def run_fen_generation_job(
    ctx: dict[str, Any],
    total_games_to_process: int,
    batch_size: int,
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Claim games and replay their SAN moves in one of the FEN worker processes."""
    worker_id = str(ctx.get("job_id") or "unknown")[:6]
    prefix = f"[FEN GEN {worker_id}]"
    associations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    claimed_links: set[int] = set()
    successful_links: set[int] = set()
    failed_links: set[int] = set()
    processed_games = 0
    expected_positions_total = 0
    started = time.monotonic()

    while processed_games < total_games_to_process:
        current_batch_size = min(
            max(1, int(batch_size)),
            total_games_to_process - processed_games,
        )
        async with AsyncDBSession() as session:
            try:
                game_links = await claim_games_needing_fens(session, current_batch_size)
                if not game_links:
                    break
                moves_by_link = await read_moves_for_games(session, game_links)
                missing_links = set(game_links) - set(moves_by_link)
                for link in missing_links:
                    failures.append({
                        "link": link,
                        "move_num": -1,
                        "san": "N/A",
                        "error": "No stored moves",
                    })
                    failed_links.add(link)

                expected_by_link = {
                    link: count_expected_fen_positions(moves)
                    for link, moves in moves_by_link.items()
                }
                batch_expected = sum(expected_by_link.values())
                expected_positions_total += batch_expected
                if parent_job_id:
                    await increment_stage_progress(
                        ctx["redis"],
                        job_id=parent_job_id,
                        ingestion_run_id=ingestion_run_id,
                        phase="extracting",
                        total_delta=batch_expected,
                        detail="Extracting positions from game moves.",
                    )

                # There are already multiple FEN worker processes. python-chess
                # replay is Python CPU work, so per-game threads add scheduling
                # overhead without bypassing the GIL.
                for link, moves in moves_by_link.items():
                    game_associations, game_failures = process_single_game_sync(
                        (link, moves)
                    )
                    expected = expected_by_link[link]
                    if parent_job_id:
                        await increment_stage_progress(
                            ctx["redis"],
                            job_id=parent_job_id,
                            ingestion_run_id=ingestion_run_id,
                            phase="extracting",
                            processed_delta=len(game_associations),
                            failed_delta=max(0, expected - len(game_associations)),
                            detail="Extracting positions from game moves.",
                        )
                    if game_failures or not game_associations:
                        failures.extend(game_failures or [{
                            "link": link,
                            "move_num": -1,
                            "san": "N/A",
                            "error": "No FEN associations generated",
                        }])
                        failed_links.add(link)
                    else:
                        associations.extend(game_associations)
                        successful_links.add(link)

                await set_game_fen_state(session, game_links, processing=True)
                await session.commit()
                claimed_links.update(game_links)
                processed_games += len(game_links)
            except Exception:
                await session.rollback()
                raise

    _write_failures(worker_id, failures)
    print(
        f"{prefix} processed {processed_games} games and "
        f"{len(associations)} positions in {time.monotonic() - started:.2f}s.",
        flush=True,
    )
    return {
        "associations": associations,
        "claimed_game_links": sorted(claimed_links),
        "successful_game_links": sorted(successful_links),
        "failed_game_links": sorted(failed_links),
        "failure_count": len(failures),
        "expected_positions": expected_positions_total,
    }


async def _insert_rows(
    ctx: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    model: type,
    progress_phase: str,
    progress_detail: str,
    parent_job_id: str | None,
    ingestion_run_id: str | None,
) -> None:
    interface = DBInterface(model)
    for start in range(0, len(rows), INSERT_BATCH_SIZE):
        batch = rows[start:start + INSERT_BATCH_SIZE]
        await interface.create_all(batch)
        if parent_job_id:
            await increment_stage_progress(
                ctx["redis"],
                job_id=parent_job_id,
                ingestion_run_id=ingestion_run_id,
                phase=progress_phase,
                processed_delta=len(batch),
                detail=progress_detail,
            )


async def run_fen_insertion_job(
    ctx: dict[str, Any],
    fens_to_insert: list[dict[str, Any]],
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
    **_kwargs: Any,
) -> bool:
    await _insert_rows(
        ctx,
        fens_to_insert,
        model=Fen,
        progress_phase="saving_fens",
        progress_detail="Saving unique positions.",
        parent_job_id=parent_job_id,
        ingestion_run_id=ingestion_run_id,
    )
    return True

async def run_association_insertion_job(
    ctx: dict[str, Any],
    associations_to_insert: list[dict[str, Any]],
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
    **_kwargs: Any,
) -> bool:
    await _insert_rows(
        ctx,
        associations_to_insert,
        model=GameFenAssociation,
        progress_phase="saving_games",
        progress_detail="Linking positions back to games.",
        parent_job_id=parent_job_id,
        ingestion_run_id=ingestion_run_id,
    )
    return True
