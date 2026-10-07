"""Verified incremental imports; results, receipts, counts and leases commit together."""
import json

from sqlalchemy import delete, select, text

from . import runtime  # Set up the standalone package path for the host controller.
from stockfish_batch.checkpoints import BatchCheckpoints, digest
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim, Fen
from chessism_api.operations.analysis import _format_engine_results
from chessism_api.operations.fen_results import stage_fen_results


def validate_batch(raw, index, positions, contract):
    if len(raw) > BatchCheckpoints.MAX_BYTES:
        raise ValueError("Oversized checkpoint")
    group = positions[index * 500:(index + 1) * 500]
    if not group:
        raise ValueError("Checkpoint index outside selected positions")
    checkpoints = BatchCheckpoints(None, "", contract, group, 500)
    records = checkpoints.validate_batch(json.loads(raw), 0)
    name = f"batches/{index:06d}.json"
    receipt = {"sha256": digest(raw), "records": [
        {"id": record["id"], "object": name, "sha256": record["result_sha256"]}
        for record in records.values()
    ]}
    return name, receipt, [record["engine_result"] for record in records.values()]


def validate_manifest(manifest, positions, contract, receipts):
    records = [record for name in sorted(receipts) for record in receipts[name]["records"]]
    expected = {"status": "complete", "fingerprint": contract["fingerprint"],
                "position_count": len(positions), "records": records}
    if [row["id"] for row in positions] != [row["id"] for row in records] or manifest != expected:
        raise ValueError("Final manifest does not exactly match committed input IDs and result hashes")


async def import_batch(run_id, raw, index):
    async with AsyncDBSession() as session, session.begin():
        run = await session.get(CloudAnalysisRun, run_id, with_for_update=True)
        name, receipt, results = validate_batch(raw, index, run.positions, run.contract)
        if name in run.receipts:
            if run.receipts[name] != receipt:
                raise ValueError("An already imported checkpoint has changed")
            return 0
        fens = [result["fen"] for result in results]
        rows = (await session.scalars(select(Fen).where(Fen.fen.in_(fens))
                                      .order_by(Fen.fen).with_for_update())).all()
        claims = set((await session.scalars(select(CloudFenClaim.fen).where(
            CloudFenClaim.run_id == run.id, CloudFenClaim.fen.in_(fens)
        ))).all())
        if len(rows) != len(fens) or claims != set(fens) or any(row.score is not None for row in rows):
            raise ValueError("Reserved FENs changed or disappeared; refusing to overwrite analysis")
        formatted, continuations = _format_engine_results(results)
        if len(formatted) != len(fens):
            raise ValueError("Not every downloaded result can be persisted")
        await stage_fen_results(session, formatted, continuations)
        # Same counters as the local writer, but atomic with the cloud receipt.
        await session.execute(text("""
            UPDATE database_summary SET analyzed_fens = analyzed_fens + :count,
              unscored_fens = GREATEST(unscored_fens - :count, 0),
              scored_fens = scored_fens + :nonzero,
              nonzero_scored_fens = nonzero_scored_fens + :nonzero,
              refreshed_at = CURRENT_TIMESTAMP WHERE id = 1
        """), {"count": len(formatted), "nonzero": sum(row["score"] != 0 for row in formatted)})
        await session.execute(delete(CloudFenClaim).where(
            CloudFenClaim.run_id == run.id, CloudFenClaim.fen.in_(fens)))
        job = await session.get(CloudAnalysisJob, run.job_id, with_for_update=True)
        job.imported += len(formatted)
        run.receipts = {**run.receipts, name: receipt}
        return len(formatted)


async def refresh_projections(positions):
    """Absolute, retry-safe derived views; never replay incremental counters."""
    from chessism_api.database.models import GameFenAssociation
    from chessism_api.database.ask_db import (
        refresh_game_analysis_summary,
    )
    links = set()
    async with AsyncDBSession() as session:
        for offset in range(0, len(positions), 1000):
            links.update((await session.scalars(select(GameFenAssociation.game_link).where(
                GameFenAssociation.fen_fen.in_([row["fen"] for row in positions[offset:offset + 1000]])
            ).distinct())).all())
    if links:
        await refresh_game_analysis_summary(links, strict=True)


async def refresh_global_projections():
    # Full-catalog scans run once per UI request, not once per uploaded file/chunk.
    from chessism_api.database.ask_db import refresh_scored_position_summary, refresh_scored_rating_summary
    await refresh_scored_position_summary()
    await refresh_scored_rating_summary()
