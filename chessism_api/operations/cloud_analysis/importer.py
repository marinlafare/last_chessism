"""Verified incremental imports; results, receipts, counts and leases commit together."""
import json
import time
from datetime import datetime, timezone

from sqlalchemy import delete, select, text, update

from . import runtime  # Set up the standalone package path for the host controller.
from stockfish_batch.checkpoints import BatchCheckpoints, digest
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim, Fen, CloudResultBatch, CloudBatchUnit
from chessism_api.database import cloud_columns
from chessism_api.database.cloud_codec import read_document, write_document, checksum
from chessism_api.operations.analysis import _format_engine_results
from chessism_api.operations.fen_results import stage_fen_results


def validate_batch(raw, index, positions, contract, task=None):
    if type(index) is not int or index < 0:
        raise ValueError("Invalid checkpoint index")
    if task and (task["start"] < 0 or task["count"] < 1 or task["start"] + task["count"] > len(positions)):
        raise ValueError("Invalid task range")
    if len(raw) > BatchCheckpoints.MAX_BYTES:
        raise ValueError("Oversized checkpoint")
    offset = task["start"] if task else 0
    end = offset + task["count"] if task else len(positions)
    group = positions[offset + index * 500:min(end, offset + (index + 1) * 500)]
    if not group:
        raise ValueError("Checkpoint index outside selected positions")
    checkpoints = BatchCheckpoints(None, "", task["contract"] if task else contract, group, 500)
    records = checkpoints.validate_batch(json.loads(raw), 0)
    name = f"batches/{index:06d}.json"
    receipt = {"sha256": digest(raw), "records": [
        {"id": record["id"], "object": name, "sha256": record["result_sha256"]}
        for record in records.values()
    ]}
    if task:
        name = f"tasks/{task['index']:06d}/" + name
        for record in receipt["records"]:
            record["object"] = name
    return name, receipt, [record["engine_result"] for record in records.values()]


def validate_manifest(manifest, positions, contract, receipts):
    records = [record for name in sorted(receipts) for record in receipts[name]["records"]]
    expected = {"status": "complete", "fingerprint": contract["fingerprint"],
                "position_count": len(positions), "records": records}
    if [row["id"] for row in positions] != [row["id"] for row in records] or manifest != expected:
        raise ValueError("Final manifest does not exactly match committed input IDs and result hashes")


async def import_batch(run_id, raw, index, *, task_index=None, download_seconds=None):
    started = time.monotonic()
    if type(index) is not int or index < 0:
        raise ValueError('Invalid checkpoint index')
    async with AsyncDBSession() as session, session.begin():
        # Read only control references, not every selected FEN and all previous
        # receipts. A run lock serializes import/cleanup across process restarts.
        run = (await session.execute(select(CloudAnalysisRun.id, CloudAnalysisRun.job_id,
            CloudAnalysisRun._launch_ref, CloudAnalysisRun._contract_ref, CloudAnalysisRun._positions_ref,
            CloudAnalysisRun._receipts_ref, CloudAnalysisRun.position_count, CloudAnalysisRun.details_pruned)
            .where(CloudAnalysisRun.id == run_id).with_for_update())).one()
        if run.details_pruned:
            raise ValueError('Completed job has released its temporary import details')
        connection = await session.connection()
        launch = await connection.run_sync(read_document, run._launch_ref)
        if task_index is not None and (type(task_index) is not int or not 0 <= task_index < len(launch["tasks"])):
            raise ValueError("Invalid task index")
        task = launch['tasks'][task_index] if task_index is not None else None
        contract = task['contract'] if task else await connection.run_sync(read_document, run._contract_ref)
        start = (task['start'] if task else 0) + index * 500
        end = min((task['start'] + task['count']) if task else run.position_count, start + 500)
        if start >= end:
            raise ValueError('Checkpoint index outside selected positions')
        table = cloud_columns.TABLES['position']
        packs = (await session.execute(select(table.c.id, table.c.fen).where(
            table.c.document_id == run._positions_ref,
            table.c.ordinal.between(start // 500, (end - 1) // 500)).order_by(table.c.ordinal))).all()
        positions = [{'id': ident, 'fen': fen} for pack in packs for ident, fen in zip(pack.id, pack.fen)]
        positions = positions[start % 500:start % 500 + end - start]
        if len(positions) != end - start:
            raise ValueError('Reserved input batch is missing')
        validation_started = time.monotonic()
        _, receipt, results = validate_batch(raw, 0, positions, contract)
        name = (f'tasks/{task_index:06d}/' if task is not None else '') + f'batches/{index:06d}.json'
        for record in receipt['records']:
            record['object'] = name
        validation_seconds = time.monotonic() - validation_started
        key = {'run_id': run_id, 'task_index': task_index if task_index is not None else -1, 'batch_index': index}
        previous = await session.get(CloudResultBatch, key)
        if previous:
            if previous.sha256 != receipt['sha256'] or previous.receipt_sha256 != checksum(receipt):
                raise ValueError("An already imported checkpoint has changed")
            return 0
        # Older incomplete jobs retain their receipts until the migration's
        # verified copy is available in the compact per-batch ledger.
        legacy = await connection.run_sync(read_document, run._receipts_ref)
        if name in legacy:
            if legacy[name] != receipt:
                raise ValueError('An already imported checkpoint has changed')
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
        await session.execute(update(CloudAnalysisJob).where(CloudAnalysisJob.id == run.job_id)
            .values(imported=CloudAnalysisJob.imported + len(formatted)))
        await session.execute(update(CloudAnalysisRun).where(CloudAnalysisRun.id == run_id)
            .values(imported_count=CloudAnalysisRun.imported_count + len(formatted)))
        root = await session.scalar(select(CloudBatchUnit.root_run_id).where(CloudBatchUnit.run_id == run_id))
        if root:
            await session.execute(update(CloudAnalysisRun).where(CloudAnalysisRun.id == root)
                .values(imported_count=CloudAnalysisRun.imported_count + len(formatted)))
        detail = f'{run_id}:batch:{key["task_index"]}:{index}'
        await connection.run_sync(write_document, detail, 'receipt', receipt)
        session.add(CloudResultBatch(**key, object_name=name, sha256=receipt['sha256'],
            receipt_sha256=checksum(receipt), position_count=len(formatted), size_bytes=len(raw),
            download_seconds=download_seconds, validation_seconds=validation_seconds,
            detail_record_id=detail))
    # Actual commit duration is observed only after COMMIT. An interruption
    # here leaves a valid receipt with optional timing absent, not a lost import.
    async with AsyncDBSession() as session, session.begin():
        await session.execute(update(CloudResultBatch).where(
            CloudResultBatch.run_id == run_id, CloudResultBatch.task_index == key['task_index'],
            CloudResultBatch.batch_index == index).values(transaction_seconds=time.monotonic() - started,
                                                        committed_at=datetime.now(timezone.utc)))
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


async def refresh_unit_projections(run_ids):
    """Deduplicate affected games in SQL; never materialize the parent FEN set."""
    from chessism_api.database.ask_db import refresh_game_analysis_summary
    async with AsyncDBSession() as session:
        refs = (await session.scalars(select(CloudAnalysisRun._positions_ref).where(
            CloudAnalysisRun.id.in_(run_ids)))).all()
        if len(refs) != len(set(run_ids)) or not all(refs):
            raise ValueError('Missing work-unit input references')
        links = (await session.scalars(text('''
            SELECT DISTINCT gfa.game_link
            FROM cloud_control_position p
            CROSS JOIN LATERAL unnest(p.fen) selected(fen)
            JOIN game_fen_association gfa ON gfa.fen_fen = selected.fen
            WHERE p.document_id = ANY(CAST(:refs AS text[]))
        '''), {'refs': refs})).all()
    if links:
        await refresh_game_analysis_summary(links, strict=True)
