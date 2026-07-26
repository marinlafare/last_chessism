import gzip
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import bindparam, delete, func, insert, select, update

from chessism_api.database.ask_db import (
    refresh_database_summary_fen_counts,
    refresh_game_analysis_summary,
    refresh_scored_position_summary,
    refresh_scored_rating_summary,
)
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation


BACKUP_FORMAT = "chessism-fen-analysis"
BACKUP_VERSION = 2
SUPPORTED_BACKUP_VERSIONS = {1, 2}
BACKUP_DIR = Path(os.getenv(
    "FEN_ANALYSIS_BACKUP_DIR",
    "/home/jon/Desktop/workshop/db_backups/chessism",
))
BACKUP_DISPLAY_DIR = os.getenv(
    "FEN_ANALYSIS_BACKUP_DISPLAY_DIR",
    "/home/jon/Desktop/workshop/db_backups/chessism",
)
BACKUP_FILE_PATTERN = re.compile(
    r"^fen-analysis-\d{8}T\d{6}_\d{6}Z\.jsonl\.gz$"
)
RESTORE_BATCH_SIZE = 1_000
PROGRESS_INTERVAL = 1_000
PROGRESS_TTL_SECONDS = 60 * 60 * 24


async def _write_progress(
    ctx: dict,
    job_id: str,
    *,
    kind: str,
    total: int,
    processed: int,
    phase: str,
    detail: str,
    failed: int = 0,
    result: dict[str, Any] | None = None,
) -> None:
    redis = ctx.get("redis")
    if not redis:
        return

    payload = {
        "job_id": job_id,
        "kind": kind,
        "total": int(total),
        "processed": int(processed),
        "failed": int(failed),
        "phase": phase,
        "detail": detail,
        "result": result,
        "updated_at": datetime.now(timezone.utc).timestamp(),
    }
    try:
        await redis.set(
            f"chessism:job_progress:{job_id}",
            json.dumps(payload),
            ex=PROGRESS_TTL_SECONDS,
        )
    except Exception as error:
        print(f"Failed to write backup progress for {job_id}: {error!r}", flush=True)


def _backup_path(filename: str) -> Path:
    if Path(filename).name != filename or not BACKUP_FILE_PATTERN.fullmatch(filename):
        raise ValueError("Invalid FEN-analysis backup filename")
    return BACKUP_DIR / filename


def _metadata_path(backup_path: Path) -> Path:
    return backup_path.with_name(f"{backup_path.name}.meta.json")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as backup_file:
        for chunk in iter(lambda: backup_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_name(f".{path.name}.tmp")
    try:
        with temporary_path.open("w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _analysis_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "fen_analysis",
        "fen": row["fen"],
        "score": row["score"],
        "next_moves": row["next_moves"],
        "wdl_win": row["wdl_win"],
        "wdl_draw": row["wdl_draw"],
        "wdl_loss": row["wdl_loss"],
        "piece_count": row["piece_count"],
        "analysis_source": row["analysis_source"],
        "tablebase_wdl": row["tablebase_wdl"],
        "tablebase_dtz": row["tablebase_dtz"],
        "analyzed_at": (
            row["analyzed_at"].isoformat() if row["analyzed_at"] else None
        ),
        "continuations": [],
    }


def _append_continuation(record: dict[str, Any], row: dict[str, Any]) -> None:
    if row["continuation_rank"] is None:
        return
    record["continuations"].append({
        "rank": int(row["continuation_rank"]),
        "move": row["continuation_move"],
        "score": float(row["continuation_score"]),
    })


async def create_fen_analysis_backup(ctx: dict) -> dict[str, Any]:
    """Write all committed Stockfish and tablebase results to a compressed JSONL file."""
    job_id = str(ctx.get("job_id") or "fen-analysis-backup")
    created_at = datetime.now(timezone.utc)
    filename = f"fen-analysis-{created_at.strftime('%Y%m%dT%H%M%S_%fZ')}.jsonl.gz"
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    final_path = _backup_path(filename)
    temporary_path = final_path.with_name(f".{filename}.tmp")

    async with AsyncDBSession() as session:
        total = int(await session.scalar(
            select(func.count()).select_from(Fen).where(Fen.score.is_not(None))
        ) or 0)

        await _write_progress(
            ctx,
            job_id,
            kind="fen_analysis_backup",
            total=total,
            processed=0,
            phase="exporting",
            detail=f"Saving {total} analyzed positions.",
        )

        statement = (
            select(
                Fen.fen.label("fen"),
                Fen.score.label("score"),
                Fen.next_moves.label("next_moves"),
                Fen.wdl_win.label("wdl_win"),
                Fen.wdl_draw.label("wdl_draw"),
                Fen.wdl_loss.label("wdl_loss"),
                Fen.piece_count.label("piece_count"),
                Fen.analysis_source.label("analysis_source"),
                Fen.tablebase_wdl.label("tablebase_wdl"),
                Fen.tablebase_dtz.label("tablebase_dtz"),
                Fen.analyzed_at.label("analyzed_at"),
                FenContinuation.rank.label("continuation_rank"),
                FenContinuation.move.label("continuation_move"),
                FenContinuation.score.label("continuation_score"),
            )
            .outerjoin(FenContinuation, FenContinuation.fen_fen == Fen.fen)
            .where(Fen.score.is_not(None))
            .order_by(Fen.fen, FenContinuation.rank)
            .execution_options(yield_per=2_000)
        )

        processed = 0
        current_record: dict[str, Any] | None = None
        try:
            with gzip.open(
                temporary_path,
                "wt",
                encoding="utf-8",
                compresslevel=6,
            ) as output:
                header = {
                    "type": "metadata",
                    "format": BACKUP_FORMAT,
                    "version": BACKUP_VERSION,
                    "created_at": created_at.isoformat(),
                    "records": total,
                }
                output.write(json.dumps(header, separators=(",", ":")) + "\n")

                stream = await session.stream(statement)
                async for row in stream.mappings():
                    row_data = dict(row)
                    if current_record is None or current_record["fen"] != row_data["fen"]:
                        if current_record is not None:
                            output.write(json.dumps(current_record, separators=(",", ":")) + "\n")
                            processed += 1
                            if processed % PROGRESS_INTERVAL == 0:
                                await _write_progress(
                                    ctx,
                                    job_id,
                                    kind="fen_analysis_backup",
                                    total=total,
                                    processed=processed,
                                    phase="exporting",
                                    detail=f"Saved {processed}/{total} analyzed positions.",
                                )
                        current_record = _analysis_record(row_data)
                    _append_continuation(current_record, row_data)

                if current_record is not None:
                    output.write(json.dumps(current_record, separators=(",", ":")) + "\n")
                    processed += 1

            with temporary_path.open("rb") as backup_file:
                os.fsync(backup_file.fileno())
            os.replace(temporary_path, final_path)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

    checksum = _sha256(final_path)
    result = {
        "filename": filename,
        "created_at": created_at.isoformat(),
        "records": processed,
        "bytes": final_path.stat().st_size,
        "sha256": checksum,
    }
    metadata_path = _metadata_path(final_path)
    _atomic_write_json(metadata_path, result)

    await _write_progress(
        ctx,
        job_id,
        kind="fen_analysis_backup",
        total=total,
        processed=processed,
        phase="complete",
        detail=f"Saved {filename}.",
        result=result,
    )
    return result


def list_fen_analysis_backups() -> list[dict[str, Any]]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = []
    for backup_path in BACKUP_DIR.glob("fen-analysis-*.jsonl.gz"):
        if not BACKUP_FILE_PATTERN.fullmatch(backup_path.name):
            continue
        metadata_path = _metadata_path(backup_path)
        metadata: dict[str, Any] = {}
        if metadata_path.is_file():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                metadata = {}
        backups.append({
            "filename": backup_path.name,
            "created_at": metadata.get("created_at"),
            "records": metadata.get("records"),
            "bytes": int(metadata.get("bytes") or backup_path.stat().st_size),
            "sha256": metadata.get("sha256"),
        })
    backups.sort(key=lambda item: item["filename"], reverse=True)
    return backups


def _backup_records(backup_path: Path) -> tuple[dict[str, Any], Iterable[dict[str, Any]]]:
    backup_file = gzip.open(backup_path, "rt", encoding="utf-8")
    try:
        header = json.loads(backup_file.readline())
    except Exception:
        backup_file.close()
        raise

    if (
        header.get("type") != "metadata"
        or header.get("format") != BACKUP_FORMAT
        or int(header.get("version") or 0) not in SUPPORTED_BACKUP_VERSIONS
    ):
        backup_file.close()
        raise ValueError("Unsupported FEN-analysis backup format")

    def records() -> Iterable[dict[str, Any]]:
        try:
            for line in backup_file:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("type") == "fen_analysis":
                    yield record
        finally:
            backup_file.close()

    return header, records()


async def _restore_batch(records: list[dict[str, Any]]) -> tuple[int, int]:
    requested_fens = [str(record.get("fen") or "") for record in records]
    async with AsyncDBSession() as session:
        existing_result = await session.execute(
            select(Fen.fen).where(Fen.fen.in_(requested_fens))
        )
        existing_fens = set(existing_result.scalars().all())
        prepared = []
        continuation_rows = []
        for record in records:
            fen = str(record.get("fen") or "")
            if fen not in existing_fens:
                continue
            analyzed_at = record.get("analyzed_at")
            if isinstance(analyzed_at, str):
                analyzed_at = datetime.fromisoformat(analyzed_at)
            piece_count = record.get("piece_count")
            if piece_count is None:
                piece_count = sum(
                    character.isalpha()
                    for character in fen.split(" ", 1)[0]
                )
            prepared.append({
                "p_fen": fen,
                "p_score": record.get("score"),
                "p_next_moves": record.get("next_moves"),
                "p_wdl_win": record.get("wdl_win"),
                "p_wdl_draw": record.get("wdl_draw"),
                "p_wdl_loss": record.get("wdl_loss"),
                "p_piece_count": piece_count,
                "p_analysis_source": record.get("analysis_source") or "stockfish_backup",
                "p_tablebase_wdl": record.get("tablebase_wdl"),
                "p_tablebase_dtz": record.get("tablebase_dtz"),
                "p_analyzed_at": analyzed_at,
            })
            for continuation in record.get("continuations") or []:
                continuation_rows.append({
                    "fen_fen": fen,
                    "rank": int(continuation["rank"]),
                    "move": str(continuation["move"]),
                    "score": float(continuation["score"]),
                })

        if prepared:
            statement = (
                update(Fen.__table__)
                .where(Fen.fen == bindparam("p_fen"))
                .values(
                    score=bindparam("p_score"),
                    next_moves=bindparam("p_next_moves"),
                    wdl_win=bindparam("p_wdl_win"),
                    wdl_draw=bindparam("p_wdl_draw"),
                    wdl_loss=bindparam("p_wdl_loss"),
                    piece_count=bindparam("p_piece_count"),
                    analysis_source=bindparam("p_analysis_source"),
                    tablebase_wdl=bindparam("p_tablebase_wdl"),
                    tablebase_dtz=bindparam("p_tablebase_dtz"),
                    analyzed_at=bindparam("p_analyzed_at"),
                )
            )
            await session.execute(
                statement,
                prepared,
                execution_options={"synchronize_session": False},
            )
            await session.execute(
                delete(FenContinuation).where(FenContinuation.fen_fen.in_(existing_fens))
            )
            if continuation_rows:
                await session.execute(insert(FenContinuation.__table__), continuation_rows)
        await session.commit()
    return len(existing_fens), len(records) - len(existing_fens)


async def restore_fen_analysis_backup(ctx: dict, filename: str) -> dict[str, Any]:
    """Reapply saved analysis to FEN rows regenerated in the current database."""
    job_id = str(ctx.get("job_id") or "fen-analysis-restore")
    backup_path = _backup_path(filename)
    if not backup_path.is_file():
        raise FileNotFoundError(f"FEN-analysis backup not found: {filename}")

    metadata_path = _metadata_path(backup_path)
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        expected_checksum = str(metadata.get("sha256") or "")
        if expected_checksum and _sha256(backup_path) != expected_checksum:
            raise ValueError("FEN-analysis backup checksum does not match")

    header, records_iterator = _backup_records(backup_path)
    total = int(header.get("records") or 0)
    processed = restored = missing = 0
    await _write_progress(
        ctx,
        job_id,
        kind="fen_analysis_restore",
        total=total,
        processed=0,
        phase="restoring",
        detail=f"Restoring {filename}.",
    )

    batch = []
    for record in records_iterator:
        batch.append(record)
        if len(batch) < RESTORE_BATCH_SIZE:
            continue
        batch_restored, batch_missing = await _restore_batch(batch)
        processed += len(batch)
        restored += batch_restored
        missing += batch_missing
        batch = []
        await _write_progress(
            ctx,
            job_id,
            kind="fen_analysis_restore",
            total=total,
            processed=processed,
            phase="restoring",
            detail=f"Restored {restored}; {missing} FENs not regenerated yet.",
        )

    if batch:
        batch_restored, batch_missing = await _restore_batch(batch)
        processed += len(batch)
        restored += batch_restored
        missing += batch_missing

    await refresh_database_summary_fen_counts()
    await refresh_game_analysis_summary()
    await refresh_scored_position_summary()
    await refresh_scored_rating_summary()

    result = {
        "filename": filename,
        "records": processed,
        "restored": restored,
        "missing": missing,
    }
    await _write_progress(
        ctx,
        job_id,
        kind="fen_analysis_restore",
        total=total,
        processed=processed,
        phase="complete",
        detail=f"Restored {restored} analyzed positions from {filename}.",
        result=result,
    )
    return result


async def run_fen_analysis_backup_job(ctx: dict, **kwargs) -> dict[str, Any]:
    try:
        return await create_fen_analysis_backup(ctx)
    except Exception as error:
        await _write_progress(
            ctx,
            str(ctx.get("job_id") or "fen-analysis-backup"),
            kind="fen_analysis_backup",
            total=0,
            processed=0,
            failed=1,
            phase="failed",
            detail=str(error),
        )
        raise


async def run_fen_analysis_restore_job(
    ctx: dict,
    filename: str,
    **kwargs,
) -> dict[str, Any]:
    try:
        return await restore_fen_analysis_backup(ctx, filename)
    except Exception as error:
        await _write_progress(
            ctx,
            str(ctx.get("job_id") or "fen-analysis-restore"),
            kind="fen_analysis_restore",
            total=0,
            processed=0,
            failed=1,
            phase="failed",
            detail=str(error),
        )
        raise
