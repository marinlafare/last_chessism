"""Background construction of immutable, bounded NumPy matrix snapshots."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import MatrixArtifact

from .arrays import TypedArrayWriter, validate_typed_arrays
from .catalog import ROW_TYPES
from .queries import (
    ARTIFACT_DISPLAY_ROOT, ARTIFACT_ROOT, estimate_matrix,
    matrix_sql, normalize_matrix_config, storage_status,
)


PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
BATCH_ROWS = 2_048
logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def _write_progress(redis, job_id, *, artifact_id, phase, total, processed, detail):
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps({
            "job_id": job_id, "kind": "matrix_constructor", "artifact_id": artifact_id,
            "phase": phase, "total": max(0, int(total)), "processed": max(0, int(processed)),
            "failed": int(phase == "failed"), "detail": detail, "updated_at": time.time(),
        }),
        ex=PROGRESS_TTL_SECONDS,
    )


async def _update_artifact(artifact_id: str, **values: Any) -> None:
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            return
        for key, value in values.items():
            setattr(artifact, key, value)
        await session.commit()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finish_files(temporary, manifest, dictionaries):
    # Dict insertion order already equals the assigned category-code order.
    # Sorting millions of dictionary entries used to allocate another full copy.
    with gzip.open(temporary / "dictionaries.json.gz", "wt", encoding="utf-8", compresslevel=1) as output:
        output.write("{")
        for index, (key, mapping) in enumerate(dictionaries.items()):
            if index:
                output.write(",")
            output.write(json.dumps(key) + ":[")
            for category_index, category in enumerate(mapping):
                if category_index:
                    output.write(",")
                output.write(json.dumps(category, ensure_ascii=False))
            output.write("]")
        output.write("}")
    manifest["validation"] = validate_typed_arrays(
        temporary, rows=manifest["row_count"], layouts=manifest["columns"],
    )
    manifest["files"] = [
        {"name": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for path in sorted(temporary.iterdir()) if path.name != "manifest.json"
    ]
    manifest_path = temporary / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
    usage = storage_status(0)
    if usage["free_bytes"] < usage["reserved_floor_bytes"]:
        raise ValueError("Free space fell below the protected floor during matrix construction.")
    return sum(path.stat().st_size for path in temporary.iterdir())


async def _construct_artifact(*, artifact_id: str, config: dict, redis, job_id: str) -> dict:
    if str(uuid.UUID(artifact_id)) != artifact_id:
        raise ValueError("Invalid matrix artifact identifier.")
    config = normalize_matrix_config(config)
    temporary = ARTIFACT_ROOT / f".{artifact_id}.building"
    target = ARTIFACT_ROOT / artifact_id
    if target.exists():
        raise ValueError("An immutable matrix with this identifier already exists.")
    temporary.mkdir(parents=True, exist_ok=False)
    writer = None
    try:
        async with AsyncDBSession() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            estimate = await estimate_matrix(config, session=session)
            total = int(estimate["selected_rows"])
            if total < 1:
                raise ValueError("The selected scope contains no rows.")
            if not estimate["storage"]["safe_to_build"]:
                raise ValueError("The matrix would cross the protected free-space floor.")
            await _write_progress(
                redis, job_id, artifact_id=artifact_id, phase="allocating",
                total=total, processed=0, detail=f"Allocating {total:,} rows.",
            )
            writer = TypedArrayWriter(temporary, ROW_TYPES[config["row_type"]], config, total)
            _, select_sql, params = matrix_sql(config)
            stream = await session.stream(text(select_sql), params, execution_options={"yield_per": BATCH_ROWS})
            try:
                with gzip.open(temporary / "row_keys.jsonl.gz", "wt", encoding="utf-8", compresslevel=1) as keys:
                    async for block in stream.mappings().partitions(BATCH_ROWS):
                        writer.write(block)
                        keys.writelines(json.dumps(str(row["row_key"]), ensure_ascii=False) + "\n" for row in block)
                        space = storage_status(0)
                        if space["free_bytes"] < space["reserved_floor_bytes"]:
                            raise ValueError("Free space fell below the protected floor during construction.")
                        await _write_progress(
                            redis, job_id, artifact_id=artifact_id, phase="writing",
                            total=total, processed=writer.processed,
                            detail=f"Encoded {writer.processed:,} / {total:,} rows.",
                        )
            finally:
                await stream.close()
            profiles = writer.finish()
            writer.close()
        await _write_progress(
            redis, job_id, artifact_id=artifact_id, phase="finalizing",
            total=total, processed=total, detail="Validating and checksumming artifact files.",
        )
        manifest = {
            "format": "chessism_matrix", "version": 2,
            "constructor_revision": 3,
            "artifact_id": artifact_id, "created_at": _utc_now().isoformat(),
            "config": config, "row_count": total,
            "feature_count": len(config["feature_columns"]), "label_count": len(config["label_columns"]),
            "storage_layout": "dtype_grouped_numpy", "columns": writer.layouts,
            "profiles": profiles, "missing_mask_dtype": "uint8",
            "categorical_encoding": "zero-based dictionary",
            "snapshot_isolation": "repeatable_read", "estimate": estimate,
            "selection_order": ROW_TYPES[config["row_type"]].order_sql,
        }
        # Final I/O and hashing do not keep the database transaction open.
        finalizer = asyncio.create_task(asyncio.to_thread(
            _finish_files, temporary, manifest, writer.dictionaries,
        ))
        try:
            size = await asyncio.shield(finalizer)
        except asyncio.CancelledError:
            # A thread cannot be canceled safely. Let it release its files
            # before removing the unfinished directory; never publish it.
            await finalizer
            raise
        os.rename(temporary, target)
        return {
            "row_count": total, "feature_count": manifest["feature_count"],
            "label_count": manifest["label_count"], "size_bytes": size,
            "artifact_path": str(ARTIFACT_DISPLAY_ROOT / artifact_id / "manifest.json"),
            "manifest": manifest,
        }
    except BaseException:
        if writer is not None:
            writer.close()
        shutil.rmtree(temporary, ignore_errors=True)
        raise


async def run_matrix_construction_job(ctx: dict, *, artifact_id: str) -> dict:
    """ARQ entrypoint; research workers serialize construction jobs."""
    redis = ctx["redis"]
    job_id = str(ctx.get("job_id") or f"matrix-{artifact_id}")
    async with AsyncDBSession() as session:
        artifact = await session.get(MatrixArtifact, artifact_id)
        if artifact is None:
            raise ValueError("Matrix artifact request no longer exists.")
        if artifact.status == "complete":
            return {"artifact_id": artifact_id, "status": "already_complete"}
        config = dict(artifact.config)
    await _update_artifact(artifact_id, status="running", started_at=_utc_now(), error=None)
    try:
        await _write_progress(
            redis, job_id, artifact_id=artifact_id, phase="starting", total=0, processed=0,
            detail="Taking a repeatable database snapshot.",
        )
        result = await _construct_artifact(
            artifact_id=artifact_id, config=config, redis=redis, job_id=job_id,
        )
        await _update_artifact(
            artifact_id, status="complete", finished_at=_utc_now(),
            artifact_path=result["artifact_path"], row_count=result["row_count"],
            feature_count=result["feature_count"], label_count=result["label_count"],
            size_bytes=result["size_bytes"], result=result["manifest"],
        )
        try:
            await _write_progress(
                redis, job_id, artifact_id=artifact_id, phase="complete",
                total=result["row_count"], processed=result["row_count"], detail="Matrix snapshot is complete.",
            )
        except Exception:
            # PostgreSQL and the immutable artifact are authoritative. A lost
            # progress notification must not turn a completed export into failed.
            logger.exception("Unable to publish completed matrix progress: %s", artifact_id)
        return result
    except (Exception, asyncio.CancelledError) as error:
        message = str(error) or "Matrix construction was interrupted."
        await _update_artifact(artifact_id, status="failed", finished_at=_utc_now(), error=message[:4000])
        await _write_progress(
            redis, job_id, artifact_id=artifact_id, phase="failed",
            total=0, processed=0, detail=message[:1000],
        )
        raise
