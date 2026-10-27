"""CPU jobs with bounded memory, cooperative cancellation, and guaranteed cleanup."""

import asyncio
import gzip
import json
import time

import numpy as np

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import AlgorithmRun
from ..matrix_constructor.artifact_files import file_operation
from ..matrix_constructor.storage import matrix_catalog_lock, MatrixCatalogBusy
from .config import BATCH_ROWS, SCATTER_LIMIT, VERSION, normalize_algorithm
from .inputs import materialize
from .numerics import Relationships, prepare_block
from .repository import Reporter, RunCancelled, now, terminal
from .storage import workspace


async def calculate(config, folder, source, reporter):
    if config["missing"] == "column_mean" and (source["non_missing"] == 0).any():
        raise ValueError("Cannot impute a column with no available values. Remove that column or change the matrix scope.")
    columns = config["columns"] + (["rating_difference"] if config["rating_difference"] else [])
    accumulator = Relationships(columns, seed=config["seed"], sample_limit=SCATTER_LIMIT)
    affected = 0
    values = np.load(folder / "values.npy", mmap_mode="r", allow_pickle=False)
    try:
        with gzip.open(folder / "row_keys.jsonl.gz", "rt", encoding="utf-8") as stream:
            for start in range(0, source["rows"], BATCH_ROWS):
                await reporter.check()
                end = min(source["rows"], start + BATCH_ROWS)
                keys = [json.loads(next(stream)) for _ in range(end - start)]
                block, keys, count = prepare_block(np.asarray(values[start:end]), keys, config, source["means"])
                affected += count
                await file_operation(accumulator.add, block, keys)
                await reporter.progress("calculating", end, source["rows"], "Calculating float64 summaries and Pearson correlations.")
        result = accumulator.finish(scaling=config["scaling"], x=config["x"], y=config["y"])
    finally:
        values._mmap.close()
    result.update({
        "source": source["source"], "input_rows": source["rows"], "included_rows": accumulator.count,
        "excluded_rows": affected if config["missing"] == "drop_rows" else 0,
        "imputed_rows": affected if config["missing"] == "column_mean" else 0,
        "missing_by_column": {key: source["rows"] - int(source["non_missing"][i]) for i, key in enumerate(config["columns"])},
        "backend": "numpy_cpu", "precision": "float64", "implementation_version": VERSION,
        "scaling": config["scaling"], "missing": config["missing"], "seed": config["seed"],
        "warnings": ["Associations do not establish causation. Summaries use prepared, unscaled values.",
                     "Constant columns have undefined correlations (N/A).",
                     "Inputs are temporary; rerunning against a changed database can change results."],
    })
    if len(config["matrix"]["filters"]["modes"]) != 1:
        result["warnings"].append("Multiple or unrestricted game types: their rating scales may not be comparable. Prefer a single-type definition for rating research.")
    return result


async def publish(run_id, result, reporter):
    await reporter.progress("saving_results", detail="Saving compact results; publication waits while a database backup is active.")
    while True:
        await reporter.check()
        try:
            async with matrix_catalog_lock(wait=False) as session:
                row = await session.get(AlgorithmRun, run_id, with_for_update=True)
                if row is None or row.cancel_requested or row.status != "running":
                    raise RunCancelled("Cancelled before result publication.")
                row.result, row.status, row.finished_at = result, "complete", now()
                row.progress = {"phase": "complete", "processed": result["included_rows"], "total": result["included_rows"],
                                "detail": "Results saved. Temporary inputs removed."}
            return
        except MatrixCatalogBusy:
            await asyncio.sleep(0.5)


async def run_algorithm_job(ctx, *, run_id):
    async with AsyncDBSession() as session, session.begin():
        row = await session.get(AlgorithmRun, run_id, with_for_update=True)
        if row is None or row.status != "queued":
            return {"run_id": run_id, "status": "not_queued"}
        config = dict(row.config)
        row.status, row.started_at = "running", now()
    reporter = Reporter(run_id)
    started = time.monotonic()
    try:
        from ..canonical_blunders.benchmark_job import OPERATION as CANONICAL_TIMING, execute as time_canonical
        if config.get('operation') == CANONICAL_TIMING:
            result = await time_canonical(config, reporter)
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'comparison_status': result['status']}
        from .blunder_full_job import OPERATION as FULL_DEVELOPMENT, execute as train_full_development
        if config.get('operation') == FULL_DEVELOPMENT:
            result = await train_full_development(config, reporter)
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'model_status': result['status']}
        from .blunder_strength_job import OPERATION as STRENGTH_COMPARISON, execute as compare_strengths
        if config.get('operation') == STRENGTH_COMPARISON:
            result = await compare_strengths(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'comparison_status': result['status']}
        from .blunder_cohort_job import OPERATION as COHORT_BLUNDERS, execute as summarize_blunder_cohort
        if config.get('operation') == COHORT_BLUNDERS:
            result = await summarize_blunder_cohort(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'cohort_status': result['status']}
        from .blunder_threshold_compare import OPERATION as THRESHOLD_COMPARISON, execute as compare_thresholds
        if config.get('operation') == THRESHOLD_COMPARISON:
            result = await compare_thresholds(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'comparison_status': result['status']}
        from .blunder_shadow_job import OPERATION as SHADOW_BLUNDERS, execute as shadow_blunders
        if config.get('operation') == SHADOW_BLUNDERS:
            result = await shadow_blunders(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'shadow_status': result['status']}
        from .blunder_final_evaluation import OPERATION as FINAL_BLUNDERS, execute as evaluate_final_blunders
        if config.get('operation') == FINAL_BLUNDERS:
            result = await evaluate_final_blunders(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'model_status': result['status']}
        from .blunder_v2_job import OPERATION as SMOOTH_BLUNDERS, execute as train_smooth_blunders
        if config.get('operation') == SMOOTH_BLUNDERS:
            result = await train_smooth_blunders(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'model_status': result['status']}
        from .blunder_validation_job import OPERATION as FIXED_BLUNDERS, execute as validate_blunders
        if config.get('operation') == FIXED_BLUNDERS:
            result = await validate_blunders(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'validation': result['status']}
        from .blunder_training import OPERATION as BLUNDER_TRAINING, execute as train_blunders
        if config.get('operation') == BLUNDER_TRAINING:
            result = await train_blunders(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'model_status': result['status']}
        from .score_validation_job import OPERATION as CUDA_VALIDATION, execute as validate_cuda
        if config.get('operation') == CUDA_VALIDATION:
            result = await validate_cuda(config, reporter)
            result['timings']['total_compute_seconds'] = time.monotonic() - started
            await publish(run_id, result, reporter)
            return {'run_id': run_id, 'status': 'complete', 'validation': result['status']}
        expected = 2 if config.get('operation') == 'pipeline' else VERSION
        if config.get("implementation_version") != expected:
            raise ValueError("This algorithm implementation version is no longer supported.")
        stored = config
        config = normalize_algorithm(config, config["matrix"])
        config['sample_run'] = bool(stored.get('sample_run'))
        await reporter.check()
        with workspace(run_id) as folder:
            if config['operation'] == 'pipeline':
                from .builder_runner import execute
                result = await execute(config, reporter)
            else:
                source = await materialize(config, folder, reporter)
                calculation_started = time.monotonic()
                result = await calculate(config, folder, source, reporter)
                result["timings"] = {"extraction_seconds": source["seconds"], "calculation_seconds": time.monotonic() - calculation_started}
            await reporter.progress("cleaning", detail="Removing temporary numerical arrays and row identifiers.")
        result["timings"]["total_compute_seconds"] = time.monotonic() - started
        await publish(run_id, result, reporter)
        return {"run_id": run_id, "status": "complete"}
    except RunCancelled as error:
        await terminal(run_id, "cancelled", str(error))
        return {"run_id": run_id, "status": "cancelled"}
    except asyncio.CancelledError:
        await asyncio.shield(terminal(run_id, "failed", "Worker stopped or the configured run timeout was reached. Temporary inputs were removed."))
        raise
    except Exception as error:
        await terminal(run_id, "failed", str(error)[:2000])
        raise
