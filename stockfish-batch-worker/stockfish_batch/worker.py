"""Finite analysis, durable progress, and bounded parallelism. No database access."""
import asyncio
import hashlib
import json
import os
import time

from .checkpoints import BatchCheckpoints, Checkpoints, encode, make_contract, parse_input, semantic_digest
from .engine import Engine
from .metrics import Sampler
from .performance import validate_performance
from .storage import MAX_INPUT_BYTES, Storage, child
from .watchdog import progress_timeout


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, allow_nan=False), flush=True)


def binary_digest(path):
    with open(path, "rb") as binary:
        return hashlib.file_digest(binary, "sha256").hexdigest()


async def run(config, *, storage=None, engine_factory=Engine, engine_sha=None, progress=emit, measure=False):
    sampler = Sampler() if measure or config.compact_results else None
    stop = asyncio.Event()
    sampling = asyncio.create_task(sampler.sample(stop)) if sampler else None
    try:
        return await _run(config, storage=storage, engine_factory=engine_factory,
                          engine_sha=engine_sha, progress=progress, sampler=sampler)
    finally:
        stop.set()
        if sampling:
            await sampling


async def _run(config, *, storage, engine_factory, engine_sha, progress, sampler):
    config.validate()
    storage = storage or Storage()
    started = time.monotonic()
    async with asyncio.timeout(config.run_timeout or None), progress_timeout(config.stall_timeout) as advanced:
        raw = await asyncio.to_thread(storage.read, config.input, MAX_INPUT_BYTES)
        advanced()
        if raw is None:
            raise ValueError("Input object does not exist")
        rows = parse_input(raw, config.max_positions)  # Validate the entire input before any writes/search.
        engine_sha = engine_sha or await asyncio.to_thread(binary_digest, config.engine)
        contract = make_contract(raw, rows, config, engine_sha)
        del raw  # Large immutable input bytes are no longer needed during search.
        checkpoints = (BatchCheckpoints(storage, config.output, contract, rows, config.batch_size)
                       if config.batch_size > 1 else Checkpoints(storage, config.output, contract))
        await asyncio.to_thread(checkpoints.open)
        advanced()
        results = {}
        queue = asyncio.Queue()
        # Preserve the baseline's per-record reads; batched modes inspect only their batch objects.
        if config.batch_size > 1:
            for index in range(len(checkpoints.groups)):
                loaded = await asyncio.to_thread(checkpoints.load_batch, index)
                if config.compact_results:
                    results.update({row["id"]: checkpoints.reference(row, loaded[row["id"]])
                                    for row in checkpoints.groups[index] if row["id"] in loaded})
                else:
                    results.update(loaded)
                del loaded
                advanced()
        else:
            for row in rows:
                existing = await asyncio.to_thread(checkpoints.load, row)
                advanced()
                if existing is not None:
                    results[row["id"]] = existing
        for row in rows:
            if row["id"] not in results:
                queue.put_nowait(row)
        resumed = len(results)
        engines = min(config.workers, queue.qsize())
        progress("started", total=len(rows), saved=resumed, resumed=resumed, engines=engines,
                 threads_per_engine=config.threads, hash_mb_per_engine=config.hash_mb,
                 fingerprint=contract["fingerprint"], visible_cpus=os.cpu_count(),
                 upload_mode=config.upload_mode, batch_size=config.batch_size)
        startup_seconds = time.monotonic() - started
        upload_queue = asyncio.Queue(maxsize=config.upload_queue_size)
        # Whole-batch handoffs separate CPU validation/encoding from network I/O.
        # These limits do not scale with the number of selected FENs.
        raw_batches = asyncio.Queue(maxsize=2)
        prepared_batches = asyncio.Queue(maxsize=2)
        pipeline = {"raw_batch_queue_peak": 0, "prepared_batch_queue_peak": 0,
                    "pending_result_bytes_peak": 0, "batch_prepare_seconds": 0.0,
                    "batch_commit_seconds": 0.0, "batch_saved_events": 0}
        analysis_seconds = wait_seconds = engine_start_seconds = 0.0
        max_queue = 0
        workers = []

        def record_saved(row, value, *, notify=True):
            advanced()
            results[row["id"]] = checkpoints.reference(row, value) if config.compact_results else value
            if notify:
                progress("saved", id=row["id"], saved=len(results), total=len(rows),
                         elapsed_ms=value["elapsed_ms"], wall_seconds=round(time.monotonic() - started, 3))

        async def upload():
            pending = {}
            sizes = {}
            pending_bytes = 0
            while True:
                item = await upload_queue.get()
                if item is None:
                    if pending:
                        raise ValueError("Incomplete batch at end of analysis")
                    if config.batch_size > 1:
                        await raw_batches.put(None)
                    return
                row, result, elapsed = item
                if config.batch_size == 1:
                    value = await asyncio.to_thread(checkpoints.save, row, result, elapsed)
                    record_saved(row, value)
                else:
                    # Cheap serialized-size accounting; the expensive legal-PV
                    # validation happens once per complete group in prepare().
                    size = len(encode(result))
                    if size > 256 * 1024:
                        raise ValueError("Checkpoint exceeds the 256 KiB record limit")
                    pending_bytes += size
                    pipeline["pending_result_bytes_peak"] = max(pipeline["pending_result_bytes_peak"], pending_bytes)
                    if pending_bytes > 64 * 1024 * 1024:
                        raise ValueError("Pending batches exceed 64 MiB; use a smaller batch size")
                    index = checkpoints.group_index[row["id"]]
                    group = pending.setdefault(index, {})
                    group[row["id"]] = (result, elapsed)
                    sizes[index] = sizes.get(index, 0) + size
                    if sizes[index] > checkpoints.MAX_BYTES:
                        raise ValueError("Batch exceeds 16 MiB; use a smaller batch size")
                    if len(group) == len(checkpoints.groups[index]):
                        await raw_batches.put((index, group))
                        pipeline["raw_batch_queue_peak"] = max(pipeline["raw_batch_queue_peak"], raw_batches.qsize())
                        pending_bytes -= sizes.pop(index)
                        del pending[index]

        async def prepare():
            while True:
                item = await raw_batches.get()
                if item is None:
                    await prepared_batches.put(None)
                    return
                before = time.monotonic()
                batch = await asyncio.to_thread(checkpoints.prepare_batch, *item)
                pipeline["batch_prepare_seconds"] += time.monotonic() - before
                del item
                await prepared_batches.put(batch)
                pipeline["prepared_batch_queue_peak"] = max(pipeline["prepared_batch_queue_peak"], prepared_batches.qsize())
                del batch

        async def commit():
            while True:
                batch = await prepared_batches.get()
                if batch is None:
                    return
                before = time.monotonic()
                saved = await asyncio.to_thread(checkpoints.commit_prepared_batch, batch)
                pipeline["batch_commit_seconds"] += time.monotonic() - before
                for row in checkpoints.groups[batch.index]:
                    record_saved(row, saved[row["id"]], notify=False)
                pipeline["batch_saved_events"] += 1
                progress("saved", batch=batch.index, batch_positions=len(saved), saved=len(results), total=len(rows),
                         wall_seconds=round(time.monotonic() - started, 3))
                del saved, batch

        async def consume(worker_id):
            nonlocal analysis_seconds, wait_seconds, engine_start_seconds, max_queue
            stats = {"worker": worker_id, "positions": 0, "analysis_seconds": 0.0,
                     "upload_wait_seconds": 0.0, "first_search_seconds": None, "last_search_seconds": None}
            workers.append(stats)
            initializing = time.monotonic()
            async with engine_factory(config) as engine:
                engine.progress_callback = advanced
                advanced()
                engine_start_seconds += time.monotonic() - initializing
                while True:
                    try:
                        row = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    search_started = time.monotonic()
                    if stats["first_search_seconds"] is None:
                        stats["first_search_seconds"] = search_started - started
                    result = await engine.analyse(row)
                    fen_completed()
                    advanced()
                    elapsed = (time.monotonic() - search_started) * 1000
                    analysis_seconds += elapsed / 1000
                    stats["positions"] += 1
                    stats["analysis_seconds"] += elapsed / 1000
                    stats["last_search_seconds"] = time.monotonic() - started
                    waiting = time.monotonic()
                    if config.upload_mode == "blocking":
                        value = await asyncio.to_thread(checkpoints.save, row, result, elapsed)
                        wait_seconds += time.monotonic() - waiting
                        record_saved(row, value)
                    else:
                        await upload_queue.put((row, result, elapsed))
                        wait_seconds += time.monotonic() - waiting
                        max_queue = max(max_queue, upload_queue.qsize())
                    stats["upload_wait_seconds"] += time.monotonic() - waiting

        # TaskGroup cancels siblings and closes their engines if any analysis/upload fails.
        async with asyncio.TaskGroup() as group:
            uploader = group.create_task(upload()) if config.upload_mode == "background" else None
            preparer = group.create_task(prepare()) if config.batch_size > 1 else None
            committer = group.create_task(commit()) if config.batch_size > 1 else None
            # This deadline measures completed searches, independently of UCI
            # node updates, checkpoint reads and background upload activity.
            # Start it AFTER input/recovery, and end it BEFORE final upload drain.
            async with progress_timeout(config.stall_timeout if engines else 0,
                                        completed_fens_only=True) as fen_completed:
                producers = [group.create_task(consume(index)) for index in range(engines)]
                await asyncio.gather(*producers)
            if uploader:
                await upload_queue.put(None)
                await uploader
                if preparer:
                    await preparer
                    await committer
        finish = checkpoints.finish_references if config.compact_results else checkpoints.finish
        manifest = await asyncio.to_thread(finish, rows, results)
        elapsed = time.monotonic() - started
        metrics = {"worker_seconds": elapsed, "startup_seconds": startup_seconds,
                   "semantic_sha256": None if config.compact_results else semantic_digest(rows, results),
                   "fen_per_second": (len(rows) - resumed) / elapsed,
                   "engine_analysis_seconds_sum": analysis_seconds,
                   "engine_initialization_seconds_sum": engine_start_seconds,
                   "engine_save_wait_seconds_sum": wait_seconds, "upload_queue_peak": max_queue,
                   "result_pipeline": pipeline,
                   "storage": storage.stats.snapshot()}
        if sampler:
            metrics.update(sampler.finish())
        if config.compact_results:
            report = {"schema_version": 1, "fingerprint": contract["fingerprint"],
                      "position_count": len(rows), "resumed": resumed,
                      "analyzed_this_attempt": len(rows) - resumed,
                      "workers": workers, "metrics": metrics,
                      "note": "Successful reporting attempt only; VM CPU is averaged over startup, analysis and uploads."}
            uri = child(config.output, "performance.json")
            if not await asyncio.to_thread(storage.create, uri, encode(report)):
                report = json.loads(await asyncio.to_thread(storage.read, uri))
            validate_performance(report, contract, config.workers)
        progress("complete", saved=len(results), total=len(rows), resumed=resumed,
                 analyzed_this_attempt=len(rows) - resumed,
                 wall_seconds=round(time.monotonic() - started, 3),
                 manifest=child(config.output, "manifest.json"), metrics=metrics)
        return manifest
