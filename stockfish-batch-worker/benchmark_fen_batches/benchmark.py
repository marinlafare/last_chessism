"""Exactly four sequential 1,000-FEN trials in one container/VM. No job submission."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import platform
import sys
import time

import chess

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stockfish_batch.checkpoints import encode, digest, parse_input
from stockfish_batch.config import Config
from stockfish_batch.storage import Storage, child, MAX_INPUT_BYTES
from stockfish_batch.worker import run, emit

MODES = (("a-blocking-1", "blocking", 1), ("b-background-1", "background", 1),
         ("c-background-100", "background", 100), ("d-background-500", "background", 500))


async def benchmark(source, destination):
    storage = Storage()
    raw = await asyncio.to_thread(storage.read, source, MAX_INPUT_BYTES)
    rows = parse_input(raw, 1000)
    if len(rows) != 1000 or len({row["fen"] for row in rows}) != 1000:
        raise ValueError("Benchmark requires exactly 1,000 distinct FENs")
    if any(chess.Board(row["fen"]).is_game_over() for row in rows):
        raise ValueError("Terminal positions are excluded from the speed benchmark")
    identity = {"hostname": platform.node(), "cpu_count": os.cpu_count(),
                "cpu_models": sorted({line.split(":", 1)[1].strip() for line in
                                      Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")}),
                "input_sha256": digest(raw), "positions": len(rows), "order": [m[0] for m in MODES]}
    if not await asyncio.to_thread(storage.create, child(destination, "benchmark-start.json"), encode(identity)):
        raise ValueError("Benchmark prefix already used; refusing repeated trials")
    results = []
    start = time.monotonic()
    async with asyncio.timeout(3540):
        for name, mode, size in MODES:
            config = Config(source, child(destination, name), max_positions=1000,
                            upload_mode=mode, batch_size=size).validate()
            completed = []

            def progress(event, **fields):
                if event == "complete":
                    completed.append(fields)
                # Identical log volume in every mode, including the baseline.
                emit(event, variant=name, **fields)

            emit("benchmark_trial_start", variant=name, mode=mode, batch_size=size)
            manifest = await asyncio.wait_for(run(config, progress=progress, measure=True), 900)
            summary = completed[0]
            if summary["resumed"] != 0 or summary["saved"] != 1000 or manifest["position_count"] != 1000:
                raise ValueError("A speed trial reused work or did not finish all positions")
            result = {"variant": name, "upload_mode": mode, "batch_size": size,
                      "manifest": summary["manifest"], **summary["metrics"]}
            results.append(result)
            if not await asyncio.to_thread(storage.create, child(destination, name + "-metrics.json"), encode(result)):
                raise ValueError("Metrics already exist")
            emit("benchmark_trial_complete", **result)
        if len({result["semantic_sha256"] for result in results}) != 1:
            raise ValueError("Chess analysis differs across variants; inspect outputs before accepting timings")
        report = {"status": "complete", **identity, "trials": results,
                  "all_chess_results_match": True, "benchmark_wall_seconds": time.monotonic() - start,
                  "winner": max(results, key=lambda result: result["fen_per_second"])["variant"],
                  "limitations": ["One pass per variant; fixed order, no confidence intervals.",
                                  "Worker timing includes final manifest, excludes provisioning and metric uploads.",
                                  "Storage counters count SDK calls, not hidden HTTP retries or a billing invoice."]}
        if not await asyncio.to_thread(storage.create, child(destination, "benchmark-report.json"), encode(report)):
            raise ValueError("Report already exists")
        emit("benchmark_complete", **report)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    Config(args.input, args.output).validate()
    try:
        asyncio.run(benchmark(args.input, args.output))
    except Exception as exc:
        emit("benchmark_failed", error=repr(exc))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
