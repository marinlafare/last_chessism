"""Render one bounded, four-variant upload benchmark. Never submits a job."""
import argparse
import json
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.render_job import render as render_smoke

# Published images are immutable: preserve the original entry point when rendering
# for the already-published benchmark image or auditing its completed job.
LEGACY_IMAGE = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:4422aef3fbe93477a09a3c8f6fca2e3f1d5753e0af223a125cbc11c3ae2f431d"


def render(image, input_uri, output_uri):
    job = render_smoke(image, input_uri, output_uri)
    spec = job["taskGroups"][0]["taskSpec"]
    spec["maxRunDuration"] = "3600s"
    container = spec["runnables"][0]["container"]
    container["entrypoint"] = "python"
    module = "stockfish_batch.benchmark" if image == LEGACY_IMAGE else "benchmark_fen_batches.benchmark"
    container["commands"] = ["-m", module, "--input", input_uri,
                             "--output", output_uri.rstrip("/")]
    labels = {"app": "chessism", "purpose": "upload-benchmark"}
    job["labels"] = labels.copy()
    job["allocationPolicy"]["labels"] = labels.copy()
    return job


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--save", type=Path, required=True)
    args = parser.parse_args()
    job = render(args.image, args.input, args.output)
    with args.save.open("x") as handle:
        json.dump(job, handle, indent=2)
        handle.write("\n")
    print(args.save)


if __name__ == "__main__":
    main()
