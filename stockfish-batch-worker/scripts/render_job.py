"""Render the bounded smoke-test config to stdout. Never calls Google or submits a job."""
import argparse
import json
from pathlib import Path
import re

REPOSITORY = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/"
BUCKET = "gs://chessism-batch-276704059200-us-central1/"
TEMPLATE = Path(__file__).resolve().parents[1] / "batch" / "smoke-test.json"


def render(image, input_uri, output_uri):
    if not re.fullmatch(re.escape(REPOSITORY) + r"[a-z0-9_-]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Use a sha256-pinned image in the chessism-workers repository, not a tag")
    for uri, prefix in ((input_uri, "inputs/"), (output_uri, "results/")):
        suffix = uri.removeprefix(BUCKET + prefix)
        if not uri.startswith(BUCKET + prefix) or not re.fullmatch(r"[A-Za-z0-9_./-]+", suffix):
            raise ValueError(f"Expected an object under {BUCKET}{prefix}")
        if any(part in {"", ".", ".."} for part in suffix.rstrip("/").split("/")):
            raise ValueError("Unsafe object path")
    if input_uri.endswith("/"):
        raise ValueError("Input must identify a JSONL object, not a prefix")
    mapping = {"__IMAGE_DIGEST__": image, "__INPUT_URI__": input_uri, "__OUTPUT_URI__": output_uri.rstrip("/")}

    def replace(value):
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item) for item in value]
        return mapping.get(value, value) if isinstance(value, str) else value

    return replace(json.loads(TEMPLATE.read_text()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        job = render(args.image, args.input, args.output)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(job, indent=2))


if __name__ == "__main__":
    main()
