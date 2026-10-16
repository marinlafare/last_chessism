"""Run from stockfish-batch-worker/: python3 -m cleaning_job --help."""

import argparse
import json
from pathlib import Path
import sys

from .cloud import Cloud, CleanupError, REGION
from .cleanup import OUT, cleaning_job, verify
from .shared import cleanup_shared


def main(argv=None):
    parser = argparse.ArgumentParser(description="Chessism cloud cleanup. Preview by default; --execute never prompts.")
    parser.add_argument("--gcloud", default="gcloud", help="Path to authenticated Google Cloud CLI")
    commands = parser.add_subparsers(dest="command", required=True)
    job = commands.add_parser("job", help="Clean terminal Batch metadata, temporary input/results and unused job images")
    source = job.add_mutually_exclusive_group(required=True)
    source.add_argument("--job", action="append", help="Repeat for finished attempts sharing data")
    source.add_argument("--plan", type=Path, help="Resume using the exact saved inventory")
    job.add_argument("--region", default=REGION)
    job.add_argument("--include-failed", action="store_true", help="Explicitly abandon failed jobs' checkpoints; no retries remain")
    job.add_argument("--execute", action="store_true", help="Delete unattended; caller must already have saved results")
    job.add_argument("--plan-directory", type=Path, default=OUT)
    job.add_argument("--wait-seconds", type=int, default=60, choices=range(301), metavar="0..300",
                     help="Bounded Batch resource polling; 0 returns immediately if resources remain")
    check = commands.add_parser("verify", help="Read-only verification against a saved cleanup plan")
    check.add_argument("--plan", type=Path, required=True)
    shared = commands.add_parser("shared", help="Optional whole-project end-of-testing cleanup, not per job")
    shared.add_argument("--image", action="append", default=[], help="Exact chessism-workers image@sha256 digest")
    shared.add_argument("--log", action="append", default=[], help="An allowlisted Batch/VM log ID")
    shared.add_argument("--execute", action="store_true")
    shared.add_argument("--dedicated-test-project", action="store_true",
                        help="Selected images/logs are disposable and no external image consumers need them")
    args = parser.parse_args(argv)
    try:
        cloud = Cloud(args.gcloud)
        if args.command == "job":
            plan = json.loads(args.plan.read_text()) if args.plan else None
            result = cleaning_job(args.job, plan=plan, execute=args.execute, region=args.region,
                                  include_failed=args.include_failed, plan_directory=args.plan_directory,
                                  wait_seconds=args.wait_seconds, cloud=cloud)
        elif args.command == "verify":
            result = verify(cloud, json.loads(args.plan.read_text()))
        else:
            result = cleanup_shared(cloud, images=args.image, logs=args.log, execute=args.execute,
                                    dedicated_test_project=args.dedicated_test_project)
        print(json.dumps(result, indent=2))
        return 2 if result.get("complete") is False or result.get("pending_images") else 0
    except (CleanupError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Cleanup stopped: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
