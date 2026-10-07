"""Read-only cloud status/audit for upload-benchmark-001; never submits or deletes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmark_fen_batches.benchmark import MODES
from stockfish_batch.checkpoints import BatchCheckpoints, Checkpoints, digest, encode, make_contract, parse_input, semantic_digest
from stockfish_batch.config import Config
from stockfish_batch.storage import Storage, child
from benchmark_fen_batches.render_benchmark import render

PROJECT = "chessism-production"
REGION = "us-central1"
JOB = "chessism-upload-benchmark-001"
UID = "chessism-upload-be-7e3efd7d-1392-44c40"
PREFIX = "gs://chessism-batch-276704059200-us-central1/results/upload-benchmark-001"
INPUT = "gs://chessism-batch-276704059200-us-central1/inputs/upload-benchmark-001/input.jsonl"
IMAGE = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:4422aef3fbe93477a09a3c8f6fca2e3f1d5753e0af223a125cbc11c3ae2f431d"
ENGINE_SHA = "01a8cbe27fabf6aad1c6393d2510982805f682c8c61243cad2e84f51e974505a"
OUT = ROOT / "out" / "upload-benchmark-001"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def command(executable, *args, as_json=True):
    args = [executable, *args, f"--project={PROJECT}"]
    if as_json:
        args.append("--format=json")
    result = subprocess.run(args, capture_output=True, text=True, timeout=55, check=True)
    return json.loads(result.stdout or "null") if as_json else result.stdout.strip()


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_bytes(encode(value))


def status(executable):
    job = command(executable, "batch", "jobs", "describe", JOB, f"--location={REGION}")
    require(job["uid"] == UID, "Unexpected job UID")
    expected = render(IMAGE, INPUT, PREFIX)
    group = job["taskGroups"][0]
    require(len(job["taskGroups"]) == 1 and group["taskCount"] == group["parallelism"] == "1", "Unexpected task count")
    spec = group["taskSpec"]
    require(spec.get("maxRetryCount", 0) == 0 and spec["maxRunDuration"] == "3600s", "Unsafe runtime/retry settings")
    require(spec["runnables"] == expected["taskGroups"][0]["taskSpec"]["runnables"], "Unexpected container configuration")
    policy = job["allocationPolicy"]["instances"][0]["policy"]
    require(policy["machineType"] == "n2d-standard-4" and policy["provisioningModel"] == "SPOT", "Unexpected VM policy")
    save("cloud-job.json", job)
    return job


def watch(executable):
    deadline = time.monotonic() + 4500
    while time.monotonic() < deadline:
        job = status(executable)
        logs = command(executable, "logging", "read",
                       f'labels.job_uid="{UID}" AND logName="projects/{PROJECT}/logs/batch_task_logs"',
                       "--freshness=2h", "--limit=2")
        events = []
        for item in logs or []:
            payload = item.get("jsonPayload")
            if not payload:
                try:
                    payload = json.loads(item.get("textPayload", ""))
                except ValueError:
                    continue
            events.append({k: payload[k] for k in ("event", "variant", "saved", "total", "worker_seconds", "error") if k in payload})
        print(json.dumps({"state": job["status"]["state"], "runDuration": job["status"].get("runDuration"), "recent": events}), flush=True)
        if job["status"]["state"] in {"SUCCEEDED", "FAILED", "DELETION_IN_PROGRESS"}:
            return
        time.sleep(45)
    raise TimeoutError("Monitoring deadline reached; inspect this exact job before any further action")


def cleanup(executable):
    observations = []
    for _ in range(2):
        resources = {kind: command(executable, "compute", kind, "list", f"--filter=labels.batch-job-uid={UID}")
                     for kind in ("instances", "disks")}
        observations.append(resources)
        require(not any(resources.values()), "Benchmark resources still exist; wait for Batch cleanup")
        if len(observations) == 1:
            time.sleep(10)
    save("cleanup.json", observations)
    return True


def audit(executable):
    job = status(executable)
    require(job["status"]["state"] == "SUCCEEDED", "Job has not succeeded")
    cleaned = cleanup(executable)
    from google.cloud import storage as gcs
    from google.oauth2.credentials import Credentials
    token = command(executable, "auth", "print-access-token", as_json=False)
    storage = Storage()
    storage._client = gcs.Client(project=PROJECT, credentials=Credentials(token=token))
    del token
    raw = storage.read(INPUT)
    require(raw == (OUT / "input.jsonl").read_bytes(), "Cloud/local inputs differ")
    rows = parse_input(raw, 1000)
    require(len(rows) == len({r["fen"] for r in rows}) == 1000, "Input is not 1,000 distinct positions")
    report = json.loads(storage.read(child(PREFIX, "benchmark-report.json")))
    require(report["status"] == "complete" and report["all_chess_results_match"] is True, "Incomplete report")
    require(report["input_sha256"] == digest(raw), "Report input mismatch")
    require(report["order"] == [m[0] for m in MODES] and len(report["trials"]) == 4, "Unexpected variants")
    verified = []
    for (name, mode, size), measured in zip(MODES, report["trials"]):
        prefix = child(PREFIX, name)
        config = Config(INPUT, prefix, max_positions=1000, upload_mode=mode, batch_size=size)
        contract = json.loads(storage.read(child(prefix, "contract.json")))
        expected = make_contract(raw, rows, config, ENGINE_SHA)
        expected.pop("fingerprint")
        expected["worker_version"] = "1.1.0"
        expected["fingerprint"] = digest(encode(expected))
        require(contract == expected, "Contract does not bind the expected input, settings and engine")
        validator = BatchCheckpoints(storage, prefix, contract, rows, size) if size > 1 else Checkpoints(storage, prefix, contract)
        manifest = json.loads(storage.read(child(prefix, "manifest.json")))
        require(manifest["status"] == "complete" and manifest["position_count"] == 1000, "Incomplete manifest")
        require(manifest["fingerprint"] == contract["fingerprint"], "Manifest fingerprint mismatch")
        require([r["id"] for r in manifest["records"]] == [r["id"] for r in rows], "Missing or reordered IDs")
        names = sorted({validator.name(row) for row in rows})
        def fetch(name):
            return name, json.loads(storage.read(child(prefix, name), 16 * 1024 * 1024))
        with ThreadPoolExecutor(max_workers=8) as pool:
            objects = dict(pool.map(fetch, names))
        values = {}
        if size > 1:
            for index in range(len(validator.groups)):
                values.update(validator.validate_batch(objects[f"batches/{index:06d}.json"], index))
        else:
            values = {row["id"]: validator.validate(objects[validator.name(row)], row) for row in rows}
        for row, entry in zip(rows, manifest["records"]):
            require(entry["object"] == validator.name(row), "Unexpected manifest object path")
            require(entry["sha256"] == values[row["id"]]["result_sha256"], "Manifest checksum mismatch")
        require(measured == json.loads(storage.read(child(PREFIX, name + "-metrics.json"))), "Metrics/report mismatch")
        require(measured["variant"] == name and measured["semantic_sha256"] == semantic_digest(rows, values), "Analysis checksum differs from report")
        save(name + "-manifest.json", manifest)
        save(name + "-metrics.json", measured)
        verified.append({"variant": name, "validated_positions": len(values), "result_objects": len(objects),
                         "semantic_sha256": semantic_digest(rows, values)})
        print(json.dumps({"event": "audited", **verified[-1]}), flush=True)
    require(len({v["semantic_sha256"] for v in verified}) == 1, "Chess results differ between modes")
    save("report.json", report)
    result = {"status": "PASS", "job": JOB, "job_uid": UID, "image": IMAGE, "input_sha256": digest(raw),
              "variants": verified, "vm_and_disk_cleanup_verified": cleaned, "production_database_writes": 0}
    save("audit.json", result)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gcloud", default="gcloud")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--watch", action="store_true")
    group.add_argument("--audit", action="store_true")
    args = parser.parse_args()
    (watch if args.watch else audit)(args.gcloud)


if __name__ == "__main__":
    main()
