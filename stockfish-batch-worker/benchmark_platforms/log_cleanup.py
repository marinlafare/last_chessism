"""Optional, scoped cleanup of benchmark-only log streams after resource cleanup.

Cloud Logging cannot remove just one job's entries. A whole stream is eligible
only when a full-history query proves it has NO entry outside this benchmark.
Audit/workflow streams are never candidates. Query errors or incomplete paging
stop deletion. Runtime receipts are retained next to the downloaded results.
"""
import argparse
import json
from pathlib import Path
from urllib.parse import quote, unquote

from cleaning_job.cloud import PROJECT
from cleaning_job.cleanup import digest, validate_plan
from cleaning_job.shared import TEST_LOGS, idle_project
from cloud_job.launch import Client
from .run import PARENT, execution_state, require, save

ELIGIBLE = TEST_LOGS | {"diagnostic-log", "ping", "run.googleapis.com/stdout",
                       "run.googleapis.com/stderr", "run.googleapis.com/varlog/system"}


def exists(cloud, query):
    body = {"resourceNames": [f"projects/{PROJECT}/locations/global/buckets/_Default/views/_AllLogs"],
            "filter": query, "pageSize": 1, "orderBy": "timestamp desc"}
    seen = set()
    for _ in range(30):
        response = cloud.request("logging", "entries:list", method="POST", body=body)
        if response.get("entries"):
            return True
        token = response.get("nextPageToken")
        if not token:
            return False
        require(token not in seen, "Repeated log page token; refusing deletion")
        seen.add(token)
        body["pageToken"] = token
    raise ValueError("Incomplete log inventory; refusing deletion")


def owned_filter(directory):
    # The user's watch command keeps replacing last-status.json after resource
    # deletion. Use durable pre-deletion receipts, never that live snapshot.
    state = json.loads((directory / "cloud-run-cleanup-plan.json").read_bytes())
    receipt = json.loads((directory / "cleanup-result.json").read_bytes())
    batch = json.loads(Path(receipt["plan"]).read_bytes())
    validate_plan(batch)
    require(execution_state(state["execution"]) == "SUCCEEDED"
            and all(j["state"] == "SUCCEEDED" for j in batch["jobs"]), "Benchmark must have succeeded")
    job = state["job"]["name"].rsplit("/", 1)[1]
    execution = state["execution"]["name"].rsplit("/", 1)[1]
    instances = json.loads((directory / "owned-vm-identities.json").read_bytes())
    require(instances, "No audit-verified VM identities")
    owner = []
    for target in batch["jobs"]:
        owner.append('(resource.type="batch.googleapis.com/Job" AND resource.labels.resource_container='
                     + json.dumps(PROJECT) + ' AND resource.labels.job_id=' + json.dumps(target["uid"]) + ')')
    for value in instances:
        zone, ident = value.split("/")
        require(ident.isdigit() and zone.startswith("us-central1-"), "Unexpected VM identity")
        owner.append('(resource.type="gce_instance" AND resource.labels.project_id=' + json.dumps(PROJECT)
                     + ' AND resource.labels.zone=' + json.dumps(zone)
                     + ' AND resource.labels.instance_id=' + json.dumps(ident) + ')')
    owner.append('(resource.type="cloud_run_job" AND resource.labels.project_id=' + json.dumps(PROJECT)
                 + ' AND resource.labels.location="us-central1" AND resource.labels.job_name=' + json.dumps(job)
                 + ' AND labels."run.googleapis.com/execution_name"=' + json.dumps(execution) + ')')
    return '(' + ' OR '.join(owner) + ')'


def idle(cloud):
    idle_project(cloud)
    require(not any(page.get("jobs") for page in cloud.pages("run", PARENT + "/jobs")), "Cloud Run jobs remain")
    require(not any(page.get("services") for page in cloud.pages("run", PARENT + "/services")), "Cloud Run services remain")


def inspect(cloud, log, owner):
    require(log in ELIGIBLE, "Ineligible log stream")
    query = f'logName="projects/{PROJECT}/logs/{quote(log, safe="")}"'
    # No time filter: old or unrelated entries protect the WHOLE stream.
    if exists(cloud, query + " AND NOT " + owner):
        return "retained_unrelated"
    return "eligible" if exists(cloud, query) else "empty"


def main():
    from concurrent.futures import ThreadPoolExecutor
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--gcloud", default="gcloud")
    args = parser.parse_args()
    cloud = Client(args.gcloud)
    directory = args.directory.resolve()
    receipt = json.loads((directory / "cleanup-result.json").read_bytes())
    require(receipt["complete"], "Job data cleanup is incomplete")
    owner = owned_filter(directory)
    idle(cloud)
    logs = {unquote(name.split("/logs/", 1)[1]) for name in cloud.logs()} & ELIGIBLE
    def check(log):
        result = inspect(cloud, log, owner)
        print(json.dumps({"log": log, "inspection": result}), flush=True)
        return log, result
    with ThreadPoolExecutor(max_workers=4) as pool:
        inspection = dict(pool.map(check, sorted(logs)))
    plan = {"project": PROJECT, "owner_filter": owner, "inspection": inspection}
    plan["sha256"] = digest(plan)
    save(directory, "benchmark-log-cleanup-plan.json", plan)
    previous_file = directory / "benchmark-log-cleanup-result.json"
    previous = json.loads(previous_file.read_bytes()) if args.execute and previous_file.exists() else {}
    result = {"deleted": previous.get("deleted", []),
              "retained": [k for k, v in inspection.items() if v == "retained_unrelated"],
              "execute": args.execute}
    if args.execute:
        for log, outcome in inspection.items():
            if outcome != "eligible":
                continue
            idle(cloud)
            require(inspect(cloud, log, owner) != "retained_unrelated", "Log acquired unrelated entries")
            cloud.delete_log(log)
            result["deleted"].append(log)
            save(directory, "benchmark-log-cleanup-result.json", result)
    save(directory, "benchmark-log-cleanup-result.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
