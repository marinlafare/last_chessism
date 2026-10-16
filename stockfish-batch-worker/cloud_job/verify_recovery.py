"""Opt-in deployment checks; never creates a Batch job or VM.

Runs small billable Workflows executions. The control check creates three tiny
test objects and removes their exact generations after all executions stop.
"""
import argparse
import json
import time
from uuid import uuid4

from cloud_job.launch import Client
from cloud_job.recovery import WORKFLOW, control_prefix, execution_identity, validate_state
from stockfish_batch.checkpoints import digest, encode


def execute(client, argument, labels=None):
    created = client.request("executions", WORKFLOW + "/executions", method="POST", body={
        "argument": json.dumps(argument), "callLogLevel": "LOG_NONE",
        "labels": {"verification": "no-vm", **(labels or {})},
    })
    name = execution_identity(created["name"])
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        value = client.request("executions", name)
        if value["state"] == "SUCCEEDED":
            return json.loads(value["result"]), name
        if value["state"] not in {"ACTIVE", "QUEUED"}:
            raise RuntimeError(f"Verification stopped: {name}: {value.get('error', value['state'])}")
        time.sleep(2)
    raise TimeoutError(f"Verification still active; inspect {name}. No automatic cleanup attempted.")


def policy_checks(client):
    for codes, expected in (([50001] * 12 + [0], ("SUCCEEDED", 0, 12)),
                            ([124, 50001, 124], ("FAILED", 2, 1)),
                            ([143, 143], ("FAILED", 2, 0)),
                            ([50002, 50002], ("FAILED", 2, 0)),
                            ([None, None], ("FAILED", 2, 0)),
                            ([124, 50001, 0], ("SUCCEEDED", 1, 1))):
        result, _ = execute(client, {"mode": "policy_test", "codes": codes})
        actual = (result["status"], result["application_failures"], result["preemptions"])
        if actual != expected:
            raise AssertionError((codes, result, expected))
        print("Policy verified:", actual, flush=True)


def control_check(client):
    run_id = uuid4().hex
    prefix = control_prefix(run_id, 1)
    # Deliberately not a runnable Batch specification: no accidental CPU launch
    # even if the workflow's explicit control_test safety branch regresses.
    spec = {"taskGroups": [{"taskCount": "1", "taskSpec": {"maxRetryCount": 0}}],
            "allocationPolicy": {"serviceAccount": {
                "email": "chessism-batch-worker@chessism-production.iam.gserviceaccount.com"}}}
    argument = {"mode": "control_test", "run_id": run_id, "session": 1, "spec": spec,
                "spec_hash": digest(encode(spec)), "prior_jobs": [], "prior_uids": {}}
    client.cancel_recovery(run_id, 1)
    print("Control check prefix:", prefix, flush=True)
    result, name = execute(client, argument, {"run_id": run_id, "session": "1"})
    validate_state(result, run_id, 1, argument["spec_hash"])
    if result["status"] != "CANCELLED" or result["jobs"]:
        raise AssertionError(result)
    duplicate, _ = execute(client, argument, {"run_id": run_id, "session": "1"})
    if duplicate != {"status": "DUPLICATE", "owner": name}:
        raise AssertionError(duplicate)
    if len(client.recovery_executions(run_id, 1)) != 2:
        raise AssertionError("Execution discovery did not find both fenced attempts")
    state = client.recovery_document(run_id, 1, "state.json")
    if state != result:
        raise AssertionError("Durable state differs from the workflow result")
    object_prefix = f"inputs/ui-{run_id}/recovery-1/"
    objects = client.objects(object_prefix)
    if {obj["name"] for obj in objects} != {
            object_prefix + name for name in ("owner.json", "state.json", "cancel.json")}:
        raise AssertionError("Unexpected test objects; refusing deletion")
    for obj in objects:
        client.delete_object(obj)
    if client.objects(object_prefix):
        raise AssertionError("Test objects still present")
    print("Control writes, replacement, reads, duplicate fence and cancellation verified; test objects removed.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Allow small Workflows executions, never Batch/VMs")
    parser.add_argument("--check", choices=("policy", "control", "access", "all"), default="all")
    parser.add_argument("--gcloud", default="gcloud")
    args = parser.parse_args()
    if not args.execute:
        parser.error("Pass --execute to run deployment checks with small workflow/storage charges")
    client = Client(gcloud=args.gcloud)
    client.require_recovery()
    if args.check in {"policy", "all"}:
        policy_checks(client)
    if args.check in {"control", "all"}:
        control_check(client)
    if args.check in {"access", "all"}:
        result, _ = execute(client, {"mode": "access_test"})
        print("Cancellation permission probe:", json.dumps(result), flush=True)
        if result.get("code") != 404:
            raise RuntimeError("Cancellation not yet verified; do not launch analysis")


if __name__ == "__main__":
    main()
