"""Opt-in, bounded two-attempt cloud test. No worker/image changes or production DB access."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import time

from recovery_support import (Cloud, OUT, PROJECT, REGION, RUN, JOBS, RESULT_KEY, INPUT_KEY,
    TERMINAL, fixture, job_config, expected_contract, require, emit, validate_vm, verify_preemption,
    verify_results, worker_events, verify_resume_events, verify_vm_recreation)


def save(name, value):
    """Generated diagnostic artifacts, deliberately outside version control."""
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare():
    require(not OUT.exists(), f"{OUT} already exists; inspect it rather than overwriting an earlier test")
    raw, rows = fixture()
    OUT.mkdir(parents=True)
    (OUT / "input.jsonl").write_bytes(raw)
    save("job.json", job_config())
    save("expected-contract.json", expected_contract(raw, rows))
    emit("prepared", directory=str(OUT), records=200, jobs=list(JOBS), max_attempts=2,
         per_task_limit_seconds=900, automatic_retries=0)


def cleanup_wait(cloud, uid, seconds=300):
    deadline, clean_observations = time.monotonic() + seconds, 0
    while time.monotonic() < deadline:
        vms, disks = cloud.resources("instances", uid), cloud.resources("disks", uid)
        emit("cleanup", uid=uid, instances=len(vms), disks=len(disks))
        clean_observations = clean_observations + 1 if not vms and not disks else 0
        if clean_observations >= 2:
            return
        time.sleep(10)
    raise TimeoutError("Temporary resources did not disappear; refusing to launch another attempt")


def interrupt_first(cloud, job):
    deadline = time.monotonic() + 1200
    triggered = False
    uid = job["uid"]
    while time.monotonic() < deadline:
        job = cloud.job(JOBS[0])
        require(job["uid"] == uid, "Job UID changed")
        state = job["status"]["state"]
        snapshot = cloud.inventory()
        emit("attempt_a", state=state, saved=len(snapshot), interruption_requested=triggered)
        if state in TERMINAL:
            save("attempt-a.json", job)
            require(triggered, "Attempt ended before the planned interruption; test is inconclusive")
            require(state == "FAILED", "Interrupted attempt did not fail; test is inconclusive")
            task = cloud.command("batch", "tasks", "describe", "0", f"--job={JOBS[0]}",
                                 "--task_group=group0", f"--location={REGION}")
            save("attempt-a-task.json", task)
            verify_preemption(job, task)
            return job
        if not triggered and state in {"SCHEDULED", "RUNNING"} and 8 <= len(snapshot) < 200:
            raw, rows = fixture()
            require(cloud.read_json(RESULT_KEY + "/contract.json") == expected_contract(raw, rows),
                    "Partial run does not match the expected input/settings/engine")
            vms = cloud.resources("instances", uid)
            require(len(vms) == 1, "Expected exactly one VM for the first attempt")
            name, zone = validate_vm(vms[0], job)
            fresh_vm = cloud.command("compute", "instances", "describe", name, f"--zone={zone}")
            job = cloud.job(JOBS[0])
            validate_vm(fresh_vm, job)
            require(fresh_vm["id"] == vms[0]["id"], "Instance was replaced during target verification")
            require(len(cloud.inventory()) < 200, "Already complete; refusing to interrupt")
            require(cloud.read_json(RESULT_KEY + "/manifest.json") is None, "Already complete")
            # Write evidence BEFORE the one-shot mutation; never auto-repeat an ambiguous request.
            save("interruption-target.json", {"vm": fresh_vm, "job_uid": uid, "saved_before_request": snapshot,
                 "requested_at": datetime.now(timezone.utc).isoformat()})
            triggered = True
            emit("interrupting_verified_vm", name=name, zone=zone, uid=uid, saved=len(snapshot))
            operation = cloud.command("compute", "instances", "simulate-maintenance-event", name,
                                      f"--zone={zone}", "--async")
            save("interruption-operation.json", operation)
        time.sleep(5)
    raise TimeoutError("Attempt A exceeded the controller's 20-minute monitoring limit")


def wait_second(cloud, job):
    deadline = time.monotonic() + 1200
    uid = job["uid"]
    while time.monotonic() < deadline:
        job = cloud.job(JOBS[1])
        require(job["uid"] == uid, "Job UID changed")
        state = job["status"]["state"]
        emit("attempt_b", state=state, saved=len(cloud.inventory()))
        if state in TERMINAL:
            save("attempt-b.json", job)
            require(state == "SUCCEEDED", "Recovery attempt failed; no further jobs will be submitted")
            return job
        time.sleep(10)
    raise TimeoutError("Attempt B exceeded the controller's 20-minute monitoring limit")


def audit(cloud, raw, rows, before, first, second):
    after = cloud.inventory()
    require(len(after) == 200, "Expected 200 result objects")
    require(all(after.get(key) == value for key, value in before.items()), "An old checkpoint changed")
    require(cloud.read(INPUT_KEY) == raw, "Input object changed")
    contract = cloud.read_json(RESULT_KEY + "/contract.json")
    manifest = cloud.read_json(RESULT_KEY + "/manifest.json")

    def fetch(row):
        key = RESULT_KEY + "/positions/" + row["id"] + ".json"
        return row["id"], cloud.read_json(key, int(after[key]["generation"]))

    with ThreadPoolExecutor(max_workers=4) as pool:
        values = dict(pool.map(fetch, rows))
    verify_results(raw, rows, contract, manifest, values)
    save("after.snapshot.json", after)
    save("manifest.json", manifest)
    before_ids = {key.rsplit("/", 1)[-1].removesuffix(".json") for key in before}
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        logs = cloud.logs(second["uid"])
        events = worker_events(logs)
        save("recovery-events.json", events)
        if verify_resume_events(events, before_ids, rows):
            break
        emit("waiting_for_recovery_logs", worker_events=len(events))
        time.sleep(10)
    else:
        raise TimeoutError("Results validate, but complete resume logs have not arrived")
    report = {"status": "PASS", "project": PROJECT, "region": REGION, "test": RUN,
              "test_scope": "checkpoint_recovery_after_vm_interruption",
              "interrupted_job": JOBS[0], "interrupted_job_uid": first["uid"],
              "recovery_job": JOBS[1], "recovery_job_uid": second["uid"],
              "total_positions": 200, "resumed_positions": len(before),
              "newly_analyzed_positions": 200 - len(before), "checksums_verified": 200,
              "old_object_generations_and_checksums_unchanged": True,
              "fingerprint": contract["fingerprint"], "cleanup_verified": True,
              "task_running_durations": {"a": first["status"].get("runDuration"),
                                         "b": second["status"].get("runDuration")},
              "finished_at": datetime.now(timezone.utc).isoformat(),
              "production_database_modified": False}
    evidence = OUT / "interruption-evidence.json"
    if evidence.exists():
        report["interruption"] = json.loads(evidence.read_text())
    save("report.json", report)
    emit("recovery_test_passed", **report)


def resume_after_vm_recreation(cloud):
    """Explicit continuation of B only after separately verified 50006 evidence.

    Does not repeat A, re-interrupt a VM, change inputs or permit a third job.
    The original strict-preemption error remains in error.json for provenance.
    """
    raw, rows = fixture()
    require((OUT / "input.jsonl").read_bytes() == raw, "Prepared input changed")
    require(json.loads((OUT / "job.json").read_text()) == job_config(), "Prepared job config changed")
    guard = OUT / "recovery-started.json"
    require(not guard.exists() and not (OUT / "attempt-b-submitted.json").exists(),
            "Recovery already attempted; do not repeat a possibly billable submission")
    submitted = json.loads((OUT / "attempt-a-submitted.json").read_text())
    target = json.loads((OUT / "interruption-target.json").read_text())
    operations = json.loads((OUT / "interruption-operation.json").read_text())
    require(len(operations) == 1, "Expected exactly one interruption operation")
    first = cloud.job(JOBS[0])
    require(first["uid"] == submitted["uid"], "First job was replaced")
    task = cloud.command("batch", "tasks", "describe", "0", f"--job={JOBS[0]}",
                         "--task_group=group0", f"--location={REGION}")
    zone = target["vm"]["zone"].rsplit("/", 1)[-1]
    operation = cloud.command("compute", "operations", "describe", operations[0]["name"], f"--zone={zone}")
    evidence = verify_vm_recreation(first, task, operation, target)
    save("interruption-operation-final.json", operation)
    save("interruption-evidence.json", evidence)
    save("attempt-a-task.json", task)
    existing = cloud.command("batch", "jobs", "list", f"--location={REGION}")
    require(not any(j["name"].rsplit("/", 1)[-1] == JOBS[1] for j in existing), "Second job already exists")
    cleanup_wait(cloud, first["uid"])
    before = cloud.inventory()
    require(0 < len(before) < 200, "No partial work to recover")
    require(cloud.read(INPUT_KEY) == raw, "Cloud input changed")
    require(cloud.read_json(RESULT_KEY + "/contract.json") == expected_contract(raw, rows), "Contract changed")
    require(cloud.read_json(RESULT_KEY + "/manifest.json") is None, "Run already complete")
    save("before.snapshot.json", before)
    # Exclusive creation also protects against two local continuations racing.
    with guard.open("x") as output:
        json.dump({"job": JOBS[1], "started_at": datetime.now(timezone.utc).isoformat()}, output)
    emit("resuming_after_verified_vm_recreation", saved=len(before), remaining=200-len(before), **evidence)
    second = cloud.submit(JOBS[1])
    save("attempt-b-submitted.json", second)
    emit("submitted", job=JOBS[1], uid=second["uid"])
    try:
        second = wait_second(cloud, second)
        cleanup_wait(cloud, second["uid"])
        audit(cloud, raw, rows, before, first, second)
    except BaseException as exc:
        save("recovery-error.json", {"error": repr(exc), "job": JOBS[1], "uid": second["uid"]})
        current = cloud.job(JOBS[1])
        require(current["uid"] == second["uid"], "Recovery job UID changed")
        if current["status"]["state"] not in TERMINAL:
            cloud.command("batch", "jobs", "delete", JOBS[1], f"--location={REGION}")
        cleanup_wait(cloud, second["uid"])
        raise


def execute(cloud):
    raw, rows = fixture()
    require((OUT / "input.jsonl").read_bytes() == raw, "Prepared input changed")
    require(json.loads((OUT / "job.json").read_text()) == job_config(), "Prepared job limits/configuration changed")
    # Durable guard plus fixed job IDs prevent accidental repeated billable attempts.
    require(not (OUT / "execution-started.json").exists(), "Test already started; inspect its artifacts before continuing")
    existing = cloud.command("batch", "jobs", "list", f"--location={REGION}")
    require(not any(j["name"].rsplit("/", 1)[-1] in JOBS for j in existing), "Test job names already exist")
    require(not cloud.objects(RESULT_KEY + "/"), "Test output prefix is not empty")
    require(cloud.read(INPUT_KEY) is None, "Test input already exists; refusing to overwrite")
    save("execution-started.json", {"started_at": datetime.now(timezone.utc).isoformat(), "jobs": list(JOBS)})
    tracked = {}
    try:
        cloud.upload_input(raw)
        require(cloud.read(INPUT_KEY) == raw, "Uploaded test input differs")
        first = cloud.submit(JOBS[0])
        tracked[JOBS[0]] = first["uid"]
        save("attempt-a-submitted.json", first)
        emit("submitted", job=JOBS[0], uid=first["uid"])
        first = interrupt_first(cloud, first)
        cleanup_wait(cloud, first["uid"])
        before = cloud.inventory()
        require(0 < len(before) < 200, "Need partially completed work to test recovery")
        require(cloud.read_json(RESULT_KEY + "/manifest.json") is None, "Unexpected complete manifest after interruption")
        require(cloud.read_json(RESULT_KEY + "/contract.json") == expected_contract(raw, rows), "Partial run contract mismatch")
        save("before.snapshot.json", before)
        emit("partial_work_preserved", saved=len(before), remaining=200-len(before))
        second = cloud.submit(JOBS[1])
        tracked[JOBS[1]] = second["uid"]
        save("attempt-b-submitted.json", second)
        emit("submitted", job=JOBS[1], uid=second["uid"])
        second = wait_second(cloud, second)
        cleanup_wait(cloud, second["uid"])
        audit(cloud, raw, rows, before, first, second)
    except BaseException as exc:
        save("error.json", {"error": repr(exc), "tracked_jobs": tracked})
        # Only this controller's unfinished jobs may be canceled. Preserve all bucket objects.
        for name, uid in tracked.items():
            try:
                current = cloud.job(name)
                save(name + "-on-error.json", current)
                if current["uid"] == uid and current["status"]["state"] not in TERMINAL:
                    emit("canceling_owned_test_job", job=name, uid=uid)
                    cloud.command("batch", "jobs", "delete", name, f"--location={REGION}")
                cleanup_wait(cloud, uid)
            except Exception as cleanup_error:
                emit("cleanup_requires_attention", job=name, error=repr(cleanup_error))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--prepare", action="store_true", help="Generate local fixtures only")
    action.add_argument("--execute", action="store_true", help="BILLABLE: two fixed Spot jobs and one verified preemption")
    action.add_argument("--resume-after-vm-recreation", action="store_true",
                        help="BILLABLE: submit only B after verifying a 50006 VM-loss event; never repeats A")
    parser.add_argument("--gcloud", default="gcloud")
    args = parser.parse_args()
    if args.prepare:
        prepare()
    elif args.resume_after_vm_recreation:
        resume_after_vm_recreation(Cloud(args.gcloud))
    else:
        execute(Cloud(args.gcloud))


if __name__ == "__main__":
    main()
