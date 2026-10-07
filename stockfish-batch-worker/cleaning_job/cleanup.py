"""Per-job cleanup, with saved inventories for unattended, resumable execution."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time

from .cloud import BUCKET, Cloud, CleanupError, PROJECT, PROJECT_NUMBER, REGION, REPOSITORY
from . import images as image_cleanup

OUT = Path(__file__).resolve().parents[1] / "out" / "cleaning_job"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
NOTICE = ("Logs, bucket, repository, IAM, firewall and billing records retained. "
          "Job images are removed only when no other Batch job references them. "
          "Image layers are reclaimed by Google asynchronously, not immediately. "
          "Previously soft-deleted objects are not purged; their original retention still applies.")


def require(condition, message):
    if not condition:
        raise CleanupError(message)


def notice(plan):
    return NOTICE + (" Legacy v1 plan: image cleanup is not included." if plan["version"] == 1 else "")


def job_id(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9-]{0,62}", value), "Invalid Batch job ID")
    return value


def region_id(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z]+-[a-z]+[0-9]+", value), "Invalid Batch region")
    return value


def identity(job):
    parts = job.get("name", "").split("/")
    require(len(parts) == 6 and parts[0] == "projects" and parts[1] in {PROJECT, PROJECT_NUMBER}
            and parts[2] == "locations" and parts[4] == "jobs", "Unexpected Batch resource name")
    uid = job.get("uid", "")
    require(isinstance(uid, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{7,127}", uid), "Missing/invalid Batch UID")
    return {"id": job_id(parts[5]), "region": region_id(parts[3]), "uid": uid}


def prefixes(job):
    """Delete only the run directories named by literal worker CLI arguments."""
    scopes = set()
    for group in job.get("taskGroups", []):
        for runnable in group.get("taskSpec", {}).get("runnables", []):
            container = runnable.get("container", {})
            commands = container.get("commands", [])
            if "--input" not in commands and "--output" not in commands:
                require(not container.get("imageUri", "").startswith(REPOSITORY),
                        "Worker paths must be explicit --input/--output arguments; cannot infer cleanup scope")
                continue
            for flag, root in (("--input", "inputs"), ("--output", "results")):
                require(commands.count(flag) == 1, f"Expected one literal {flag} argument")
                index = commands.index(flag) + 1
                require(index < len(commands), f"Missing value for {flag}")
                uri = commands[index]
                base = f"gs://{BUCKET}/"
                require(isinstance(uri, str) and uri.startswith(base), "Unexpected storage bucket")
                path = uri[len(base):]
                parts = path.rstrip("/").split("/")
                require(re.fullmatch(r"[A-Za-z0-9_./-]+", path) and len(parts) >= 2
                        and parts[0] == root and all(p not in {"", ".", ".."} for p in parts),
                        f"Unsafe {flag} path: {uri}")
                if flag == "--input":
                    require(len(parts) >= 3 and not uri.endswith("/"), "Input must name a file in a run directory")
                scopes.add("/".join(parts[:2]) + "/")
    # Script-only smoke jobs have no files and can still be cleaned.
    return sorted(scopes)


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def check_other_jobs(cloud, plan):
    selected = {(j["region"], j["id"]): j["uid"] for j in plan["jobs"]}
    for job in cloud.jobs():
        ident = identity(job)
        key = (ident["region"], ident["id"])
        if key in selected:
            require(ident["uid"] == selected[key], "Job ID reused with a different UID; refusing cleanup")
            continue
        # A job in another region, failed retry or pending task also owns its data.
        for text in strings(job):
            for bucket, path in re.findall(r"gs://([^/\s\"'<>]+)(/[A-Za-z0-9_./*?\[\]-]*)?", text):
                if bucket != BUCKET:
                    continue
                # Ancestor/bucket references and wildcards may read the run too.
                reference = re.split(r"[*?\[]", path, maxsplit=1)[0].strip("/")
                for prefix in plan["prefixes"]:
                    root = prefix.rstrip("/")
                    overlaps = (not reference or reference == root or reference.startswith(prefix)
                                or root.startswith(reference + "/")
                                or (any(char in path for char in "*?[") and root.startswith(reference)))
                    require(not overlaps,
                            f"{ident['id']} also references {prefix}; include all finished attempts together")
    # Batch cleanup must not invalidate a Cloud Run job or saved execution.
    for resource in cloud.run_resources():
        for value in strings(resource):
            for bucket, path in re.findall(r"gs://([^/\s\"'<>]+)(/[A-Za-z0-9_./*?\[\]-]*)?", value):
                if bucket != BUCKET:
                    continue
                reference = re.split(r"[*?\[]", path, maxsplit=1)[0].strip("/")
                for prefix in plan["prefixes"]:
                    root = prefix.rstrip("/")
                    require(not (not reference or reference == root or reference.startswith(prefix)
                                 or root.startswith(reference + "/")
                                 or (any(char in path for char in "*?[") and root.startswith(reference))),
                            "Cloud Run resource references Batch checkpoint files")


def check_bucket(cloud):
    metadata = cloud.bucket()
    require(metadata.get("name") == BUCKET, "Unexpected bucket metadata")
    policy = metadata.get("softDeletePolicy") or {}
    require(int(policy.get("retentionDurationSeconds", 0)) == 0,
            "Soft delete is enabled. Cleanup will not change the bucket policy or promise a hard deletion")
    require(not metadata.get("retentionPolicy"), "Bucket retention policy present; refusing cleanup")


def inventory(cloud, scopes):
    result = {}
    for scope in scopes:
        for obj in cloud.objects(scope):
            name, generation = obj.get("name", ""), str(obj.get("generation", ""))
            require(name.startswith(scope) and generation.isdigit(), "Unexpected object in inventory")
            require(not obj.get("temporaryHold") and not obj.get("eventBasedHold") and not obj.get("retention"),
                    f"Object is protected by retention/hold: {name}")
            result[(name, generation)] = {"name": name, "generation": generation, "size": str(obj.get("size", "0"))}
    return sorted(result.values(), key=lambda item: (item["name"], item["generation"]))


def digest(plan):
    return hashlib.sha256(json.dumps({k: v for k, v in plan.items() if k != "sha256"},
                                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_plan(plan):
    require(plan.get("version") in {1, 2} and plan.get("project") == PROJECT and plan.get("bucket") == BUCKET,
            "Plan does not belong to this cleanup tool/project/bucket")
    require(plan.get("sha256") == digest(plan), "Plan checksum mismatch; do not edit cleanup plans")
    require(isinstance(plan.get("jobs"), list) and plan["jobs"], "Plan has no jobs")
    for target in plan["jobs"]:
        identity({"name": f"projects/{PROJECT}/locations/{target['region']}/jobs/{target['id']}", "uid": target["uid"]})
        require(target.get("state") in TERMINAL, "Plan is not for terminal jobs")
    if plan["version"] == 2:
        expected_images = sorted({uri for j in plan["jobs"] for uri in j["images"]})
        require([item["uri"] for item in plan["images"]] == expected_images, "Plan image scope mismatch")
        for item in plan["images"]:
            image_cleanup.validate_uri(item["uri"])
            require(item["upload_time"] is None or (isinstance(item["upload_time"], str) and item["upload_time"]),
                    "Invalid planned image upload timestamp")
    else:
        require("images" not in plan and all("images" not in j for j in plan["jobs"]),
                "Legacy plans cannot add image deletion; create a new plan from existing job metadata")
    require(plan["prefixes"] == sorted({p for j in plan["jobs"] for p in j["prefixes"]}), "Plan scope mismatch")
    for prefix in plan["prefixes"]:
        require(re.fullmatch(r"(?:inputs|results)/[A-Za-z0-9_-][A-Za-z0-9_.-]*/", prefix), "Unsafe plan prefix")
    seen = set()
    for obj in plan["objects"]:
        key = (obj["name"], obj["generation"])
        require(any(obj["name"].startswith(p) for p in plan["prefixes"])
                and isinstance(obj["generation"], str) and obj["generation"].isdigit()
                and key not in seen, "Unsafe or duplicate planned object")
        seen.add(key)


def prepare(cloud, jobs, *, region=REGION, include_failed=False):
    region_id(region)
    require(jobs and len(jobs) == len(set(jobs)), "Provide distinct job IDs")
    for name in jobs:
        job_id(name)
    targets = []
    for name in jobs:
        job = cloud.job(name, region)
        require(job is not None, f"Job {name} not found. Use its saved cleanup plan to resume; do not guess its paths")
        ident = identity(job)
        require(ident["id"] == name and ident["region"] == region, "Unexpected job identity")
        state = job.get("status", {}).get("state")
        require(state in TERMINAL, f"{name} is {state}, not terminal; never cancel running work")
        require(state == "SUCCEEDED" or include_failed, f"{name} failed; preserve checkpoints unless --include-failed is set")
        targets.append({**ident, "state": state, "prefixes": prefixes(job), "images": image_cleanup.job_images(job)})
    plan = {"version": 2, "project": PROJECT, "bucket": BUCKET,
            "created_at": datetime.now(timezone.utc).isoformat(), "jobs": targets,
            "prefixes": sorted({p for j in targets for p in j["prefixes"]})}
    check_other_jobs(cloud, plan)
    if plan["prefixes"]:
        check_bucket(cloud)
    plan["objects"] = inventory(cloud, plan["prefixes"])
    plan["images"] = image_cleanup.inventory(cloud, {uri for j in targets for uri in j["images"]})
    plan["sha256"] = digest(plan)
    validate_plan(plan)
    return plan


def save_plan(plan, directory=OUT):
    """Persist BEFORE any cloud deletion, so a crash can resume without the job."""
    validate_plan(plan)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (plan["sha256"] + ".json")
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(plan, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        require(json.loads(path.read_text()) == plan, "Saved plan differs; refusing to overwrite")
    return path


def current_jobs(cloud, plan):
    remaining = []
    for target in plan["jobs"]:
        job = cloud.job(target["id"], target["region"])
        if job is None:
            continue
        ident = identity(job)
        require(all(ident[k] == target[k] for k in ("id", "region", "uid")), "Job identity changed; refusing cleanup")
        state = job.get("status", {}).get("state")
        require(state in TERMINAL | {"DELETION_IN_PROGRESS"}, "Job is no longer terminal")
        require(prefixes(job) == target["prefixes"], "Job storage paths changed")
        if plan["version"] == 2:
            require(image_cleanup.job_images(job) == target["images"], "Job image references changed")
        remaining.append((target, state))
    return remaining


def owned_resources(cloud, plan):
    uids = {j["uid"] for j in plan["jobs"]}
    result = []
    for resource in cloud.resources():
        labels = resource.get("labels", {})
        nested = resource.get("properties", {}).get("labels", {})
        if (labels.get("batch-job-uid") in uids or nested.get("batch-job-uid") in uids
                or any(resource.get("name", "").startswith(uid + "-") for uid in uids)):
            result.append({k: resource.get(k) for k in ("kind", "scope", "name")})
    return result


def check_inventory(cloud, plan):
    current = inventory(cloud, plan["prefixes"])
    expected = {(o["name"], o["generation"]) for o in plan["objects"]}
    require(all((o["name"], o["generation"]) in expected for o in current),
            "New/replaced objects appeared after planning; cleanup stopped without deleting those objects")
    return current


def verify(cloud, plan):
    validate_plan(plan)
    jobs = [j["id"] for j, _ in current_jobs(cloud, plan)]
    resources = owned_resources(cloud, plan)
    objects = inventory(cloud, plan["prefixes"])
    images = image_cleanup.remaining(cloud, plan)
    return {"complete": not (jobs or resources or objects or images), "remaining_jobs": jobs,
            "remaining_compute": resources, "remaining_object_versions": len(objects),
            "remaining_images": images, "image_blockers": image_cleanup.blockers(cloud, plan, images),
            "image_cleanup_included": plan["version"] == 2, "note": notice(plan)}


def apply(cloud, plan, *, wait_seconds=60):
    """No prompts. Caller must have saved results or deliberately abandoned retries."""
    validate_plan(plan)
    require(0 <= wait_seconds <= 300, "wait_seconds must be between 0 and 300")
    check_other_jobs(cloud, plan)
    if plan["prefixes"]:
        check_bucket(cloud)
    current_jobs(cloud, plan)
    check_inventory(cloud, plan)
    image_cleanup.remaining(cloud, plan)
    for target, state in current_jobs(cloud, plan):
        if state != "DELETION_IN_PROGRESS":
            cloud.delete_job(target["id"], target["region"], target["uid"])
    deadline = time.monotonic() + wait_seconds
    while True:
        jobs = current_jobs(cloud, plan)
        resources = owned_resources(cloud, plan)
        if not jobs and not resources:
            break
        if time.monotonic() >= deadline:
            return {"complete": False, "remaining_jobs": [j["id"] for j, _ in jobs],
                    "remaining_compute": resources, "data_deletion_deferred": True,
                    "image_deletion_deferred": bool(plan.get("images")),
                    "note": "Batch cleanup still pending. Resume the same saved plan later; no forced VM/disk deletion."}
        time.sleep(min(5, max(0, deadline - time.monotonic())))
    # A retry may have been submitted during cleanup. Check again before files.
    check_other_jobs(cloud, plan)
    if plan["prefixes"]:
        check_bucket(cloud)
    for obj in check_inventory(cloud, plan):
        cloud.delete_object(obj)
    # Images come last, after all temporary data and job compute are gone.
    report = verify(cloud, plan)
    if report["remaining_jobs"] or report["remaining_compute"] or report["remaining_object_versions"]:
        return report
    operations = []
    for uri in report["remaining_images"]:
        # Refresh immediately before each image deletion. The controller must
        # still serialize pushes/submissions with cleanup (the API has no CAS).
        if uri not in image_cleanup.remaining(cloud, plan):
            continue
        if image_cleanup.blockers(cloud, plan, [uri]):
            continue
        operation = cloud.delete_image(uri)
        if operation and operation.get("name"):
            operations.append(operation["name"])
    return {**verify(cloud, plan), "image_delete_operations": operations}


def cleaning_job(jobs=None, *, execute=False, plan=None, region=REGION, include_failed=False,
                 plan_directory=OUT, wait_seconds=60, cloud=None):
    """Reusable entry point. --execute is consent once, never a per-file prompt.

    Integrators: call only AFTER downloading/validating/durably saving results.
    This helper does not import results into Chessism and is not an importer.
    Pass the returned saved plan on retries, including after Batch metadata is gone.
    """
    require((jobs is None) != (plan is None), "Provide job IDs OR a saved plan")
    if isinstance(jobs, str):
        jobs = [jobs]
    cloud = cloud or Cloud()
    plan = plan if plan is not None else prepare(cloud, jobs, region=region, include_failed=include_failed)
    validate_plan(plan)
    path = save_plan(plan, plan_directory)
    if execute:
        try:
            result = apply(cloud, plan, wait_seconds=wait_seconds)
        except CleanupError as exc:
            raise CleanupError(f"{exc}. Saved cleanup plan: {path}") from exc
    else:
        result = {"dry_run": True, "jobs": [j["id"] for j in plan["jobs"]],
                  "prefixes": [f"gs://{BUCKET}/{p}" for p in plan["prefixes"]],
                  "object_versions": len(plan["objects"]),
                  "images": plan.get("images", []),
                  "image_blockers": image_cleanup.blockers(cloud, plan, [i["uri"] for i in plan.get("images", [])]),
                  "image_cleanup_included": plan["version"] == 2,
                  "bytes": sum(int(o["size"]) for o in plan["objects"]), "note": notice(plan)}
    return {**result, "plan": str(path)}
