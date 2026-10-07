"""Receipt-gated Cloud Run cleanup. Exact job UID, object generations, image upload."""
from urllib.parse import quote

from cleaning_job.cloud import BUCKET, PROJECT, REGION
from cleaning_job.cleanup import require, digest, check_bucket, inventory, check_inventory
from cleaning_job import images
from .cloud_run import PARENT, job_name, executions, terminal, matches

STREAMS = {"run.googleapis.com/stdout", "run.googleapis.com/stderr", "run.googleapis.com/varlog/system"}


def validate(plan):
    require(plan.get("backend") == "cloud_run" and plan.get("project") == PROJECT
            and plan.get("sha256") == digest(plan), "Invalid Cloud Run cleanup plan")
    require(plan["job"] == job_name(plan["run_id"]) and bool(plan["uid"]), "Unsafe Cloud Run job scope")
    require(plan["prefixes"] == [f"inputs/ui-{plan['run_id']}/", f"results/ui-{plan['run_id']}/"], "Unsafe cleanup paths")
    for obj in plan["objects"]:
        require(any(obj["name"].startswith(p) for p in plan["prefixes"]) and obj["generation"].isdigit(), "Unsafe object generation")
    for item in plan["images"]:
        images.validate_uri(item["uri"])
        require(item["uri"].split("/")[-1].startswith("ui-" + plan["run_id"] + "@"), "Image is not job-owned")
    require(bool(plan["executions"]), "Missing completed execution identities")


def current_job(cloud, plan):
    remote = cloud.request("run", plan["job"], missing_ok=True)
    if remote is None:
        return None
    require(remote.get("uid") == plan["uid"] and matches(remote, plan["spec"]), "Cloud Run job replaced or changed")
    if remote.get("deleteTime") and not remote.get("reconciling"):
        return None
    known = {e["name"]: e["uid"] for e in plan["executions"]}
    for execution in executions(cloud, plan["job"]):
        require(known.get(execution["name"]) == execution["uid"] and terminal(execution) != "RUNNING",
                "Unknown/running execution protects Cloud Run inputs and results")
    # Cloud Run can retain a deleted-resource tombstone. It is no longer a
    # runnable job; still require all visible executions terminal above.
    return remote


def other_references(cloud, plan):
    """Protect other job/service references, including old execution snapshots."""
    resources = list(cloud.jobs())
    known = {e["name"].rsplit("/", 1)[1]: e["uid"] for e in plan["executions"]}
    for resource in cloud.run_resources():
        meta = resource.get("metadata", {})
        if resource.get("name") == plan["job"]:
            require(meta.get("uid") == plan["uid"], "Global Cloud Run job identity changed")
            continue
        if resource.get("kind") == "Execution" and meta.get("name") in known:
            require(meta.get("uid") == known[meta["name"]], "Global Cloud Run execution identity changed")
            continue
        resources.append(resource)
    packages = [item["uri"].split("@", 1)[0] for item in plan["images"]]
    for resource in resources:
        for value in images.strings(resource):
            require(not any(package in value for package in packages), "Another cloud resource references this image")
            for prefix in plan["prefixes"]:
                base = f"gs://{BUCKET}/"
                if base in value:
                    reference = value.split(base, 1)[1].strip("/")
                    require(not (prefix.rstrip("/") in reference or not reference or "*" in reference
                            or prefix.startswith(reference + "/")), "Another cloud resource references job files")


def prepare(cloud, launch, run_id):
    plan = {"backend": "cloud_run", "project": PROJECT, "run_id": run_id,
            "job": launch["run_job"], "uid": launch["run_uid"], "spec": launch["spec"],
            "executions": launch["run_executions"], "prefixes": [f"inputs/ui-{run_id}/", f"results/ui-{run_id}/"],
            "images": images.inventory(cloud, [launch["image"]])}
    current_job(cloud, plan)
    other_references(cloud, plan)
    check_bucket(cloud)
    plan["objects"] = inventory(cloud, plan["prefixes"])
    plan["sha256"] = digest(plan)
    validate(plan)
    return plan


def clean_logs(cloud, plan):
    known = {e["name"].rsplit("/", 1)[1] for e in plan["executions"]}
    removed, shared, pending = [], [], []
    def competing_resources():
        identities = {e["uid"] for e in plan["executions"]} | {plan["uid"]}
        # Log deletion is stream-wide. Even an unrelated idle resource can start
        # emitting into the same stream, so retain logs when any other Run
        # job/service/revision/execution exists anywhere in this project.
        return any(r.get("metadata", {}).get("uid") not in identities for r in cloud.run_resources())
    def owned(entry):
        resource = entry.get("resource", {})
        labels = resource.get("labels", {})
        return (resource.get("type") == "cloud_run_job" and labels.get("project_id") == PROJECT
                and labels.get("location") == REGION and labels.get("job_name") == plan["job"].rsplit("/", 1)[1]
                and entry.get("labels", {}).get("run.googleapis.com/execution_name") in known)
    for stream in sorted(STREAMS):
        if competing_resources():
            shared.append(stream)
            continue
        query = f'logName="projects/{PROJECT}/logs/{quote(stream, safe="")}"'
        entries = list(cloud.log_entries(query, default_bucket=True))
        if not entries:
            continue
        if not all(owned(entry) for entry in entries):
            shared.append(stream)
            continue
        other_references(cloud, plan)
        # The job is gone and all its executions were terminal before deletion.
        if competing_resources() or any(not owned(entry) for entry in cloud.log_entries(query, default_bucket=True)):
            shared.append(stream)
            continue
        cloud.delete_log(stream)
        (pending if any(cloud.log_entries(query, default_bucket=True)) else removed).append(stream)
    return {"deleted": removed, "retained_shared": shared, "pending": pending,
            "note": "Audit/billing records, shared logs and other log buckets remain."}


def apply(cloud, plan):
    validate(plan)
    other_references(cloud, plan)
    check_bucket(cloud)
    check_inventory(cloud, plan)
    images.remaining(cloud, plan)
    remote = current_job(cloud, plan)
    if remote:
        if not remote.get("deleteTime"):
            cloud.operation(cloud.request("run", plan["job"], method="DELETE", params={"etag": remote["etag"]}))
        return {"complete": False, "phase": "deleting_cloud_run_job"}
    other_references(cloud, plan)
    for obj in check_inventory(cloud, plan):
        cloud.delete_object(obj)
    if inventory(cloud, plan["prefixes"]):
        return {"complete": False, "phase": "deleting_objects"}
    for uri in images.remaining(cloud, plan):
        other_references(cloud, plan)
        cloud.delete_image(uri)
    if images.remaining(cloud, plan):
        return {"complete": False, "phase": "deleting_image"}
    logs = clean_logs(cloud, plan)
    return {"complete": not logs["pending"], "logs": logs}
