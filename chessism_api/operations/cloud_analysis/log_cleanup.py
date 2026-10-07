"""Only delete allowlisted _Default streams wholly owned by completed UI runs.

No timestamp filter on stream inspection: older/unrelated entries protect the
ENTIRE stream. Google-required audit records are read for identity, never deleted.
The controller saves the plan in PostgreSQL before calling remove_logs.
"""
from collections import Counter
from urllib.parse import quote, unquote

from cleaning_job.cloud import PROJECT, PROJECT_NUMBER, CleanupError
from cleaning_job.cleanup import digest, require
from cleaning_job.shared import TEST_LOGS, idle_project

ELIGIBLE_LOGS = TEST_LOGS | {"diagnostic-log", "ping"}


def instance_identity(entry):
    resource = entry.get("resource", {})
    labels = resource.get("labels", {})
    if (resource.get("type") != "gce_instance" or labels.get("project_id") != PROJECT
            or not str(labels.get("instance_id", "")).isdigit() or not labels.get("zone")):
        return None
    return labels["zone"] + "/" + str(labels["instance_id"])


def owned_instances(cloud, uids):
    owned = set()
    query = (f'logName="projects/{PROJECT}/logs/cloudaudit.googleapis.com%2Factivity" '
             'AND protoPayload.serviceName="compute.googleapis.com" '
             'AND protoPayload.methodName="v1.compute.instances.insert"')
    for entry in cloud.log_entries(query):
        payload = entry.get("protoPayload", {})
        parts = payload.get("resourceName", "").split("/")
        ident = instance_identity(entry)
        if (len(parts) == 6 and parts[0] == "projects" and parts[1] in {PROJECT, PROJECT_NUMBER}
                and parts[2] == "zones" and parts[4] == "instances" and ident
                and ident.split("/")[0] == parts[3]
                and any(parts[5].startswith(uid + "-group") for uid in uids)):
            owned.add(ident)
    return owned


def inspect_stream(cloud, log, owned):
    require(log in ELIGIBLE_LOGS, "Log stream is not allowlisted")
    query = f'logName="projects/{PROJECT}/logs/{quote(log, safe="")}"'
    count, unrelated, levels = 0, False, Counter()
    for entry in cloud.log_entries(query, default_bucket=True):
        count += 1
        unrelated |= instance_identity(entry) not in owned
        levels[entry.get("severity", "DEFAULT")] += 1
    return {"entries": count, "unrelated": unrelated, "severities": dict(levels)}


def plan_logs(cloud, uids):
    idle_project(cloud)
    owned = owned_instances(cloud, uids)
    plan = {"version": 1, "project": PROJECT, "instances": sorted(owned),
            "streams": {}, "retained": {}}
    for name in cloud.logs():
        log = unquote(name.split("/logs/", 1)[1])
        if log not in ELIGIBLE_LOGS:
            continue
        summary = inspect_stream(cloud, log, owned)
        if summary["entries"]:
            target = plan["retained"] if summary["unrelated"] else plan["streams"]
            target[log] = summary
    plan["sha256"] = digest(plan)
    return plan


def remove_logs(cloud, plan):
    require(plan.get("version") == 1 and plan.get("project") == PROJECT
            and plan.get("sha256") == digest(plan), "Invalid saved log cleanup plan")
    require(set(plan["streams"]) <= ELIGIBLE_LOGS, "Unsafe log deletion scope")
    idle_project(cloud)
    owned = set(plan["instances"])
    # Validate ALL targets before deleting the first stream.
    for log in plan["streams"]:
        if inspect_stream(cloud, log, owned)["unrelated"]:
            raise CleanupError("Log acquired unrelated entries; entire stream retained: " + log)
    for log in plan["streams"]:
        idle_project(cloud)
        if inspect_stream(cloud, log, owned)["unrelated"]:
            raise CleanupError("Log acquired unrelated entries; entire stream retained: " + log)
        cloud.delete_log(log)
    remaining = [log for log in plan["streams"] if inspect_stream(cloud, log, owned)["entries"]]
    return {"complete": not remaining, "deleted": sorted(set(plan["streams"]) - set(remaining)),
            "remaining": remaining, "retained_shared": sorted(plan["retained"]),
            "note": "Only verified owned streams in global _Default. Audit, billing, monitoring history and other log buckets remain."}
