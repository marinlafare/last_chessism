"""Optional end-of-testing cleanup, NEVER part of routine per-job cleanup."""

import re
from urllib.parse import unquote

from .cloud import PROJECT, REPOSITORY
from .cleanup import require
from .parallel import read_parallel

TEST_LOGS = frozenset({
    "batch_agent_logs", "batch_task_logs", "GCEGuestAgent", "GCEGuestAgentManager",
    "OSConfigAgent", "compute.googleapis.com/shielded_vm_integrity", "google_metadata_script_runner",
})


def idle_project(cloud):
    jobs, resources = read_parallel(cloud.jobs, cloud.resources)
    require(not jobs, "Shared cleanup requires no Batch jobs anywhere in this dedicated test project")
    require(not resources, "Shared cleanup requires no VMs, disks, templates or managed instance groups")


def cleanup_shared(cloud, *, images=(), logs=(), execute=False, dedicated_test_project=False):
    """Explicit selections only. Image tags and entire selected log streams go away.

    The operator must ensure no external consumer uses the selected image digests.
    There is no interactive prompt; the distinct flags are automation-friendly.
    """
    require(images or logs, "Select at least one exact image digest or allowlisted test log")
    for image in images:
        require(re.fullmatch(re.escape(REPOSITORY) + r"[a-z0-9_-]+@sha256:[0-9a-f]{64}", image),
                "Images must be exact sha256 digests in chessism-workers, not tags")
    require(set(logs) <= TEST_LOGS, "Only the allowlisted Batch/VM test logs can be deleted; no audit logs")
    if execute:
        require(dedicated_test_project, "Shared deletion requires --dedicated-test-project (not routine job cleanup)")
    idle_project(cloud)
    present_images = {item["uri"] for item in cloud.images()} if images else set()
    present_logs = {unquote(name.split("/logs/", 1)[1]) for name in cloud.logs()} if logs else set()
    selected_images = sorted(set(images) & present_images)
    selected_logs = sorted(set(logs) & present_logs)
    operations = []
    if execute:
        idle_project(cloud)
        for image in selected_images:
            operation = cloud.delete_image(image)
            if operation and operation.get("name"):
                operations.append(operation["name"])
        for log in selected_logs:
            cloud.delete_log(log)
    remaining_images = sorted(set(images) & {i["uri"] for i in cloud.images()}) if execute and images else []
    return {"dry_run": not execute, "images": selected_images, "logs": selected_logs,
            "pending_images": remaining_images, "operations": operations,
            "note": "Whole selected log streams in global _Default, not just one job. Image deletion may finish asynchronously. "
                    "Keep this mode out of the normal batch controller; buckets/repositories/audit/billing remain."}
