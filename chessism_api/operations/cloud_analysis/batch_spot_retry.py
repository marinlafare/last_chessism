"""Database-only retry intent. Importable by the API without worker/cloud SDKs."""


def retry_launch(launch):
    """Reset failed shards only. Successful shards and all receipts are retained."""
    if launch.get("recovery_session", 0) >= 3 or not launch.get("tasks"):
        raise ValueError("Batch retry unavailable; maximum three explicit recovery sessions")
    tasks = []
    for original in launch["tasks"]:
        task = dict(original)
        state = task.get("recovery_summary", {})
        if task.get("status") == "SUCCEEDED":
            tasks.append(task)
            continue
        if task.get("status") not in {"FAILED", "CANCELLED"} or state.get("status") != task["status"]:
            raise ValueError("All VM recovery controllers must be terminal before retry")
        task.update(recovery_session=task["recovery_session"] + 1, status="PENDING",
                    prior_jobs=state["jobs"], prior_uids=state["uids"])
        task.pop("recovery_execution", None)
        task.pop("recovery_summary", None)
        tasks.append(task)
    return {**launch, "tasks": tasks, "recovery_session": launch["recovery_session"] + 1}
