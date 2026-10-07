"""Fail closed before publishing if earlier cloud work has not been cleaned."""
from cleaning_job.cloud import CleanupError


def verify_clean_workspace(cloud):
    # Never infer that an unknown/failed job's checkpoints are disposable. Known
    # jobs are cleaned after import by the durable controller cleanup phase.
    # This check also protects against manual jobs/images created outside the UI.
    inventories = (
        ("Batch jobs", cloud.jobs),
        ("Cloud Run resources", cloud.run_resources),
        ("compute resources", cloud.resources),
        ("worker images", cloud.images),
        ("job files", lambda: cloud.objects("")),
    )
    leftovers = [f"{len(items)} {kind}" for kind, read in inventories if (items := read())]
    if leftovers:
        raise CleanupError("Previous cloud resources remain: " + ", ".join(leftovers) +
                           ". Finish recovery/cleanup before starting another chunk; nothing new was uploaded.")
