"""Job-owned image snapshots and conservative cross-job usage checks."""

import re

from .cloud import CleanupError, REPOSITORY


def validate_uri(uri):
    if not isinstance(uri, str) or not re.fullmatch(
        re.escape(REPOSITORY) + r"[a-z0-9_-]+@sha256:[0-9a-f]{64}", uri
    ):
        raise CleanupError("Job images must be exact sha256 digests in chessism-workers, not tags")


def job_images(job):
    images = set()
    for group in job.get("taskGroups", []):
        for runnable in group.get("taskSpec", {}).get("runnables", []):
            uri = runnable.get("container", {}).get("imageUri", "")
            if uri.startswith(REPOSITORY):
                validate_uri(uri)
                images.add(uri)
    return sorted(images)


def snapshot(uri, metadata):
    validate_uri(uri)
    uploaded = metadata.get("uploadTime") if metadata is not None else None
    if metadata is not None and (not isinstance(uploaded, str) or not uploaded):
        raise CleanupError(f"Image upload timestamp missing; cannot safely identify this upload: {uri}")
    return {"uri": uri, "upload_time": uploaded}


def inventory(cloud, uris):
    if not uris:
        return []
    present = {image["uri"]: image for image in cloud.images()}
    return [snapshot(uri, present.get(uri)) for uri in sorted(uris)]


def remaining(cloud, plan):
    """Old v1 plans retain their original no-image-deletion scope."""
    expected = {image["uri"]: image for image in plan.get("images", [])}
    if not expected:
        return []
    present = {image["uri"]: image for image in cloud.images() if image["uri"] in expected}
    for uri, metadata in present.items():
        if snapshot(uri, metadata) != expected[uri]:
            raise CleanupError(f"Image was uploaded/replaced after planning; refusing to delete it: {uri}")
    return sorted(present)


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def blockers(cloud, plan, uris):
    """Any other Batch reference is kept, even a failed attempt awaiting retry.

    A tag/bare-package reference conservatively protects all digests of that
    package: resolving a mutable tag just once would not protect later retries.
    """
    if not uris:
        return {}
    selected = {j["uid"] for j in plan["jobs"]}
    blocked = {}
    for job in [*cloud.jobs(), *cloud.run_resources()]:
        if job.get("uid") in selected:
            continue
        for uri in uris:
            package = uri.split("@", 1)[0]
            pattern = re.escape(package) + r"(?:@sha256:[0-9a-f]{64}|:[A-Za-z0-9_.-]+)?(?![A-Za-z0-9_./:@-])"
            references = (match.group(0) for text in strings(job) for match in re.finditer(pattern, text))
            if any(ref == uri or "@" not in ref for ref in references):
                blocked.setdefault(uri, []).append(job["name"])
    return blocked
