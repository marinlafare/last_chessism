"""Exact leftover files/images for explicitly abandoned, quiescent UI jobs.

Includes uploaded but never started work units. This is deliberately separate
from success-only cleanup: it must only be called by the cancellation controller
after all supervisors/executions stop and completed batches are saved locally.
"""
from datetime import datetime, timezone
import re
from .cloud import PROJECT, BUCKET, REPOSITORY
from .cleanup import require, digest, inventory, check_bucket, check_inventory, check_other_jobs
from . import images
from .deletion import remove_objects, notify


def prefixes(run_ids):
    require(bool(run_ids) and all(re.fullmatch(r'[a-f0-9]{32}', ident) for ident in run_ids), 'Invalid cancelled run identities')
    return sorted({f'{root}/ui-{ident}/' for ident in run_ids for root in ('inputs', 'results')})


def validate(plan, run_ids):
    require(plan.get('version') == 2 and plan.get('project') == PROJECT and plan.get('bucket') == BUCKET
            and plan.get('jobs') == [] and plan.get('sha256') == digest(plan), 'Invalid abandoned-artifact plan')
    require(plan['prefixes'] == prefixes(run_ids), 'Cancelled artifact scope changed')
    seen = set()
    for obj in plan['objects']:
        key = obj['name'], obj['generation']
        require(any(obj['name'].startswith(p) for p in plan['prefixes']) and obj['generation'].isdigit()
                and key not in seen, 'Unsafe abandoned-object generation')
        seen.add(key)
    for image in plan['images']:
        images.validate_uri(image['uri'])
        require(any(image['uri'].startswith(f'{REPOSITORY}ui-{ident}@') for ident in run_ids), 'Image is not owned by this cancellation')


def prepare(cloud, run_ids):
    plan = {'version': 2, 'project': PROJECT, 'bucket': BUCKET, 'jobs': [],
            'created_at': datetime.now(timezone.utc).isoformat(), 'prefixes': prefixes(run_ids)}
    # A stopped Batch/Run resource can still reference the inputs. The normal
    # resource cleanup removes it first; this inventory only records exact files.
    check_bucket(cloud)
    plan['objects'] = inventory(cloud, plan['prefixes'])
    owned = [item for item in cloud.images() if any(item['uri'].startswith(f'{REPOSITORY}ui-{ident}@') for ident in run_ids)]
    plan['images'] = sorted([images.snapshot(item['uri'], item) for item in owned], key=lambda item: item['uri'])
    plan['sha256'] = digest(plan)
    validate(plan, run_ids)
    return plan


def apply(cloud, plan, run_ids, *, timings=None):
    validate(plan, run_ids)
    check_other_jobs(cloud, plan)
    check_bucket(cloud)
    images.remaining(cloud, plan)
    objects = check_inventory(cloud, plan)
    remove_objects(cloud, plan, objects, timings=timings)
    if inventory(cloud, plan['prefixes']):
        return False
    notify(timings, plan, 'image')
    for uri in images.remaining(cloud, plan):
        require(not images.blockers(cloud, plan, [uri]), 'Another resource still references a cancelled-job image')
        cloud.delete_image(uri)
    return not images.remaining(cloud, plan)
