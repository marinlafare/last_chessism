# Reusable cloud job cleanup

Host-side tools for the temporary FEN batches sent to Google Cloud. Run them on
your PC/controller, not inside the analysis worker. Python 3.10+ and an
authenticated `gcloud` on PATH are sufficient; no additional Python packages,
service-account keys, or Docker rebuild are needed.

The scope is intentionally fixed to `chessism-production`, its
`chessism-batch-276704059200-us-central1` bucket and `chessism-workers` repository.
Nothing runs merely by importing this folder. The UI controller calls these
helpers automatically only after verified results are committed locally.

## Normal workflow: no confirmation prompts

Cleanup is the **final phase of the whole job**, never an action between FEN
chunks or individual result uploads. The controller should follow this order:

1. Upload the locally built worker image and FEN input, then submit the job.
2. Finish analysis, including any retries.
3. Download, validate and **commit the results to the local database**.
4. Call `cleaning_job(..., execute=True)` automatically. It removes the job's
   temporary resources and, **last**, its unused uploaded image version(s).

There is **no per-job or per-file yes/no question**, and no human acknowledgement
flag. Calling with `execute=True` is the controller's instruction to dispose of
the temporary cloud copy. The helper does not download/import the results or
independently establish that your local copy is safe: the caller owns step 3.
Do not call cleanup in a `finally` block that also runs after failed downloads.

From `stockfish-batch-worker/`, preview a future completed job:

```bash
python3 -m cleaning_job job --job YOUR-JOB-ID
```

Run unattended cleanup instead:

```bash
python3 -m cleaning_job job --job YOUR-JOB-ID --execute
```

These are placeholders, not jobs to submit. The previously deleted test jobs
cannot be rediscovered: this tool needs either existing Batch metadata or a plan
saved before that metadata was deleted. It never guesses old storage paths.

For use by a host-side controller (worker root on Python's import path):

```python
from cleaning_job import cleaning_job

# Only reached after the controller has validated and committed local results.
report = cleaning_job(job_id, execute=True, wait_seconds=0)
# Persist report["plan"] with the controller's batch record. If incomplete,
# retry that saved plan on a later controller tick, not a fresh guessed job ID.
```

The opt-in [UI host controller](../cloud_job/README.md) now connects image upload,
job submission, incremental database imports and this final cleanup hook. These
cleanup scripts themselves do not import results. Keep the built Docker image on your PC for the next push;
cleanup never deletes local Docker images or invokes a rebuild.

New UI requests use one VM for the whole selection, not one VM per 1,000 FENs.
They also save a worker performance summary locally before resource cleanup.
The UI's separate `log_cleanup` phase then deletes only allowlisted global
`_Default` streams proven wholly owned by completed UI runs, using audit-verified
VM identities and full-stream inspection. Any unrelated entry protects a stream.
This scoped phase is not the blanket `shared` CLI mode documented below.

## What routine cleanup does

- Checks terminal job state and exact UID. By default only `SUCCEEDED` is allowed.
- Reads the literal worker `--input` and `--output` arguments. Deletes all object
  generations in those **run directories**, e.g. `inputs/run-123/` and
  `results/run-123/`. Use a unique run directory for each independent batch;
  never mix unrelated files under the same run directory.
- Checks other Batch jobs across all regions for references to those paths.
- Captures only the job's digest-pinned `chessism-workers` image references and
  their upload timestamps. Tagged worker references are refused; unrelated image
  versions and images outside this repository are not deletion targets.
- Saves the exact UID/path/object-generation/image-upload inventory under
  `../out/cleaning_job/` **before** cloud mutations. The printed `plan` path is the
  recovery handle. Plans contain metadata, not FEN contents or credentials.
- Deletes terminal Batch job records. Waits up to 60 seconds of polling by
  default for Batch-owned VMs, disks, managed instance groups and templates to
  disappear. Individual network calls have separate 30-second timeouts.
- Lets Batch remove its compute resources; it never force-deletes a disk/VM.
  If cleanup is still pending, it leaves files alone and reports exact leftovers.
- Rechecks for shared paths/new objects and deletes **only inventoried object
  generations**, with generation preconditions. Already missing targets are OK.
- Once job metadata, compute and files are gone, deletes the exact planned image
  versions and their tags, unless another Batch job references them.
- Verifies no matching jobs, compute resources, live/noncurrent objects or planned
  image versions remain. It never deletes the repository, bucket or permissions.

If another job in **any region** still references an image, only image deletion
is deferred: unrelated temporary job files can still be cleaned. `remaining_images`
and `image_blockers` report what is retained and the referring job names. Even
failed/completed job records protect their referenced image until cleaned, since
failed attempts may still be needed for recovery. A mutable-tag/bare-package
reference conservatively protects every planned digest of that package.

Image deletion is asynchronous. While a planned version is still listed,
`complete` stays false (exit 2); reuse the saved plan later. `image_delete_operations`
contains operation names returned by deletion requests. A re-upload of the same
digest with a different upload timestamp makes an old plan stop safely rather
than delete the new upload. Missing images are harmless, but a version absent at
planning time cannot silently become a later deletion target.

Old version-1 cleanup plans remain usable with their original **no-image-deletion**
scope (`image_cleanup_included: false`). Create a new plan while job metadata
still exists to include image cleanup; do not edit old plans to add targets.

API errors, inaccessible regions, active jobs, changed UIDs, changed object
generations, holds and retention policies stop cleanup. No permission error is
treated as “already gone.” No scripts launch a VM or submit an analysis job.
Metadata/storage API requests themselves may have normal service charges.

### Retry attempts and resuming cleanup

Do not clean a failed attempt while planning to resume its checkpoints. After
the final attempt succeeds and its results are saved, clean all attempts that
share the prefix together:

```bash
python3 -m cleaning_job job --job YOUR-FAILED-ATTEMPT --job YOUR-SUCCESSFUL-ATTEMPT --include-failed --execute
```

`--include-failed` is an explicit noninteractive policy choice to abandon further
retries, not a prompt. It also supports deliberately discarded failed tests.

After a partial failure or pending Batch teardown, use the printed plan:

```bash
python3 -m cleaning_job job --plan out/cleaning_job/PLAN-HASH.json --execute
python3 -m cleaning_job verify --plan out/cleaning_job/PLAN-HASH.json
```

Exit codes: `0` successful preview/complete cleanup; `1` error or safety check;
`2` teardown/image deletion pending or an image still needed by another job.
Rerunning the same plan is safe when targets are already absent. A changed/new
object or re-uploaded image makes an old plan fail closed.

The controller must serialize cleanup with pushes/submissions that reuse an image,
run directory or job ID. Checks are not a distributed lock, and Artifact Registry's
version-delete API has no upload-timestamp precondition. Do not push or submit new
consumers of an image during its cleanup. Do not recreate job IDs or upload new
files to a run directory once its cleanup begins. A plan saved before a job's
deletion is essential if the cleanup process crashes afterwards.

This repository is for these Batch workers only. The cross-job guard inspects
Batch jobs in this project, not external consumers, Cloud Run or Kubernetes.
Do not share automatically cleaned worker images with those systems.

### Hard deletion

The bucket must already have soft-delete retention set to zero (as configured
for these temporary batches). The script checks this before deleting files; it
does not silently change bucket-wide protection. When changing the policy,
[Google says to allow at least 30 seconds for propagation](https://docs.cloud.google.com/storage/docs/disable-soft-delete).
Old soft-deleted objects keep their original retention and cannot be purged
early. “Complete” here means the planned job resources/files/image versions are
gone, **not** that historical soft-deleted copies, logs or billing records vanished.
[Image deletion does not reclaim layers immediately](https://docs.cloud.google.com/artifact-registry/docs/docker/manage-images):
Google removes unreferenced image layers daily. An empty repository is kept for
the next upload, along with its existing access grants.

## Optional end-of-testing cleanup: shared resources

Routine cleanup now removes its job's unused image versions. It **keeps** shared
logs, the bucket, repository, accounts/IAM, firewall rules, APIs and billing export.
The separate mode below is for whole log streams and explicitly selected orphan
image digests that have no saved per-job plan, not for the normal job loop.

Preview explicit disposable image digests and/or selected log IDs:

```bash
python3 -m cleaning_job shared --log batch_task_logs --log batch_agent_logs
python3 -m cleaning_job shared --image us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:REPLACE_WITH_64_HEX_DIGEST
```

To execute a shared cleanup, add `--execute --dedicated-test-project`. This also
runs without prompts, but requires **no Batch jobs in any region** and no Compute
VMs/disks/templates/managed groups. Ensure the selected image has no consumers
outside this project. Re-run a preview to inspect asynchronous image deletion.
Tagged versions are deleted with their tags; the empty repository stays.

Allowed log IDs are only:

```text
batch_task_logs
batch_agent_logs
GCEGuestAgent
GCEGuestAgentManager
OSConfigAgent
compute.googleapis.com/shielded_vm_integrity
google_metadata_script_runner
```

[Google's log deletion API](https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/projects.logs/delete)
deletes an entire selected log stream in the global `_Default` bucket, not one
job's entries; routed copies in other buckets are outside its scope. New entries
can recreate the log. This mode excludes audit logs, `ping`, `diagnostic-log`,
billing records and all storage/log buckets. Do not put it in the per-job loop.

## Offline tests

From the worker root:

```bash
python3 -m unittest discover -s tests -p 'test_cleaning_job*.py' -v
```

Tests use fake cloud state and mocked HTTP. They do not authenticate, submit jobs,
delete real resources or require a working network.
