# Cloud-resident recovery

`recovery.yaml` is a Google Cloud Workflows supervisor, not another always-on VM.
The local controller uploads the entire input/image first, then starts this
workflow. Every 60 seconds it checks the current Batch job. It keeps working
while the PC is off; local imports and final cleanup resume when the PC returns.
Workflows obtains short-lived credentials from its dedicated service account;
it does not depend on the host's cached access token or a downloaded key.

## Policy

- Native Batch retries are zero, avoiding a shared retry counter.
- Only a FAILED task with structured `taskExecution.exitCode == 50001` counts as
  confirmed Google Spot preemption. Wait 60 seconds, then submit a replacement
  using the exact same input, digest-pinned image, options and checkpoints.
  There is no preemption-count limit in this workflow.
- Other failures (including watchdog exit 124, exit 143, unknown/missing reasons,
  VM-reporting timeout 50002 and failures before task creation) consume the normal
  allowance: two failed application attempts total, then stop and retain data.
- During analysis, worker 1.3.1 exits if **no FEN completes for 300 seconds**.
  Node counts, logs and uploads cannot renew this timer. Input/checkpoint loading
  and final upload drain have their own progress checks, not the analysis timer.
- User cancellation wins over automatic retries. The UI's stop action writes
  `cancel.json`; the supervisor cancels the current Batch job and stops. Retain
  committed results/checkpoints. Do not delete the job or cancel the workflow
  alone: cancelling only the workflow does not stop its Batch VM.
- The manual retry UI allows up to three explicitly requested recovery sessions;
  each gets the same two-application-failure allowance. Confirmed preemptions do
  not use up these explicit sessions.
- No monetary/total-runtime cap was added. Google platform limits, IAM/API
  failures, quota, Spot capacity and workflow lifetime limits still apply. This
  is not a guarantee of an infinite execution. Unexpected supervisor failures
  pause local orchestration; inspect the current Batch job before intervention.

`owner.json` is a create-only execution fence. Lost submission responses and
duplicate executions cannot independently submit replacement jobs. `state.json`
records job IDs, verified UIDs, counters and terminal state. Both live under
`inputs/ui-<run-id>/recovery-<session>/`; no FEN contents enter workflow arguments.
The local DB saves the verified snapshot. Cleanup requires completed import,
the final manifest, a stopped authoritative execution, and no active/queued
duplicates in any session. Only then can it remove these fences and checkpoints.

## Permissions and deployment

Project `chessism-production`, region `us-central1`, workflow and service account
`chessism-batch-recovery`. The deployment uses two custom roles:

| Grant | Scope | Permission |
| --- | --- | --- |
| `chessismBatchRecovery` | Project | `batch.jobs.create/get/delete`, `batch.tasks.get` |
| Service Account User | `chessism-batch-worker` only | Attach that worker identity |
| Storage Object Viewer | Chessism bucket only | Read checkpoints/control files |
| `chessismRecoveryStateWriter` | Conditional bucket binding | Create/replace only `owner.json` / `state.json` under `inputs/ui-*` |

Google's cancellation API requires **`batch.jobs.delete`**; no `batch.jobs.cancel`
permission exists. This also allows deletion of Batch job records. The deployed
workflow uses cancellation, not deletion. This additional capability was
explicitly approved. Result-object deletion, VM administration and IAM changes
are not granted to the supervisor. The state-writer role includes object delete
only because replacing a JSON object requires it; the binding excludes results.

Review the role YAML files and conditional grant before running the repeatable
deployment script (it creates or updates the named resources/grants):

```sh
bash stockfish-batch-worker/cloud_job/deploy_recovery.sh --apply-approved-iam
```

Use `CHESSISM_GCLOUD=/absolute/path/to/gcloud` if needed. No service-account key
is created. IAM changes can take several minutes to propagate.

## Deployment checks without Batch VMs

```sh
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m cloud_job.verify_recovery --execute
```

These checks incur small Workflows/Storage usage, not VM charges. They test:

- Twelve consecutive preemptions, mixed preemption/application failures, missing
  reasons and exit 50002, using the deployed policy subworkflow.
- Real scoped control-file writes/replacement/reads, cancel-before-launch, and
  duplicate execution ownership. The test has no runnable Batch specification
  and an explicit no-launch branch. Its three exact test object generations are
  removed once both executions stop; nothing else is deleted.
- Cancellation permission against a reserved job name that must first be proven
  absent. A 404 after permission checking is expected; 403 is a failed check.

These do **not** prove real Spot interruption recovery end to end. Follow with a
separately approved small billable test before a long unattended analysis. Test
repeated real interruptions, checkpoint reuse, host offline catch-up and cleanup.
Never label a locally injected application exit as Google preemption.

## What remains after cleanup

The reusable workflow/SA, empty bucket/repository, IAM/firewalls and local job
history remain. Google-managed workflow execution history, audit/billing and
shared logs are not erased. Workflows has usage-based step charges while running;
no always-on supervisor VM is provisioned. See [Workflows pricing](https://cloud.google.com/workflows/pricing).

References: [Batch retries](https://docs.cloud.google.com/batch/docs/automate-task-retries),
[exit codes](https://docs.cloud.google.com/batch/docs/troubleshooting),
[cancelling jobs](https://docs.cloud.google.com/batch/docs/cancel-job),
[Batch limits](https://docs.cloud.google.com/batch/quotas).
