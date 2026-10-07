# On-demand UI cloud analysis

The Analyze Positions page has four independent cloud selection containers below
the existing local containers. They use the same all/player/game-selection queries.
The UI/API never receive Google credentials or a Docker socket.

## Selected backend: Batch Spot

After the 25,000-FEN platform comparison on October 6, 2026, Batch Spot remains
the selected production backend: lower cost matters more than time to completion
for this workload. Cloud Run was benchmark-only; it is not used by the UI
controller. See the [comparison](../benchmark_platforms/README.md#first-completed-comparison-20261006ab01).

Keep the existing baseline until another configuration is measured: one
`n2d-standard-4` Spot VM, four independent one-thread Stockfish engines,
100,000 nodes/FEN, and background checkpoints of up to 500 results. The
200,000-FEN selection limit and 300-second no-completed-FEN watchdog remain
unchanged. Selecting Batch does not authorize larger limits or a new cloud job.

The platform benchmark did **not** exercise preemption or the production
recovery supervisor. Before a long unattended run, the next recommended gate is
a separately approved small end-to-end recovery test: use Google's simulated
Spot maintenance/preemption, verify structured exit `50001`, interrupt a second
attempt as well, and confirm checkpoint reuse with the local cloud-controller
stopped. Then resume local imports and verify final cleanup. A plain process
exit or manually invented `50001` does not prove Google's preemption reporting.
Keep machine-family/VM-count cost benchmarks separate from this recovery test.

## Lifecycle

The Compose controller runs on your PC using your existing `gcloud` login:

1. The authenticated UI records a bounded request in PostgreSQL.
2. The controller selects the whole request (up to 200,000 unscored FENs) using `FOR UPDATE SKIP LOCKED`,
   inserts durable cloud reservations, and commits **before uploading anything**.
3. Before the single execution, it verifies that earlier Batch jobs, compute resources,
   repository images and live/noncurrent bucket objects are gone. Unexpected
   leftovers pause the job before upload; they are not blindly deleted because
   they may still be needed for another job's recovery. It then uploads an already-built worker image into a unique, job-owned Artifact
   Registry package and starts the [cloud-resident recovery workflow](RECOVERY.md).
   It submits one digest-pinned Batch Spot task at a time, with separate counters
   for confirmed Google preemptions and application failures.
4. Every 15 seconds it checks for completed background uploads (500 results per
   file; the final partial group is also uploaded). It validates input identity,
   engine/settings contract, legal PVs, IDs and checksums before importing.
5. FEN updates, continuations, database counters, imported-file receipts and
   reservation releases commit in **one transaction**. Results have exactly the
   existing `analysis_source='stockfish'` and normal score/PV/WDL fields.
6. After Batch succeeds, the recovery workflow and any duplicate executions stop,
   and the final manifest exactly matches committed receipts,
   derived game/score views are refreshed. A cleanup plan is persisted in the DB
   before deletion. `cleaning_job` removes job metadata, checks that its compute
   resources disappeared, hard-deletes the exact temporary object generations,
   and deletes the uploaded image last. No deletion confirmation is required.
7. The worker uploads a small, contract-bound performance report. The controller
   persists it in PostgreSQL (`CloudAnalysisRun.launch.performance`) BEFORE any
   resource deletion. It includes per-worker FEN counts, search time, upload-queue
   wait, throughput, and sampled VM-wide CPU/memory statistics. The UI exposes
   it under **Performance report — saved locally**. It describes the successful
   reporting attempt; resumed FENs are reported separately, not counted as new work.
8. After compute teardown, automatic log cleanup maps completed runs' exact Batch
   UIDs to VM IDs using Google Compute audit entries. It examines every retained
   entry in each allowlisted global `_Default` stream, without a time filter.
   Any unrelated/unattributed entry protects the entire stream. The checked plan
   and severity counts are saved in the local DB before deletion, then rechecked
   immediately before deletion. Pending deletion is verified on subsequent ticks.
   API errors pause cleanup; no other cloud job starts while it is pending.

All FENs feed one shared queue and four persistent single-thread engines. A faster
engine takes the next available FEN rather than waiting for a fixed allocation.
Uploads still contain up to 500 results, but do not trigger VM teardown. Only
compact committed references remain in worker memory; full results are discarded
after durable batch upload. Input/manifest limits support the 200,000-FEN cap.
The local controller has a 2-GiB memory ceiling for large manifests; the UI API
reads aggregate counts rather than downloading full FEN/receipt lists every poll.

Local engines and the automatic tablebase sweep skip cloud reservations. Cloud
selection skips rows locked by local engines. A second reservation check **after**
row locking closes the selection-snapshot race. Reservations survive restarts and
are not removed merely because a worker stopped reporting progress.

Only one unfinished cloud request is admitted. A PostgreSQL transaction advisory
lock rejects simultaneous submissions from different browser tabs/API processes
with HTTP 409. Queued/running jobs and paused/waiting/failed recovery requests
block other cloud selections; completed/cancelled/limit-reached history does not.
The UI disables all four selection fieldsets while the cloud slot is occupied,
including through final import and cleanup. Recovery actions remain available
only for the blocking job. Local analysis is unaffected.

FEN progress appears inside the active selection card and its job history card:
committed FENs / target, percentage, and a visible progress bar. It refreshes every
5 seconds and advances when batches of up to 500 results reach the local DB, not
for unsaved in-flight engine searches. 100% imported does not imply cleanup is done.

Cleanup plans remain authoritative in PostgreSQL. The helper's local JSON copy
uses `CLOUD_CLEANUP_DIRECTORY` (`/tmp/chessism-cloud-cleanup` in Compose), never the
read-only application directory, and is recreated from the DB after restart.
Clean-start checks exclude retained audit/billing logs and any previously
soft-deleted object versions still subject to Google's retention period.

## Normal startup with Docker Compose

From the repository root, `docker compose up` (or `docker compose up -d`) now starts
`cloud-controller` with the main app. No host virtualenv or separate terminal is
needed. `cloud-worker-image` builds/checks the Stockfish execution image and exits
successfully; it does not analyze positions locally. The controller waits for the
API to finish database initialization, verifies the worker version, acquires the
single-controller database lock, then reports a heartbeat. It restarts on failure.

Before the first start, run `gcloud auth login` on the host if not already logged
in. The default credential source is `${HOME}/.config/gcloud`; override with
`CLOUD_GCLOUD_CONFIG_DIR` if using a custom SDK config directory. The Docker socket
defaults to `/var/run/docker.sock`; rootless installations can override
`CLOUD_DOCKER_SOCKET`. Neither missing mount source is created automatically.

The bootstrap uses SETUID/SETGID, then drops to the mounted
config owner's non-root UID and the socket's group. It mounts the host gcloud
directory read-only and snapshots just login databases/configuration into private
tmpfs. Refresh-token caches and an isolated Docker credential-helper config are
ephemeral; no credentials enter build layers, the API, source control, or logs.
After logging in again on the host, run `docker compose restart cloud-controller`
to refresh the snapshot. Use a normal non-root host user's gcloud configuration.
The controller uses gcloud's actual token expiration, refreshing five minutes
before expiry as often as needed, including jobs exceeding ten hours. REST calls
and Storage SDK transfers share this refreshable provider. An HTTP 401 forces
one credential refresh and one identical-request retry; repeated 401s, denied
permissions or a revoked login pause the saved phase safely, without submitting
a replacement job or deleting unimported results. Tokens never appear in errors.
This is credential renewal, not a maximum runtime or an extended-lived token.
The Compose launcher accepts only the internal `db` hostname, using the existing
non-TLS PostgreSQL service unless TLS is explicitly configured in the URL. Google
connections continue to use HTTPS. The manual host launcher keeps its existing
database connection behavior.
Docker's minimal init retains KILL only for forwarding stop signals to its
non-root child; the Python controller retains no effective capabilities.

**Trust boundary:** Docker socket access grants broad control over the host; a
read-only socket bind does NOT restrict Docker API operations. Only this trusted,
non-public controller receives that socket, not the web/API or remote worker.
See [Docker's daemon security guidance](https://docs.docker.com/engine/security/).

```sh
docker compose logs --tail=30 cloud-controller
docker compose ps cloud-controller
docker compose stop cloud-controller
```

Stopping Compose stops the controller but does **not** cancel running Google
jobs. Restarting resumes polling/import/cleanup from saved state. Previously
paused/failed jobs are not automatically resumed; use the UI's explicit action.
Queued or running UI jobs **can create billable resources** once the controller
starts. Starting an idle controller or the image check does not launch a Batch job.

For source updates, rebuild before startup:
`docker compose build cloud-controller cloud-worker-image`.
First deployment of cloud coordination also requires current API/local-worker
images so local analysis honors reservations. Do not mix old local workers with
the cloud controller. If an old manually started host controller is running,
stop it first (`systemctl --user stop chessism-cloud-controller` for the previous
user service). The database lock prevents duplicate controllers if one is missed.

## Optional manual host controller (instead of Compose)

Use this only for development, with the Compose controller stopped. API startup
creates the tracking tables; the controller never runs migrations itself.

Use Python 3.11+ on the host, from the repository root:

```sh
python3 -m venv .venv-cloud
.venv-cloud/bin/pip install -r requirements.txt -r stockfish-batch-worker/requirements.txt
```

Build the current standalone worker (version 1.3.1) from the repository root,
or use your already-built matching local image:

```sh
docker build --platform linux/amd64 -t chessism-stockfish-batch:fen-compat-v1 stockfish-batch-worker
```

Version 1.3.1 accepts the database's four-field FEN keys as well as six-field
FENs. Results keep the original keys; no database rewrite is required. A paused
job that failed input validation before creating a run can be resumed with this
image. Existing runs retain their pinned image and checkpoint contract.

You must also have
`gcloud auth login` and `gcloud auth configure-docker us-central1-docker.pkg.dev`
configured for the host user. Do not create/download service-account keys.

Set `DATABASE_URL` privately in the controller environment to the existing local
database using `postgresql+asyncpg://...` and its **host** port (normally 5433), not
the Compose-only hostname. Do not commit the URL or paste its password in chat.
The controller deliberately does not call `init_db` or run migrations.

```sh
.venv-cloud/bin/python -m chessism_api.operations.cloud_analysis \
  --local-image chessism-stockfish-batch:fen-compat-v1
```

Optional: `--gcloud /absolute/path/to/gcloud` and `--poll-seconds 15`.
For this repository's local Compose database, the host launcher reads only the
database URL from `.env` and maps the Docker `db` hostname to `127.0.0.1:5433`,
without placing the password in shell history or process arguments:

```sh
.venv-cloud/bin/python stockfish-batch-worker/cloud_job/run_host.py \
  --local-image chessism-stockfish-batch:fen-compat-v1
```

It supports `--env-file`, `--db-port`, and `--gcloud`, and adds the Google SDK's
bin directory to this process's PATH for Docker credential-helper access.

To keep it running independently of the terminal on this Linux workstation,
start a user service from the repository root:

```sh
systemd-run --user --unit=chessism-cloud-controller --collect \
  --description="Chessism on-demand cloud analysis controller" \
  --property=Restart=on-failure --property=RestartSec=10 --property=UMask=0077 \
  --setenv=PYTHONUNBUFFERED=1 --setenv=PYTHONDONTWRITEBYTECODE=1 \
  --working-directory="$PWD" \
  "$PWD/.venv-cloud/bin/python" "$PWD/stockfish-batch-worker/cloud_job/run_host.py" \
  --local-image chessism-stockfish-batch:fen-compat-v1
```

Check: `systemctl --user status chessism-cloud-controller`.
Logs: `journalctl --user -u chessism-cloud-controller -n 30 --no-pager`.
Stop: `systemctl --user stop chessism-cloud-controller` (does not cancel Google jobs).
This is a transient service, not a new boot-time service. Repeat the start command
after reboot or an explicit stop, once the updated application services are running.

The API reports the controller online after its first heartbeat. Creating a cloud
job from the UI explicitly authorizes its uploads, billable compute and automatic
post-import cleanup. Merely importing these modules or viewing the page does not.

Only one controller may run per database (PostgreSQL session advisory lock).
Do not manually upload images or submit jobs into its `ui-<run-id>` packages or
prefixes while it operates. Those names are owned by the corresponding job.

## Bounds and recovery

- New requests (`execution_mode=single_vm_v1`) use one task for up to 200,000 FENs.
  Loop/group counts multiply the selection size; they no longer launch separate
  VMs. If some selected FENs are unavailable at reservation, the existing
  `limit_reached` status reports that fact without launching an extra VM. Already
  saved legacy requests keep their original chunk/recovery contract.
- One `n2d-standard-4` Spot task for the whole request, 12 GiB task memory, four single-thread
  Stockfish engines, 2 GiB hash each, MultiPV 4, no tablebases. Every new UI run
  uses **100,000 nodes per FEN**, fixed by the server, not an editable request
  parameter. Local analysis page requests use the same 100,000-node default.
  Recovery preserves a previously launched run's exact checkpoint configuration.
- New jobs use a fixed, system-owned inactivity timeout of 300 seconds (five
  minutes). Application failures have two attempts total (initial + one retry).
  Only Batch-confirmed exit code **50001** triggers a replacement without consuming
  that allowance; the workflow imposes no preemption-count cap. User cancellation
  and Google platform limits still apply. Native Batch retries are disabled so
  preemption and application failures do not share the same counter.
  This is not a superuser/UI setting, and
  the API rejects client timeout overrides. There is no application-imposed total runtime cutoff:
  `maxRunDuration` is omitted and the worker's absolute `run_timeout` is disabled.
  During analysis, a dedicated timer is renewed ONLY when a new FEN finishes.
  Increasing node counts, logs, checkpoint reads and background uploads cannot
  keep an attempt alive without completed FENs. The timer starts after input and
  checkpoint loading, and ends after the engines finish, before final uploads.
  Separate progress checks protect startup, recovery and storage operations.
  Each searching engine also has its own node-progress inactivity timer,
  so other engines cannot conceal a stalled search. Repeated UCI messages with
  no additional searched nodes do not count as progress. Uploading every 500
  results does not make active analysis look idle.
  Watchdog/timeout failures exit with application code 124 (including timeouts
  wrapped by TaskGroup); this must never be classified as Google preemption.
  Retries reuse saved batches, not unsaved in-memory results. This is not a
  monetary or task-hour cap: charges continue while work progresses. Google's
  [platform time limits](https://docs.cloud.google.com/batch/docs/set-timeouts)
  and VM-failure detection still apply. Old saved launches retain their original
  timeout/specification for recovery; the new policy applies to new requests.
- For game jobs, use Preview to freeze latest/oldest/date-range/fair-range game
  selection. The server derives the FEN target from that saved preview's missing
  positions; there is no editable maximum-FEN field for game selections. Empty
  or over-200,000-FEN selections are rejected, never silently truncated. Select
  fewer games and preview again if the existing safety limit is exceeded.
- Closing the browser has no effect on the host controller. Stopping the controller
  does **not** cancel a running cloud task. Restart it to resume imports/cleanup.
  The Workflows supervisor continues retry decisions while the PC is off.
  `Stop cloud job and recovery` forwards durable cancellation to the supervisor;
  do not delete a live job or cancel only the workflow to stop analysis.
- API/download/validation/cleanup errors pause the job and block further cloud
  launches. `Resume saved job` retries the saved phase, without silently changing
  its input, settings or image. Missing remote jobs are errors, not empty results.
- A terminal failed cloud task retains its input, checkpoints and remaining claims.
  `Retry cloud — billable` explicitly authorizes a new submission against the same
  immutable checkpoints (maximum three explicitly requested recovery sessions;
  unlimited confirmed preemptions within each session, subject to platform limits).
  Legacy unsupervised runs keep their original three-submission limit. Successful
  retry cleanup includes previous failed/cancelled attempts. A failure never automatically deletes
  unimported results. Exhausted retries require operator diagnosis; claims must
  not simply be cleared while a cloud task could still run.
- Completed cloud groups are retained until final cleanup so Spot retries can skip
  them. An importer restart skips already committed receipts, without double-counting
  FENs or continuations. Game views refresh at execution completion; full-catalog score
  projections refresh once at the end of the UI request, avoiding repeated scans.

## What cleanup retains

Empty bucket/repository, the reusable recovery workflow/service account,
IAM/firewall configuration, and local durable job history remain. Google-managed
workflow execution history and cloud audit/billing records are not erased by job
cleanup; Artifact Registry layer
garbage collection can lag image deletion. Historical soft-deleted objects remain
until their original retention expires. New launches require bucket soft delete to
be disabled. Mixed-owner/shared streams are never deleted. Eligible streams are
the explicit Batch/VM allowlist plus `diagnostic-log` and `ping`, only when every
entry is attributable to a completed UI job. This does not erase audit, billing,
Monitoring history, custom log buckets, or logs that arrive after verification.
An unrelated active Batch job or compute resource blocks whole-stream deletion.
New jobs omit `logsPolicy`, so Batch task
and agent logs are not exported to Cloud Logging by default:
[Google's logging documentation](https://docs.cloud.google.com/batch/docs/analyze-job-using-logs).

## Tests

`python -m unittest tests.test_cloud_analysis` runs offline with API dependencies.
`tests.test_cloud_analysis_database` is opt-in, using only an isolated database named
`chessism_cloud_test` through `CHESSISM_CLOUD_TEST_DATABASE_URL`; it creates/drops
random test schemas. Never use the application DB for these tests.
Frontend tests: `node --test chessism_web/tests/cloudAnalysisForm.test.mjs`.
