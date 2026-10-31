# On-demand UI cloud analysis

The Analyze Positions page has two cloud selection cards below the existing
local controls: **Cloud Run** and **Batch Spot**. Both support the same
all/player/game/grouped-position selection queries.
The UI/API never receive Google credentials or a Docker socket.

## Batch Spot: fixed-size multi-VM fleet

Batch Spot is the lower-cost choice measured by the platform benchmark. Cloud
Run is also implemented in the UI, with a separate `n_cpus` control. See the
[comparison](../benchmark_platforms/README.md#first-completed-comparison-20261006ab01).

New Batch requests accept `n_vms` (1–10, default 1). Each VM is the tested
`n2d-highcpu-16`: 16 vCPUs and 16 GiB RAM. A 12-GiB worker/container budget
leaves 4 GiB for the OS/agents. Each runs 16 persistent **single-threaded**
Stockfish engines, 100,000 nodes/FEN, 256-MiB hash, and background uploads of
up to 500 results. Each new automatic cycle assigns at most **1,000,000 FENs
per VM**; each VM processes up to twenty sequential work units of at most
50,000 FENs, without releasing the VM between units.
The 300-second no-completed-FEN watchdog is independent on each VM.

The controller evenly partitions the reserved FEN list without duplicates,
uploads one shared digest-pinned image and separate shard inputs, then starts
one Batch job per VM, supervised within one cloud-sequence execution. The number
of VMs is only reduced if there are fewer FENs than requested VMs. There is a
shared work queue across the 16 engines **inside** each VM; cross-VM partitions
are static and do not promise identical finish times or cross-VM work stealing.
Each interrupted VM resumes its own checkpoints. Confirmed Google exit 50001
does not consume its two-application-attempt allowance. Recovery remains in
Google when the PC is off; local imports/cleanup resume when the PC returns.

Quota checks cover the requested fleet's available regional CPU pool, global
CPUs, instance count, external IPs and balanced-disk storage **before publishing**.
Positive dedicated preemptible CPU quota replaces the N2D pool; zero/ungranted
quota uses the standard pool, as in this project's successful tests. Google is
still authoritative about Spot capacity and quota applicability. There is no
fallback to STANDARD VMs or a larger machine. Do not treat historical quota
measurements as current limits: the preflight reads the requested fleet's
current quotas before publishing.

The durable `batch_work_units_v1` execution format preserves unit IDs, contracts,
workflow ownership and receipts across restarts. Partial launch resumes only
missing workflow owners. Cancellation reaches every unfinished shard. A manual
retry keeps successful shards and restarts only failed/cancelled shards. All
attempt records, results, control files and the shared image are cleaned
together **after every VM has succeeded and all results/reports are saved**.
Per-VM states and timing reports appear in the UI. Legacy saved single-VM jobs
keep their original configuration/recovery format.

Implementation: `cloud_job/batch_spot.py`, `cloud_job/work_units.py`, API
`work_units_controller.py`, `work_units_results.py`, and database-only
`batch_spot_retry.py`. The original `batch_spot_controller.py` and
`batch_spot_results.py` still handle saved legacy launches. Offline tests:
`tests/test_batch_spot_analysis.py`, the sharded-import database tests, and the
mocked browser test. A real multi-VM run still requires sufficient quota and a
separately requested paid test; local tests do not establish cloud performance.

The platform benchmark did **not** exercise preemption or the production
recovery supervisor. Before a long unattended run, the next recommended gate is
a separately approved small end-to-end recovery test: use Google's simulated
Spot maintenance/preemption, verify structured exit `50001`, interrupt a second
attempt as well, and confirm checkpoint reuse with the local cloud-controller
stopped. Then resume local imports and verify final cleanup. A plain process
exit or manually invented `50001` does not prove Google's preemption reporting.
Keep machine-family/VM-count cost benchmarks separate from this recovery test.

## Lifecycle

### Indexed preparation queue for Batch game selections

`game_fen_queue.py` builds the ordered, deduplicated candidate list once per job,
instead of rebuilding it for every 50,000-FEN work unit. It stores only native
FEN IDs and ordering keys in `cloud_game_fen_queue` / `cloud_game_fen_candidate`.
The ordering matches the original game-priority, move-priority and FEN-text
tie-break. A selection fingerprint rejects reuse with a different game order.

The initial build is atomic. Subsequent reads page the `(job_id, ordinal)` index
and lock bounded FEN lookups. A fresh snapshot rechecks scores and cloud claims.
Locked FENs remain queued for a subsequent attempt; positions found scored or
claimed are excluded. Queue consumption commits in the same transaction as
the validated work unit and its reservations, so failures restore the queue.
The inexpensive claim-table statistics refresh is retained. Timings distinguish
the one-time `prepare_queue_build` from `prepare_select`, `prepare_recheck` and
`prepare_queue_consume` for each unit.

The candidate snapshot is preparation scratch data, not a replacement for durable
work-unit inputs, claims or result receipts. It is discarded after freezing the
selection, on cancellation, and on terminal completion as a fallback. Existing
cloud execution, retries, import and cleanup contracts are unchanged. All-position
and player-frequency selection paths, local analysis and Cloud Run are unchanged.

Deployment is deliberately deferred while a cloud job is running. After all
cloud jobs are terminal, stop only the idle cloud controller and apply the
additive migration using the new code and normal database environment:

```bash
python -m chessism_api.operations.cloud_analysis.migrate_game_fen_queue
```

The migration refuses a running controller, a concurrent submission or any
unfinished request. Deploy/restart the updated controller only after this succeeds;
never switch a live reservation process mid-flight. No Google calls or resources
are needed for the migration. The API's schema initialization must not be used
as a substitute for this explicit rollout gate.

Validation: `tests/test_cloud_game_queue.py` and
`tests/check_cloud_game_queue_postgres.py`. The latter starts its own disposable
PostgreSQL cluster and covers parity, concurrent claims, restart/rollback,
coordinator integration, scoped cleanup, migration guards and a synthetic timing
comparison. Run it without networking, live database volumes or cloud credentials.

### Automatic cloud tasks over one frozen FEN set

Enter **Total FEN positions**, not Times. Batch still accepts at most 10,000,000
FENs overall. Each cycle holds at most `n_vms × 1,000,000` FENs. For example,
2,000,000 uses one cycle with two VMs (1,000,000 each), or two cycles with one VM.
1,963,096 with two VMs uses one cycle of 981,548 FENs per VM. Larger selections
advance to further cycles automatically. Game previews are divided, never
multiplied. Cloud Run accepts up to 6,000,000 FENs. Saved cycle records and
legacy explicit 500k loops keep their original partitioning on recovery.

The local controller freezes and reserves the entire selection in bounded 50k
transactions, publishes one shared image, and uploads **all** inputs before
launch. It uploads small immutable, checksum-bound cycle definitions and a plan.
VM specifications are sealed in separate `task-{cycle}-{vm}.json` files so each
Workflows branch loads only its own bounded definition, not the entire fleet.
Previously saved inline definitions remain readable. Deploy the updated workflow
source together with API/controller code; no new IAM permissions, engine image
build or database migration is needed for this sizing change.
One Google Workflows execution owns the sequence. It runs each cycle's VM fleet,
verifies its small completion proofs, then advances without waiting for local
imports. There is a 60-second interval between cycles for Batch's asynchronous
compute teardown; quota/capacity scheduling remains Google's responsibility.

Keep the PC on until the status API reports `sequence.safe_to_power_off: true`. This
requires a validated durable owner/state acknowledgement from Google, not just
a successful submit HTTP response. Once acknowledged, Google advances later
tasks and runs preemption recovery while the PC is offline. Batch tears down
compute as tasks finish. Inputs, results and the image remain stored (billable)
until Docker returns, catches up, verifies every receipt and performs one shared
cleanup. Normal online imports continue while subsequent cloud tasks run.

**Stop and clean up** records durable stop intent, signals the supervisor and
active compute independently of lengthy local imports, then waits for quiescence.
It imports valid completed batches, verifies receipts, cleans up owned resources,
refreshes summaries and finally releases this job's remaining reservations.
Cancelled jobs cannot restart; partial work is never marked successful. A cleanup
failure pauses safely for **Resume cleanup**. Ordinary failed jobs still retain
checkpoints for explicit recovery. The obsolete stop-after-cycle endpoint returns
HTTP 410. Confirmed preemptions do not consume the application-attempt allowance.
Unexpected workflow failure pauses orchestration rather than starting a new sequence.

`batch_sequence_v2` reuses `cloud_batch_sequence` / `cloud_batch_cycle` and adds
one small native-column `cloud_sequence_control` row per request plus an upload
flag per work unit. No new JSON columns, permanent VM, Scheduler, or IAM grant
is needed. Existing `batch_sequence_v1` jobs retain their original host-driven
behavior; already-started jobs are never converted.

Activation, with no unfinished cloud requests: deploy `recovery.yaml` to the
existing workflow/service account with labels `recovery_schema=1,sequence_schema=2`;
stop API/controller; run the additive, repeatable migration with the normal DB
environment; then start the updated API/controller together:

```bash
PYTHONPATH=stockfish-batch-worker python -m chessism_api.operations.cloud_analysis.migrate_auto_sequences
```

Tests: `test_cloud_auto_sequences.py`, `test_cloud_auto_sequences_database.py`,
legacy sequence regressions and the mocked browser fixture. Database tests must
explicitly use the disposable `chessism_cloud_test` database. Mocked tests and a
successful workflow deployment do not prove live offline cloud handoff. A small,
separately authorized two-task offline run is the next validation gate before a
multi-million-FEN request. Compute/storage/workflow charges continue while offline;
there is no total-runtime or monetary cap.

### Combined player-game drafts

In the Batch Spot card, **Combined** opens a shared draft. Choose the process-wide
VM count, select a player's games, click **Calculate**, then **Add**. Repeat for
other players or date ranges. Numbered steps can be edited (requiring a new
calculation) or removed. **Group data** recalculates after each saved change and
counts the union of unique game IDs and unique missing, unreserved FEN keys; it
does not add the individual preview counts. The union is counted in PostgreSQL,
not by loading all FENs into API memory.

Draft edits and previews are allowed while another cloud job runs; they never
launch Google work or reserve FENs. Exact game references and filters persist in
three new native-column tables (`cloud_game_draft`, `cloud_game_draft_entry`,
`cloud_game_draft_game`), independent of the temporary Redis preview token after
an entry is added. One open draft is shared by authorized superusers. Revision
checks protect against edits from stale tabs. At most 100 selections are allowed
per draft; the existing 10-million-FEN job limit is unchanged.

Submitting requires a current combined preview, an online host controller, and
no unfinished cloud job. The server rechecks eligible FEN counts; a lower count
is accepted, but an increase requires a fresh preview. Submission is idempotent
by draft ID and atomically records a normal `games` / `batch_sequence_v2` job and
its source draft. That existing controller reserves and freezes the selection,
uploads all inputs, and runs cycles of at most 1,000,000 FENs **per VM** (the final
cycle can be smaller). Changing VM count updates the displayed cycle breakdown
without deleting, reselecting or invalidating the saved draft. One FEN can
benefit many games/players, with no duplicated
engine work. Existing 50k work units, 500-result files, offline handoff, Google
preemption recovery, incremental import and verified final cleanup are unchanged.

Activation: run `python -m chessism_api.operations.cloud_analysis.migrate_game_drafts`
with the normal database environment, then deploy the API and frontend. The
migration creates only the three draft tables and is safe during an existing
cloud run. No controller restart, worker image upload, Google deployment or IAM
change is needed. Keep the PC on until `sequence.safe_to_power_off` is true for
each newly submitted job, just as before.

Tests: `test_cloud_game_drafts.py`, `test_cloud_game_drafts_database.py` (only the
isolated `chessism_cloud_test` DB), `cloudGameDraft.test.mjs`, and the mocked
`test-cloud-analysis-browser.mjs` fixture. None starts paid cloud work.

### Import and cleanup lifecycle

For automatic sequences, the following import/verification steps run per task,
but shared cleanup and final global-summary refresh happen once for the whole
request. They do not block Google from starting its next cycle.

The Compose controller runs on your PC using your existing `gcloud` login:

1. The authenticated UI records a bounded request in PostgreSQL.
2. The controller selects unscored FENs using `FOR UPDATE SKIP LOCKED`, inserts
   durable cloud reservations, and commits **before uploading anything**. Batch
   requests reserve at most 50,000 per unit, up to 1,000,000 per VM per cycle
   for new automatic sequences; older explicit 500,000-FEN loops retain their
   saved partitioning. Cloud Run retains
   a 6,000,000-FEN whole-request bound with at most 5,000 FENs per task.
3. Before fleet creation, it verifies that earlier Batch jobs, compute resources,
   repository images and live/noncurrent bucket objects are gone. Unexpected
   leftovers pause the job before upload; they are not blindly deleted because
   they may still be needed for another job's recovery. It then uploads an already-built worker image into a unique, job-owned Artifact
   Registry package and starts the [cloud-resident recovery workflow](RECOVERY.md).
   It starts the VM shards concurrently, each with a digest-pinned Batch Spot
   task and separate counters for Google preemptions and application failures.
4. Every 15 seconds it checks for completed background uploads (500 results per
   file; the final partial group is also uploaded). Up to four downloads overlap
   with one transactional database writer, fairly across task shards. It validates
   input identity, engine/settings contract, legal PVs, IDs and checksums before importing.
5. FEN updates, continuations, database counters, imported-file receipts and
   reservation releases commit in **one transaction**. Results have exactly the
   existing `analysis_source='stockfish'` and normal score/PV/WDL fields.
6. After Batch succeeds, the recovery workflow and any duplicate executions stop,
   and the final manifest exactly matches committed receipts,
   a cleanup plan is persisted in the DB before deletion. `cleaning_job` removes
   job metadata, checks that its compute
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
9. Only after cloud cleanup does local game-summary refresh run. Completed run
   details are compacted; global score/rating summaries refresh once per request.
   The UI reports completion only after these local phases also finish. Ready
   phase transitions proceed immediately; the 15-second interval applies to
   waiting for external progress, not every step.

Cleanup optimization (2026-10-08): independent inventories use up to four
readers; nested readers share an eight-request bound per REST client. Log
planning, whole-plan validation and post-deletion verification each combine
the exact allowlisted names into one paginated
[Google Logging OR query](https://docs.cloud.google.com/logging/docs/view/logging-query-language).
There is still no time/instance filter that could hide unrelated entries.
Each stream retains its fresh project-idle and ownership checks immediately
before deletion. Job UID, object-generation and image-upload identity checks
remain mandatory; inventories are not cached across deletion boundaries.
Incomplete scans and failed readers abort, after all in-flight readers drain.
Existing saved plans and retries use the same format; no migration is needed.

For the previous test's five deletable streams plus two retained streams,
the mocked lifecycle now needs eight logical log scans instead of 22 (actual
page counts depend on volume). A read-only Google comparison across all nine
allowlisted names returned identical 186-entry classifications in 2.052 seconds
combined versus 11.068 seconds separately. This measures reads only, not a new
end-to-end cleanup run. Recovery-workflow polling remains 60 seconds and the
local waiting interval remains 15 seconds; no extra workflow step usage or new
IAM permissions are introduced. Cleanup-only changes require rebuilding the
host controller, not publishing a new Stockfish engine image.

## Column-based history and bounded ingestion

Cloud records use native PostgreSQL columns/typed arrays and explicit child
tables, including launch, recovery and cleanup instructions. There are no JSON
or JSONB columns in the cloud tables. Dictionary/JSON representations still exist
at the Google/HTTP/file boundaries; the relational adapter verifies a lossless
checksum when reading recovery documents. Unknown fields fail closed.

`cloud_result_batch` retains one compact receipt per result file (normally up to
500 FENs), not one history row per FEN. Task boundaries can add partial files.
For example, one million FENs in full files produces 2,000 receipt rows. Input
identities and result hash lists are stored in temporary packs of up to 500;
they are pruned only after verified import, cloud cleanup and local summaries.
Failed/recoverable runs retain those packs. Actual analysis stays in the normal
FEN tables and is never duplicated in usage history.

`cloud_phase_timing`, `cloud_usage`, and `cloud_execution_attempt` preserve phase,
worker and provider-attempt measurements. Allocated VM memory and worker memory
budget have distinct columns. Missing historical measurements remain NULL;
worker runtime is **not** billed duration, and costs remain NULL until sourced
estimates or billing reconciliation are added. The authenticated
`GET /analysis/cloud/{job_id}/usage` route returns this local history without
calling Google. Download-time sums represent overlapping I/O, not elapsed time.

The importer keeps at most four pending downloads plus the current result file
(up to 80 MiB of raw result bytes). Scores, counters, reservations and receipt
commit atomically. A crash before COMMIT rolls back; after COMMIT, redelivery
verifies the immutable receipt and cannot double-count results. Each transaction
reads only the selected input packs, not the entire growing receipt history.

For installations using the former JSON cloud columns, stop the API and
cloud-controller before running the explicit local migration with the normal
database environment and worker package path:

```bash
PYTHONPATH=stockfish-batch-worker python -m chessism_api.operations.cloud_analysis.migrate_columns
PYTHONPATH=stockfish-batch-worker python -m chessism_api.operations.cloud_analysis.migrate_columns --apply --compact-completed
```

The first command is read-only. The second rejects active jobs, checksum-checks
all documents inside a transaction before dropping old columns, backfills
compact receipts/available metrics, and optionally prunes verified completed
history. Repeat execution is safe. Start only the updated API/controller after
the migration; old code expects the removed columns. Neither command starts or
modifies Google resources.

For the initial column schema's pending-cleanup report correction, stop the API
and cloud-controller and run the migration command with
`--repair-cleanup-resources` instead of `--apply`. This adds typed `kind`, `scope`
and `name` child records, verifies saved document checksums and removes only the
obsolete empty-array columns. It supports an already-paused cleanup run without
changing its phase, imported results or cleanup targets. Resume that same job
after restarting the updated services; no reanalysis is needed.

Each VM's FENs feed one shared queue and 16 persistent single-thread engines. A
faster engine takes the next available FEN rather than waiting for a fixed allocation.
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
The UI disables both cloud selection fieldsets while the cloud slot is occupied,
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

## Bounded work units within large Batch requests

The `batch_work_units_v1` implementation underpins the automatic sequences above:
each user request is internally split into units of at most **50,000 FENs**,
with **500-result files**.
A 500,000-FEN request with one VM produces ten sequential container runnables
inside one Google Batch task, on the **same n2d-highcpu-16 Spot VM**. More VMs split
the selection evenly first; each VM runs its own ordered list of bounded units.
The engine profile, 16-GiB VM/12-GiB container budgets, and five-minute analysis
inactivity watchdog are unchanged. Cloud Run's whole-request limit is 6,000,000 FENs.

Every input is reserved and uploaded before any supervisor is started. Once all
supervisors are running, Google schedules the remaining units even if the PC is
off. A preempted VM replays its ordered runnable list: `--skip-completed-unit`
recognizes a completed unit by its matching immutable input/engine contract,
completion manifest and performance report; unfinished units resume their saved
500-result files. Missing or conflicting completion metadata never permits
silent success. Existing Google-owned 50001 recovery and the two application
attempts per VM remain in force. The million-FEN-per-VM change requires the updated
workflow source but no additional IAM grants. Container transitions add some overhead and are measured by Batch time,
not the sum of successful unit worker timings.

The parent record contains only a small unit index/proofs, not 500,000 FENs or a
single giant manifest. Units use the existing bounded input/receipt tables, plus
two native-column tables (`cloud_batch_unit`, `cloud_control_work_unit`); no JSON
database columns are introduced. Incremental imports update the parent/UI count
atomically with the job and FEN rows. Offline catch-up loads one unit at a time.
Final verification matches every unit's durable proof, committed batch counts
and released claims. **No unit files are deleted early.** All VM supervisors must
stop before shared job/image/data cleanup. Object deletion uses at most eight
generation-pinned requests concurrently, and remains resumable on error.
Affected-game summaries run once after cloud cleanup; temporary local FEN detail
is then compacted one unit per controller tick. The UI reports done only after
cleanup and local summary finalization.

Activation: with no queued/running/paused/failed cloud work, stop the API and cloud
controller, run `python -m chessism_api.operations.cloud_analysis.migrate_work_units`
with the normal database environment, then rebuild/recreate the API, controller
and cloud-worker-image (worker **1.5.0**). The additive migration refuses active
cloud requests and is repeatable; it does not create Google resources. Deploy the
updated frontend with the API. Old <=200,000-FEN launches retain their saved mode.

Preparation refreshes statistics for the small `cloud_fen_claim` table before
each unit (including after a restart), avoiding stale empty-table estimates after
the previous unit commits thousands of reservations. The main `fen` table is not
analyzed. Selection still locks rows with `SKIP LOCKED`; a separate READ COMMITTED
recheck excludes newly scored/claimed FENs using bounded 5,000-element array joins.
Input validation and atomic claims/parent updates are unchanged.

Read-only cloud inventories run at most four at a time, then quota/recovery/bucket
checks run concurrently. All checks must succeed before local image inspection or
any cloud write. Preparation records compact per-unit substep timings in the existing
native-column `cloud_phase_timing` table (statistics, selection, recheck, encoding,
validation, persistence, claims, commit). Timing writes happen only after releasing
reservation locks and never invalidate committed work. No new migration is needed
for these optimizations. A 10,000-FEN request uses one unit on one VM and exercises
the same preparation path; the stale-statistics multi-unit case also has an isolated
PostgreSQL regression test in `test_cloud_work_units_database`.

Offline tests: `tests.test_cloud_work_units`,
`stockfish-batch-worker/tests/test_work_unit.py`, and the opt-in
`tests.test_cloud_work_units_database` suite use fake cloud calls; the latter
requires the same isolated `chessism_cloud_test` database as existing DB tests.
These do not replace the next user-launched, paid 500,000-FEN integration test.

## Bounds and recovery

- Previously saved Batch requests (`execution_mode=batch_multi_vm_v1`) split up to 200,000
  FENs across the selected `n_vms` (1–10, subject to available quota).
  Loop/group counts multiply the selection size, not the VM count.
  If some selected FENs are unavailable at reservation, the existing
  `limit_reached` status reports that fact without launching an extra VM. Already
  saved legacy requests keep their original chunk/recovery contract.
- Each Batch VM is `n2d-highcpu-16` with 16 GiB RAM, a 12 GiB worker budget,
  and sixteen single-thread Stockfish engines, 256 MiB hash each, MultiPV 4,
  no tablebases. Every new UI run
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
  or over-limit selections (10,000,000 for automatic Batch sequences; 6,000,000 for Cloud Run) are rejected,
  never silently truncated. Select
  fewer games and preview again if the existing safety limit is exceeded.
- Closing the browser has no effect on the host controller. Stopping the controller
  does **not** cancel a running cloud task. Restart it to resume imports/cleanup.
  The Workflows supervisor continues retry decisions while the PC is off.
  `Stop and clean up` forwards durable cancellation to the supervisor and compute;
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

Code cleanup is separate from deleting a completed cloud job's resources.
The API's `cloud_analysis/artifacts.py` owns the bounded completion-manifest and
performance-report reads for single-VM, fleet, Cloud Run and work-unit paths.
Only an absent object returns `None`; malformed JSON and storage/permission
failures abort. Each backend still validates its own contract, committed receipts
and recovery state before authorizing cleanup. Reading a report is not proof of
successful database import.

Keep the saved-format dispatchers and migration commands: legacy single-VM,
`batch_sequence_v1` and current `batch_sequence_v2` jobs have different durable
recovery formats. Old benchmark folders are retained intentionally for comparison.
Do not prune those modules, pending downloads, checkpoints, credentials or job
history merely because no job is currently running.

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

## Cloud Run backend

The analysis page has two cloud selection cards: **Cloud Run** and **Batch Spot**.
Both cards are available; Batch Spot supports independently recoverable VM shards.
Existing unfinished Batch jobs and their recovery controls remain visible.
The Cloud Run card offers one FEN-selection dropdown for all positions, player
positions, player games (with a frozen preview), or grouped selections. Only
missing, unreserved FENs are selected. Changing the selection invalidates its
preview; changing CPU concurrency preserves it. Choose `n_cpus` (1–200, default 4)
for the maximum simultaneous **one-vCPU, 1-GiB tasks**. Small selections
can use fewer CPUs. The input limit is 6,000,000 FENs per UI request, including
combined selections. UI validation, both submission endpoints, and the controller
task planner enforce the same bound. Task inputs remain capped at 5,000 FENs
(at most 1,200 tasks for a maximum-size request); worker memory/CPU settings,
checkpoint size and retry/cancellation policies are unchanged. The
controller verifies regional CPU/memory quota before uploading; quotas are shared
with other Cloud Run workloads and do not guarantee capacity or immediate start.

Worker release 1.5.1 shares the 1,200-task bound with the planner and job-spec
validator. The old worker's 400-task guard rejected larger selections even when
the planner accepted them. Deploy the matching controller AND worker image;
the worker-version preflight rejects stale local images before publishing.
Regression tests initialize every planned task (including the 753-task,
3,760,444-FEN case and the 1,200-task maximum), not just the partition counts.
Preparation failures with no published launch can resume their existing
reservation after this update; do not recreate or duplicate the job.

The local controller still materializes whole-run Cloud Run metadata. Its Compose
memory ceiling is 12 GiB (previously 2 GiB); this is an allowance, not a reservation.
The five-CPU budget is unchanged. A future streaming-metadata refactor can reduce
that local overhead independently of the selection limit. Boundary tests cover
3.7 million, exactly 6 million, and rejection above 6 million; these are not an
end-to-end billable benchmark of a 6-million-FEN run.

Finalization avoids duplicate whole-run snapshots: scalar status updates load
no payload documents; successful imports refresh only changed receipts, and
normal compaction never reloads the positions/receipts it is about to release.
Receipt SQL snapshots decode in groups of at most 32 batches, and canonical
checksums stream the same JSON bytes instead of allocating whole-document text
and bytes copies. Integrity checks and exact-result/cleanup gates are unchanged.
This reduces peak memory, but is not a fully streaming whole-run representation.

Upload-step counters are read-only. Automatic Batch sequences use the saved
`uploaded` work-unit flags. Cloud Run lists only completed input-object names
under the exact run prefix through the isolated `cloud-billing` reader, then
maps those task indices to their saved FEN counts (including the short final
task). No FEN contents are downloaded and no job is changed. The reader caches
observations for ten seconds, bounds its queue, and returns immediately while a
refresh is pending. Unknown/unavailable observations remain indeterminate, never
zero or a time-based estimate. The authenticated `/analysis/cloud/{job_id}/upload-progress`
endpoint reads only small native counters; it does not load positions/receipts.
Only an active upload requests telemetry. Completed upload rows retain their
confirmed totals in the progress tooltip. Existing per-step progress/detail
Edit styles targets control the counter and bar. API/reader updates do not
require restarting a running cloud controller.

The UI shows each workflow step from reservation through uploads, analysis/import,
verification, cleanup and the final database-summary refresh. A successful Google
execution or 100% imported FENs alone does not mean the job is done. Once the
controller confirms completion, progress remains until **Reviewed — dismiss
progress** is clicked. Acknowledgements are saved in this browser; previously
unseen old terminal jobs are not resurrected on first load. Ingestion results
use the same explicit-review behavior. This is display-only: DB results, job
records and performance reports are not deleted.
Failed, paused and waiting jobs stay visible for recovery, including cleanup errors.

The status API uses one read-only, repeatable-read snapshot and batches
display-only reads across jobs and runs, excluding saved
game-link arrays and unrelated task/identity payloads. These projections are not
recovery documents: controller reads still validate complete checksums. The UI
has one current status request, cancels superseded reads, pauses periodic fetches
in hidden tabs, and backs off on errors while retaining the last visible progress.

Cleanup verification (October 2026): with the existing 34-job history, local
read-only status checks fell from 455 to 19 SQL statements. Three consecutive
calls measured 0.580/0.447/0.405 seconds before and 0.167/0.086/0.058 afterward;
the canonical job-response SHA-256 was identical. This measures status reporting,
not cloud analysis throughput. Regression coverage includes offline cloud/import
tests, isolated PostgreSQL lifecycle tests, frontend unit tests, and mocked
browser checks for combined selections, stop/review, and ingestion review.
No billable jobs were launched during this cleanup; the live Cloud Run stop
exercise remains a separate operator test.

Both production backends and local bulk database analysis use the shared
`stockfish_core` profile: the pinned Stockfish 16.1 AVX2 binary and embedded NNUE,
`chess==1.11.2`, 100,000 nodes, Threads=1, Hash=256 MiB, MultiPV=4, WDL on, no
tablebases, and a fresh game token/reset between independent positions. No search
time budget substitutes for the node limit. Five minutes without a completed
FEN fails the worker; partial searches are not accepted as results. Existing
checkpoint contracts and historical DB rows are not rewritten. The interactive
live board keeps its existing non-persisted node/MultiPV request settings.

The controller reserves the complete selection before launch and splits it into
disjoint 500–5,000-FEN task inputs (the last can be smaller). Each task runs one
engine and uploads every 500 results, plus its final partial file. Inputs/results
live under `inputs/ui-<run>/tasks/<index>/` and `results/ui-<run>/tasks/<index>/`.
One Cloud Run execution schedules all tasks; the PC is not required during
analysis. Each task has one automatic retry and resumes its own immutable files.
Cloud Run's seven-day per-task ceiling is a platform limit, not a per-FEN budget.
The Batch Spot 50001 recovery workflow is not used by Cloud Run.

The local controller imports independently available files from every task,
validates checksums and original FEN identities, commits normal DB values and
receipts atomically, and then releases those FEN claims. On restart it catches up.
Final cleanup requires every task manifest, all committed receipts, and saved
per-task performance reports. Local DB summaries refresh after cloud cleanup.

### Bounded completion I/O and timing comparisons

Cloud Run verifies up to four task manifest/performance pairs concurrently, with
one Storage/HTTP client per reader. Legacy multi-VM Batch uses the same verifier
while retaining its per-shard proofs. Modern Batch work units download the
manifest and performance report together using two independent clients; units
and their database commits remain serialized and bounded to 50,000 FENs.
All existing manifest/receipt, fingerprint, performance and cleanup gates remain.
An I/O error drains the bounded in-flight work and withholds completion.

Both import paths skip contract downloads for fully committed tasks on later
polls, without skipping their final manifest checks. Both cleanup paths list up
to four exact run-owned prefixes concurrently. File deletion uses a rolling queue
of four JSON API batches, each containing at most 100 generation-pinned DELETEs,
with one persistent HTTP session per worker. Fresh ownership/UID checks still precede
deletion, and image/log cleanup still follows successful data/compute cleanup.
No VM sizing, engine profile, retry policy or cleanup scope changes are involved.

Cloud Run now uses the saved-input set-based affected-game lookup already used
by Batch. Absolute game/player summary refreshes remain retry-safe; full global
summaries still run at the existing lifecycle boundary. Verification and summary
I/O changes need only a local controller release. Batched-deletion progress also
needs the small additive table and API release described below; remote workers
and workflows are unchanged.

The batch-deletion transport is also shared by stop-and-cleanup. It parses every
multipart sub-response: an HTTP 200 envelope alone never acknowledges deletion.
Only successful/absent exact versions advance the counter; transient failures or
lost responses retry the remaining pinned versions with bounded backoff. A 401
can refresh authentication once; permission/generation conflicts fail closed.
Replacement objects, unrelated resources, bucket policies and audit logs remain
protected. Batching reduces HTTP overhead, not Google's logical operation count.

Cleanup progress is saved in `cloud_cleanup_progress`, one compact row per
job/plan, and returned by the existing cloud-jobs endpoint. Apply this additive
table before releasing the API/controller (normal schema initialization creates
it on a fresh installation). The UI reuses its existing editable step meters and
shows acknowledged file counts, then image/log status. Counters never authorize
deletion: resumption rechecks the saved plan against current cloud inventory.
Overlapping cancellation plans are displayed separately, never summed.

An explicitly authorized 512-file-per-case microbenchmark on 2026-10-28 UTC
measured the old eight-request waves at 55.733 s, two 100-file batches at 6.016 s,
and four at 3.928 s (six HTTP requests for each batch case, no retries). All 1,536
tiny benchmark files were removed and their random prefix verified empty.
This supports the four-batch default, not a 14x end-to-end cleanup promise:
compute deletion, ownership checks, images and logs have their own latencies.
`scripts/benchmark_cleanup_deletion.py --execute` is a live, opt-in test; it must
not be included in offline suites or run without authorization.

Detailed durations are buffered into existing `cloud_phase_timing` rows:
`verify_task_artifacts`, `verify_unit_artifacts`, `verify_unit_commit`,
`cleanup_preconditions`, `cleanup_run_job` / `cleanup_batch_compute`,
`cleanup_objects`, `cleanup_image`, `cleanup_logs` (Cloud Run),
`summary_affected_games`, `summary_game_coverage_and_players`, and
`summary_compaction` (non-unit runs). These are substeps inside the existing
lifecycle phases, not additional elapsed time: do not sum parents and children.
Repeated cleanup attempts retain separate timing rows. Provider deletion waits
and polling gaps remain part of the parent phase's wall time.

For comparisons, keep FEN count and requested CPU/VM count constant and compare
import completion, verification, cleanup and local-summary times separately.
Different FENs and Google scheduling can change analysis duration independently
of local improvements. Offline checks do not establish live speedups.

Cleanup deletes only the known
terminal Cloud Run job, exact object generations and the job-owned image. Shared
logs, other jobs/services/revisions, and audit/billing records are protected.
Google can retain deleted-resource tombstones and delayed log entries; cleanup
does not promise to erase provider history. Failed work retains results for an
explicit billable retry (maximum three executions per logical run); explicit
stop-and-cleanup instead salvages completed results and retires the request.

`jobs:run` has no request-id/idempotency field. Before posting, the controller
commits a durable submission fence. If the response is lost, it discovers the
execution instead of submitting again. If no execution can be found, the job
pauses safely for inspection; **do not reset its phase or create another job
blindly**. Resume only repeats discovery. Cancellation is a durable UI request
forwarded to the known execution. Checkpoints are removed only after compute
stops and all valid completed batches have been imported and verified.

### Activation and first test

No IAM grants, remote deployment, or paid test is performed by building the code.
Use an authenticated controller identity with Run job create/get/list/run/delete,
execution get/list/cancel, operation get, quota read, and global Run inventory read
(jobs/executions/services/revisions). It must act as the existing worker service
account. Existing Storage/Artifact Registry/log-cleanup access remains necessary.
Worker credentials stay attached to the task, never in the image or browser.
Missing permissions fail closed; request scoped grants separately if needed.

With no active or queued analysis work, rebuild the API, local workers, frontend,
cloud-controller and cloud-worker-image together using Docker Compose. Worker
version 1.5.0 is checked before publishing. Do not mix old local engines with the
new profile. Building alone does not update running containers.

Then use the Cloud Run card, start with **1,000 missing FENs and
2 CPUs**, and inspect incremental progress, local import and cleanup. This next
step is billable and must be explicitly requested. Test controller restart/offline
catch-up and retry using a separate bounded workload before a long run. The
offline suites `tests/test_cloud_run_analysis.py` and
`stockfish-batch-worker/tests/test_cloud_run_cleanup.py` cover the safety boundaries
without credentials; they do not replace a real provider integration test.
