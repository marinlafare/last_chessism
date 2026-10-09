# Stockfish Batch worker

Standalone, finite analysis worker for Google Cloud Batch + Spot or Cloud Run Jobs. It does **not**
start a web server, connect to Postgres/Redis, import results into Chessism, or
start the existing `stockfish-service/`. Building or testing this folder does
not submit a cloud job. Cloud execution requires an explicit manual submission or
a UI request handled by the [Compose controller](cloud_job/README.md).

## Layout

- `stockfish_batch/`: input checks, persistent engine pool, immutable checkpoints,
  local/GCS storage, CLI and signal handling.
- `stockfish_core/`: shared local/Batch/Run research profile and UCI search setup.
- `Dockerfile`, `requirements.txt`: pinned Python base, runtime dependencies,
  official Stockfish 16.1 Linux amd64 AVX2 binary verified by SHA-256.
- `scripts/fetch_engine.py`: build-time download only; keeps engine source/license.
- `scripts/render_job.py`: validates image/input/output references and prints JSON;
  makes no cloud calls and never submits a job.
- `benchmark_fen_batches/`: all four upload-benchmark scripts, separated from the
  reusable worker: dataset preparation, job rendering, trial execution and audit.
  See [its README](benchmark_fen_batches/README.md) for entry points.
- `cleaning_job/`: reusable host-side cleanup, with previews and unattended
  execution after the local DB commit; removes temporary job data and unused
  job images while keeping the empty bucket/repository and permissions. Broad
  log/orphan-image cleanup is a separate mode.
  See [its README](cleaning_job/README.md). The cloud controller invokes it
  automatically only after verified final imports commit.
- `cloud_job/`: generic bounded launcher and host-controller setup instructions.
  The durable UI/controller/importer lives in
  `chessism_api/operations/cloud_analysis/`; it shares normal local FEN persistence.
- `scripts/preemption_test.py`, `scripts/recovery_support.py`: explicitly opt-in,
  bounded cloud interruption/recovery test; `--prepare` is local-only, while
  `--execute` submits billable jobs and interrupts its verified first test VM.
- `batch/smoke-test.json`: template, not directly runnable before rendering.
- `examples/input.jsonl`: 20 synthetic positions including both sides to move,
  mate, stalemate, promotion, en passant, castling and a repeated FEN.
- `tests/`: offline automated tests plus an opt-in real-engine test.

## Runtime contract

Each input line contains exactly these fields:

```json
{"id":"stable-position-id","fen":"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"}
```

IDs must be unique strings using letters, digits, `_` or `-` (1–128 characters).
Repeated FENs with different IDs are preserved, not deduplicated. Both full
six-field FENs and Chessism's four-field database position keys are accepted.
For four-field keys, python-chess supplies halfmove/fullmove counters `0 1`
for the engine, exactly as the local service does. Original FEN strings remain
unchanged in input IDs, checkpoints and results so imports use the existing DB
keys. Six-field inputs retain their supplied counters. All input is validated
before any search or output writes. Invalid chess positions, other field counts,
duplicate IDs, empty inputs and oversized inputs fail the whole task.
The default cap is 20 positions. Standard modes support up to 1,000; the UI's
`--compact-results` background-batch mode supports up to 200,000 per invocation.
The application supports up to 500,000 FENs per Batch request by scheduling
bounded 50,000-FEN invocations sequentially on each VM, retaining 500-result
files. See [large Batch requests](cloud_job/README.md#large-batch-requests-up-to-500000-fens).
Input is capped at 96 MiB and completion manifests at 80 MiB. Compact mode keeps
only committed references in memory and writes a `performance.json` summary for
local preservation before cloud cleanup.
A FEN does not contain game history, so historical repetition cannot be inferred.

Default analysis: 4 independent engines, 1 thread each, 2,048 MiB hash each,
1,000,000 nodes per position, MultiPV 4, WDL on, no Syzygy tablebases.
Each engine stays alive across positions, but receives a new-game token for each
FEN so search hash state is not carried between unrelated positions.
Python orchestrates the engines; Stockfish's compiled binary does the search.

Those standalone CLI defaults are retained for legacy benchmark scripts. New UI
jobs explicitly use the shared research profile: **100,000 nodes, Threads=1,
Hash=256 MiB, MultiPV=4**, WDL on, no Syzygy, identical verified Stockfish 16.1
binary and `chess==1.11.2`. Local bulk DB requests enforce this profile too;
the interactive live board retains its separate non-persisted request budget.
Old saved cloud runs retain their checkpoint settings; historical DB results are
not rewritten. Hardware-dependent timings are not part of reproducibility.
See [Cloud Run operation and activation](cloud_job/README.md#cloud-run-backend).

Successful records retain the existing local service's nested result shape:

```text
id, fingerprint, elapsed_ms, result_sha256,
engine_result: {fen, is_valid: true, analysis: [...]}
```

Scores and WDL are from White's perspective. Mate uses the existing `mate_score=10000`
mapping. Nonterminal analysis is a MultiPV list containing scores, legal UCI PVs,
WDL, search node counts and available engine timing/depth information. Terminal
analysis is `{pv: [], score: ...}`, as in the local service. Search errors are
failures, **not** completed records with an `analysis.error` placeholder.
The opt-in [UI host controller](cloud_job/README.md) validates these records and
imports them through the same FEN persistence helper as local analysis.

## Checkpoints and recovery

For an output prefix `gs://.../results/smoke-001/`:

```text
contract.json          input SHA-256, engine binary SHA-256, library/worker versions,
                       analysis settings and their combined fingerprint
positions/<id>.json    one validated, immutable result per position
manifest.json          created only after all expected results are durably saved
```

Progress JSON logs count **saved** results, not just finished searches. Batched
mode emits one `saved` event per committed batch, with `batch`, `batch_positions`,
and cumulative `saved`/`total`; single-position mode keeps its per-FEN events.
Logs include resumed/new counts, overall attempt wall time and resource
settings. Per-FEN timing and engine search/node information remain in each result. Neither
these timings nor Batch's RUNNING duration are a billable-time estimate.

Re-run with exactly the same input bytes, engine binary and analysis settings,
and the same output prefix to resume. The worker version must also match.
Worker parallelism and timeouts may change;
nodes, MultiPV, threads, hash and engine version may not. Use a new output prefix
for changed inputs/settings. A damaged or incompatible checkpoint fails closed.
An already completed run verifies its checkpoints and starts no engines.

Cloud writes use `if_generation_match=0`: no overwrites or deletes are required.
Creation races read and validate the winning checkpoint. Losing a Spot VM may
repeat in-flight work; **only committed positions are skipped** on the next run.
SIGTERM/SIGINT cancel work and close engines; abrupt termination is also safe
because each checkpoint is independent. Network calls have finite timeouts/retries;
an in-flight upload can finish after cancellation and is reusable on the next run.
The first cloud template deliberately has zero automatic task retries. Manual
checkpoint recovery has been verified in Google Cloud after a maintenance-induced
VM recreation (`50006`): 104 records reused and 96 newly analyzed. This is not
an automatic-retry test, and the strict Spot-preemption code (`50001`) was not observed.

Version 1.1.0 additionally supports background uploads and bounded batch objects;
the original per-position blocking mode remains the default. Use fresh prefixes
for the new version. The historical recovery controller stays pinned to the
original 1.0.0 image and contract.

## Background uploads and output batches

The optional CLI settings are `--upload-mode background`, `--batch-size N`
(1–500), and `--upload-queue-size N` (1–128; default 64). Blocking mode requires
batch size 1. Engines always pull individual FENs from one shared queue. A batch
size of 500 **does not assign 500 positions to one CPU**: it groups saved output
from all engines, while a separate uploader lets analysis continue.

For batch sizes above 1, consecutive input records define stable groups:

```text
contract.json          includes checkpoint_format and batch_size
batches/000000.json    fingerprint + ordered records for the first group
batches/000001.json    next group (the final group may be smaller)
manifest.json          one entry per input ID, pointing to its batch object
```

Groups upload when all their members are ready. Results from later groups may
finish first; they stay in a bounded buffer until their group is complete. The
final partial group is uploaded too. A committed batch is immutable and validated
as a whole on resume; an unsaved group is reanalyzed after interruption. Completed
but uncommitted work can exceed one batch across all engines. Changing batch size
requires a new output prefix. The production incremental importer verifies and
imports this same batch format.

Uploads have a bounded queue (backpressure rather than unlimited memory growth),
a 256 KiB per-record limit, 16 MiB per-batch limit, and 64 MiB pending-batch limit.
An upload error fails the attempt, cancels sibling work, and never marks unsaved
positions complete. A completed manifest requires every expected durable result.

The compact production path now has separate collection, preparation and upload
stages. Preparation validates new results once (including all legal PVs) and
serializes immutable upload bytes. Two raw batches and two prepared batches can
queue between stages; both queues apply backpressure. Pending incomplete groups
remain capped at 64 MiB of serialized result payload. These are payload bounds,
not exact Python heap bounds. Preparation runs off the async event loop; network
uploads do not hold up preparation. Recovered files and create-conflict winners
still receive full checksum/PV validation. Compact references, immutable writes,
checkpoint/manifest formats and analysis settings are unchanged. The completed-FEN
300-second watchdog remains independent of upload activity.

`performance.json` includes `metrics.result_pipeline`: queue peaks, pending
payload high-water mark, preparation/commit time and saved-batch event count.
CPU utilization and producer wait time must still be measured in a fresh cloud
benchmark; faster offline validation alone does not prove higher VM utilization.

### Offline RAM and pipeline probe

`benchmark_vm16/profile_pipeline.py` replays 1,000 saved result records through
the checkpoint pipeline and measures Python input/queue/reference/manifest RAM
at 25,000, 100,000 and 200,000 FENs in fresh subprocesses. It makes no cloud calls
and runs no Stockfish searches. Repeated real FENs get unique 64-character IDs;
this measures bookkeeping growth, not whole-VM RAM or chess throughput.

```sh
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python \
  stockfish-batch-worker/benchmark_vm16/profile_pipeline.py \
  --source stockfish-batch-worker/out/vm16-20261006c016
```

For a fair before/after comparison use the same Python runtime/container resource
limits and the saved old image with `--cpu-mode legacy`. Reports from the October
6 local comparison are under `out/pipeline-memory-20261006/` (git-ignored).
The local `chessism-stockfish-batch:pipeline-v2` tag contains the optimized worker.
Promoting this image to the Compose `chessism-stockfish-batch:fen-compat-v1` tag
makes new UI launches use it without changing their CPU/RAM/hash defaults. Saved
launches remain pinned to their existing image ID/digest. No source change or
local image build launches a cloud benchmark by itself.

### Useful 200,000-FEN sizing and recovery test

`benchmark_vm16/large.py` prepares and submits one `n2d-highcpu-16` Spot VM
with a two-hour task ceiling and no automatic task retries. Two sequential
containers use the same input/output contract. The first supervisor kills only
its own analyzer process group after 25,000 saved FENs; the second container
resumes from immutable checkpoints. This tests process-loss recovery, not VM
recreation or the Google Spot `50001` signal. It runs without the host PC after
submission. Neither test phase calls the production recovery workflow.

The user-approved useful-work variant reserves 200,000 unscored/unclaimed FENs
through the app's normal selection order. A paused, terminal-managed database job
blocks competing cloud jobs; `CloudFenClaim` excludes these FENs from local
analysis. Do not use the UI's Resume button for this dedicated benchmark.
The durable plan stores the database job/run IDs before reservation or cloud
publication. Existing analyses are not overwritten.

From the repository root, with a NEW output directory and identity:

```sh
PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large prepare \
  --id 20261006d200 --directory stockfish-batch-worker/out/vm200-20261006d200
PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large launch \
  --directory stockfish-batch-worker/out/vm200-20261006d200
watch -n 30 'PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large status --directory stockfish-batch-worker/out/vm200-20261006d200'
```

After success, `collect` with the same directory downloads and verifies every
batch, final manifest and recovery evidence, then uses the normal transactional
`import_batch` path to persist results/continuations/counters/receipts and release
claims. Imports are idempotent; changed checkpoints or preexisting scores fail
closed. `import-receipt.json` is written only after complete imports and derived
view refreshes succeed. Keep cloud results until this receipt and database
receipts are verified, then perform scoped cleanup and finalize the paused job.
Use these separate, resumable commands (no new compute is submitted):

```sh
PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large collect \
  --directory stockfish-batch-worker/out/vm200-20261006d200
PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large_finish inspect \
  --directory stockfish-batch-worker/out/vm200-20261006d200
PYTHONPATH=stockfish-batch-worker:. .venv-cloud/bin/python -m benchmark_vm16.large_finish cleanup \
  --directory stockfish-batch-worker/out/vm200-20261006d200
```

`inspect` saves timing, memory, VM-lifecycle and warning-log evidence locally.
`cleanup` rechecks the import receipt, database batch receipts, all 200,000 scored
rows and absence of claims before deleting anything. It reuses the normal
generation-scoped cleanup and removes only log streams proved wholly owned by
this benchmark. Only verified cleanup marks the paused database job complete.
Empty shared infrastructure and Google-required audit/billing history remain.
If `collect` is still importing/refreshing summaries in another terminal, use
`large_finish wait-cleanup` with the same directory instead of `cleanup`. It
waits locally for at most 30 minutes for matching local/DB import receipts,
then runs the same guarded cleanup. Watch `finish-status.json` in that directory;
`complete` means both cloud cleanup and database finalization succeeded. On
`failed`, inspect the importer before retrying; no missing receipt can authorize
deletion. Keep the PC and database running during local import/finalization.
If all scores are committed but the summary refresh failed, run
`large_finish finalize-import` with the same directory. It verifies every batch
receipt and zero remaining claims, retries only the derived summaries (with a
15-minute per-statement ceiling rather than the import's two-minute ceiling),
then runs guarded cleanup. It never resubmits cloud compute, reimports scores or
increments counters again. It also reports progress/errors in `finish-status.json`.
Failures retain checkpoints and reservations for inspection; never rerun
`launch` or remove its at-most-once fence after an ambiguous response.

## Bounded upload benchmark

`upload-benchmark-001` compares four modes **sequentially on one**
`n2d-standard-4` Spot VM: blocking/1, background/1, background/100, background/500.
Every trial uses identical input bytes/order, four engines, one thread per engine,
2,048 MiB hash, MultiPV 4, and 1,000,000 nodes per position. A fresh result prefix
per mode prevents accidentally timing resumed work. The benchmark runner rejects
anything other than 1,000 distinct nonterminal FENs and refuses a reused prefix.

The local fixture is sampled read-only from `fen` with PostgreSQL `TABLESAMPLE`,
a ten-second statement timeout, and at most 30,000 candidates. It selects 300
positions with 25+ pieces, 400 with 13–24 pieces, and 300 with at most 12 pieces,
then shuffles with a fixed seed. This is a diverse comparison fixture, not an
unbiased production-throughput estimate. Four-field database FENs receive fixed
counters `0 1`; the results are **never imported** into production.

Each trial has an 840-second worker timeout and 900-second outer timeout. The
benchmark loop is capped at 3,540 seconds; the Batch task at 3,600 seconds, with
zero retries and no machine-type fallback. Provisioning and cleanup are additional
and these timeouts are not a monetary spending cap. A preempted/incomplete test
requires review before another paid attempt; it is not silently rerun.

Timing includes input/checkpoint checks, engine startup, analysis, all uploads and
the final manifest. Startup is reported separately: per-record modes check 1,000
checkpoint objects, while batch modes check only 10 or 2. CPU metrics are VM-wide
`/proc/stat` averages, and memory is a sampled container peak, not a guaranteed
instantaneous maximum. Engine wait times are sums across engines, not wall time.
Storage counters count SDK operations, not hidden HTTP retries or invoice items.

The four passes use a fixed order, with no repeats or statistical confidence
intervals; cache/host variance can affect close results. Scores, PVs, WDL, depths,
node counts and tablebase-hit counts must match across modes. Time/NPS/hash-usage
counters are intentionally excluded from this semantic comparison. The host
audit independently downloads every result, checks identities/checksums/legal
PVs and manifests, and confirms VM/disk cleanup. Tests and successful benchmarks
do not enable automatic retries, change production analysis, or select a new
default by themselves.

Generated fixture, job, metrics, manifests, report and audit evidence live in
gitignored `out/upload-benchmark-001/`; cloud results use the corresponding
private `results/upload-benchmark-001/` prefix. The input is 79,329 bytes, SHA-256
`34cac0351a97a1289892a20bbf384fbb6d12d0ef33aca2614e192484ba6d7882`.
The benchmark image is published separately as `sf16.1-upload-benchmark-v1`, digest
`sha256:4422aef3fbe93477a09a3c8f6fca2e3f1d5753e0af223a125cbc11c3ae2f431d`;
`sf16.1-worker-v1` remains unchanged.

From this folder, these commands only read cloud state and save local audit files:

```bash
.venv/bin/python -m benchmark_fen_batches.benchmark_audit --watch
.venv/bin/python -m benchmark_fen_batches.benchmark_audit --audit
```

The audit requires the completed job and cleaned-up VM/disk. Do not reuse this
job ID or prefix to launch another test. `benchmark_fen_batches/render_benchmark.py --help`
describes rendering a fresh configuration; rendering alone never starts a VM.

### Observed upload benchmark — 2026-10-06 UTC

The single Batch job succeeded in 1,855.72 seconds (all four trials combined).
The worker reported these measurements on the same four-vCPU AMD EPYC 7B13 VM:

| Mode | Worker seconds / 1,000 FENs | FEN/s | VM CPU busy | Result uploads |
| --- | ---: | ---: | ---: | ---: |
| Blocking, one result/file | 476.90 | 2.097 | 90.0% | 1,000 |
| Background, one result/file | 471.30 | 2.122 | 94.2% | 1,000 |
| Background, 100 results/file | 458.50 | 2.181 | 99.4% | 10 |
| Background, 500 results/file | 444.69 | 2.249 | 99.3% | 2 |

Contract and manifest add two uploads per trial, excluded from the last column.
All four analysis digests matched. Background/500 had 7.24% higher throughput
(6.75% less elapsed time) than blocking/1, and 3.10% higher throughput than
background/100. It is the best observed candidate, not a statistically established
speed advantage from this single fixed-order pass. Engine analysis-time sums also
varied, so the entire timing difference cannot be attributed to upload batching.

Batching reduced startup checkpoint checks from roughly 26–28 seconds to under
one second. Upload-call time fell from roughly 81–82 summed seconds to 1.04
seconds (100) or 0.46 seconds (500); these are not all recoverable wall-clock
seconds because uploads overlap other work. Peak sampled container memory was
about 8.6 GiB in every mode. No new production default or automatic retry policy
was enabled. Use `--upload-mode background --batch-size 500` to explicitly select
the fastest observed candidate in the new worker image.

Local evidence: `out/upload-benchmark-001/report.json`, the per-mode metrics and
manifests, `audit.json`, `cloud-job.json`, `vm-observed.json` and `cleanup.json`.
The cleanup file records two consecutive observations with no job VM or disk.

## Local checks (no Google resources)

From this folder, with Python 3.11 or later:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

From the repository root, build the separate image:

```bash
docker build --platform linux/amd64 -t chessism-stockfish-batch:local-test stockfish-batch-worker
```

Run all tests against the image's actual Python/dependencies/engine. The real
engine test uses only 2,000 nodes per position, one engine and 16 MiB hash; it is
not the million-node benchmark. It also verifies that a rerun resumes all 20.
The source mount is read-only; temporary outputs disappear with the container.

```bash
docker run --rm --network=none --cpus=1 --memory=1g --pids-limit=128 \
  --read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --mount "type=bind,src=$PWD/stockfish-batch-worker,dst=/checks,readonly" \
  -e STOCKFISH_INTEGRATION=1 --entrypoint python \
  chessism-stockfish-batch:local-test -m unittest discover -s /checks/tests -v
```

For a local CLI run, supply an existing Stockfish **16.1** binary and a fresh output
directory. This writes only under `out/local-001/` (gitignored):

```bash
cd stockfish-batch-worker
.venv/bin/python -m stockfish_batch \
  --input examples/input.jsonl --output out/local-001 \
  --engine /absolute/path/to/stockfish-16.1 \
  --workers 1 --threads 1 --hash-mb 16 --memory-mib 1024 --nodes 2000
```

Changing nodes to 1,000,000 requires a new output directory. Hash size is per engine;
`--memory-mib` is a validation budget, not an OS memory limit. Container/Batch limits
must also be set appropriately.

## First Google test — manual submission

Prerequisites already planned for this project:

- Project `chessism-production`, region `us-central1`.
- Private Docker repository `chessism-workers`.
- Bucket `gs://chessism-batch-276704059200-us-central1`.
- Attached identity `chessism-batch-worker@chessism-production.iam.gserviceaccount.com`:
  Batch agent reporter and logs writer at project scope; repository reader;
  bucket object viewer; object creator restricted to `results/`.
- No inbound connectivity required. No database credentials, service-account keys,
  Google CLI, or full application code are included in the image.

Before running commands that incur charges: review quota, the image digest,
the rendered JSON and the input. Upload the built image manually to
`us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer`
and upload the example JSONL to `inputs/smoke-001/input.jsonl` in the bucket.
Use your authenticated developer account for those uploads, not the restricted
worker account. The runtime obtains credentials through the attached account.

Render locally or in Cloud Shell from this folder (replace the digest placeholder):

```bash
python3 scripts/render_job.py \
  --image 'us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:REPLACE_WITH_64_HEX_DIGEST' \
  --input gs://chessism-batch-276704059200-us-central1/inputs/smoke-001/input.jsonl \
  --output gs://chessism-batch-276704059200-us-central1/results/smoke-001 \
  > job.rendered.json
```

The placeholder is rejected until replaced. The rendered job requests one
`n2d-standard-4` Spot VM, one task, 4 vCPUs, 12 GiB task memory, a 30 GB boot disk,
at most 20 FENs, 900 seconds task runtime and zero retries. The worker has its own
840-second timeout. There is no standard-VM fallback or recurring schedule.
The image runs as UID 10001 with a read-only root filesystem.
The task timeout is **not a monetary cap**: provisioning, image downloads, cleanup,
storage, network and logging can add costs. Queued jobs are not an active benchmark.

Only when ready, this explicit command submits the job and can incur charges:

```bash
gcloud batch jobs submit chessism-stockfish-smoke-001 \
  --project=chessism-production --location=us-central1 --config=job.rendered.json
```

Inspect status and the manifest without changing anything:

```bash
gcloud batch jobs describe chessism-stockfish-smoke-001 \
  --project=chessism-production --location=us-central1 --format='yaml(status)'
gcloud storage cat gs://chessism-batch-276704059200-us-central1/results/smoke-001/manifest.json
```

Accept only a successful job plus a complete manifest with 20 records and matching
checkpoint identities/checksums. Do not import to Postgres during this test.
Verify Batch's VMs/disks have been cleaned up. The bucket's seven-day soft-delete
setting is not an automatic expiration policy; results remain until separately
cleaned up, and deleted bytes can remain billable during soft deletion.

Cloud input/image/output permissions and checkpoint recovery using a new job name
with the **same result prefix** have been exercised (see the outcome below).
Next gates: explicitly bounded retry policy testing and a fixed 1,000-FEN
cost/throughput comparison across machine families. Neither automatic production
retries nor production ingestion is enabled by this package.

## Controlled Spot preemption test — explicit cloud execution

This host-side controller reuses the published image without rebuilding it. It
repeats the 20 synthetic example records ten times with unique IDs (200 records,
not 200 distinct positions). The current test writes only to `inputs/preemption-002/`
and `results/preemption-002/`; `smoke-001` and `preemption-001` are untouched.

With the dependencies above installed and your developer `gcloud` login configured:

```bash
.venv/bin/python scripts/preemption_test.py --prepare
# Review out/preemption-002/job.json before the next, BILLABLE step.
.venv/bin/python scripts/preemption_test.py --execute
```

`--gcloud /absolute/path/to/gcloud` is optional. The controller uses a short-lived
access token in memory; it does not create keys, save tokens or copy credentials
into the worker. These fixed test names and existing local/cloud artifacts are
deliberate guards against accidentally repeating the experiment. Do not delete
them just to bypass a failed or already-started test; inspect the evidence first.

The sequence is bounded to two named jobs, `chessism-preemption-002-a` and `-b`,
each with one `n2d-standard-4` Spot VM, 900-second task limit and zero retries.
After some checkpoints exist, the controller verifies the first VM's Batch job
ID/UID, test label, service account, machine size, region and Spot setting before
issuing Google's `simulate-maintenance-event` once. This forces actual preemption
of that Spot VM; no SSH or production-service interruption is needed.
The controller also verifies the partial run's contract. Batch's reported job
status can lag actual execution; `SCHEDULED` is acceptable only with these saved
results and a verified running VM, not on the job status alone.

The second attempt starts only after the first reports preemption (exit code
50001), has partially completed results, and its temporary VMs/disks disappear.
It uses the identical input, image, settings and output prefix. The final audit
checks all 200 result identities, checksums and legal principal variations,
unchanged generations/checksums for earlier checkpoints, and log evidence that
only missing positions were analyzed. Temporary VM/disk cleanup is checked again.

Generated input, job configuration, before/after inventories, preemption evidence,
recovery events and `report.json` are kept in gitignored `out/preemption-002/`.
An exception produces `error.json`; the controller attempts to cancel only its
own unfinished tracked jobs, preserves bucket objects, and never submits a third
attempt. If the host stops or loses connectivity, inspect the two named jobs and
their resource labels manually; the task limit is not a total billing cap.

Google can instead report VM recreation during execution (`50006`) after a
simulated maintenance event. The strict `50001` check deliberately stops in that
case. After reviewing the failure, this explicit continuation submits **only B**:

```bash
.venv/bin/python scripts/preemption_test.py --resume-after-vm-recreation
```

It verifies the completed maintenance operation against the exact recorded VM
identity, requires a matching `50006` failure and partial results, confirms A's
cleanup, and refuses an already-started or existing B. It never resubmits A or
changes inputs/results. The report distinguishes VM-recreation recovery from
strict Spot preemption; the original `error.json` remains as diagnostic evidence.
This continuation is still billable and requires approval to run the second job.

Reference: Google's [Spot preemption test instructions](https://docs.cloud.google.com/compute/docs/instances/create-use-spot#test_preemption_settings).

Observed `preemption-001` outcome (2026-10-06 UTC): **INCONCLUSIVE for partial
recovery**. Attempt A finished all 200 records before preemption stopped work;
all 200 checkpoints and manifest entries validated. The second job was not
submitted, and the first job's VM/disk cleanup was verified. The controller was
then corrected to account for delayed Batch job status, with offline regression
coverage, before the second experiment below.
See local `out/preemption-001/report.json`; do not interpret this completed
analysis run as proof of interrupted-work recovery.

Observed `preemption-002` outcome (2026-10-06 UTC): **PASS for checkpoint recovery
after VM interruption**. The maintenance request was issued after observing 14
saved records; 104 were durable when work stopped. Google reported `50006`
(VM recreated), not `50001` (Spot preemption). The explicit second job reused
all 104 saved records, analyzed the remaining 96, and succeeded with 200 validated
results. Every earlier object's generation and checksum was unchanged, and worker
logs contained saves only for the 96 missing IDs. Both jobs' VM/disk cleanup was
verified; no third job was submitted and the production database was untouched.
Evidence: local `out/preemption-002/report.json`, `before.snapshot.json`,
`after.snapshot.json`, `interruption-evidence.json`, and `recovery-events.json`.
These artifacts are gitignored; cloud results remain under the matching bucket
prefix. This verifies recovery from this VM-loss event, not every possible failure
mode or an enabled automatic-retry policy.

## Provenance and reference

The image includes Stockfish's GPLv3 license, authors, README and release source
under `/usr/local/share/stockfish/`, with the release URL/checksum in `PROVENANCE.txt`.
Stockfish and python-chess keep their own upstream licenses; the root repository's
license does not relicense these dependencies. AVX2 amd64 is required; ARM images
and machines are intentionally unsupported in this initial package.

- [Stockfish 16.1 release](https://github.com/official-stockfish/Stockfish/releases/tag/sf_16.1)
- [python-chess engine API](https://python-chess.readthedocs.io/en/stable/engine.html)
- [GCS create-only preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions)
- [Batch job schema](https://docs.cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs)
