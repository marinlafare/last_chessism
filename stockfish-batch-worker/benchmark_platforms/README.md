# Batch Spot versus Cloud Run Jobs

Host-side tooling, separate from production `cloud_job/` and the earlier
`benchmark_fen_batches/` upload experiment. It never imports results into the
production database. Do not start a UI cloud job during this isolated benchmark.

Each platform analyzes the **same 25,000 distinct FENs**, selected with seed 106
from the completed 70,058-position run `fcfc109ab7bf4f94b515e7b15a2b0fe6`.
This is a repeatable sample of that Magnus corpus, not a universal chess workload.
The database export runs read-only. Input and settings fingerprints are retained.

| Setting | Both platforms |
| --- | --- |
| Image | One immutable Stockfish 16.1 worker digest |
| Search | 100,000 nodes/FEN, MultiPV 4, no tablebases |
| Engines | Four independent workers, one engine thread each |
| Hash | 2 GiB per engine |
| Checkpoints | Background uploads, 500 results/file |
| Task count / parallelism | 1 / 1 |
| Runtime / retries | 3,600 seconds maximum per task, zero automatic retries |
| Worker safeguards | 3,540-second wall-clock limit, 300-second no-finished-FEN limit |

Batch uses one `n2d-standard-4` Spot VM (4 vCPU, 16 GiB physical VM RAM;
worker budget 12 GiB). Cloud Run uses 4 vCPU and 12 GiB. This measures the
deployable configurations, not identical physical CPU models. It is one sample
per platform, not statistically conclusive; an interrupted run is not a valid
completed-throughput comparison. No production recovery policy is modified.

## Run from the repository root

Use a fresh 12-character lowercase hex identifier and a NEW output directory.
The example below is the first run; do not reuse its identity for another test.

```sh
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m benchmark_platforms.run prepare \
  --id 20261006ab01 --directory stockfish-batch-worker/out/platform-20261006ab01
```

Preparation publishes a dedicated image package, uploads input, and creates the
Cloud Run job definition **without executing it**. It does not grant IAM or
enable APIs. The Cloud Run API and existing worker permissions must be ready.
Check that the new Cloud Run job is ready, then launch both submissions together:

```sh
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m benchmark_platforms.run launch \
  --directory stockfish-batch-worker/out/platform-20261006ab01
```

`launch.started` is a durable at-most-once fence. **Never delete it to retry.**
If a response is lost, inspect cloud jobs/executions and local receipts; don't
execute Cloud Run again. Partial submission leaves the successful arm bounded.
No watcher or local controller is required to keep these jobs running.

```sh
watch -n 30 'PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m benchmark_platforms.run status --directory stockfish-batch-worker/out/platform-20261006ab01'
```

Uploaded FEN counts move in increments of 500 and lag actual analysis. `status`
performs a single check and records it locally; it does not resubmit or cancel.

After BOTH succeed:

```sh
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m benchmark_platforms.run collect \
  --directory stockfish-batch-worker/out/platform-20261006ab01
```

Collection downloads all results into `download/`, verifies contracts, legal
chess results, manifest checksums, counts, and performance records, and writes
`report.json` and a durable `completion-status.json`. It compares chess outputs across platforms. It does not delete
anything or change the database. Compare worker seconds, FEN/s, per-engine
distribution, startup and full execution timing; `/proc/stat` VM CPU utilization
is **not comparable** between a dedicated VM and Cloud Run's container host.
Actual billed resource time differs from worker time and billing exports lag.

## Scope and cleanup handoff

All receipts, specs, input and reports are under the git-ignored `out/` directory.
For this example the exact temporary cloud scope is:

- Batch job: `chessism-platform-20261006ab01-spot` in `us-central1`.
- Cloud Run job and its execution: `chessism-platform-20261006ab01-run` in `us-central1`.
- Input prefix: `inputs/platform-20261006ab01/` in the Chessism bucket.
- Both output prefixes: `results/platform-20261006ab01/` in that bucket.
- Artifact package: `chessism-workers/platform-20261006ab01` (digest in `plan.json`).

After verified download, remove these exact job records, objects and image;
verify Batch's VM/disks are gone. Before deleting the shared benchmark image,
both platform jobs must be terminal and no remaining execution may need it.
Do not use the existing Batch-only image-reference scan as proof that Cloud Run
doesn't reference an image. Cleanup here is a deliberate follow-up after the
report, not an automatic production-controller action. Completed job definitions
do not keep CPUs running; results/image storage remain billable until deleted.
Do not delete project-wide log streams when other jobs could share them.
Google's audit/billing records and retained execution history are not erasable
as part of ordinary job cleanup.

After cleanup the status helper shows `CLEANED`, not `NOT_STARTED`. The live
`last-status.json` is overwritten by every watch check; it is not a durable
success receipt. Cleanup must rely on the archived result report and the
pre-deletion Cloud Run/Batch receipts instead.

The optional `benchmark_platforms.log_cleanup` helper handles Batch's job
resource type, audit-verified VM instance identities, and the exact Cloud Run
execution. It requires saved successful cleanup receipts and an idle project.
It uses full-history queries with no date cutoff: any entry outside the verified
owners retains that entire log stream. Audit and workflow streams are excluded.
Use `--execute` only after the resource/image cleanup is complete; without it the
helper writes an inspection plan. Unidentifiable project-level logs stay intact.

## First completed comparison: 20261006ab01

Both arms succeeded without retries. All 25,000 chess results matched; no
production database rows were changed. Four workers finished within 0.2 seconds
of one another on each platform, with approximately 6,250 FENs per worker.

| Measurement | Batch Spot | Cloud Run Job |
| --- | --- | --- |
| Worker elapsed | 28m 00s | 19m 28s |
| Submission to success | 29m 03s | 22m 34s |
| Throughput | 14.88 FEN/s | 21.41 FEN/s |
| Sampled container peak memory | 8.63 GiB | 8.72 GiB |
| Estimated compute cost, USD before credits | ~$0.042 | ~$0.11–0.13 |

Cloud Run used 30.5% less worker time (43.9% higher throughput), while Spot's
estimated compute cost was lower. This is one comparison, not a guarantee for
other hardware placements or interruption rates. No production backend was
switched as part of the benchmark.

Prices retrieved October 6, 2026: [N2D Spot](https://cloud.google.com/spot-vms/pricing)
in Iowa was $0.086056/hour for `n2d-standard-4`;
[standard Cloud Run Jobs](https://cloud.google.com/run/pricing) were
$0.000018/vCPU-second and $0.000002/GiB-second, or $0.3456/hour for 4 vCPU/12 GiB.
Spot's estimate includes the observed VM creation-to-deletion interval. The
Cloud Run estimate uses worker time and execution start-to-completion time as
planning estimates, not a verified invoice range. Storage, disk/IP, network,
logging, tax and free-tier/trial credits are excluded. The billing export was
still empty; **these are not confirmed charges**.

Local input, full verified output copies, raw metrics, billing-related monitoring
samples, deletion receipts and `comparison-summary.json` remain in
`out/platform-20261006ab01/` (git-ignored).
