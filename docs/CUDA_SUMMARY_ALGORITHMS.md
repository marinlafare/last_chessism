# Summary algorithms: research history and canonical blunders

Current status: a superuser requests summaries from **Run CUDA summaries** on
Analyze Positions. Rating-aware blunders, score distribution and unchanged
per-game accuracy share bounded input reads and run on the RTX 4060. Daily
accuracy is displayed as the most common of six game-accuracy bands, including
all ties and hover counts. Ingestion/FEN analysis mark work pending, not automatic
GPU execution. An opt-in metadata stage also computes hours/days, rating history
and counts, opponents/openings and Unexpected endings on CUDA; it reuses the same
manual control and retains prior snapshots during replacement. Salience is
unchanged. See `chessism_api/operations/cuda_summaries/README.md` for the current
writer, revision checks and tests. No GPU job starts on page load.
The research sections below record historical stages; their statements
about unchanged production behavior apply to those stages, before the
"Provisional canonical adoption" section near the end.

## Defaults

Matrices → Default matrices and Algorithms → Input formulas → Default expose:

- `score-distribution` v2: scored position occurrences only, after either color
  moves. Five columns: `player`, `mode`, `player_score`, `score_kind`, `mate_moves`.
  Repeated FENs remain repeated. Scores use that player's perspective. Eligibility
  is applied before row limits/counts, even when `analyzed_only` is false. Unscored
  summary counters, if still displayed elsewhere, are not inputs to this matrix.
- `blunder-moves`: own moves in fully analyzed games; immediate predecessor
  evaluation, game-time player/opponent ratings, mode, and game ID. No `player_color`,
  `started_at`, `ply`, or `result` columns in the v2 default. Game outcomes must be
  joined into the separate training dataset, never used as classifier inputs.
  Missing ratings stay missing. `initial_prior=1` records an assumed +15 CP
  (+0.15 pawns) before the first White move, preserving existing behavior;
  otherwise it is 0. Other missing predecessors are not replaced or skipped.

Ordering, date filtering, color conversion and occurrence row keys are extraction
metadata, not calculation columns. Optional old columns remain allowlisted for
historical recipes. `summary_input_version` freezes population semantics: legacy
saved v1 recipes retain their old meaning; new formulas/defaults use v2.

These are read-only reusable definitions, not eagerly constructed arrays.
Their previews are bounded and explicit. Saving/copying columns never scans data.
Scope is selected in Algorithm creation. The generic editor is still a bounded
CPU research editor (12 selected input columns); it is not the automatic GPU
summary pipeline. The simplified blunder default fits the 12-column editor limit.

## CUDA runner

`summary_validation.py` accepts a frozen JSON array with at most 100,000 rows,
processes at most 8,192 rows per GPU batch and limits its CuPy pool to 256 MiB
(CUDA context/compiler memory is additional). No database or cloud connection,
canonical registry, automatic writer, or CPU fallback is provided.

The score-distribution runner compares a CPU bin reference using the existing boundaries with
CUDA classification and grouped 64-bit counts. It checks every bin, mate distance,
mate side and tablebase classification, not only the grand total. Null/invalid
scores and the `unscored` kind are rejected, not silently discarded or counted.
There are 15 counters: 13 CP bins, mate, tablebase. Input preparation reads source
provenance to derive `score_kind` and trustworthy `mate_moves` using the existing
case-sensitive `stockfish*` rule; raw source strings are not GPU inputs. Missing
mate distances remain null. End-of-game counters and popup rankings are outside
this kernel's scope. Optional `row_key` metadata enables occurrence identity
checks; identical numerical rows without keys are valid separate occurrences.

The optional isolated image leaves host drivers and existing containers alone:

```sh
docker build -f dev_tools/Dockerfile.cuda-validation -t chessism-cuda-validation:20261025 .
docker run --rm --network none --gpus device=1 --cpus 1 --memory 1g \
  --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD:/candidate:ro" -e PYTHONPATH=/candidate \
  chessism-cuda-validation:20261025 scripts/validate_summary_cuda.py --synthetic
```

`device=1` selected the RTX 4060 on the tested workstation; confirm device indices
before using this on another machine. The selected GPU appears as device 0 inside
the container. No driver/toolkit installation on the host is necessary.

For a saved report, mount a dedicated writable output directory and pass
`--output /output/report.json`. Existing report files are never overwritten.
For frozen real inputs, replace `--synthetic` with `--input /path/rows.json`.
Use only an explicitly prepared consistent export; this tool does not extract it.

Algorithms → CUDA validation → Open comparison report displays the JSON locally.
It does not upload or trust it as an approval certificate. CUDA context/compilation,
input transfer, calculation and output transfer are timed separately with stream
synchronization. Timings exclude database reads and publication, and the CPU
reference is a correctness oracle, not an optimized performance competitor.
Version 2 reports are explicitly scored-only. The viewer can still open version 1
reports, labeled as historical rather than validation of the new contract.

## One-player GPU validation in Algorithm creation

`CUDA validation → Player → Run on GPU` explicitly queues
`POST /research/algorithms/cuda-validation` (superuser-only), using the existing
single-job research queue and durable run history. No GPU work starts on load.
The research worker is built with `dev_tools/Dockerfile.algorithms-cuda`; its
dependency image is built with `dev_tools/Dockerfile.cuda-validation` first.
Only this worker receives a GPU: Compose device ID defaults to 1 (RTX 4060 on
this workstation), configurable with `CHESSISM_ALGORITHMS_GPU_DEVICE`. It is
device 0 inside the worker. Existing CPU research operations retain their paths.

### Opt-in batch-size / input-reuse experiment

In CUDA validation, check **Compare batch sizes and input reuse** before Run on
GPU. The same endpoint accepts `benchmark: true`; the server fixes the experiment
to 8,192 / 65,536 / 262,144 rows and three repeats. Clients cannot supply custom
code, arbitrary GPU sizes or unbounded repeats. Normal validation is unchanged.

`score_benchmark.py` reuses the one anonymous frozen numerical input inside the
run, after closing the read-only source transaction. All three GPU sizes must
pass independent per-occurrence, aggregate, source-count and input-hash checks.
They are compared with the normal validated baseline, which must already match
both SQL and the saved summary. No candidate is timed/accepted on partial data.

After warm-up, timed aggregate-only GPU and NumPy paths both read the file,
prepare numeric arrays, hash the input and accumulate CP/mate/tablebase counts.
The GPU timing includes host/device transfers and returns only the small summary,
not every position's classification. Every timed pass must match the baseline's
input fingerprint and counts. Backend order and batch-size order alternate across
three repeats; reports retain individual samples, medians, measurement order,
the fastest measured GPU size, and setup/whole-run timing. This experiment does
not silently change the normal 8,192-row validation batch or canonical registry.

The UI distinguishes reused-input time from **fresh input + GPU** (the sum of
measured extraction and warm GPU time, not a full ingestion/update benchmark).
SQL aggregation is still measured separately, after extraction, and can benefit
from the warmed database cache. Reuse is **within one run only**. A persistent
cache would additionally require revision-aware invalidation, atomic publication,
capacity/retention policies, and tests covering ingestion, FEN rescoring and
player deletion; this experiment does not claim those are implemented.

Maximum numerical input is still two million rows / 64 MB on temporary disk.
Database fetches stay at 8,192 records; only the experiment's numerical/GPU batch
grows, to at most 262,144 rows (8 MiB encoded input). The 256 MiB CUDA pool and
2 GiB worker limits remain unchanged. Cancellation checks occur between validation
batches and timed passes; the anonymous file closes on failure/cancellation.

The streamed validation accepts an entire player across bullet/blitz/rapid, with
1–2,000,000 scored occurrences and a current published CPU summary. It refuses
oversized players rather than silently sampling/truncating, and refuses new
validations while a cloud workflow is active. These are validation limits, not
the eventual production summary limits.

`score_validation_job.py` freezes the five-column matrix, existing SQL classifier
and saved CPU summary in one REPEATABLE READ / READ ONLY transaction, bounded by
25-second statements, a 180-second extraction/reference deadline and no parallel query workers. The transaction closes before
GPU calculation. Every GPU counter and mate-distance/side count must match both
references as well as the independent per-row CPU oracle. Missing CUDA fails
explicitly before extraction. Native indexed source relations avoid generic view
expansion, string occurrence keys and the full-history sort. A server cursor
fetches at most 8,192 records at a time and spools fixed-width numerical records
to a private anonymous temporary file (32 bytes per occurrence, at most 64 MB).
Player is constant; mode/kind dictionaries are fixed; the matrix still has its
original five semantic columns. Every occurrence is preserved, without deduplication.
Both CPU and CUDA comparisons consume one batch at a time, retaining only small
counters and at most 20 mismatch examples. Files close/remove on completion,
failure, cancellation or worker termination. Compact reports,
encoded input hashes, source snapshot IDs and timings are retained in
`algorithm_run`, covered by the existing backup manifest. No chart summaries or
canonical registry are written. Cancellation is checked between bounded batches;
restart marks interrupted research runs failed, not approved or silently retried.

Extraction, SQL reference, CUDA setup, transfers and calculation timings are
separate. Whole-validation time includes all cross-checks. One small cached run
does not establish a production speedup. A passed report remains evidence only,
not authorization to promote an implementation.

Streaming reports retain the v2 score algorithm but explicitly identify
`streaming_version: 1`, extraction version 2 and `score_distribution_le64_v1`.
The input hash covers the player and stream bytes/order, not a canonical sorted
dataset; it is not directly comparable with old JSON-row hashes. The generic
offline JSON validator retains its 100,000-row limit. Historical reports still
open unchanged. GPU buffers remain bounded to the existing 256 MiB pool (CUDA
context/compiler allocations are additional); the worker retains its 2 GiB limit.

Initial live check (2026-10-25): `chefshouse`, 46,519 scored occurrences across
all three modes, RTX 4060. Run `e65ccb5c-44b8-499a-b239-42730ad75bb1` passed with
zero row/counter, existing-SQL or saved-summary mismatches. Coverage included
2,659 zero scores, 291 known mate distances, 33 unknown mate distances and 508
tablebase occurrences. The saved chart summary revision/timestamp was unchanged.
GPU calculation was 10.94 ms (13.32 ms warm transfer/calculation path), extraction
841.94 ms, SQL baseline 242.40 ms, CPU oracle 67.27 ms; CUDA warm-up 3.225 s and
full validation 4.964 s. These are single-run, cache/order-sensitive timings,
not an end-to-end speedup claim. Improve extraction and bounded streaming, then
benchmark larger workloads before considering canonical promotion.

Streamed validation checks (2026-10-25, same RTX 4060):

| Player | Scored occurrences | Extraction | SQL reference | Warm GPU path | Full validation |
| --- | ---: | ---: | ---: | ---: | ---: |
| chefshouse (warm repeat) | 46,519 | 0.322 s | 0.233 s | 0.015 s | 0.638 s |
| levonaronian | 132,279 | 1.274 s | 0.658 s | 0.042 s | 2.065 s |
| galeaj | 1,036,684 | 6.574 s | 3.726 s | 0.417 s | 11.205 s |

All comparisons passed with zero per-row, counter, SQL-reference and saved-summary
mismatches. Maximum input batch was 8,192 rows; the million-row temporary file
was 33,173,888 bytes and was removed. Summary revisions/publication timestamps
were unchanged. The first streamed run after worker startup took 4.305 s for
chefshouse, including 3.288 s CUDA warm-up; do not compare the warm-repeat wall
time with an older cold run as a claimed optimization speedup.

Durable reports: `ccdb7b6c-3676-4e00-a040-07b9657320d6` (cold chefshouse),
`cf799d0e-010a-4998-b042-93a93ba26053` (levonaronian),
`ad9b040b-6cb4-4978-bad4-17d027be7eab` (galeaj),
`e71f6358-2fc8-4025-8280-602543e640e4` (warm chefshouse).

The explicit read-only `scripts/benchmark_score_extraction.py chefshouse`
compares legacy/new/new/legacy loading on one snapshot, capped at the original
100,000-row limit, and checks exact input multisets including repeats. With the
research worker idle: legacy 0.724/0.711 s, streamed 0.336/0.337 s (~53% lower
mean loading time). These are warm-cache measurements, not a CUDA speedup claim.
Database loading remains the bottleneck: the million-row extraction plus GPU
path (~6.99 s) still exceeds SQL aggregation (~3.73 s, measured after extraction).
The optimized NumPy correctness oracle itself took 0.112 s for that run, versus
0.417 s for the warm GPU path; small-batch launch overhead matters. Do not
canonicalize solely on correctness or kernel-only timings. Investigate shared
numeric input reuse and larger bounded GPU batches with controlled end-to-end
benchmarks before proposing a production switch.

Verification: 110 isolated backend tests, disposable PostgreSQL legacy/native
and generic-matrix parity, concurrent-source snapshot isolation, count/staleness
guards, cancellation cleanup, actual-GPU synthetic boundaries, mocked-browser
submission/streamed reports, frontend report tests and production build passed.
Only API and idle research worker were deployed; protected pipeline containers
retained their original IDs/start times.

Batch/reuse experiment results (local 2026-10-25, UTC 2026-10-26): all three
players passed every row/counter, SQL, saved-summary, input-fingerprint and timed
aggregate comparison. The benchmarks used the same RTX 4060 and one-CPU worker.

| Player | Occurrences | GPU median at 8,192 | GPU median at 65,536 | GPU median at 262,144 | Best CPU median |
| --- | ---: | ---: | ---: | ---: | ---: |
| chefshouse | 46,519 | 18.09 ms | 5.90 ms | 6.27 ms | 6.89 ms |
| levonaronian | 132,279 | 46.01 ms | 17.16 ms | 12.34 ms | 19.94 ms |
| galeaj | 1,036,684 | 329.27 ms | 111.66 ms | 92.14 ms | 153.01 ms |

These are medians of three prepared-input passes including file reading,
numeric preparation, hashing and aggregation (and GPU transfers). For chefshouse,
both larger limits fit the entire player in one batch, so their small difference
is measurement variation, not evidence for a distinct batch geometry.

For galeaj the larger GPU batch reduced prepared-input time by ~72%, and was
~40% lower than the fastest tested CPU batch. Database extraction still took
5.678 s, versus 3.740 s for the existing SQL aggregation after extraction. Thus
fresh extraction plus the best warm GPU path was ~5.771 s; this experiment does
not justify replacing a standalone SQL refresh. Whole validation/benchmark took
14.027 s because it deliberately includes the CPU/SQL oracle, all candidate
validations and all 18 timed CPU/GPU passes. That is not a production runtime.

Durable benchmark runs:
`962f00b4-0049-4094-9382-0f946875ab56` (chefshouse),
`1922e076-57b0-4837-bf39-9e592a96bcd1` (levonaronian),
`c0c71d0f-f66d-4bd6-97af-618b63493ff1` (galeaj).

118 isolated backend tests, actual-GPU synthetic checks (310,400 rows with all
bin/mate boundaries), disposable PostgreSQL checks, frontend parser tests,
mocked-browser opt-in submission/report rendering and the production UI build
passed. The frontend parser also accepted the actual saved million-row report.
All source summary revisions/timestamps and protected container IDs/start times
remained unchanged; no research/cloud jobs remained active after the tests.
Normal validation still uses 8,192 rows; 262,144 is the measured experimental
candidate, not a newly canonical default. The next integration gate is a
versioned shared-input stage for actual summary jobs, with explicit change
invalidation and end-to-end measurements before any production replacement.

CuPy references:
- https://docs.cupy.dev/en/stable/install.html
- https://docs.cupy.dev/en/stable/user_guide/performance.html

## Explicit all-player experiment

`scripts/benchmark_all_score_distributions.py --output <new-report.jsonl>` is an
administrator-run, read-only cohort benchmark, not a new summary writer or an
API option. Run it in the idle GPU research environment. It refuses active
cloud/research jobs and requires current saved summaries for **every** eligible
player (`joined > 0`, not deleted). It never silently skips oversized players.

For the existing local worker (choose a new output name for every run):

```sh
docker exec -i last_chessism-worker-algorithms-1 python - \
  --output /research-data/algorithms/benchmarks/score-cohort-NEW-RUN.jsonl \
  < scripts/benchmark_all_score_distributions.py
```

This does not restart the worker or alter its image. Its existing CPU/memory/GPU
limits also apply to the command. The report appears under the host's
`research_data/algorithms/benchmarks/` directory; it contains no raw input rows.

The source-query bound remains 1,000 games, matching the production histogram
writer. Each player's extraction and independent SQL reference share a read-only
repeatable snapshot; query order alternates by game batch. The transaction closes
before CUDA. Anonymous temporary files retain scored occurrences only, including
repeats; just one player's input is retained at a time. Larger players are covered
by successive 2-million-row validation windows, not by raising the API validator's
limit or truncating input. GPU batches are 262,144 rows. Explicit experiment bounds
are 20 million occurrences per player, 100 million per cohort, and 100 players.

Every occurrence is compared with the independent CPU oracle; all mode/CP/mate
distance/side/tablebase counts must match both independent SQL and saved summaries.
Three timed passes per backend must reproduce the validated input fingerprint and
counters. The final eligible-player inventory, revisions, and publication times
must exactly match the initial inventory. Failures retain diagnostic JSONL events,
never a successful completion or production publication. Anonymous inputs close
on failure/cancellation and are not persistent cache artifacts.

Timing separates game selection, numeric extraction/packing/spooling, SQL
aggregation, and warm prepared-input CPU/GPU passes (including reads and GPU
transfers). Prepared totals sum per-player medians of three passes, not a single
global kernel execution. Fresh GPU estimates include extraction; GPU initialization, independent
validation, production partition scheduling, endings/unscored coverage, and
publication are not disguised as GPU compute time. Warm database caches and one
source pass remain limitations: this is a histogram comparison, not a measured
end-to-end ingestion replacement. No canonical promotion follows automatically.

Verification: `tests.test_score_cohort_benchmark` checks windows, full-cohort
readiness/bounds, exact multi-window reduction, truncated/extended inputs and
failed timed passes. `tests/check_cuda_player_postgres.py` also checks the cohort
extractor against disposable legacy/native PostgreSQL and the existing matrix/SQL
references. A real RTX 4060 synthetic run covered 2,100,003 rows across two windows
with zero per-row or counter mismatches.

### All-player results — 2026-10-26 UTC

Completed report: `research_data/algorithms/benchmarks/score-cohort-20261026-all-players.jsonl`.
All **46 eligible players**, **76,815,379 scored occurrences**, **138 player/mode
groups**, and **68 bounded validation windows** passed with zero per-row or
aggregate mismatches. No sampling or truncation. The largest player,
`danielnaroditsky`, contributed 12,762,972 occurrences. All saved-summary
revisions/publication timestamps and the eligible-player inventory were unchanged
at completion. Kernel implementation SHA-256:
`ce3ca239f3f22ea2eef03ffb894673f88a36ea4a0018f812971029311c77963f`.

| Measured work across all players | Seconds |
| --- | ---: |
| Shared game selection | 5.346 |
| Numerical extraction/packing/spooling | 556.000 |
| SQL histogram aggregation | 429.025 |
| Prepared-input CUDA (sum of per-player medians) | 7.201 |
| Prepared-input NumPy CPU (sum of per-player medians) | 14.706 |
| Fresh source + CUDA histogram | 568.547 |
| Fresh source + SQL histogram | 434.371 |
| Whole validation experiment, including all references/repeats | 1,078.463 |

CUDA approximately halved prepared-input calculation time, but the measured
fresh-source histogram path was **31% slower than SQL**. This does **not** justify
making this implementation the official summary writer. SQL remains in place;
no summary rows, canonical registrations, API/worker images or cloud workflows
were changed. Every protected service and the research worker retained its
container identity and start time. These numbers exclude production daily
partition scheduling, endings/unscored coverage, and summary publication.
The final focused backend regression run passed all 122 tests; disposable
PostgreSQL and actual-GPU multi-window checks also passed.

The live API was confirmed to have saved Stockfish summaries enabled. A separate
post-benchmark read of the existing handler for all 46 players found every
summary current: median **1.960 ms**, p95 **2.692 ms**, maximum **3.768 ms** including
JSON encoding. This is backend read time, not HTTP/auth/network/browser latency.
Chart display already reads the saved result; changing its calculation backend
would not remove additional chart-time calculation.

The next performance gate is cheaper/versioned shared input reused by actual
summary calculations, followed by ingestion/update measurements. Do not replace
the writer solely because the kernels match or run faster once input is prepared.

## Rating-aware blunder candidate v1 (2026-10-26)

Algorithms → **Rating-aware blunders → Fit candidate on GPU** explicitly queues
`POST /research/algorithms/blunder-training` on the existing single-job CUDA
research worker. This is a fixed, versioned research recipe, not arbitrary user
code or a canonical summary writer. The default blunder matrix stays unchanged.
Score-distribution production summaries remain CPU-calculated.

The model is our own transparent calibration, not an implementation of proprietary
Chess.com CAPS. Its prediction is **expected points**, with final outcomes 0/.5/1
as training labels, using `sigmoid(k × clip(cp, -1000, 1000))`. It learns one
positive `k` for each average-game-rating band and mode (27 possible groups).
The game-time ratings select a group; no final outcome or future position enters
inference. Sparse groups do not inherit a made-up or global coefficient.

Selection is deterministic and stratified, at most 1,000 games per group by
default (configurable 100–3,000), and at most 3 million source positions. Metadata
selects whole games before loading FENs. Only standard-chess, fully analyzed games
with a complete, non-deleted ingested profile, valid complementary results and
positive ratings separated by at most 200 qualify. Timeout/abandoned endings
are excluded. This population does **not** establish validity on time losses,
larger rating gaps, other variants or unrated players.

Each unique game enters once regardless of how many ingested players share it.
A fixed SHA-256 game split assigns 70% training / 15% validation / 15% test.
Every position in that game stays in the same split. First 20 plies, non-Stockfish
sources, mates and tablebases are excluded from fitting. The remaining positions
share total weight one per game. Training uses integer CP rounded half away from
zero; inference retains the original CP. Compact sufficient statistics retain
exact weighted squared loss for this rounded-CP contract.

CUDA fits bounded positive coefficients using a logarithmic grid and local
golden-section refinement. CPU independently runs the numerical reference; no
CPU fallback can make a failed GPU run appear successful. Groups require at
least **500 training, 50 validation and 50 test games**. Held-out outcomes never
choose coefficients. Reports include expected-points MSE against the current
fixed coefficient, calibration bins, fit-boundary warnings, unsupported groups,
and CPU/CUDA prediction differences. Test-only move comparisons report old/new
blunder counts and disagreements separately from backend decision agreement.

CP loss threshold stays **15 expected-percentage points**. Existing forced-mate
rules stay separate and unchanged. Tablebase/missing transitions are unsupported;
missing immediate predecessors are not skipped. The first White move retains the
explicit +15 CP initial prior for comparison, but never supplies a fitting row.

All source reads share one repeatable-read, read-only transaction, with 60-second
statement timeouts, no parallel SQL workers and a 10-minute extraction deadline.
Each source query is bounded by both 128 games and 32,768 positions; the full
input is never held in Python memory. Temporary transition data is deleted on
completion/failure/cancellation. CUDA has a 256 MiB allocation-pool ceiling
(context/compiler memory is additional). Only the compact AlgorithmRun report
is persisted, including input/config/implementation identity and model hash.

This is a **candidate**, even if CPU/CUDA agree. The hash split is not temporal
or player-held-out; players and repeated positions may cross partitions. Count
changes alone do not validate a definition. Further checks below are still
required before replacing any live calculation.

Checks:

- `tests.test_blunder_model`: model, weights, split, no-label inference, bounds,
  authorization, dispatch and no-fallback checks.
- `tests/check_blunder_training_postgres.py`: disposable legacy/native PostgreSQL,
  selection, coverage corruption and active-cloud protections.
- `scripts/validate_blunder_cuda.py`: synthetic-only actual CUDA, two known fitted
  curves and 100,008 transitions; no database, credentials or writes.
- `test-summary-algorithms-browser.mjs`: mocked candidate submission/report.

### First saved real-data candidate

AlgorithmRun `84e07805-84ad-41c7-b23f-7ae1e4ff091e` completed on
2026-10-26 at 01:37:54 UTC. Model ID:
`0d6535ab4b75c88bee482a1077d7a032c52808709203bff46df945b3ac28e902`.

- Selected 25,494 games / 1,824,287 source positions. Fitting retained 25,408
  games / 1,196,256 positions; 86 games had no qualifying post-opening CP rows.
- 24/27 groups fitted; unsupported: bullet 1600–1799, rapid 2000–2199 and
  rapid 2200–2399. No fitted coefficient hit the search bounds.
- Source preparation: 27.114 s. Fit plus CPU reference: 1.952 s. Held-out move
  comparison: 2.510 s. Total computation: 31.905 s. These are first-candidate
  workflow timings, **not** evidence of an end-to-end GPU speedup.
- Zero CPU/CUDA decision mismatches, including 200,719 assessed test moves.
  Largest fitted-curve CPU/CUDA prediction difference was approximately 7.6e-9.
- Test expected-points MSE improved in 20 groups and worsened in four; these
  point estimates have not yet been qualified by whole-game uncertainty intervals.
- On the same assessed test moves, old rule: 12,062 flags; candidate: 14,904
  (+23.6%). Some high-rating groups exceed twice the old flag count. This is an
  important definition change, not a numerical implementation error, and it needs
  review of concrete disagreements and calibration before promotion.

Two earlier failed runs remain in history: the initial native query referenced
an archived-away rules field, and the next attempt completed computation but
failed strict JSON publication on a NumPy boolean. The native fixture now omits
normalized-away columns and the GPU/unit tests serialize the full fitted report.
Neither failure nor the successful candidate changed production summaries.

Final checks: 141 backend tests, disposable PostgreSQL, actual RTX 4060 synthetic
check, mocked browser check and frontend build passed. Only API/research-worker
containers were replaced; every other running container retained its identity.

## Coefficient-only expanded-corpus research (2026-10-26)

At the user's request, coefficient calibration was rerun before any further
blunder classification. Experiment `ae06630e-1fc7-46f5-b32a-9b8cfdab8bff`
completed from 02:07:43 to 02:15:13 UTC (449.470 seconds). This used the existing
**CPU coefficient-research pipeline**, not the CUDA blunder runner. No blunder
counts, production coefficients, thresholds or player summaries were changed.

The prior experiment's settings were preserved: bullet/blitz/rapid, rating gap
at most 200, first 10 full moves excluded, CP clipped at 1,000, equal total weight
per game, 70/15/15 whole-game hash split with seed 20260930, and at least 500
training games per fit. The source contract is now version 2: both outcome
labels must be valid and complementary (1/0, 0.5/0.5, 0/1). A metadata preflight
found 171 inconsistent outcomes, which were excluded without modifying games.

- Retained 448,601 games and 23,409,541 scored-position **appearances**, not unique
  FENs; the October 1 experiment retained 32,649 games / 1,565,045 appearances.
- Split: 314,038 training, 67,509 validation and 67,054 test games.
- Aggregated input: 2,757,165 rows; SHA-256
  `7e127efeb5b71f825ee13e1980cfb3710743ed86d7a5726b2c740c61e5d67b12`.
- Overall coefficient: **0.0050650636**, previously 0.0043072032. Held-out MSE
  improved 1.3726% against the Lichess coefficient, **not** against the old fitted
  coefficient. Metrics use symmetric -1/0/1 outcomes; expected-points MSE is one
  quarter of this MSE and has the same relative improvement.

| All-rating fit | New coefficient | Test MSE reduction vs Lichess |
| --- | ---: | ---: |
| Bullet | 0.0044297525 | 0.4415% |
| Blitz | 0.0059078403 | 3.0183% |
| Rapid | 0.0046306992 | 0.7815% |

Of 27 rating/mode groups, 24 fitted: 20 improved test MSE and four worsened.
The regressions are rapid 1200–1399 and bullet 1400–1599, 1800–1999, 2200–2399.
Still below 500 training games: bullet 1600–1799 (458), rapid 2000–2199 (306),
rapid 2200–2399 (463). No fitted coefficient reached the search bounds.
More coverage does not guarantee improvement in every group. These are
point-estimate research results, not a universal replacement constant or a
validated deployment decision. The game split still permits shared players and
repeated positions across partitions; the reported curvature intervals are not
whole-game bootstrap uncertainty. This older research split also differs from
the CUDA candidate's SHA-256 split, so their errors are not directly comparable.

The complete immutable report is saved in the research experiment and at
`/main-monitor-db-backups/chessism/research/chessism-coefficient-ae06630e-1fc7-46f5-b32a-9b8cfdab8bff.json`.
Only the idle research worker and API were updated, with a separate persistent
research image tag; database/cloud/ingestion/FEN workers were not restarted.
The 20 focused coefficient/blunder-model unit tests passed. Review calibration
and sparse-group handling before a separate version tests the requested **0.20**
expected-points blunder cutoff; that threshold was not changed by this rerun.

## Fixed-coefficient blunder CUDA validation v1 (2026-10-26)

The user postponed rating calibration and requested a fixed-Lichess-coefficient
GPU candidate first. **Production remains unchanged.** This implements the
matrix/candidate/one-player validation stages, not canonical publication or an
ingestion migration. The 0.20 cutoff remains a separate proposed version.

Matrices now includes **Blunder moves — fixed coefficient** (`blunder-fixed`),
an eight-column reusable input formula: player, game, mode, previous score,
previous kind, current score, current kind and initial-prior indicator. No rating
or final outcome is an inference input. The existing rating-aware formula and
research history remain intact.

Algorithms → **Fixed-coefficient blunders → Validate blunders on GPU** submits
`POST /research/algorithms/blunder-validation` with only a player username.
Superuser access is required; extra parameters/threshold overrides are rejected.
The frozen configuration records `k=0.00368208`, 0.15 drop on the 0–1 scale,
±1,000 CP clipping, mover perspective, existing forced-mate rules, and the +15 CP
first-White-move assumption. Tablebase transitions remain unassessed and add no
blunders, as in existing saved counts. This is not rating-aware and performs no
fitting, coefficient activation, cloud work or production-summary writes.

`blunder_validation_job.py` reads one complete, non-deleted player's **entire
fully analyzed game set**, all three modes, at most 100,000 games and 2 million
source plies. This is an explicit whole-player cap, not sampling or truncation.
Unlike coefficient-training selection, it does not exclude openings, rating gaps
or timeout games. Native/legacy SQL freezes metadata, transitions, independent
SQL decisions and saved CPU game counts in one read-only repeatable snapshot.
Each source batch is bounded to 128 games/32,768 plies. Coverage inconsistencies,
missing/duplicate/gapped/nonfinite plies, missing saved summaries and active cloud
work fail before publication. SQL statements have 30-second timeouts; extraction
has a 300-second deadline. Temporary numeric input is removed on completion,
cancellation or failure, and the existing GPU memory cap remains 256 MiB plus
context/compiler memory. No CPU fallback is allowed.

The actual scalar `is_lichess_blunder` is the independent CPU oracle. Every move
must agree with both it and the SQL expression; every game's totals must agree
with scalar CPU, SQL and existing saved counts. Per-game move counts are checked
too. Only a compact, versioned AlgorithmRun report is persisted. Its UI shows
checks, mode totals, timing stages, input identity and mismatch examples; the two
new containers have their own Edit styles targets.

### Saved one-player checks

Player **chefshouse**, 538 fully analyzed games, 46,519 source plies and 23,277
own moves (264 unassessed tablebase transitions), RTX 4060. All six checks passed
with **zero mismatches** in both runs:

| Mode | Games | Blunders: CPU = GPU = SQL = saved |
| --- | ---: | ---: |
| Bullet | 83 | 92 |
| Blitz | 312 | 224 |
| Rapid | 143 | 36 |

- First run: `630d59fc-8b6c-4d29-8f9e-0a96a5e28147`, 02:51:53–02:51:57 UTC.
- Warm repeat: `f6e304bb-8aa8-4865-a2d2-4a2f4f879202`, 02:53:57–02:53:58 UTC.
- Identical input SHA-256:
  `e5f5aba5b62b293615ec9475c344d513778e1ff5e1c138540674590aa6b4bac1`.
- Implementation SHA-256:
  `b5bfc8b59edd8639f40d298deed79c59cd96d5b1df6f4c12dd0bf503d335cece`.

| Stage | First run | Warm repeat |
| --- | ---: | ---: |
| Metadata | 0.062 s | 0.065 s |
| Input extraction/preparation | 0.845 s | 0.750 s |
| Independent SQL reference | 0.270 s | 0.293 s |
| Scalar CPU reference | 0.131 s | 0.220 s |
| GPU preflight | 0.328 s | 0.002 s |
| GPU warm-up | 2.109 s | 0.010 s |
| GPU calculation/counting | 0.173 s | 0.0014 s |
| GPU transfers, both directions | 0.0007 s | 0.0006 s |
| Complete validation job, including report publication | 4.015 s | 1.398 s |

These are two correctness runs, not a statistically robust benchmark. GPU-stage
speed is **not** end-to-end speedup: input preparation dominates the warm path,
and SQL can count this small player's blunders without transferring all moves.
No production summary-publication performance was measured. Optimize/measure
input preparation and larger cohorts before choosing the canonical writer.

Acceptance: 111 focused backend tests, disposable PostgreSQL (including the
actual production summary writer, default-matrix/native/legacy parity and
concurrent-update snapshot checks), 100,016 synthetic actual-GPU transitions,
mocked browser controls/report and frontend build passed. Negative GPU tests
cover zero-blunder batches, stale saved counters, wrong SQL flags and immutable
report snapshots. Only API and the idle algorithm worker were recreated;
database/cloud/ingestion/FEN/summary/research-worker identities were preserved.

## Smooth rating-aware blunders v2 (2026-10-26)

The user reaffirmed that rating awareness is required, approved a 0.20
expected-point cutoff and requested this candidate workflow. This supersedes
the **proposed** fixed-rule production migration, not its useful parity tests.
Neither candidate is a canonical writer. No ingestion, FEN-analysis, live
blunder, accuracy, salience or score-distribution writer was changed.

Algorithms → **Rating-aware blunders v2 → Fit and validate v2 on GPU** calls
the superuser-only `POST /research/algorithms/blunder-training-v2`. It accepts
only `games_per_group` (100–3,000; default 1,000). Model/threshold overrides
are rejected. The frozen run embeds the existing `blunder-moves` input formula,
fixed model contract, source hash, implementation hash and fitted model ID.
The older v1 fitting panel and saved reports remain available independently.

### Numerical definition

For game-time ratings `rp,ro`, mode `m` and player-perspective CP `c`:

```
k0 = 0.00368208
u = ((rp + ro)/2 - 2300)/400
d = (rp - ro)/400
E = sigmoid(k0 * clip(c,-1000,1000) * exp(a[m]*u) + b[m]*d)
loss = max(0, E_before - E_after)
CP blunder = loss >= 0.20
```

Expected points means `P(win) + 0.5*P(draw)`, not a separately estimated win
probability. There are two fitted parameters per mode, not independent rating
bucket slopes. The fixed baseline remains fixed but the effective CP sensitivity
changes with `exp(a*u)`. `a` is not assumed positive; its sign must be learned.
The 2300 reference and 400 scale are fixed in this version. This is our model,
not a reproduction of a private Chess.com fitting formula.

Search safeguards are `-1 <= a <= 1`, `0 <= b <= 4`. The CPU controls a
deterministic multistart pattern search; bounded CUDA batches calculate losses.
Select L2 regularization from `[0, .0001, .001, .01]` using validation MSE only.
Training fits parameters; neither chronological test nor held-out-player labels
select parameters or regularization. Search-boundary/nonconverged results cannot
pass the search gate. These guardrails and the candidate model itself remain
subject to empirical validation.

CP is float64, not rounded to a histogram. Encoded mates/tablebases are masked
out of the CP conversion. Existing explicit forced-mate exceptions remain:
CP→mate against is a blunder when preceding CP is at least −700; mate for→mate
against is a blunder; mate for→CP is a blunder when resulting CP is at most +700.
Thus **+105 CP → mate against is always a blunder**, independent of the fitted
ratings. Other supported mate transitions retain their old judgments. Missing,
unsupported and tablebase transitions are unassessed, never zero-blunder labels.
The first White move retains the explicit +15 CP prior; all other moves use
their exact immediate predecessor and mover perspective.

### Frozen source and validation

Source selection is a fixed-seed, rating/mode-stratified sample of unique standard
games from completed profiles. Include timeouts/abandonments and rating gaps over
200, unlike v1. Require complementary valid outcomes, historical ratings in
1–4,000, dates and complete scored coverage. Legacy custom starts are excluded;
the native core already archives non-standard games. Unknown analysis provenance,
mates and tablebases are not fitted as Stockfish CP observations. All eligible
CP plies, including openings, are fitted with equal total weight per game.
Training uses one White-perspective observation per position, avoiding duplicate
game contributions from shared ingested players.

Chronological boundaries use 70%/85% ordered timestamps without splitting ties.
Player holdout is deterministic SHA256(normalized username) modulo 5. Any game
involving a held-out player is excluded from training and validation; only later
held-player games enter the separate test. Other later games form the primary
test. Audit FEN-ID overlap and separately score test positions not observed in
training. Ratings, mode and evaluation are the inference inputs; game outcomes,
player identity, FEN identity and dates are training/audit metadata only.

At most 3 million source plies are read, in bounded whole-game blocks. Excess is
an explicit error, never truncation. Source reads use one read-only repeatable
snapshot, 60-second statement timeouts, no parallel SQL, and a 600-second extraction
deadline. Active cloud work blocks new fitting. Fitting/validation has a
30-minute budget and cancellation checks between bounded operations. The shared
single-job GPU worker owns a disposable workspace removed on completion/failure/
cancellation and recovered on startup; the GPU pool stays capped at 256 MiB
plus CUDA context/compiler memory. No host-driver installation or CPU fallback.

Reports show independent scalar CPU/CUDA decisions and per-game-side counts,
before/after probability differences (maximum 1e-10), and exact zero-decision-
mismatch requirements. Compare **old15 / fixed20 / rating-aware20 on the same
assessed moves**, not arbitrary closeness to prior totals. Unfitted modes show
unassessed/null counts. Disagreement examples separate threshold from model effects.

CUDA computes per-game MSE, ten-bin game-weighted calibration error and paired
1,000-replicate whole-game-bootstrap reductions. CPU generates identical seeded
resampling identities, avoiding a new cuRAND dependency. Required support is
500 training / 100 validation / 100 test games. Gates expose overall positive
improvement (upper 95% relative-MSE-change bound below zero), subgroup regression
upper bound <=2%, overall calibration <=.03 and subgroup calibration <=.05.
Also report mode/rating-band/rating-gap/ending, game-phase, held-player and unseen-
position checks, and sigmoid range/monotonicity/complement invariants.

Sparse groups remain explicitly unvalidated; no invented coefficient or hidden
baseline substitution is published. Aggregate metrics describe the stratified
sample, not the whole game population. Whole-game resampling does not eliminate
cross-game dependence within players. The held-player check is separate.
There are no clock or complexity inputs; calibration alone cannot establish them.

`promotion_ready` is always false in this research workflow. Production recovery
and full-pipeline performance gates display **not run**, not passed. The
validation report is not permission to replace a live writer. Accuracy and
other numerical statistics remain future migration stages using shared input.

### Checks

- `tests.test_blunder_v2`: explicit +105-to-mate case, boundaries, symmetry,
  parameter recovery, equal-game weighting, train/test separation, player/date
  splits, unsupported coverage, negative gates, cancellation and authorization.
- `tests/check_blunder_v2_postgres.py`: disposable PostgreSQL native/legacy
  extraction, added timeout/gap populations, coverage and active-cloud guards.
- `scripts/validate_blunder_v2_cuda.py`: actual RTX 4060, 100,016 synthetic moves,
  zero decision mismatches, threshold/mate cases, known parameter recovery,
  GPU metrics/bootstrap and deliberate corrupted-count detection.
- Mocked browser fixture covers v2 submission/report/gates alongside existing
  candidate controls; frontend build and architecture/source checks pass.
- Broad backend regression suite: 126 tests passed.

### First bounded real-data v2 check

Run `635d2cf0-4774-4da1-bd29-7f7c80197b78` used 200 games per rating/mode
stratum and completed on 2026-10-26 in **94.071 seconds**. It froze 5,400
unique games / 391,529 source positions. Input SHA256:
`0ec6d1b66ef567815387f29565ce9c45d293ddd6f7659161416b60637f1ebef0`.
Read/preparation took 71.828 s; fitting 14.948 s. These research timings include
validation work and do not establish an end-to-end production speedup.

All three modes converged without search-boundary fits. Independent scalar CPU
and RTX 4060 CUDA agreed on all **96,929** checked validation/test moves and all
per-game-side counts. Maximum predicted-point difference was 1.11e-16.

| Mode | a | b | Chosen regularization | Test blunders: old15 / fixed20 / v2 |
| --- | ---: | ---: | ---: | ---: |
| Bullet | 0.2105293274 | 3.3976211548 | 0 | 816 / 576 / 379 |
| Blitz | 0.1230516434 | 1.5854492188 | 0 | 476 / 322 / 257 |
| Rapid | 0.1153631210 | 0.5944271088 | 0.01 | 670 / 454 / 328 |

The primary test had 602 games: MSE 0.17586692 vs baseline 0.19067926
(about 7.8% reduction). The paired whole-game-bootstrap relative-change interval
was [-14.13%, -1.54%]. Nevertheless **calibration failed**: ECE 0.06087695,
above the 0.03 overall limit. Held-player ECE was 0.08066735 on 192 games.
Search, mathematical, numerical and overall-improvement gates passed; calibration,
subgroups, phase/held-player/unseen-position checks did not pass, and rating-group
coverage was insufficient at this small sample size. No acceptance threshold was
relaxed to make this result pass. Production recovery/performance were not tested.

The saved research result remains a noncanonical candidate. The automatic writer
still uses the original coefficient and 0.15 probability-drop threshold. Only API
and the idle algorithm-worker containers were recreated; database, cloud,
ingestion, FEN, other summary and research workers kept their identities.
Temporary candidate input was removed; only the compact research report remains.

### Paired calibration investigation

The first report included baseline MSE but not baseline calibration. Diagnostics
version 1 adds both models' ten-bin calibration on exactly the same CP rows,
with equal total weight per game. This does not change fitting, the coefficient,
the 0.20 cutoff, mate rules, or any acceptance gate. Baseline calibration uses
the existing clipped fixed-coefficient expected-points curve; changing its
blunder cutoff from 0.15 to 0.20 does not change that curve.

Each model retains separate bin weights, predicted points and observed points.
The report includes each bin's signed gap and contribution to total ECE, plus
1,000 paired whole-game bootstrap resamples for candidate ECE, baseline ECE and
their difference. Negative differences favor the candidate. These percentile
intervals are exploratory, conditional on the fitted model and sampled cohort:
they are not bias-corrected, independent-player intervals, or simultaneous
confidence statements across all groups. Groups below 20 games have no interval;
the original, stricter support/promotion gates remain unchanged.

Five-bin ECE is a sensitivity diagnostic, not a substitute selected to pass a
gate. Calibration measurements depend on binning and sample support; see the
primary methodological discussion in
[Evaluating model calibration in classification](https://proceedings.mlr.press/v89/vaicenavicius19a.html).
Here the target is expected game points, including half points for draws, not
binary win probability. Cross-groups by mode/gap/ending and a player-concentration
audit help interpret aggregate results without introducing new fitted features.

Tests cover hand-calculated ECE, separate model bins, equal-game weighting,
paired identical-model zero differences, cancellation, bin boundaries, and
CPU/CUDA agreement for both point estimates and intervals. The Algorithms report
displays calibration in **percentage points** to avoid confusing 0.0609 with
0.0609 percent. Older reports remain readable and are not rewritten.

#### Completed audit (2026-10-26)

Run `f7538ab5-d9dc-49f5-adcd-888d8cf2abdc` (`Rating-aware v2 · calibration
audit · 200/group`) completed in 36.11 s wall / 36.07 s recorded compute.
Source SHA256, configuration and every fitted parameter/trial exactly match the
first bounded run above. This is a diagnostic rerun, not new independent test
evidence. Extraction took 11.79 s and fitting 15.22 s; the faster read is not
evidence of a pipeline optimization. All 96,929 checked decisions and all
per-game-side counts still agree between scalar CPU and CUDA.

| Test population | Games | Baseline ECE (pp) | Candidate ECE (pp) |
| --- | ---: | ---: | ---: |
| Primary | 602 | 4.019 | 6.088 |
| Bullet | 170 | 11.950 | 7.492 |
| Blitz | 127 | 5.049 | 7.627 |
| Rapid | 305 | 5.732 | 9.353 |
| Held-out players | 192 | 5.547 | 8.067 |
| Rapid, rating gap >400 | 206 | 4.780 | 12.085 |
| Time-ending games, all modes | 125 | 16.908 | 8.601 |

Primary candidate ECE's approximate bootstrap interval is [4.748, 9.206] pp;
baseline [2.631, 7.214] pp. The **paired difference** is +2.068 pp with interval
[+0.0066, +4.5383] pp. Its lower end is very close to zero; shared-player
dependence and bootstrap/binning limitations preclude a strong population-wide
significance claim. Held-player difference is +2.519 pp, interval
[-0.835, +5.356] pp. MSE still improves by about 7.8% overall, but rapid MSE
worsens by about 3.9%. Different metrics reveal different weaknesses.

The main primary ECE contributions are candidate predictions in [0,.1),
[.4,.5), and [.9,1], contributing 1.431, 1.537, and 1.398 pp respectively.
Five-bin and ten-bin ECE agree for the aggregate, but several subgroup estimates
are bin-sensitive. No binning or acceptance threshold was changed to pass a gate.

One player occurs in **299/602** primary-test CP games, and one occurs in 61/192
held-player CP games. A further read-only metadata audit of the same selection
(including games with no fitted CP rows) found rapid's median rating gap changes
from 82 in training to 819 in primary test; 265/315 selected rapid test games
involve one player. This establishes sample concentration and a large temporal
distribution shift, not the cause of any individual game's error. Every
rating/mode band remains below the predeclared coverage requirements.

Recommended next experiment: version a broader, player-diverse sampling plan
with explicit mode/rating-gap coverage, preserving difficult rapid games and
chronological/player-held-out evaluation. Compare conservative rating-effect
regularization using training/validation only, then evaluate on a new untouched
test set. Do not use this repeatedly inspected test set to select parameters or
claim final acceptance. Do not enable canonical publication yet.

Verification: 131 offline backend tests passed, synthetic RTX 4060 calibration
point estimates and paired intervals match NumPy to 1e-12, mocked browser test
passed, frontend production build passed (existing large-chunk warning only).
Only the idle algorithm worker was replaced; API, database, cloud/ingestion/FEN
and other summary workers kept their container identities. Worker image:
`chessism-algorithms:blunder-calibration-audit-20261026`, persistent runtime tag
also updated. The previous immutable worker image remains available. Only the
new compact research report was published; temporary source arrays were removed.

#### Five-times-larger sample (2026-10-26)

At the user's request, run `f2cb38df-79bc-455c-a294-feb718865d92`
(`Rating-aware v2 · larger sample · 1000/group`) increased only
`games_per_group` from 200 to 1,000. Model formula, coefficient, cutoff,
regularization candidates, source selection method, and validation gates were
unchanged. No code deployment or service restart was needed. The existing
algorithm worker completed in **146.565 seconds wall time** (2 min 27 s):
69.040 s extraction, 70.181 s fitting, 146.512 s total recorded compute.

The source contained **27,000 unique games / 1,936,410 positions**, below the
3-million-position guard. Source SHA256:
`2ddb421585e39d8a655d409f2187171db4dfc5ae6c46c933f3c344d2b3b0d3fb`.
The chronological boundaries were 2023-11-09T05:57:45Z (end training) and
2025-06-07T01:54:22Z (start testing). The larger fixed-seed selection includes
the previous sample, so this is an expanded exploratory analysis, not a newly
untouched acceptance set. Sample concentration was disclosed before submission;
this run did not introduce a player-balanced sampling method.

| Population | CP test games | Baseline ECE (pp) | Rating-aware ECE (pp) |
| --- | ---: | ---: | ---: |
| Primary | 3,014 | 1.671 | 6.332 |
| Bullet | 835 | 6.518 | 5.475 |
| Blitz | 679 | 3.531 | 5.070 |
| Rapid | 1,500 | 4.221 | 8.697 |
| Held-out players | 955 | 2.064 | 4.775 |
| Rapid, rating gap >400 | 996 | 4.257 | 11.633 |

Primary MSE was 0.17138560 versus baseline 0.18715457 (about **8.4% lower**).
Primary ECE's approximate whole-game-bootstrap interval was [5.602, 7.518] pp,
baseline [1.098, 2.995] pp; the paired increase was +4.661 pp, interval
[+3.281, +5.793] pp. These retain the shared-player and cohort limitations above.
One player occurs in **1,471/3,014 test games (48.8%)** and one in 330/955
held-player games (34.6%). More observations did not resolve the concentration.

All three fitted modes converged without boundary hits:

| Mode | a | b | Selected regularization |
| --- | ---: | ---: | ---: |
| Bullet | 0.2790031433 | 2.9518871307 | 0 |
| Blitz | 0.1104345322 | 1.6982059479 | 0 |
| Rapid | 0.0826654434 | 0.6385345459 | 0.01 |

Independent scalar CPU and CUDA agreed on **471,288** validation/test decisions
and all per-game-side counts, with zero mismatches. Numerical, mathematical,
search, and overall-MSE-improvement gates passed. The held-player gate passed
its predeclared 5-pp group limit, but this does not mean it beat baseline
calibration. Overall calibration, subgroup, phase, and unseen-position gates
remain unpassed. Only 1/27 rating/mode groups meets all the 500/100/100
train/validation/test support requirements; coverage is still insufficient.
Production performance and recovery remain untested, and promotion is disabled.

Next: version a player-diverse, mode/rating-gap-aware sampling design and reserve
an untouched evaluation set before refining the rating effect. This result
does not justify canonicalizing the model or changing the live 0.15 rule.
Only a new research report was saved; no live summaries were changed.

### Player-balanced sampling v1

The existing v2 research endpoint now accepts `sampling: "balanced_v1"` (legacy
requests remain supported). Algorithms defaults its v2 sampling checkbox to the
balanced variant. This versions the **input sampling**, not the blunder formula:
k0, 0.20 cutoff, mate exceptions, parameter search, calibration and numerical
gates remain unchanged. No automatic canonical publication is enabled.

The frozen plan defines:

- A maximum 10% share for **either player, in either color**, within each mode
  and train/validation/test/held-player partition. Caps are recomputed against
  actual selected counts, not just desired sample size; sparse groups do not
  waive the constraint.
- Nine average-rating bands per mode and three absolute-rating-gap bands:
  0–200, 201–400, and >400. Desired counts are 60% training, 20% validation,
  20% primary test, plus 10% held-player test relative to `games_per_group`.
  Each rating band divides its quota equally among the three gaps. Missing
  cells are not backfilled from another group.
- Time boundaries fixed before fitting: the earliest training cutoff and latest
  test cutoff among referenced prior v2 cohorts. The existing deterministic
  held-player assignment still excludes those players from all fitting and
  model selection. Neither outcomes nor CP values choose sampling priorities.
- A bounded candidate pool of up to 512 games per time/mode/rating/gap cell,
  prioritized round-robin across complete-profile owners, then a fixed hash seed.
  Selection enforces caps on both participants, not merely the owner. This is
  a bounded diversity heuristic, not a proof of a maximum-size feasible sample.
- A second source-coverage check excludes games without any Stockfish CP
  observation and rebalances before fitting, preventing the usable CP subset
  from breaking the cap. Mate transitions inside retained games remain intact.

Old v2 reports lacked game-ID manifests. The first balanced run reconstructs
each distinct historical cohort inside its read-only snapshot and requires an
**exact full-input SHA256 match** before trusting those IDs. Any mismatch fails
before fitting. All verified prior v2 games are excluded from all partitions.
Subsequent balanced reports retain selected IDs by partition and a hashed union
of previously seen IDs, making exclusions durable without repeatedly replaying
old sources. Metadata/identity manifests are retained; numerical position arrays
remain temporary. These checks concern the referenced v2 experiments, not every
unrelated historical research project.

Resource guards remain explicit: 3 million selected source positions, at most
6 million historical verification rows, 200,000 seen IDs, 512 candidates per
cell, existing SQL timeouts and ten-minute extraction deadline. The candidate
pool cap may leave additional eligible games unexplored; coverage is reported
as observed, not assumed. Balanced-cohort calibration is not a population-
weighted estimate. Existing support requirements still apply, and new player-
balance and prior-overlap gates cannot override a failing model gate.

Tests cover both-color caps, actual-count shrinkage, deterministic selection,
outcome independence, missing cells, chronological boundaries, held players,
cancellation, CP-subset rebalancing, corrupt manifests and historical hash
mismatches. Disposable PostgreSQL checks cover legacy/native extraction and
verified replay. Mocked-browser coverage verifies the sampling option, request
payload and concentration table; synthetic CUDA checks retain the +105 CP to
mate-against case and exact CPU/GPU decision agreement.

#### Completed balanced run (2026-10-26)

Run `231dc3f6-4975-4962-a36f-031b284a7720` (`Rating-aware v2 · balanced sample
v1 · 1000/group`) completed in **148.748 s wall time** (2 min 29 s), including
90.306 s source preparation/historical verification and 46.746 s fitting.
The source had **18,671 games / 1,341,896 positions**; SHA256
`8579d2cce335ffe2c1fc2443720cb6e3f911e6916daca39e926d37b6f43ecae0`.

The bounded pool held 63,034 candidate games. Initial balancing selected 18,952;
260 lacked any Stockfish CP observation, and 21 more were removed when rechecking
caps on the usable subset. The run exactly reproduced both prior distinct v2
source hashes, excluded their union of **27,000 games**, and recorded **zero
overlap**. Its manifest retains 45,952 seen identities (including those inspected
but removed), plus final selected IDs by partition.

All twelve mode/partition player-balance checks passed. The most frequent player
in primary test appears in **206/2,138 games (9.64%)**, compared with 48.8% in
the preceding unbalanced cohort. Each individual mode/partition remains below
10%, counting both colors. All three fits converged without boundary hits.

| Population | CP test games | Baseline ECE (pp) | Rating-aware ECE (pp) |
| --- | ---: | ---: | ---: |
| Primary | 2,138 | 2.412 | 4.591 |
| Bullet | 872 | 2.831 | 4.481 |
| Blitz | 1,021 | 4.200 | 4.851 |
| Rapid | 245 | 2.902 | 6.510 |
| Held-out players | 1,029 | 4.066 | 5.020 |

Primary MSE is 0.13548524 versus 0.17803093 (about **23.9% lower**). Primary
candidate ECE's approximate paired whole-game-bootstrap interval is
[3.691, 5.673] pp; baseline [1.740, 3.453] pp. The paired ECE increase is
+2.179 pp, interval [+0.862, +3.232] pp. Shared-player and stratified-sample
limitations still apply. Earlier and balanced cohorts differ, so comparing their
headline ECE values does not isolate a model improvement on a fixed population.

Scalar CPU and CUDA agree on **432,957** validation/test decisions and all
per-game-side counts, with zero mismatches. Numerical, mathematical, search,
overall-MSE-improvement, unseen-position, player-balance and prior-overlap gates
passed. Overall ECE still exceeds the unchanged 3-pp target. Held-player,
subgroup and phase gates did not pass; only **6/27** rating/mode bands satisfy
all minimum supports. Rapid has only 245 primary and 93 held-player games;
its >400-gap diagnostic has 59 games and is explicitly insufficient. Balanced
quotas are targets, not a claim of equally filled cells.

| Mode | a | b | Selected regularization |
| --- | ---: | ---: | ---: |
| Bullet | 0.2593021393 | 2.2677459717 | 0.0001 |
| Blitz | 0.0746793747 | 1.6747341156 | 0.0001 |
| Rapid | 0.0590038300 | 0.9817638397 | 0 |

Balancing succeeded; the model is still not approved for canonical use. The next
model revision should be designed/selected on training and validation data, with
the evaluated test identities kept excluded and a separately reserved evaluation
cohort. Sparse rapid/rating coverage remains an explicit limitation.

Verification: **138 offline backend tests** passed, disposable PostgreSQL checks
passed, synthetic CUDA checks and mocked-browser checks passed, frontend build
passed (existing large-bundle warning only). Only API and the idle algorithm
worker were recreated; all other container identities remained unchanged.
Persistent images are `chessism-api:balanced-blunders-20261026` and
`chessism-algorithms:balanced-blunders-20261026` (also tagged `:runtime`), with
prior immutable versions retained. The only live-data write is the separate
research report/job bookkeeping; existing blunder summaries and their 0.15
threshold are unchanged. Temporary numerical arrays were removed.

### Calibration refinement v1 (2026-10-26)

This is a versioned **research experiment**, not a canonical/live model update.
`POST /research/algorithms/blunder-training-v2` now optionally accepts
`refinement: "calibration_v1"`, requiring `sampling: "balanced_v1"`. The UI
defaults to this experiment and keeps both earlier samplers available when the
refinement checkbox is off. Old requests/configurations retain implementation
version 2; the refinement snapshots implementation version 3 and its full fixed
selection policy. The report format stays backward compatible, with an explicit
`refinement` section.

Predeclared before the first real-data refinement run:

- Same smooth equation, `k0=0.00368208`, CP clipping, rating anchor, bounds,
  0.20 expected-points-loss cutoff and explicit mate rules.
- Fit training data with L2 strengths `[0,.0001,.001,.01,.03,.1,1]`.
  For each converged fit, scale **both rating adjustments** `a,b` by one of
  `[.25,.5,.75,1]`: 28 candidates per mode, not repeated searches against test.
- CUDA computes validation MSE and ten-bin ECE, one total weight per game.
  Candidates use their own prediction bins. The baseline is `(a,b)=(0,0)`.
- A valid candidate must have validation ECE <=.03, MSE improvement >=1% of
  baseline MSE, and retain >=50% of the best valid candidate's MSE gain.
  These are explicit engineering selection rules, not universal standards.
  Select the lowest MSE among eligible candidates; ties use ECE, stronger L2,
  then weaker rating strength. Nonconverged/boundary fits cannot qualify.
- If none qualify, retain the best-calibrated predictively eligible candidate
  only for diagnostics (otherwise the best-calibrated valid fit). Do **not**
  relax targets, publish live counts or substitute a CPU model.
- Only when every mode qualifies is the separate test/held-player cohort
  evaluated once. Otherwise CPU/GPU checks and development diagnostics use
  validation only, test gates explicitly say `not_run`, and no test counts or
  misleading zero-test-error values appear. Test outcomes cannot select a fit.
- The prior-cohort exclusion manifest remains verified/transitive. A refinement
  queued against an outdated completed-cohort history is rejected before source
  selection. All inspected identities stay in the manifest even if the final
  test is withheld; a later run cannot silently recycle this sample.
- Report development calibration by mode, rating, rating gap, prediction bin,
  and game phase. These selected-on results are not independent acceptance
  evidence.
- Keep whole-game intervals, and add paired player-connected-component
  bootstrap intervals on test/held-player data. Games connected by either
  player, transitively and across colors, stay together. Require >=20 components
  and none exceeding 20% of assessed games; otherwise report insufficient
  independent components instead of claiming player-independent precision.
  This conservative method cannot resolve giant connected player networks, and
  unrelated temporal/event dependence may still remain.
- Existing overall .03 and subgroup .05 final-test gates are unchanged. A
  representative-population audit is explicitly `not_run` and required before
  promotion: balanced data alone cannot establish calibration for actual usage.
  Production performance/recovery/publication also remain outside this experiment.

Tests cover independent hand-calculated metrics, game weighting, selection
tradeoffs, poisoned test labels, failed-selection test withholding, sparse groups,
configuration/route allowlists, cancellation, both-color transitive components,
insufficient clusters, paired intervals and no promotion. Synthetic RTX 4060
checks verify all 28 candidate metrics, the chosen fit, 100,016 refined move
decisions and component intervals against CPU references. Mocked-browser checks
cover both legacy reports and refinement requests/withheld-test rendering.

#### First refinement experiment

Run `342c223a-9335-4a64-a7ce-57c7a9d77147`, **Rating-aware blunders · calibration
refinement v1**, completed on 2026-10-26 at 17:52:02 UTC. Wall time was **91.383 s**:
40.291 s read/preparation, 45.603 s fitting, 0.330 s scalar reference, and
3.210 s GPU validation inference/metrics. These timings are not a production
throughput benchmark; rapid was not fitted because development support failed.

The input contained **13,355 games / 981,485 positions**, with SHA-256
`f95ba1d00699bff6622d37c4a5f51938402dfc6a721d61cc8ec30a222a4d8fad`.
The verified prior manifest excluded **45,952 games**, with **zero overlap**.
From 56,212 candidate metadata rows, 13,537 games were initially selected;
147 lacked CP observations and 35 more were removed to preserve caps. All
nonempty mode/partition groups meet the 10% both-player cap. Rapid's held-player
partition is empty, so full player-balance/coverage approval is not claimed.
The resulting transitive identity manifest contains **59,489** unique IDs and
its saved hash was verified. Temporary numerical arrays were removed.

These are **selected-on validation results, not independent test results**:

| Mode | Training / validation games | Baseline ECE (pp) | Selected ECE (pp) | Baseline MSE | Selected MSE | Selection |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Bullet | 4,245 / 714 | 4.0076 | 1.9901 | .18702236 | .12579978 | Passed; 8 eligible candidates |
| Blitz | 4,327 / 971 | 3.0307 | 2.8622 | .17583559 | .11620861 | Passed; 4 eligible candidates |
| Rapid | 817 / 86 | — | — | — | — | Not fitted; requires 100 validation games |

Selected bullet parameters: `a=.2978200912475586`, `b=2.254697799682617`,
L2=0, rating strength=1. Selected blitz parameters: `a=.10209083557128906`,
`b=1.5330429077148438`, L2=.001, rating strength=1. Neither mode selected
additional shrinkage on this cohort. Do not attribute differences from the
earlier test reports to shrinkage: both the cohort and evaluation partition differ.

Because rapid lacked validation support, **the entire final test/held-player
evaluation was withheld**. The report is `development_only`; no test statistics
or counts were published. Scalar CPU and GPU agreed on **123,072 development
moves** and all per-game counts, with zero mismatches. Mathematical/search gates
are not globally approved because rapid has no fitted parameters. Promotion,
representative-population validation, and production performance/recovery remain
pending; no live blunder/accuracy/ingestion/summary behavior changed.

Verification: **115 offline backend tests**, disposable PostgreSQL (including
stale-queue rejection), synthetic real-GPU checks, mocked-browser checks and
frontend production build passed. The existing >500-kB bundle warning remains.
Only API and the idle algorithm worker were recreated; all protected container
IDs stayed unchanged. Persistent deployment tags:
`chessism-api:refined-calibration-20261026` and
`chessism-algorithms:refined-calibration-20261026`, also tagged `:runtime`.
Previous immutable versions remain available. The next research step is rapid
development/held-player coverage, not weakening targets or reusing inspected test
outcomes for acceptance.

### Role-locked development reuse v1 (2026-10-26)

The first refinement had only 86 rapid validation games because the original
sampler excluded **all** previous cohorts, including development data. The new
`sampling: "development_reuse_v1"` option (implementation version 4) is available
only with `refinement: "calibration_v1"`; it is now the UI default for refinement.
The old sampler remains available. No model formula, parameter grid, threshold,
validation target, chronological boundary, held-player assignment, quota or
10% both-player cap changes.

- Reconstruct historical roles from all distinct v2 source cohorts. Old cohorts
  without saved split identities require exact full-input SHA-256 replay before
  any role can be reused; changed inputs fail closed. Saved split counts must
  be disjoint and agree with the identity manifest. Identity-only records without
  provable roles remain protected.
- Training games may be reused **only as training**; validation games **only as
  validation**. Games with conflicting historical roles are blocked. Every
  historical test/held-player reservation is protected, including tests withheld
  after failed development selection. New final-test games must be absent from
  **all** known historical cohorts, including development data.
- Historical role/date filters are applied in SQL **before** the bounded pools
  are ranked. Identity/role checks are repeated after selection and CP rebalance.
  Report reused training and validation counts separately from protected-game,
  test-overlap and role-violation counts; allowed development overlap is not
  mislabeled as test leakage.
- Persist a hashed transitive role manifest plus all seen IDs before fitting.
  A failure/cancellation retains a `chessism_blunder_source_reservation_v1`
  research record, shown honestly as an incomplete report. Future history scans
  include these records, so interruption cannot make test games appear fresh.
  Source games/FENs, live summaries and canonical algorithms are never written.
- Reject queued requests if the referenced history changes. The experiment still
  withholds the final test unless every mode passes development selection.
  Reused validation data are selected-on development evidence, not independent
  acceptance evidence. More rows alone do not establish better calibration.

Tests cover unknown/conflicting roles, corrupted manifests, full legacy replay,
fresh-test isolation, training↔validation role drift, native/legacy SQL parity,
both-player caps, durable failed/cancelled source reservations, stale queues,
route allowlists and browser report/request wiring. No production publication or
statistical gate is bypassed by enabling reuse.

#### Development-reuse experiment

Run `dcfb320a-2397-4492-b785-81c5d904fc9f` completed on 2026-10-26 at
18:14:26 UTC, with **18,562 games / 1,337,876 positions** in **180.200 s** wall
time. Read/preparation (including exact historical-role replay) took 80.070 s,
fitting 93.325 s, scalar reference .598 s and GPU inference/metrics 3.802 s.
These are research-validation timings, not a production throughput claim.
Input SHA-256:
`c682d35d425c5eb81d04d035bb008e4e977118a85f9d879c0785830824980556`.

Historical roles were verified for 59,489 known games: 34,257 reusable training,
7,129 reusable validation and 18,103 protected. This run reused **13,808 training
and 2,907 validation games** in their original roles, plus five new validation
games. All 1,842 selected test/held-player games were new; **zero protected-game
overlap, test overlap or role violations**. The 62,759-row metadata pool yielded
18,617 initial games; 52 had no usable CP and three more were removed to preserve
caps. All nonempty groups meet the 10% cap. Rapid held-player coverage is still
empty, so aggregate player-balance approval is not claimed.

| Mode | Training / validation / test / held-player games | Baseline validation ECE (pp) | Selected validation ECE (pp) | Baseline MSE | Selected MSE | Selection |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Bullet | 4,961 / 1,004 / 570 / 259 | 3.8180 | 2.9467 | .18559802 | .11713551 | Passed; 8 eligible candidates |
| Blitz | 5,138 / 1,318 / 630 / 313 | 3.5770 | 2.9094 | .17639399 | .12315337 | Passed; 1 eligible candidate |
| Rapid | 3,709 / 590 / 70 / 0 | 3.1242 | 4.8198 | .16261741 | .15557320 | Not passed; 0 eligible candidates |

Rapid development coverage improved from **86 to 590 validation games** without
new FEN analysis or weaker caps. Its selected diagnostic candidate is L2=.01,
rating strength=.5, `a=.022130966186523438`, `b=.32761096954345703`. It retains
the required prediction gain but misses the 3-pp calibration target. The smallest
rapid ECE among **all** 28 candidates is 3.4908 pp (L2=1, strength=.75), also
above target and failing the predictive-gain requirement. More adequate sample
size did not make the present model family sufficiently calibrated.

Bullet selected L2=.0001, strength=1, `a=.2472667694091797`,
`b=2.3239002227783203`. Blitz selected L2=.001, strength=1,
`a=.07892704010009766`, `b=1.4801712036132812`. These are **development** metrics;
do not compare them to older final-test reports as if they used the same cohort.

Because rapid selection did not pass, the **entire final test stayed unevaluated**.
Rapid's fresh test pool is separately insufficient (70 primary / 0 held-player
games). The result remains `development_only`, not canonical. Mathematical,
search, numerical and historical-partition-safety checks pass. CPU and GPU agree
on **213,677 development moves** and all per-game counts, with zero mismatches.
No targets were changed and live CPU summaries/rules remain unchanged.

The durable role/identity manifest contains **61,349** unique games: 34,257
training, 7,134 validation and 19,958 protected, including initial reservations
removed during CP rebalance. Both hashes and the next request's transitive
lineage anchor were verified. No active algorithm jobs remain; temporary arrays
were removed (`.worker.lock` only).

Verification: **126 offline backend tests**, private PostgreSQL checks, mocked
browser and frontend build passed (existing >500-kB chunk warning only). API and
idle algorithm worker alone were recreated; all protected container IDs stayed
unchanged. Persistent tags are `chessism-api:development-reuse-20261026` and
`chessism-algorithms:development-reuse-20261026`, also tagged `:runtime`; prior
immutable versions remain available. Next research work is rapid calibration
and fresh evaluation coverage, not automatic publication or relaxed thresholds.

### Frozen final evaluation with an explicit rapid exception (2026-10-26)

The user elected to keep the selected model parameters rather than continue
rapid refinement. This authorizes a **provisional rapid calibration/coverage
exception for evaluation**, not a claim that the failed checks passed and not
automatic live publication. Bullet and blitz receive no exception.

`POST /research/algorithms/blunder-final-evaluation` accepts only a completed,
role-verified development run ID and the explicit
`rapid_provisional_evaluation_v1` acknowledgement. It creates a separate
`frozen_rating_blunder_final_evaluation_v1` research job. The parent report,
parameters, coefficient, threshold, mate handling and original quality targets
remain immutable. The request pins hashes of the entire parent report and fits.

The worker reconstructs the original source using its historical instructions,
not a fresh selection, inside a bounded read-only repeatable-read transaction.
All metadata/position bytes, source SQL fingerprint, chronological partitions,
game IDs and role/identity hashes must match before any test predictions run.
Replaying historical instructions is permitted only on this verified path;
ordinary queued development requests still reject stale lineage. Active cloud
jobs block extraction. No fitting or validation-based candidate selection is
performed; only the original primary and player-held-out tests are evaluated.

Before metrics, a parent-row lock and durable research-only reservation mark the
test consumed. Failure or cancellation after that point does not make the test
fresh again. Cancellation before this boundary leaves it unevaluated. A second
attempt cannot re-evaluate a consumed test. Temporary numerical arrays are
removed on completion/error; the compact report and reservation survive.

The report retains the original model ID/fits, identifies the new evaluation
implementation, records the exception separately and keeps all failed/unsupported
gates visible. The UI must not say that all modes passed validation when showing
this report. Incomplete final-evaluation reservations have their own honest
display. Live blunders, accuracy, salience and canonical summary writers are
unchanged. `canonical`, `production_changed` and `promotion_ready` remain false.

Verification: **135 offline backend tests** passed; private PostgreSQL verified
native/legacy exact replay, cancellation, durable consumption across failure,
active-cloud protection and immutable parent reports. Mocked browser reporting
and the production frontend build passed (existing bundle-size warning only).
Tests poison development labels in evaluation-only execution and assert that no
fitter is called. This is not a new calibration fit or relaxed statistical gate.

#### Frozen final evaluation results

Run `98e9698a-cc74-4e37-a680-21952f002655` completed on 2026-10-26,
18:42:51.940111–18:44:21.261653 UTC: **89.322 s wall time**. It replayed the
exact **18,562-game / 1,337,876-position** source of
`dcfb320a-2397-4492-b785-81c5d904fc9f`; input SHA-256 remained
`c682d35d425c5eb81d04d035bb008e4e977118a85f9d879c0785830824980556`.
The parent report hash, fit objects and model ID remained unchanged. No fitting
occurred. Extraction/historical verification took 77.913 s, scalar reference
.380 s and GPU inference/metrics 4.689 s; these include validation overhead and
are **not a GPU-vs-CPU production speed comparison**.

The RTX 4060 evaluated **1,270 primary test games and 572 held-player games**.
CPU/CUDA agreed on all **142,618 checked moves**, with zero decision or per-game
count mismatches; maximum prediction difference was `2.220446049250313e-16`.

| Primary test | Games | Baseline MSE | Candidate MSE | Baseline ECE (pp) | Candidate ECE (pp) |
| --- | ---: | ---: | ---: | ---: | ---: |
| All modes | 1,270 | .18821435 | .14028905 | 1.9605 | 4.2023 |
| Bullet | 570 | .19855343 | .15541295 | 2.9168 | 4.7684 |
| Blitz | 630 | .18294713 | .12551765 | 3.7033 | 4.2752 |
| Rapid | 70 | .15142967 | .15007984 | 5.1850 | 7.9496 |

Primary MSE improved **25.46%**, with paired whole-game relative-change 95%
interval `[-.292532, -.218408]`. Held-player MSE improved **24.11%**
(.18086663→.13726529), with ECE 3.6088→4.8941 pp. Overall calibration misses
the original 3-pp gate. Bullet/blitz mode aggregates satisfy the existing 5-pp
subgroup gate, **not** a 3-pp claim. Rapid has insufficient test support and no
held-player games; its 7.95-pp estimate is uncertain, not a robust population
error estimate. Primary ECE-change whole-game interval spans zero
`[-.000801, .031776]`; do not claim definitive calibration deterioration from
the point estimates alone.

Supported failing subgroups include bullet rating ≥2600 (5.59 pp), blitz rating
≥2600 (8.76 pp), rating gap 201–400 (6.25 pp), timeouts (7.22 pp) and other
endings (5.67 pp). Only **2/27 rating-mode groups** meet all support thresholds.
Opening plies 1–20 fail the phase check (7.77 pp); middle and late phases and
unseen-training-FEN checks pass. The player network has only 10 primary and 12
held-player connected components, with largest shares 56.3% and 58.6%, so the
stronger repeated-player uncertainty requirement remains unsupported. Whole-game
intervals alone do not resolve this dependence.

Primary test blunder totals (old .15 / baseline .20 / rating-aware .20):
bullet **2,252 / 1,558 / 1,133**, blitz **1,747 / 1,147 / 832**, rapid
**176 / 109 / 104**. These separate threshold changes from rating effects;
fewer flags alone are not evidence of more accurate blunder classification.

Numerical, mathematical, overall prediction improvement, search, held-player
aggregate, unseen-position and historical-partition-safety checks pass. Overall
calibration, supported subgroups and opening-phase checks do not. Coverage and
repeated-player uncertainty remain insufficient; representative-population and
production performance/recovery tests remain not run. Thus the newly observed
limitations are **not confined to rapid**. The narrow rapid exception does not
authorize silently treating all those checks as passed or replacing live CPU
summaries. All promotion/production flags remain false. A separate-summary shadow
pilot is a possible next step; live publication requires an explicit decision
about these broader tradeoffs as well as the operational checks.

Only API and idle algorithm worker were recreated; all protected service IDs
were preserved. Release tags are `chessism-api:frozen-final-20261026` and
`chessism-algorithms:frozen-final-20261026`, also tagged `:runtime`; previous
images remain available. The normal research queue saved the result, no active
research jobs remain, and the original development report stays immutable.

### Sampled GPU shadow pilot (2026-10-26)

The approved next step is an **operational shadow comparison**, not canonical
publication and not another independent calibration test. Frozen parameters are
read from final-evaluation run `98e9698a-cc74-4e37-a680-21952f002655`, protected
by the parent-report and fit hashes. No fitting, target changes or live UI data
replacement occurs. The original model-quality failures remain visible.

`POST /research/algorithms/blunder-shadow-pilot` takes that parent ID, exactly six
player names and `sampled_shadow_only_no_live_changes`. Inference runs only on
CUDA, with independent scalar/vector CPU and legacy SQL references for checks.
The worker first computes **500 games**, stops if any reference disagrees, saves
a separate durable checkpoint, then expands to at most **6,000 unique games**.
Each player has targets of 400 bullet, 400 blitz and 200 rapid games. Selection
is fixed-seed/outcome-blind; duplicate shared games have one assigned owner.
There is no shortage padding, and the whole-game input cap is one million
positions. This is a sampled history, never a full-player summary claim.

Selected players span the available ratings: **galeaj, prosaicjonny,
annacramling, thechessnerd, hikaru and magnuscarlsen**. A read-only inventory of
the 46 complete profiles established that each selected player has sufficient
fully analyzed games in every mode. Source positions, exact predecessors,
historical ratings, legacy SQL flags and saved CPU game counts share a bounded
read-only repeatable snapshot. Active cloud work blocks starting extraction.

The pilot compares three rules on exactly the same player moves:

1. Existing coefficient-only .15 rule, checked against the actual production
   scalar function, independent PostgreSQL expression and saved per-game counts.
2. Coefficient-only .20 rule, isolating the threshold effect.
3. Frozen rating-aware .20 rule, with exact CPU/CUDA decision and per-game-count
   agreement. Forced mates retain the explicit exceptions; tablebase transitions
   remain unassessed rather than becoming fitted CP observations.

`blunder_shadow_store` publishes research-only snapshots in `AlgorithmRun.result`.
It atomically upserts each sampled game, guards model versions/generations and
retains the last complete generation on failure. Actual pilot execution tests
500-game publication, duplicate retry (zero changed games), appending the other
5,500 games, equality with a full rebuild and durable reload. Disposable
PostgreSQL additionally tests an actual synthetic FEN-score change, recalculates
with the real legacy writer and frozen candidate, updates only the affected game,
and exercises rollback, restarted-caller recovery, stale writes and cancellation.
No live FEN or canonical summary is modified to perform those failure tests.

The report exposes per-player/mode totals and 0–6+ histograms, changed moves,
forced mates and near-threshold examples. Before/after boards and legal SAN/UCI
transitions are verified. This establishes source/arithmetic consistency, **not
human ground-truth blunder quality**. Same-formula warm CPU/GPU benchmarks include
GPU transfers and per-game reduction and use three alternating repeats; source
reads, SQL reference work and separate-summary writes are timed separately.
Automatic production scheduling and worker-outage recovery are not implemented
or claimed by this pilot.

#### First pilot and reporting correction

Run `2bb8cde0-227d-4521-accf-1a5b11030dc9` completed at
19:12:22.764673–19:12:46.896298 UTC (**24.132 s wall time**): exactly 6,000
games, 441,914 source positions and 221,312 player moves. The first 500 games
contained 18,881 player moves. All seven mismatch counters were zero; maximum
CPU/GPU prediction difference was `2.220446049250313e-16`. Each player received
exactly 1,000 assigned games. All 60 saved example rows had valid legal board
transitions (some refer to the same move under different change categories).

Review identified **three games with zero moves by the sampled player**: galeaj
rapid game 87496981883, galeaj bullet game 172947418140 and magnuscarlsen bullet
game 157543650015. Their counts correctly remained zero, but the first report
included them in zero-blunder histogram bins. The original report is preserved,
not silently rewritten. Shadow implementation/summary **version 2** records
`assessed_games` and `no_assessed_player_moves`, and excludes zero-move or wholly
unassessed games from **all three** histograms. Total sampled games and per-game
reference counts are unchanged. The UI identifies legacy reports explicitly.

The sampled model flags **9,389 old-rule / 6,418 baseline-.20 / 5,146
rating-aware-.20 blunders**. The threshold removes 2,971 flags; the rating
adjustment adds 414 and removes 1,686 relative to baseline .20 (net −1,272).
Compared with the old rule, the candidate adds 163 and removes 4,406 flags
(net −4,243). There are 3,529 unassessed transitions and 1,283 forced-mate flags.
These are definition differences, not numerical mismatches or accuracy scores.

| Mode | Sample games | Existing .15 | Baseline .20 | Rating-aware .20 |
| --- | ---: | ---: | ---: | ---: |
| Bullet | 2,400 | 4,442 | 3,099 | 2,267 |
| Blitz | 2,400 | 3,450 | 2,334 | 1,938 |
| Rapid | 1,200 | 1,497 | 985 | 941 |

Examples illustrate an important model trade-off. Galeaj bullet counts change
494→165 from the rating adjustment alone; Magnus bullet changes 252→388. A
720-rated bullet player's `f4` (game 136512236098, ply 7) drops +492→−11 CP but
has predicted loss .152942 and no longer qualifies at .20. Hikaru's `Rd2`
(game 1538093285, ply 63; ratings 3151/2860) drops −208→−354 CP and gains a
blunder label because the model loss is .204526. These follow the frozen formula;
they are not proof that the new human-facing labels are preferable. Forced-mate
examples, such as +30 CP→mate against after `dxe4` in game 85833042579, stay
blunders in all three rules.

First-run same-formula warm medians: **.037437 s vectorized CPU versus .024536 s
GPU including transfers** (~1.53× calculation speedup, ~13 ms saved). Selection
took 2.614 s, source read/packing 8.673 s, independent legacy SQL references
2.859 s and two shadow publications .263 s. The 24.132-s wall time also includes
GPU setup, scalar references, smoke repetition, examples and reporting. These
measurements do **not** establish a 1.53× end-to-end ingestion speedup; database
preparation dominates this bounded pilot, and the current CPU writer still
calculates other statistics.

Verification: **144 offline backend tests**, disposable PostgreSQL source and
operational checks, mocked browser checks and frontend build passed. Actual GPU
synthetic comparisons covered 100,016 moves on both RTX 4060 and GTX 1070 with
zero mismatches; the live pilot used the existing dedicated RTX 4060. No driver,
cloud controller, ingestion/FEN worker, database or canonical-summary worker was
restarted. Only API and idle research algorithm worker receive the new code.

#### Corrected summary-v2 repeat

Run `297a6997-66ef-4478-befd-630ec1a947c5` completed at
19:21:46.735666–19:22:06.229521 UTC (**19.494 s wall time**) on the RTX 4060.
The 500-game smoke and all seven full-sample reference checks passed again.
Every per-game record and the source hash matched the first run exactly:
`7bc7a64016309a283a5094bff3dc436e9e31505a5dda758e32cde14837fd2eed`.
The frozen parent report and fitted coefficients also matched their pinned
hashes. The count totals above are unchanged. All 60 example transitions passed.

The corrected histograms contain **5,997 assessed games**: 2,398 bullet,
2,400 blitz and 1,199 rapid. The three zero-player-move games remain in the
6,000-game audit manifest but are excluded from all three histogram series.
Each histogram sums exactly to its group's assessed-game count.

Duplicate retry changed zero games; the 500-game checkpoint and 5,500-game
append produced generations 1 and 2. Their combined result matched a fresh
full aggregation and the durable database reload. Same-formula warm medians
were **32.352 ms CPU / 21.178 ms GPU including transfers** (~1.53×). Selection
took 1.504 s, source read/packing 5.484 s, independent SQL references 2.693 s,
and shadow publication .217 s. This is an operational repeat, not a fresh
calibration sample or proof that the reporting change accelerated the pipeline;
cache state differs from the first run.

Implementation SHA-256:
`4cc91c05dc20f411fb86a4616ca75058c1a146348a8acee88ed987f6a42f31ae`.
Deployment uses persistent `:runtime` tags backed by immutable
`chessism-api:shadow-pilot-v2-20261026` and
`chessism-algorithms:shadow-pilot-v2-20261026` images. API image SHA:
`540fac9620647e664df8487198e22151c5c3b7b9c453ad26150154954d569fa3`;
research worker image SHA:
`827676e840a7c68d9b5c9dd82358096002116ec1424ca775e6a87af1e3696e2f`.
The report is available in Algorithms run history. Live summaries and canonical
model selection remain unchanged; original calibration/coverage limitations
are not waived by these successful implementation checks.

### Paired rating-aware threshold comparison (2026-10-26)

User-approved comparison run `67993424-2190-4566-8c60-6e3ee6329ee2` uses the
completed summary-v2 pilot `297a6997-66ef-4478-befd-630ec1a947c5` as its immutable
parent. `POST /research/algorithms/blunder-threshold-comparison` accepts only
that completed pilot contract and `compare_thresholds_only_no_live_changes`;
it does not accept arbitrary thresholds or fitting controls. The versioned
operation compares **.15 and .20** using the same frozen ratings formula.
The model's existing default remains .20 and live CPU summaries are unchanged.

The read-only source snapshot reproduces all 6,000 game identities, metadata,
ratings and per-game input hashes. Source SHA remains
`7bc7a64016309a283a5094bff3dc436e9e31505a5dda758e32cde14837fd2eed`.
All previous three-rule counts reproduce exactly. A changed source, parent
report or model configuration stops the run rather than silently substituting
a new sample. The new .15 GPU flags and per-game counts match an independent
scalar oracle exactly. Threshold monotonicity, unchanged mate decisions and
unchanged assessment masks all pass. Parent reports are not rewritten.

Run time: **19:34:26.674355–19:34:46.183626 UTC**, **19.509 s wall time**, RTX
4060. It covers 441,914 source positions and 221,312 selected-player moves,
including 3,529 unassessed transitions. Histograms still contain 5,997 assessed
games, excluding the same three zero-player-move games. All seven prior checks
and five new threshold checks report **zero mismatches/violations**.

| Mode | Games | Current coefficient-only .15 | Rating-aware .20 | Rating-aware .15 |
| --- | ---: | ---: | ---: | ---: |
| Bullet | 2,400 | 4,442 | 2,267 | 3,341 |
| Blitz | 2,400 | 3,450 | 1,938 | 2,919 |
| Rapid | 1,200 | 1,497 | 941 | 1,393 |
| Total | 6,000 | 9,389 | 5,146 | 7,653 |

Lowering the threshold adds **2,507 flags**: 2,075 restore current-rule blunders,
while 432 are additional rating-aware flags not present in the current rule.
Relative to the current rule, rating-aware .15 adds 595 and removes 2,331,
net **1,736 fewer (18.49%)**, versus 45.19% fewer at .20. The 595 includes 163
already added at .20; overlapping change categories must not be summed.

Historical player-rating bands expose a remaining asymmetric effect:

| Bullet rating at game time | Games | Current .15 | Rating-aware .20 | Rating-aware .15 |
| --- | ---: | ---: | ---: | ---: |
| <1000 | 341 | 649 | 148 | 230 |
| 1000–1499 | 392 | 1,042 | 407 | 598 |
| 1500–1999 | 202 | 461 | 200 | 295 |
| 2000–2499 | 661 | 1,501 | 869 | 1,242 |
| 2500–2999 | 18 | 21 | 13 | 17 |
| 3000+ | 786 | 768 | 630 | 959 |

This is **six players**, not a representative population; especially the
18-game band must not support a general quality claim. The change remains
large below 1000 (64.56% fewer flags) and reverses at 3000+ (24.87% more).
For example, Galeaj's sampled bullet counts change 724→165→256, while Magnus's
change 409→388→591. This is behavior of the frozen rating model, not a GPU error.

The report retains 36 distinct-game examples (27 bullet, six blitz, three rapid),
including 18 bullet examples below 1500. Selection is bounded and illustrative,
not a statistical prevalence sample; categories are not equally represented.
All 36 legal board transitions/SAN moves were verified. No new Stockfish search
or human ground-truth adjudication was performed. Findings from stored scores:

- Galeaj's `f4`, game 136512236098, ply 7, ratings 720/663: +492→−11 CP,
  expected-score loss .152942. **Restored at .15**, not flagged at .20.
- Galeaj's `Bd3`, game 108959756518, ply 7, ratings 767/765: −8→−439 CP,
  loss .149042. Still not flagged at .15; it is a genuine boundary case under
  this definition, not a rounding or implementation error.
- Hikaru's `Qe1`, game 3604583983, ply 43, ratings 3211/3157: +72→−36 CP,
  loss .162684. Newly flagged at .15, although the current coefficient-only
  rule does not flag it. High-rating sensitivity can change labels in the
  opposite direction to low-rating sensitivity.
- The forced-mate controls remain unchanged; in particular a +490 CP→mate
  against transition remains a blunder under all compared rules.

Conclusion: .15 restores many flags suppressed at .20, but does not resolve the
strong rating-dependent differences or the prior calibration/coverage failures.
Predictions, fitted coefficients and original quality gates are unchanged.
Closer aggregate counts are **not** evidence of better classification quality.
The decision to promote either threshold remains separate from this comparison.

Verification: **150 offline backend tests**, mocked report-browser checks and
frontend build passed (existing large-bundle warning only). Actual RTX 4060
testing on 100,016 synthetic moves passed all five new checks. The completed
live-data read-only comparison reproduced the parent hashes, totals and fits;
histogram sums and change-count identities were checked after publication.
Only the API and idle algorithm worker were updated; cloud, ingestion, database,
and canonical-summary services remained running. Research reports are separate
from live summaries and are visible in Algorithms history.

Implementation SHA:
`098eb8383bc5323021963a707f3bb0d9ee097beeb04c95e4d137520f68ecddee`.
Immutable release tags: `chessism-api:threshold-comparison-20261026` and
`chessism-algorithms:threshold-comparison-20261026`, also assigned to persistent
`:runtime` tags. Image SHAs are respectively
`d92937acf3d409817c9a75e4cea3e35e8a76fc5a82e090ee8bdc65812a8f9a5f` and
`fd8bd1be50f1676d0b7c2594441569276f2437fb8f190b0cd3ec29b0065b2660`.

### All-player shared-stream shadow summaries (2026-10-26)

The next approved run extends frozen rating-aware **.15** inference to the full
eligible histories of all complete profiles, still without canonical promotion.
Parent comparison: `67993424-2190-4566-8c60-6e3ee6329ee2`. Cohort run:
`8f56d920-b3e8-42f7-8fae-fae2067dde8b`, started **19:57:20.570994 UTC**.
This section describes the run contract; final measured results are recorded
separately below once the run completes.

Inventory: **46 profiles**, 1,059,014 recorded player-game perspectives,
940,981 fully analyzed perspectives. Of those, **48 lack supported historical
ratings** and are explicitly excluded from rating-aware inference. There are
no nonstandard games in this inventory. Thus **940,933 eligible perspectives**
cover **75,146,913 position occurrences**. The 118,033 not-fully-analyzed games
are not silently treated as zero-blunder games. Counts are not global unique
games: a shared game correctly contributes separately to each selected player.

The user requested combined GPU work rather than one separate computation job
per player. One cohort operation therefore accumulates small database blocks
into a **shared numerical buffer across player boundaries**, targeting 262,144
player moves or 8,192 whole games, whichever is reached first. A final whole
database block can slightly exceed the target; every incoming block is capped
at 32,768 moves and 128 games. CUDA kernels use at most 262,144 rows per call.
Input indexes are rebased across blocks; shared game IDs retain independent
player identities. Results are grouped back into the appropriate player/mode
and historical-rating summaries. No entire-cohort input is loaded into RAM.

The first actual GPU batch contains **262,193 moves / 6,312 games from seven
players**: agadmator, anishgiri, bodhanas, chefshouse, javokhir_sindarov05,
levonaronian and thevish. This confirms mixed-player batching rather than
merely starting multiple independent workers.

Source reads are bounded, with one read-only repeatable snapshot per player,
30-second statement limits and no parallel PostgreSQL workers. Each player's
summary revision and coverage are pinned at inventory time and checked again
before reading. Final cohort membership/revision checks distinguish current
results from stale snapshots. An active cloud workflow blocks extraction;
the run neither changes cloud jobs nor starts any Stockfish analysis.

All moves receive an independent scalar/vector CPU check of rating-aware .15.
The old coefficient-only rule is checked against its production scalar, an
independent SQL expression and saved per-game counts. CUDA .20 is retained to
isolate threshold effects. Monotonicity, mate-rule and assessment-mask checks
must pass. Separately grouped saved CPU histograms must match reconstructed
histograms exactly. The current UI's broader histogram is retained separately
from the paired eligible-game histogram. Zero/wholly unassessed games are
excluded from both paired series; no-move counts and unsupported-rating
coverage remain explicit.

Each complete player produces compact summaries/checkpoints in
`AlgorithmRun.result`, plus a checksummed gzip JSONL research artifact under
`research_data/algorithms/blunder-cohorts/<run-id>/`. The first artifact record
pins the player, model, .15 threshold and source revision; remaining records
retain game/side identity, historical ratings, source hashes, old/new counts
and changed-label totals. Temporary files are moved into this owned run
directory only after full-player validation. Existing artifacts are never
overwritten. Failed/cancelled runs retain completed player checkpoints; an
explicit resume request requires identical inventory/revisions, pinned parent
and intact artifact checksums. No automatic ingestion scheduling or canonical
game-summary writer is introduced by this research operation.

The report distinguishes incomplete checkpoints from full results and exposes
mode/rating comparisons, exclusions, original calibration gates, batch counts,
artifact identities and timings. CPU scalar references and SQL validation are
audit costs, not GPU production inference. Timed GPU candidate work includes
input/output transfers and flags needed by validation; comparisons are not a
claim about full ingestion speed or aggregate-only optimized production code.

Verification before launch: **156 offline backend tests**, disposable
PostgreSQL native/legacy full-history and multi-player-buffer tests, checksum,
checkpoint rollback/cancellation tests, mocked browser checks and frontend build
passed. RTX 4060 tests covered **300,048 synthetic moves** and a **75,000-move
mixed-player batch**, with zero mismatches on all ten checks. The dedicated
worker remains capped at one CPU / 2 GiB; no extra cloud resources were started.

Immutable images (also assigned to persistent `:runtime` tags):
`chessism-api:cohort-shadow-20261026`, SHA
`698b981d8198fdb4f4e9d1839bff6381a653c3219b37c2effd9e7d9f753c68d2`, and
`chessism-algorithms:cohort-shadow-20261026`, SHA
`8cf94a768336662eaeaf0a8c19a11f0999c8651d6a91b6088eace5ebc398ea67`.
Only API and idle algorithm worker were updated. Canonical blunder counts,
accuracy, salience, score distributions and all ingestion/cloud services remain
unchanged. Canonicalization is a separate decision after review of the report.

#### Completed all-player comparison

Run `8f56d920-b3e8-42f7-8fae-fae2067dde8b` finished at
2026-10-26 20:24:59 UTC in **1,658.924 s** (27m39s). All **46 profiles**,
**940,933 eligible player-game perspectives**, **75,146,913 source position
occurrences** and **37,629,717 own moves** were processed. All ten numerical
checks were zero; saved CPU histograms agreed, source revisions remained fresh,
and all 46 compressed artifact checksums/headers were subsequently verified.
The frozen parent report hash was unchanged. No live summaries were replaced.

| Mode | Existing .15 | Rating-aware .15 | Relative change |
| --- | ---: | ---: | ---: |
| Bullet | 941,150 | 804,844 | -14.48% |
| Blitz | 510,219 | 476,316 | -6.64% |
| Rapid | 32,596 | 30,261 | -7.16% |

Bullet changes range from -61.51% below rating 1000 to +34.43% at 3000+.
These differences describe sensitivity, not classification ground truth. Prior
calibration/coverage failures remain unresolved. Candidate GPU time including
transfers was 2.728 s versus 5.770 s for the vector CPU reference; source
read/packing was 860.555 s, legacy SQL checks 378.742 s, scalar references
306.253 s. This does not establish production end-to-end speedup.

**Scheduling clarification:** metadata/source loading still visited complete
player histories sequentially. The shared GPU buffer crossed those boundaries,
but only 36/145 batches contained multiple players. This was not a concurrently
fed all-player input pipeline. The next production input stage must eliminate
repeated preparation and overlap bounded preparation with GPU consumption;
mixing player names alone cannot fix the dominant extraction cost.

### Weaker rating-adjustment comparison (2026-10-26)

Operation `rating_blunder_strength_comparison_v1`, endpoint
`POST /research/algorithms/blunder-strength-comparison`, requires a completed,
fresh, verified cohort report and the explicit
`compare_rating_strengths_only_no_live_changes` acknowledgement. The report,
fits, historical source policy and candidates are pinned in the run config.
K0 remains **0.00368208**, the decision threshold is **.15**, and CP clipping,
mate exceptions and unsupported-transition rules do not change. The eight
predeclared `(a strength, b strength)` candidates are:

- current `(1,1)`;
- joint weakening `(.5,.5)` and `(.25,.25)`;
- average-rating-only weakening `(.5,1)` and `(.25,1)`;
- opponent-gap-only weakening `(1,.5)` and `(1,.25)`;
- coefficient-only control `(0,0)`.

The sampler reads checksummed cohort artifacts for metadata only, without
reprocessing their FEN histories. It excludes all previously known fitting/
calibration identities, previously inspected strength-comparison identities,
dates at/after the reserved test boundary, and deterministic held-player
identities. Both player identities are checked before inference. Fixed-hash
reservoirs and round-robin rating/mode/owner/gap selection target **250 unique
games per rating/mode cell**, capped at **50 per owner/cell** and **4,500 games /
one million source positions** overall. Shared games have one assigned owner.
Metadata/held-opponent exclusions may reduce the final sample; all 18 cells
report shortfalls without padding. Owner concentration is explicitly reported,
not assumed to meet a population-balance guarantee when a cell is sparse.

Before outcomes are evaluated, inspected game IDs are durably reserved in the
run's research result, including IDs later excluded. Failed/cancelled runs retain
these reservations. Existing balanced and role-locked development selectors
also exclude these inspected IDs, preventing their later use as fresh tests.
This is exploratory evidence for a subsequent decision, not a new independent
test, an automatic fit, or an automatic candidate selection.

Selected unique games are extracted once in mixed-player blocks. CUDA broadcasts
each bounded move batch across all eight candidates, including per-game counts
and expected-result metrics. CPU scalar decisions/predictions and independent
SQL/saved CPU references check the result. The full-strength candidate must also
reproduce the prior cohort's saved per-game count. Game-weighted calibration uses
CP evaluations **after the assigned player's own moves**, with outcomes 0/.5/1;
it is expected game points, not pure win probability. Report 10- and 5-bin ECE,
MSE, rating/mode and rating-gap groups, explicit unassessed games, and legally
verified changed-move examples. These metrics are not directly interchangeable
with the earlier all-ply independent evaluation, and examples are not human
blunder-quality labels. No winner is automatically selected or canonicalized.

Verification: **101 offline tests**, disposable PostgreSQL native/legacy
sample lookup and durable reservation checks, mocked-browser comparison/filter/
incomplete-report checks, and frontend build passed. Synthetic RTX 4060 testing
covered **39,424 moves across all eight candidates** in two mixed-player batches;
all eight integer checks were zero, prediction error <=1.12e-16 and metric error
<=1.89e-15. The existing frontend chunk-size warning remains.

Release images (persistent runtime tags, prior images retained):
`chessism-api:strength-comparison-20261026`
(`8cbd11f1e095f87b7cb786d2f9532a4a085b2a35d7c191848f358b4b98f10fc8`) and
`chessism-algorithms:strength-comparison-20261026`
(`8c1cb58f79210d457e31a0ab16761fc68f249e059c14e770a34ffffd407b9dce`).
Only the API and idle algorithm worker were recreated; all other container IDs
were verified unchanged. Research run
`aeaa2ad4-60ae-4e6f-8c71-869411c7f25b` was queued under the original research
account.

#### Completed strength comparison

The run finished 2026-10-26 22:52:24.730–22:52:52.413 UTC, **27.682 s** wall.
It inspected 4,001 candidate identities and retained **2,165 unique games from
38 owners** (883 bullet / 882 blitz / 400 rapid), **160,604 source positions**,
and **80,414 own moves**. Metadata checks excluded 1,795 held-opponent games
and 41 invalid outcomes. The 4,001 inspected IDs remain durably protected.
No previously known fitting/calibration games entered the sample; the prior
exclusion set held 61,349 unique IDs. Input SHA-256:
`51f739b432d0fb5b331af52c3e44c5e03476440b92359f5dfcfcfbf3c5033a3c`.

All eight integer checks passed with zero discrepancies. Maximum expected-result
prediction difference was **2.22e-16**, maximum per-game metric difference
**7.78e-16**. CUDA used three batches: the first two each included **all 38
sampled owners**, the tail included 10. All eight variants were broadcast
together. This validates the mixed-player sample path, not a replacement of
the earlier full-history sequential reader or the live automatic pipeline.

| Candidate | Overall MSE | Overall ECE (pp) | Bullet MSE | Bullet ECE (pp) |
| --- | ---: | ---: | ---: | ---: |
| Current rating adjustment | .159749 | 3.6031 | .157396 | 4.7146 |
| Both adjustments 50% | .167975 | 4.7700 | .168220 | 4.3404 |
| Both adjustments 25% | .179649 | 5.9746 | .183831 | 5.1937 |
| Average-rating effect 50% only | .160493 | 3.2921 | .158505 | 3.3907 |
| Average-rating effect 25% only | .161353 | 3.2485 | .160155 | 3.5109 |
| Rating-gap effect 50% only | .167441 | 4.9293 | .167607 | 5.3572 |
| Rating-gap effect 25% only | .178331 | 5.9345 | .181850 | 5.8564 |
| Coefficient-only control | .201350 | 7.7165 | .213809 | 6.7248 |

At bullet rating <1000 (189 games, six owners), current/half-slope/old counts
are **221 / 336 / 505**; ECE changes 9.1494→4.2503 pp. At bullet rating 3000+
(177 games, 22 owners), counts are **226 / 182 / 200** and ECE worsens
5.0065→5.7364 pp. Therefore half-slope mitigates the observed extreme count
changes but does not improve every subgroup, nor meet every earlier quality
target. Overall bullet ECE improves while MSE worsens about 0.70%. Weakening the
opponent-gap effect is not supported by the overall/bullet/blitz diagnostics.
These are exploratory comparisons, without significance/independence claims.

The suggested next candidate for independent testing is a **bullet-only 50%
average-rating adjustment**, leaving the opponent-gap effect intact, not blanket
weakening across all modes. No candidate was selected automatically and the
canonical model is unchanged. Rapid remains limited: no 3000+ observations,
28.5% of rapid games belong to one owner, and the half-slope variant worsens
rapid ECE 5.3015→5.4909 pp. Other cells also fall short of the 250-game target.

Metadata artifact selection/checks took 15.590 s; position read/packing 2.034 s;
legacy SQL validation .9995 s; GPU all-candidate inference/metrics/transfers
3.696 s (including first-use costs); CPU references/example selection 3.921 s.
This is not a same-workload speed comparison with the full cohort, and no
production-performance claim is made. Results are available under Research →
Algorithm creation → Runs → “Rating-aware blunders · weaker adjustments ·
shared GPU sample”.

### Full development pool and dedicated RTX memory (2026-10-26)

The user requested the fresh-data audit, then requested as many training games
and as much of the dedicated RTX 4060 memory as practical. This authorizes a
new development candidate, not automatic publication or reuse of test outcomes.
The preceding bullet-only half-average-rating candidate remains an immutable
comparison, not silently overwritten by a refit.

A read-only metadata audit counted **902,834 unique eligible games**, before
checking CP availability and the 10% both-player cap. Historical roles/date
boundaries were preserved: training before 2023-11-08T19:17:01+00:00; tests from
2025-06-14T23:20:43+00:00. All 65,350 previously known identities were checked.

| Mode | Potential training | Validation | Fresh primary test | Held-player test |
| --- | ---: | ---: | ---: | ---: |
| Bullet | 190,461 | 44,473 | 50,413 | 19,258 |
| Blitz | 160,679 | 38,582 | 22,828 | 17,604 |
| Rapid | 10,666 | 4,068 | 2,115 | 513 |

Potential training totals **361,806 games / 28,010,773 positions**. These are
upper bounds, not actual selected or CP-supported counts. Full population
metadata still has zero fresh primary bullet tests for average-rating bands
1600–1799, 1800–1999 and 2000–2199. Rapid training at 2200–2399 has only 311
games; several validation/test bands are below 100. Only 18/27 rating/mode
groups could meet all raw 500/100/100 minima, even before player caps. One player
occurs in 1,833/2,115 primary rapid tests and 441/513 held-player rapid tests.
The 10% count cap therefore bounds retained held-player rapid tests to at most
80 games; no claim that running more training repairs those limitations.

`scripts/audit_blunder_data.py` is a reusable, read-only metadata audit with
role, date, count, player-concentration and connection checks. It does not read
score/outcome values or consume tests; valid-result filtering remains part of
eligibility. It refuses historical position replay and partial/oversized scans.

The new `full_development_rating_blunder_v1` research operation streams all
eligible development metadata, without the old 3,000-games-per-cell sample cap.
Selection is a deterministic large feasible subset under the same both-player
10% count cap, not a claim of mathematically optimal graph matching. Independent
test dates, held players and prior test/exploration identities stay excluded.
Actual training/validation identities are durably reserved before loading their
labels; failed/cancelled attempts retain their roles. Subsequent old/new research
paths incorporate these reservations. The bounded role-manifest ceiling is now
two million identities; metadata input is at most one million games and position
input at most fifty million rows. Exceeding a bound fails, never truncates.

The source is a read-only repeatable snapshot with small whole-game queries and
native/legacy parity. Six temporary CP input files separate mode and development
role, but combine players. Every CP ply retains the existing White-perspective
formula and total game weight of one. Mate/tablebase/unknown scores never become
numeric CP training inputs. Mate-only removals and resulting CP player-cap
failures remain visible rather than silently passing a gate.

The RTX 4060 is dedicated to calculation; it does **not** need a desktop reserve.
The memory pool uses available VRAM minus **512 MiB for CUDA/runtime overhead**.
Input arrays stay GPU-resident when they fit with scratch space; larger arrays
stream through bounded, memory-sized batches. Allocation failures reduce batch
size and retry the same calculation, never switch to CPU or drop input rows.
Float64 CUDA computes losses and calibration. CPU controls the original bounded
two-parameter search; exact repeated parameter losses are reused across the
seven regularizers. Validation alone selects among the existing 28 candidates;
the frozen bullet-only half-slope reference is evaluated separately. k0 stays
0.00368208; intended blunder cutoff .15 and mate exceptions stay unchanged.

This operation has its own six-hour queue-function limit; all other algorithm
requests retain their one-hour limit. Single-worker ownership, cancellation,
temporary-file cleanup and old complete live summaries are unchanged. Reports
explicitly distinguish development selection from unrun final-test/production
gates. No final-test outcomes are loaded or automatically consumed, even if
development selection passes. The source audit shows full coverage is not yet
available, so final independent approval must not be implied.

Verification: **116 offline tests passed**, disposable PostgreSQL native/legacy
source and history-reservation checks passed, mocked-browser completed/reserved
report checks passed, and frontend build passed (existing large-chunk warning).
Actual RTX 4060 synthetic test: GPU-resident and streamed MSE/ECE match CPU,
maximum independent scalar prediction error **2.22e-16**, and recovered known
parameters a=.2100000381, b=.3700008392. This is not a complete new-model
classification or production-performance validation.

Deployed only API and idle algorithm worker images
`chessism-api:full-development-20261026` and
`chessism-algorithms:full-development-20261026`, also tagged `:runtime`; prior
strength-comparison images retained. All other service/container IDs stayed
unchanged. No cloud jobs or other research runs were active at submission.

Run **`3de20259-00bf-4b1f-861e-1ec9e2e8a565`**, “Rating-aware blunders · full
development · RTX 4060”, started **2026-10-26 23:31:21 UTC**. Its initial
balanced selection contains **296,999 training games + 75,710 validation games
= 372,709 unique development games / 29,575,564 source positions**:

| Mode | Selected training | Selected validation |
| --- | ---: | ---: |
| Bullet | 149,472 | 37,854 |
| Blitz | 138,665 | 36,164 |
| Rapid | 8,862 | 1,692 |

At the single startup check, it was running `loading_full_development`, with
1,282,335 positions read and no error. These are selected metadata counts, not
final CP-supported sample sizes or completed results. The run was left running
without ongoing polling, as requested. Progress is available in Research →
Algorithm creation → Runs; final tests and live summaries remain unchanged.

### Completed full-development run and CP-first correction

Run `3de20259-00bf-4b1f-861e-1ec9e2e8a565` subsequently completed in
445.279 s (388.516 s extraction/preparation; approximately 56.63 s thereafter).
It used 294,970 training and 75,229 validation CP-supported games, totaling
23,084,120 CP observations. Bullet/blitz development selection passed; rapid
did not (4.617 pp ECE; required 3 pp, and insufficient MSE improvement).
The frozen bullet reference had better calibration, 1.539 pp versus 2.388 pp,
although the new candidate had slightly better MSE. No final test was consumed.
Dropping 2,510 no-CP games after selection pushed all six mode/split player caps
slightly above 10%, up to 10.093%; the report correctly failed that gate.

Implementation version 2 of the same development operation fixes the ordering:
reserve all eligible development identities, stage scores once with bounded
whole-game queries, exclude games without CP, then choose the deterministic
both-player-capped subset. Source staging writes are batched per query rather
than performing a thread round-trip per game. The database snapshot is released
before local compaction and fitting. All inspected identities remain protected,
including cap-excluded/no-CP games and failed or cancelled attempts. Local staged
files are compacted with whole-game CP weights and exact move predecessors;
temporary staging files are removed and the worker's usual workspace cleanup
removes the remaining disposable input. The retained report records both staged
and final input hashes, reservation/CP-eligible/retained counts and actual caps.

`blunder_full_diagnostics.py` reuses the staged data after fitting:

- Rapid: CUDA equal-game sufficient statistics for training and validation,
  grouped by average rating, absolute rating gap, calendar year, and the top
  twelve participants (chosen by counts, not errors). Separate ten-bin
  calibration records, five-bin sensitivity, White outcomes, participant
  concentration and dates remain in the report. Overall validation must match
  the fitter's MSE/ECE through an independent aggregation path. Training scores
  are in-sample; overlapping participant groups are not additive or causal.
- Bullet: every retained validation move made by a complete profile, evaluated
  together for the current fixed-coefficient implementation, frozen half-slope
  reference, and newly fitted candidate. Mover perspective, both colors, mate
  rules, unsupported scores and per-game-side counts are checked. All decisions
  are compared against a vector CPU equation and a stratified-by-batch scalar
  sample. Changed counts are grouped by mover rating, with deterministic
  illustrative added/removed examples. This is not a fresh independent test or
  a replay against live SQL/saved summaries.

The new diagnostic report is available in the Algorithms run details. All
models use identical development inputs; no diagnostic changes fitting,
coefficient, .15 cutoff, mate rules or acceptance thresholds. The coefficient
control is the existing local implementation, **not a claim of exact current
Lichess behavior**. Adopting a provisional model or a verified Lichess version
is a later explicit decision if this investigation still fails quality gates.
No automatic production promotion or live-summary write was added.

Verification: 121 offline tests passed, disposable PostgreSQL native/legacy
source/reservation/history tests passed, mocked browser report/filter tests and
frontend build passed (existing large-chunk warning). Dedicated RTX 4060
synthetic checks passed resident/streamed metrics, known parameter recovery,
rapid grouped CPU/GPU metrics and bullet decisions/per-game-side counts/mate
rules, with zero decision mismatches and maximum sampled prediction error
2.22e-16. These are implementation checks, not real-data quality approval.

Deployed `chessism-api:cp-first-diagnostics-20261026` and
`chessism-algorithms:cp-first-diagnostics-20261026`, also tagged `:runtime`,
recreating only API and the idle algorithm worker. All database, cloud,
ingestion, FEN and other worker container identities remained unchanged.
Prior images remain available; no driver, daemon or network reset was done.

Run `929e12f6-f1d6-40bc-b32b-7e524e30c0c1`, “Rating-aware blunders ·
CP-balanced diagnostics · RTX 4060”, started 2026-10-27 00:03:12 UTC
(October 26, 18:03 local). One startup check confirmed it running without
error, staging 448,929 eligible development games / 34,749,364 source positions
with 1,229,599 positions processed. These are inspected counts, **not final
CP-balanced training counts or completed results**. It was left running for
the user to monitor; no continuing polling. Live summaries and final tests
remain unchanged.

## Provisional canonical adoption — 2026-10-26

The CP-balanced run `929e12f6-f1d6-40bc-b32b-7e524e30c0c1` completed in
452.019 seconds (7m32s): input preparation 390.868s, fit/validation 56.729s,
diagnostics 4.038s. After CP filtering and player caps it used 370,359 games /
23,090,418 CP observations (295,079 training, 75,280 development validation).

The user explicitly chose to keep this latest rating-aware model, rather than
switch to the Lichess-only definition, accepting the changed low-rating labels.
This is **provisional user approval**, not a claim that statistical gates passed.
Frozen coefficients `(a,b)` are bullet `(0.2087383270263672,2.880035400390625)`,
blitz `(0.2149028778076172,2.3756484985351562)`, and rapid
`(0.006809234619140625,0.030462265014648438)`. The base coefficient remains
`0.00368208`, threshold `0.15`, CP clip ±1000, and explicit mate exceptions are
unchanged. Missing/invalid ratings and unsupported source kinds are unassessed,
never silently zero or a substitute model. Predictions are expected game points,
not pure winning probability; the model is not clock/complexity aware.

Development calibration errors were bullet 2.3868pp, blitz 2.2259pp, rapid
4.6335pp. Rapid failed the 3pp target. The independent/population/coverage gates
remain unrun. Low-rated bullet flags fell 51.71% versus the old rule in this
development comparison. All of these limitations remain in the immutable registry
and UI; implementation correctness is not statistical validation.

Canonical package: `operations/canonical_blunders`. Additive core tables retain
immutable model/config/evidence/approval, a control pointer, a revisioned unique-
game dirty queue, per-game GPU counts/annotations, and per-player cached histograms.
Model `61c213c642df3411e1ee241b47a1dbb2778ed71f0a63353e52ae7fd2c5bc7fce`
hashes the frozen inference configuration and extraction/perspective/reduction/
decision implementation. No model is refitted by an automatic refresh.

The initial queue contains 904,526 unique fully analyzed games belonging to
complete profiles. Source extraction mixes players and processes both colors once
per game, bounded at 8,192 games / 2,097,152 rows per batch. RTX 4060 inference uses
free VRAM minus 512MiB runtime reserve, with bounded OOM retries and no CPU fallback.
The first publication waits for the initial queue to drain, retaining displayed
CPU results meanwhile. Subsequent source changes queue only affected games;
GPU outputs and affected-player histograms publish in one transaction. Changed
source revisions are discarded/requeued, not acknowledged. Failures retain the
last published results and retry. The existing serialized algorithms worker runs
the refresh jobs; it does not overlap research jobs or start while cloud jobs run.
Successful batches chain through ARQ's post-completion hook with one stable job
ID, avoiding cron's 60-second completed-job lock between batches. A minute cron
repairs wakeups after errors/restarts without building a duplicate-job backlog.

Existing source-game and engine-summary tables are not overwritten. A read-only
overlay selects GPU counts once the model is published. Native distribution
exploration and game-review move labels use the same version. Accuracy, salience,
CP gain/loss and other metrics stay unchanged. The old SQL blunder expression is
still calculated within the combined CPU accuracy aggregate and retained as a
rollback/reference field; this release does **not** claim to eliminate that
incidental CPU work. Compact aggregation of GPU-produced integers is still SQL.

Progress appears in Research → Algorithm creation → Blunder summaries · RTX 4060.
It refreshes every five seconds; the processed-game count is cumulative across
the initial rebuild and incremental updates, not specific to the last cloud job.
Operational status:

```sh
docker exec last_chessism-chessism-api-1 python -m scripts.manage_canonical_blunders
```

Explicit administrator rollback (do not run as a routine status check):

```sh
docker exec last_chessism-chessism-api-1 python -m scripts.manage_canonical_blunders \
  --rollback-to legacy_cpu --acknowledgement explicit_canonical_rollback
```

A retained model ID can replace `legacy_cpu`; it is rebuilt/staged before the
atomic switch. Evidence and previous caches remain available. Read enablement is
`CHESSISM_CANONICAL_BLUNDERS=1` only after explicit additive installation; generic
Compose defaults remain off. API image `chessism-api:canonical-blunders-20261026`
and worker image `chessism-algorithms:canonical-blunders-20261026` also carry their
persistent `runtime` tags. Prior CP-first images remain available. Only API and
the idle algorithms worker were recreated, not database/cloud/ingestion services.

Checks: seven focused canonical unit tests; disposable PostgreSQL extraction,
source-race rejection, atomic activation, malformed batch rollback, idempotence,
incremental changes, immutable models and version rollback; 18,000 synthetic
move decisions on the actual RTX 4060 agreed with the scalar oracle across all
three modes and both sides; 103 existing pipeline helper tests; 11 player summary
tests; additional fixed-rule/diagnostic regression tests; mocked canonical UI
states and frontend build. These are **implementation** checks, not newly passed
independent model-quality gates.

## Optimized canonical input and automatic refresh checks — 2026-10-26

The canonical worker explicitly uses `binary_v1`: bounded PostgreSQL binary COPY
reads and vectorized transition packing. The legacy reader remains a reference
for paired benchmarks, not a fallback for failed production reads. Reader failures
retain queued revisions and published summaries for automatic retry. Inference,
threshold, mate handling and immutable model ID are unchanged.

Paired run `18b075a8-4a87-4551-80d4-cae095fcd922` compared 100,000 games /
8,059,866 position records across 46 players in 13 alternating-order batches.
Input reading plus packing fell from 76.163s to 51.828s (32% less time).
Inputs and output flags, game counts and player histograms matched exactly;
benchmark publication used disposable scratch tables, not live summaries.
Only the idle algorithms worker was restarted to activate the optimized reader.

The subsequent user-run Cloud Run test selected 1,000 games for `prosaicjonny`:
job `2f28454753a148a68084ffec1c0a29cb`, 51,592 FENs, 100 concurrent CPUs.
All FENs were imported, verified and cleaned up; total workflow was 566.536s.
After cloud completion, the automatic worker checked 1,348 affected games and
processed 1,040 eligible games / 67,076 position records on the RTX 4060.
The whole refresh, including publication, took approximately 0.67s; this is not
GPU-kernel-only time. The saved summary was published 2.19s after cloud completion,
with no errors, stale revisions or queued games remaining.

All 1,000 selected games became numerically scored. At the time, 998 had non-null
blunder counts for the player, including partial assessments; one Daily game is unsupported, and one Blitz game has
existing scores with missing `analysis_source` metadata, leaving none of the
player's before/after pairs assessable. Unknown results remain null, never zero.
The player's published histogram covers 31,466 games, up from 30,428 before the
test. Next: investigate missing source provenance before any metadata repair;
do not infer Stockfish provenance from a non-null score alone.

Focused checks also cover binary-reader corruption/coverage rejection, exact
reference equivalence, no CPU fallback, and disposable-PostgreSQL worker recovery.
Operational success does not remove the statistical limitations below.

## Assessment coverage and durable input refresh — 2026-10-27

Read policy `complete_player_moves_v1` separates numeric score coverage from
blunder assessment coverage. A game enters the player's histogram only when
every move by that player was assessable. Partial detected counts and move flags
are retained, but partial zero counts are not classified as zero-blunder games.
The distribution response reports partial, wholly unassessed, and ineligible
counts by mode. Chart selection and game exploration use the same complete-side
projection; game review labels partial counts and exposes assessed/unassessed
move totals. Missing source provenance is never inferred from a numeric score.

The existing contiguous-ply input contract lets the projection derive exact
per-side unknown counts from saved assessed counts and unknown-ply array lengths.
No FEN scan, fitting, full GPU run, threshold change, or model-ID change is needed.
An explicit transactional upgrade installs the projection and rebuilds saved
histograms from cached game results:

```sh
python -m scripts.manage_canonical_blunders --upgrade-coverage
```

Run with the new API image before replacing the API and idle algorithms worker.
The upgrade rejects active cloud work, pending canonical changes and model
rebuilds; source scores and immutable models are untouched. Unknown/older snapshot
policies fail closed rather than being labeled complete. Benchmark configs now
version this read policy separately from inference, and private scratch storage
uses the same projection. Historical reports remain retained.

`fen_summary_refresh` is a durable outbox populated in the same transaction as
score or provenance changes. Cloud-claimed FENs keep their existing durable
cloud-job projection/recovery path. The serialized algorithms worker handles up
to 500 game refreshes before CUDA extraction, acknowledges only captured source
revisions after both coverage and CPU engine summaries succeed, and retains
failures with a 60-second retry delay. Newer updates survive acknowledgement.
Pending input refreshes are excluded from CUDA extraction and shown in Algorithms
progress. This also runs after explicit rollback to CPU reads. The queue recovers
game summaries; it does not repair global database-counter deltas or guess old
provenance. Poison-game isolation remains separate future work.

Checks: focused canonical unit tests; disposable PostgreSQL odd/even coverage,
partial-zero exclusion, chart/view parity, transactional rollback, cloud claims,
failure/backoff, concurrent revisions, production refresh failure after coverage
commit, benchmark compatibility and worker recovery; mocked browser coverage and
progress checks; UI production build. No new statistical quality gate is claimed.

Deployment verification: all 46 saved profiles use this policy, the model/kernel
hashes are unchanged, and both queues are empty with no recovery errors. The
stricter audit exposes a substantial existing provenance gap: no cached
game/player side is fully assessed; 933,730 sides are partial and 8,244 wholly
unassessed. For `prosaicjonny`, 31,466 sides are partial. In the latest 1,000-game
test, all 998 non-null player counts are partial, one side has no assessments and
one Daily game is unsupported. This corrects the earlier interpretation of those
non-null counts as usable whole-game totals. A read-only ten-game recalculation
matches the saved move flags; opening FENs have numeric scores but missing
`analysis_source`. Histograms now explicitly report no fully assessed games.
Partial detections remain visible in review. Recovering verified provenance (or
reanalyzing affected positions) requires a separate, scoped repair; do not infer
the source or turn unknown moves into zero blunders to populate the chart.

## Shared evaluation input pipeline — 2026-10-27

`chessism_api/evaluations.py` defines `verified_evaluation_v1`: CP and mate
classification requires the existing case-sensitive `stockfish*` source rule;
tablebases remain distinct; missing source is unknown, not zero blunders.
An evaluation timestamp is **not** required for classification or CUDA inference.
The canonical binary reader and the research training reader share the same SQL
expression without changing the frozen equation, threshold, mate exceptions,
kernel hash, or model ID. This is input/persistence hardening, not a new fit.

The rating-aware blunder default uses `summary_input_version=3`, sharing that verified
classification and omitting the raw `analysis_source` column. Saved v1/v2 recipes
retain their historical population and type policy. Score distribution's built-in
v2 recipe, historical fixed-rule comparison recipe, and CPU summaries are unchanged; newly created generic v3 summary
recipes use verified kinds. Unknown evaluations remain explicit rows, not silently
skipped predecessors. No unscored rows are added to the score-distribution matrix.

Fresh local/cloud Stockfish results and tablebase results are validated before
persisting. Numeric zero is valid; missing, nonfinite, fractional integer scores,
duplicate FENs, malformed continuations, conflicting source fields, and invalid
WDL distributions fail. Normal local/cloud import refuses to overwrite already
scored FENs. Bounded `UPDATE FROM VALUES` batches replace per-FEN executemany
updates, so statement-level invalidation deduplicates games once per batch.
Result/continuation writes and newly-scored database totals now commit
together, including local analysis and tablebase ingestion. Cloud receipts/claims
retain their existing transaction and retry behavior. This does not introduce an
already-scored replacement/repair mode or retrospectively reconcile old totals.

FEN backup restoration preserves saved source/timestamp verbatim. In particular,
it no longer assigns `stockfish_backup` to a missing source. The backup checksum
proves the archived bytes, not which engine originally generated a score.

`evaluation_refresh_v2` extends the existing durable input-refresh outbox to
game/FEN association and game/player ownership changes. New games composed
entirely of already-scored positions therefore retain pending refresh work even
if ingestion stops before its finalizer. Requests coalesce by game and acknowledge
only the captured revision. Publication also rejects a result when a newer input
refresh is pending, even before coverage refresh increments the canonical dirty
revision. Existing cloud projection checkpoints, CPU score summaries, and mixed-
player CUDA batching are retained. No new per-FEN GPU job or full-cohort rebuild.

The explicit, transactional deployment step rejects active cloud, ingestion,
algorithm, or pending summary work and does not scan/update FENs or snapshots:

```sh
python -m scripts.manage_canonical_blunders --upgrade-input-pipeline
```

The deployer must additionally verify local-analysis/backup queues are idle before
replacing their workers. Do not restart active workers. Tests use synthetic data,
no cloud credentials, no GPU access, and private socket-only PostgreSQL: scalar/SQL
classification parity, saved recipe compatibility, score/total rollback, duplicate
imports, missing totals, honest backup restoration, pre-scored reuse after failed
ingestion, concurrent input revisions, and worker retry/publication recovery.

The 493,363 historical missing-source evaluations are **not repaired or relabeled**
by this change. Establish their old encoding/source rules separately before
approving a compatibility policy or scoped reanalysis. Missing timestamp alone
is not a reason to reanalyze an evaluation.

Deployment verified: `evaluation-pipeline-20261027` code-only releases promoted
to the persistent API/local-worker, algorithms, research, backup-worker and cloud-
controller startup tags. Only the API and six affected idle workers/controllers
were recreated; PostgreSQL, Redis, the Stockfish service and FEN workers were not
restarted. No cloud or GPU analysis job was submitted. The input migration installed
seven triggers, changed zero scores and queued zero games. Canonical state remained
`ready` with both queues empty and no error. All 46 snapshot payloads retained digest
`ddd87a07547cbc2c2dda09be4e518be7`; database totals remained 58,730,043 scored and
6,854,044 unscored FENs. The frozen CUDA implementation hash is unchanged.

Final checks: 230-test offline suite passed (8 opt-in backup-database cases skipped);
7 isolated evaluation writer cases and 24 cloud import/recovery cases passed, plus
socket-only matrix/type parity, canonical coverage/input migration, worker retry,
and compact storage/dump-restore harnesses. All use synthetic data. The writer test
also verifies one statement-level notification for a multi-FEN batch and correct
counts across the 1,000-row statement boundary. No end-to-end live speedup or new
model-quality approval is claimed.

## Ingestion trigger regression and legacy encoding audit — 2026-10-27

The first ingestion after `evaluation_refresh_v2`, `gurelediz`
(`df608df1-3e4f-487f-9c93-972999269992`), completed without a recorded error but
took 4,439.481 s for 13,295 saved games. Position extraction/persistence took
3,661.15 s; coverage took 59.24 s; tablebase marking took 555.81 s, including
509.20 s probing 32,230 endgames. The three position workers' overlapping
`save_positions` and `appearance_counts` timings must not be added to wall time.

The regression was reproduced offline: conflict-locking ingestion statements
fire empty UPDATE transition tables before large metadata/appearance updates.
After eight empty calls, the old trigger's cached old/new join plan made a
20,000-position counter-only update exceed a 20 s statement timeout. Without
that warmup the same old fixture completed in 0.173 s, which explains why small
fresh-connection correctness checks missed this performance failure.

`evaluation_refresh_v3` compares only `(fen_id, score, analysis_source)` using
set difference, then returns immediately if no evaluations changed. Changed
IDs are local to the statement; genuine changes still persist the same
transactional, deduplicated game-refresh requests and respect cloud claims.
No session bypass flag, disabled trigger, or removed durability guarantee.

The warmed-up fixed synthetic update completed in 0.193 s (trigger 9.731 ms).
An exclusive lock on the association table proves counter-only, no-op and empty
updates do not even attempt to read it. Additional checks cover source-only and
NULL transitions, mixed changed/unchanged rows, cloud reservations, rollback,
real ingestion/replay/concurrency, coverage recovery, publication races and the
automatic canonical worker. Nine offline evaluation/mate-summary tests passed.
These are regression checks, not an end-to-end live speedup measurement.

The idle-guarded migration was applied at approximately 20:19 UTC without
restarting any service, changing scores, queuing analysis or rebuilding summaries.
All 47 cached snapshot payloads retained digest
`31a3d0efb806235c64495772ee5b0389`; totals stayed 58,762,273 scored and 7,790,639
unscored. Canonical status stayed `ready`, with no error or pending work. Code-only
`evaluation-refresh-v3-20261027` images are promoted to persistent API/local-worker,
algorithms, research, backup and cloud-controller tags for subsequent startups;
running Python services are unchanged because this fix executes in PostgreSQL.

Historical Git evidence, not an inferred per-FEN source:

- `700453b` (2026-01-23) replaced a Leela service with Stockfish. Both the earlier
  Leela serializer and the initial Stockfish serializer used White's perspective
  and encoded mate as `10000 + mate_distance`, preserving ordinary CP as `score()`.
- `95cd186` (2026-07-26) introduced `analysis_source`/`analyzed_at` and changed
  Stockfish serialization to `score(mate_score=10000)`.
- Consequently, old losing mate in 3 and modern winning mate in 3 both produce
  `9997`; the old zero-distance representation also lost the winning side.
  Numeric magnitude alone cannot establish the encoding or engine for a row.
- The earlier verified backup audit found the 493,363 missing-source evaluations
  already present with identical scores and missing source/time in the August 6
  archive and October 20 manifest chain. Git dates do not prove deployment dates
  or which writer produced each archived value.

The offline evaluation-contract regression now explicitly checks this mate-sign
collision and retains unknown classification for untyped historical values.
No historical value is relabeled, converted, discarded or reanalyzed by v3.
Any further repair needs per-row encoding evidence or an explicitly scoped fresh
evaluation replacement that retains the old value until validated atomic commit.
Absence of timestamp alone is still not grounds for recalculation.

## Bounded legacy repair shadow pilot — 2026-10-27

`operations/evaluation_repair` and `scripts/preview_evaluation_repair.py` provide
the first **read-only** repair stage. The separately reviewed matching-only
publisher below is an explicit admin command, not a background repair job,
automatic migration, or new UI endpoint.

Planning runs in a repeatable-read, read-only transaction with a 10-second query
timeout. Selection is restricted to one explicit ID window of at most 100,000
IDs, at most 16 FENs, at most 20 actual association rows per FEN and at most 16
stored alternative lines. Cached `n_games` is only a preliminary filter: a bounded
association lookup rejects stale counts that conceal high fanout. Active cloud,
ingestion and algorithm jobs and cloud claims prevent unsafe pilot selection.

The checksummed plan preserves the complete old FEN row, its row version, old
continuations, game references and player identities. Reports are created
exclusively, never overwrite existing files, and reject corruption/modified
snapshots. A separate read-only recheck identifies source/continuation/reference
drift. Checksums establish artifact integrity, not historical provenance.

The shadow runner starts its own one-thread Stockfish process, verifies the
binary SHA-256, and uses the existing `sf16-1-n100000-t1-h256-pv4-v1` profile.
Native score kind, White perspective, signed mate distance/winner, engine
identity, WDL and legal PV are retained alongside scalar storage encoding. No
database imports or credentials are required. A per-position timeout terminates
only this dedicated process; no shared-service restart or endless retry. Unknown
old numeric encodings are never interpreted as CP deltas or statistical error.

The first live-data pilot selected eight mate-like unknown-source positions in
the first 100,000 IDs. They reference 50 games and 21 eligible profiles; this is
an intentionally small, non-representative safety pilot, not a corpus estimate.
The isolated, network-disabled, one-CPU runner took 0.586 s; a repeat with the
explicit review classifications took 0.617 s and reproduced the same scores.

| FEN ID | Stored number | Fresh number | Review |
| --- | ---: | ---: | --- |
| 2234 | 10007 | 542 | Hold: mate-like legacy value not reproduced |
| 2242 | 10010 | 547 | Hold: mate-like legacy value not reproduced |
| 2489 | -10000 | -10000 | Matches numerically; historical source still unknown |
| 4159 | -9990 | -451 | Hold: mate-like legacy value not reproduced |
| 6646 | 9995 | 9995 | Matches numerically; historical source still unknown |
| 6649 | 9996 | 9996 | Matches numerically; historical source still unknown |
| 19097 | -9995 | -9995 | Matches numerically; historical source still unknown |
| 19362 | 9996 | 9996 | Matches numerically; historical source still unknown |

A further network-disabled, read-only local Syzygy check confirmed winning
outcomes for White, White and Black respectively in the three held endgames.
Their saved PVs are legal and end in checkmate. **Neither WDL/DTZ nor a single
legal mating PV establishes the historical forced mate-in-N distance.** The
current 100,000-node search failed to reproduce mate detection; this is evidence
against automatic downgrade, not proof that those old values were wrong.
Four-field FENs use the same default halfmove clock 0 as the analysis profile.

Local artifacts (intentionally outside Git):

- `research_data/evaluation-repair/pilot-mates-plan-20261027.json`
  (`7cff9a4d27c1d298a7a52a348032c1cdfcf342ac2c344c968d6f9daba1bf33e3`)
- `pilot-mates-shadow-reviewed-20261027.json`
  (`cd7d633d4b40fab7dfcfdb6ee3ceae9be7b71392757caa481d33d5c2640f65b5`)
- `pilot-mates-endgames-20261027.json`
  (`be79951f69493d8b2a3f2251cf0e2fe300b006032d91458ce797cf12c247f294`)
- `pilot-mates-recheck-reviewed-20261027.json`: all eight source snapshots unchanged.

Verification: 14 offline artifact/engine/encoding tests and a disposable
PostgreSQL harness passed, including actual read-only enforcement, bounds,
cloud claims, active work, continuation drift and association drift. No service
restart, cloud job, production score update or summary recalculation occurred.
All 47 canonical snapshots retained digest `31a3d0efb806235c64495772ee5b0389`;
totals remained 58,762,273 scored and 7,790,639 unscored, with both canonical input
queues empty and status `ready`.

### Guarded matching-only publication

`scripts/publish_evaluation_repair.py` implements `matching_score_and_wdl_v1`.
It requires the exact plan and shadow hashes, explicitly selected IDs, a reviewer
identity and `--acknowledgement publish-reviewed-matching-evaluations`. It
independently checks source identity, native mate encoding/winner/distance, legal
PVs, WDL, piece count and genuine shadow timestamps; human-readable review labels
are not authorization. Changed primary scores or WDL are refused. Consequently
all numeric aggregate deltas are **zero**, not increments of already-scored FENs.

The additive `evaluation_repair_receipt` table requires a separate `--install`.
Records retain the complete plan and shadow report plus before/after snapshots;
UPDATE, DELETE and TRUNCATE are rejected. A deterministic operation ID makes
retries return the original receipt without repeating writes or notifications.
An already-published response confirms the old commit, not that its values are
still the latest. No new engine analysis occurs during publication.

Publication refuses active cloud/ingestion/algorithm jobs or pending input/CUDA
refresh queues. Short SHARE locks acquired with NOWAIT protect claims, job
admission, FENs, alternatives, associations and ownership from phantoms. These
briefly defer new writes, never stop workers, and leave reads available. Busy
tables or FEN row locks cause immediate refusal, not waiting for a workload to
finish. FEN row locks use the ordinary writer's stable FEN order. Every plan
snapshot is checked again under READ COMMITTED after locks. SQL/lock/idle timeout
bounds protect the transaction; no automatic retry is performed.

The shared writer saves replacements and continuations, with original fresh
shadow timestamps rather than invented historical dates. The existing durable
trigger must be enabled, and every affected game's refresh request must exist
before inserting the receipt. Writes, queue requests and receipt commit together
or roll back together. The existing recovery and canonical CUDA worker consume
the affected games normally; no cohort-wide rebuild or service restart is needed.

Offline checks: 18 artifact/engine/encoding/publication tests, plus disposable
PostgreSQL planner, publication and coverage-recovery harnesses. Cases include
active work, claims, source/continuation/association/owner drift, held rows,
disabled/no-op refresh triggers, disabled audit protection, concurrent write
admission, failure after writes, receipt insertion failure, immutable receipts,
idempotent retry, unchanged totals and durable queue consumption.

This policy does **not** authorize a corpus-wide repair. Retain the three held
mate-detection cases until an explicit preservation or deeper-verification policy
exists. Do not route scored legacy rows through the ordinary unscored queue or
clear scores to force reanalysis. The admin tool imports `stockfish_core` for its
versioned contract, so mount `stockfish-batch-worker` read-only on its Python path
when using an API image that does not package that library.

First approved publication committed at **2026-10-27 20:55:24 UTC**. Receipt ID:
`a8f47537ee0ef6616d52d0c1ae5605e6d9b3d0baa7633d90c8b8ae06fc7c5373`.
Exactly IDs 2489, 6646, 6649, 19097 and 19362 were replaced. Their numeric scores
and WDL remained identical, and all database totals (including refreshed_at)
were unchanged. Full FEN rows for held IDs 2234, 2242 and 4159 remained identical
to the plan. The five new rows matched their retained after-snapshots exactly.

The transaction queued **20 distinct games / 15 eligible profiles**. Existing
automatic recovery and the unchanged canonical model completed the 20-game,
3,114-move CUDA refresh on the **RTX 4060**, reporting 1.178 s for its worker
batch. The ready status was recorded at 20:55:32 UTC (about eight seconds after
the repair receipt). Input and canonical queues were empty, Stockfish summaries
had no pending revisions or errors, and all 47 canonical snapshots remained
available with identical payload fingerprints (the small repair did not change
the displayed canonical histograms). All eight positions' continuations also
matched the expected published or held snapshots. No cloud work, inference/model change, service restart or clearing of
old scores was needed. This small pilot does not establish bulk-repair speed or
resolve the remaining historical mate encoding uncertainty.

### Coordinated pre-cleanup repair pass — 2026-10-27

This pass extends the admin workflow, not the automatic normal-analysis budget
or the blunder formula/model. No broad historical repair is authorized.

1. **Held mates:** new read-only snapshots of IDs 2234, 2242 and 4159 reference
   30 distinct games and 16 eligible profiles. One-million-node searches found
   White mate in 7, White mate in 10 and Black mate in 10. Ten-million and
   twenty-million independent, cold-position searches agreed on **White mate
   in 6, White mate in 7 and Black mate in 10**. Total shadow search times were
   1.290 s, 8.123 s and 14.152 s respectively. Local Syzygy independently confirms
   the winning sides. These are fresh engine-reported mate distances, not exact
   DTM-table proofs or verification of the historical engine/encoding.
2. **Disagreement policy:** matching-only publication remains the default.
   `reviewed_same_bucket_replacement_v1` requires the separate acknowledgement
   `replace-reviewed-legacy-evaluations`. Mate cases additionally require a
   higher-budget confirmation agreeing on typed score, distance and winner,
   plus linked local-tablebase evidence confirming the winner. Bounds-only
   engine scores, missing evidence, inconsistent mate results and category
   changes are rejected. Originals remain in immutable receipts; no automatic
   downgrade of historical mates or inference of their original source/date.
3. **Changed-score publication:** bounded old-to-new deltas update the existing
   `scored_position_summary` and, only for zero/nonzero transitions, database
   nonzero counters in the same transaction as FENs, continuations, refresh
   requests and the receipt. Already-scored totals never increase. Existing
   50/150/300/9000 bucket boundaries, the different inclusive ±50 side boundary,
   appearances, absolute/signed sums and nullable WDL populations are preserved.
   Missing/inconsistent cached totals or negative results fail closed.
4. **Second small pilot:** four CP candidates, IDs 13, 24, 164 and 188, reference
   24 games and 10 eligible profiles. The normal 100,000-node shadow took
   0.775 s and returned -40, 20, 40 and 14 CP (old values -39, 32, 41 and 11).
   All remain in the equal bucket. This is a safety/coverage test, not a
   representative quality estimate for the full legacy population.
5. **Refresh/UI regression:** disposable PostgreSQL tests cover the shared
   writer, exact delta parity with an independent SQL aggregate oracle,
   NULL/known WDL, zero/nonzero and ±50 boundaries, stale input, claim/admission
   races, failure after delta writes, immutable receipts and replay. Existing
   input recovery/CUDA coverage integration tests pass. Both fully mocked
   browser suites pass: canonical progress/retries/history and partial/stale
   game-review blunder labels. No real browser workflow submits a cloud job.
6. **Ingestion performance:** the trigger regression check updated 20,000 FEN
   metadata rows in 0.173 s, including 10.74 ms of trigger execution, despite
   400,000 synthetic associations. It structurally verifies that no-op and
   metadata-only updates never read associations. The full ingestion harness
   also passed duplicate/concurrent replay, rollback, claims, scoped/full
   coverage parity and a 1,200-link batch in 0.045 s. These are isolated timings.
   No new real ingestion has run since the fix: the latest recorded ingestion
   remains gurelediz, 73m59s overall and 61m01s in the FEN stage. Its overlapping
   worker durations must not be added as serial wall time. Real before/after
   performance remains pending the user's next ingestion; do not claim a
   production speedup yet.

Important bound: crossing an advantage category is deliberately **not** allowed
by this repair policy. That also changes the saved rating-group bucket counts
and needs its own atomic/durable projection update before expansion. Same-bucket
replacements leave rating groups, membership, category counts and total position
counts unchanged. The original max-16-FEN/max-20-reference limits remain. We can
resume backend cleanup without processing the remaining historical backlog.

Artifacts are retained under `research_data/evaluation-repair`: the
`held-mates-plan`, `held-mates-1m`, `held-mates-10m`, `held-mates-20m`,
`held-mates-tablebase`, `cp-pilot-plan` and `cp-pilot-shadow` files, each suffixed
`-20261027.json`. Each publication receipt retains its exact checked artifacts,
including stronger-search/tablebase evidence and before/after aggregate values.

Live result: the three mates committed at 21:12:47 UTC, receipt
`924368e4679e1721962c20c6ac86e374b3af701faa4794474ed7a59897525732`;
the four CP evaluations committed at 21:14:26 UTC, receipt
`3fa73522f7a71fd6f96c0254c0746bb0180004ad2c2686067b78c48baa86f6b5`.
The seven replacements affect **54 distinct games / 20 eligible profiles**.
Six primary numbers changed; the rook mate stayed -9990. Both publications
retained genuine shadow analysis timestamps, alternatives and original rows.
All 12 published FENs (including the previous five) still matched their audit
after-snapshots exactly at the final read-back.

Observed aggregate deltas matched predictions exactly: score sum -41, absolute
score sum -39, mate absolute sum -30, equal absolute sum -9, and WDL sums
(-28,+21,+7). All category/appearance/population counts and the complete
`database_summary` row stayed unchanged. The two automatic recovery batches
handled 30 and 24 games. CUDA evaluated 27 / 23 eligible games and 4,495 / 1,983
moves respectively on the RTX 4060; unsupported games retained explicit coverage
handling. Worker batch times were 1.317 s and 1.036 s, not GPU-kernel-only times.
The final canonical status was ready at 21:14:34 UTC, with no input/CUDA queue,
no Stockfish-summary pending revisions/errors, and the original model ID intact.
No services were restarted or new ingestion/cloud jobs submitted.

Final focused checks: 23 offline tests; isolated planner, changed-publication,
coverage/recovery, ingestion and trigger-cost harnesses; two mocked browser
suites. Remaining before broad repair: a category-changing rating-projection
policy and representative high-fanout/performance testing. Remaining before a
production ingestion speed claim: one new real ingestion. Neither blocks
returning to the requested backend cleanup.

## Explicit historical provenance repair — 2026-10-27

User-approved scope: freshly probe missing-source positions with at most five
pieces using local Syzygy, and attribute larger **ordinary CP** evaluations as
`stockfish_legacy_assumed`. This is an accepted historical assumption, **not**
recovered or verified engine provenance. The existing `stockfish*` input policy
accepts that source; no kernel, threshold, calibration or mate encoding changed.
The user explicitly chose to leave larger historical mate-encoded scores alone.

The frozen plan contains 493,351 already-scored FENs:

- 16,932 tablebase candidates, including former engine mate scores.
- 474,609 larger ordinary scores eligible for source-only attribution.
- 1,810 larger mate-like scores held unchanged.

`scripts/repair_legacy_provenance.py` separates read-only planning from explicit
`--apply --acknowledgement repair-legacy-cp-and-probe-tablebases`. The plan SHA-256
is `72476707db3c18ffa4cb6ec31789539e1cfeda7942f5e1751a7f8fc56df31950`.
The dedicated container `chessism-legacy-provenance-repair-20261027` started at
23:44:53 UTC with 100-FEN transactions, one CPU, and 1 GiB memory. No service was
restarted. Completion is recorded in the progress file, not inferred from launch.

Plan/progress/unsupported-position records live under
`research_data/evaluation-repair/legacy-provenance-20261027.*`. Immutable database
receipts retain original rows, replacements and removed Stockfish continuations.
Retries discover committed IDs from receipts; a commit followed by lost client
output cannot relabel or double-count those positions. Row rechecks, cloud claim
checks, fail-fast locks and active ingestion/cloud/research admission guards
protect each batch. Larger historical mates are rejected by both preparation and
publication. Unsupported local tablebase probes retain their existing scores and
missing metadata, with an explicit skip record; they are never mislabeled.

Fresh tablebase results use the normal WDL/DTZ/score/move representation and a
genuine new analysis timestamp. Assumed larger scores change only their source:
their scores, continuations and unknown timestamps are retained. Exact global
old-to-new deltas and rating-group occurrence bucket deltas commit with each
replacement, so already-scored totals never increase. Normal durable FEN refresh
triggers queue coverage/player summaries and the canonical RTX 4060 calculation;
the repair completing does not imply all downstream summaries have finished.

The repair reserves the existing backup lane and invalidates the timestamp delta
base **before** changing data. The next FEN backup must create a full snapshot so
NULL-timestamp metadata changes cannot be missed. No old backups are deleted.

Validation: 24 offline policy/evaluation/tablebase tests passed, plus a disposable
PostgreSQL test covering actual writes, transaction rollback after writes, stale
inputs, claims, active jobs, locks, idempotency, immutable originals, retained
mates/continuations/timestamps, durable queues, global aggregate parity and
rating-group parity (including repeated FEN occurrences and incomplete games).
These are repair-correctness checks, not new statistical model-quality claims.

### Source-only refresh routing (2026-10-27)

`evaluation_refresh_v4` records whether a queued game needs CPU engine/coverage
projections. Changing only a non-tablebase source label does not change their
inputs: it now transfers directly to canonical CUDA invalidation, without
recomputing accuracy, efficiency, the old CPU blunder equation, or coverage.
Score changes, transitions into/out of tablebase, and association/ownership
changes retain the full path. Coalescing uses logical OR, so a later metadata
edit cannot downgrade a genuine score change. GPU enqueue and exact-revision
input acknowledgement commit together; failures roll both back.

Unknown historical queue reasons default to full refresh. The explicit
`scripts/route_provenance_refresh.py --plan <completed-plan>` preview and
`--apply` migration are restricted to the exclusive two-phase legacy repair:
all value-changing receipts must precede source-only receipts, overlapping job
and analysis histories are rejected, queue transaction IDs must match immutable
source-only receipts, and requests predating that phase, later writers and failed
requests remain conservative. An immutable routing receipt records captured
revisions. PostgreSQL `xmin` is used only as immediate migration evidence, not as
a durable application identifier. No evaluations or timestamps are rewritten.

Source-only transfer and mixed-player selection allow up to 32,768 games, bounded
by 2,097,152 position rows per inference call. The frozen CUDA function and model
hash are unchanged; it still sizes internal CUDA chunks against available VRAM
and retries smaller chunks on allocation failure. Histograms are published once
per larger batch, not once per 500 metadata-only games. Logs report input refresh,
source extraction, CUDA and publication timings separately, plus end-to-end time.
This does not claim that database reads or JSON publication execute on CUDA.

Validation includes network-isolated PostgreSQL tests for source-only bypass,
mixed-change precedence, tablebase semantics, ingestion/ownership invalidation,
failed-handoff rollback, receipt-backed migration, historical overlap refusal,
exact-revision preservation, stale publication rejection and worker retry.

The live routing migration retained and reclassified **911,893** queued games;
none required the full CPU refresh at the migration snapshot. No FEN values were
changed. Its first attempt rolled back on the receipt-ID length constraint;
the corrected hashed ID passed the real receipt-schema test before retry. The
CUDA worker alone was replaced with
`chessism-algorithms:source-only-refresh-v4-20261027` (also tagged `runtime`).
The frozen inference hash remained
`cd1cb1e18e29abc79f83fcd0dc28469889f7633a047c805bb163914445e774e3`.

First live batch after deployment: 32,768 games / 1,874,377 position rows in
17.98 seconds end to end (input handoff 0.63 s, extraction 11.50 s, cold CUDA
3.04 s, publication 2.38 s). The next batch hit the row cap at 2,097,138 rows
and 29,552 games, with 1.07 s in CUDA. Both used the RTX 4060; input recovery
reported `engine_refreshed=0`. The remaining workload continues durably in the
background. These are initial observed batches, not a whole-run benchmark.
An obsolete ARQ max-retry result from the controlled restart was verified and
removed before issuing the normal wakeup; no database inputs or results were
deleted. The cron's ordinary deduplication then resumed normally.

### CPU blunders retired from production (2026-10-27)

Blunder results are now CUDA-only for **all** production updates, not just
provenance-only edits. The shared `refresh_game_player_engine_summaries` query
no longer computes, selects, inserts or updates a blunder count. Its remaining
accuracy, efficiency, CP gain/loss, mate and ending statistics are unchanged.
This covers new FEN evaluations, deliberate replacements, newly ingested games,
local/cloud imports, repair recovery and request-time missing-summary refreshes.

The chart endpoint, game-review annotations and compatibility view no longer
fall back to legacy CPU judgments/counts. Missing CUDA data remains pending or
unknown. The old `CHESSISM_CANONICAL_BLUNDERS` rollout flag cannot re-enable CPU
reads. Administrative rollback accepts retained CUDA models, not `legacy_cpu`.
The obsolete physical count column is retained for schema/history compatibility
but is not maintained by the CPU writer or exposed as the production count.
The scalar historical oracle lives exclusively in an explicitly invoked research
validation module; production writers/readers do not import or call it.

`scripts/manage_canonical_blunders.py --retire-cpu-blunders` updates the
read view and retires the physical counter's required/default value under
idle/current-model guards. It does not replace the model,
recalculate existing results, erase historical rows, or modify FEN scores.
Code-only `cuda-only-blunders-20261027` images update the API, ingestion/local
analysis workers, cloud importer, backup/research shared-worker callers and CUDA
worker under their persistent deployment tags. Database, Redis, Stockfish engine
and frontend services are not restarted by this deployment.

Validation: 56 focused offline tests, the complete 103-test pipeline helper
suite, and four disposable PostgreSQL test runs passed. The new database test
compares every retained numerical projection against the pre-retirement query
on synthetic data, checks that CPU count sentinels remain untouched, exercises
new-score automatic refresh through canonical publication, and refuses legacy
fallback when the CUDA model/output is absent or the old flag is disabled.
No production data is used for the CPU reference comparison.

The first live post-cloud refresh exposed an omitted schema migration: the
normalized physical `blunder_count` remained `NOT NULL` with no default even
though the CPU writer correctly omitted it. PostgreSQL checks this before
`ON CONFLICT`, so even an existing summary could fail. The former fixture's
default masked that production contract.

Canonical installation/retirement now makes this unused physical column
nullable with no default, without rewriting old values. Existing deployments
can run `scripts/manage_canonical_blunders.py --repair-retired-counter`:
the explicit, transactional metadata-only repair verifies PostgreSQL column
dependencies prove both compatibility and engine reads use CUDA assessments,
never the physical CPU counter. It has a two-second lock wait and a fifteen-second
statement limit. It does not change models, FEN scores, GPU results, queues or
job status. Resume the paused job separately through its normal recovery action.
Tests now reproduce the exact missing-default failure, cover fresh installation
and repeat standalone repair, preserve historical sentinels, leave new retired
counts NULL, and verify retained numerical results plus CUDA-only publication.

An additional network-isolated check on the actual RTX 4060 passed for 18,000
synthetic transitions across 90 games. Post-deployment read-only checks confirm
the active model/kernel is unchanged, both refresh queues are empty, and the
player-facing blunder reader returns current canonical CUDA results even when
the obsolete rollout flag is disabled. No existing player results were rebuilt.

## Remaining statistical gates (before claiming validated quality)

1. Review the rating-aware candidate's held-out fit and coverage by rating/mode.
   Address missing groups, rating-band discontinuities, large rating gaps and
   time-loss populations before claiming general coverage.
2. Add temporal/player-held-out checks and account for repeated positions. Preserve
   independent test data when changing model design; do not tune on reported test
   outcomes. Audit calibration rather than treating GPU agreement as model validity.
3. Compare the new model on CPU/CUDA for numerical and decision agreement. Compare
   old/new blunder definitions statistically, by rating and mode, with game-level
   uncertainty and representative disagreements. Closeness alone is not validity.
4. Keep future candidates separately versioned; the provisional registry above
   does not grant automatic approval to a refit or a changed inference contract.
5. Measure real production performance and recovery after the initial rebuild.
   Isolated transactional recovery checks passed, but the original run's unrun
   production-performance/recovery gates have not been retroactively relabeled.

## Checks

`tests.test_summary_algorithms`: contract/kernel tests (CPU only).
`tests/check_summary_matrices_postgres.py`: disposable PostgreSQL, synthetic data.
`tests/test_cuda_player_validation.py`: queue/dispatch, no-fallback and comparison guards.
`tests/check_cuda_player_postgres.py`: frozen live-input workflow on disposable data.
`scripts/validate_summary_cuda.py --synthetic`: actual CUDA hardware test.
`chessism_web/tests/cudaReport.test.mjs`: local-report parsing guards.
`chessism_web/scripts/test-summary-algorithms-browser.mjs`: fully mocked UI checks.

Deployment of research-only definitions requires only the API and an **idle research
algorithm worker**; the latter must understand the new row units before custom
research runs are submitted. No cloud controller, FEN/ingestion/salience worker,
Redis, database migration, or driver restart is required for research-only changes.
The separate canonical promotion above explicitly installs its additive registry,
invalidation triggers and read overlay before enabling production readers.
