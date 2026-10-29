# Manual CUDA summary orchestration

## Manual salience stage

`salience_first15_equal_cuda_v1` is **First 15 moves · equal weights**: both
sides' first 30 plies, excluding the initial position. A shorter game uses only
its available positions. Corpus frequencies count distinct games separately
for each player and player color, pooling modes, using the same 30-ply window.
Each occurrence contributes `1 / (games_with_position * occurrence_index)`;
game salience is their equal-weight mean. Empty games have no salience score.

The stage captures all pending players in the manual request. It reads only
numeric early-position identities, not evaluations, using bounded binary COPY.
All extraction checkpoints are finished before salience inference. Transport
partitions by owner/FEN, then CUDA computes complete frequency groups in large
mixed-player, available-memory-sized batches. It never approximates global
frequencies with independent chunk frequencies or loops over players for the
main inference. Remaining host work is database/file I/O, transport partitioning,
timezone metadata and serialization. Device allocation failure has no CPU
fallback; committed checkpoints survive retries.

CUDA merges game contributions, computes player totals, and refreshes the legacy
daily salience sum from already-saved accuracy scalars for compatibility. Old
per-game accuracy, blunders and score histograms are not recalculated merely
because the salience formula changed. A player's game salience, frequencies,
formula version and daily cache publish atomically. Captured input revisions,
score revisions and incomplete position extraction protect against concurrent
changes. Superseded players remain pending for another explicit request.

Deploy API, algorithm worker and the legacy salience queue consumer while idle.
The latter uses `chessism-salience:runtime` and only defers legacy queue wake-ups
once the policy is enabled; it never starts an implicit CUDA run. Install the
additive `salience_store.install` schema, then enable with `manage
--enable-salience`. This changes scheduling only: no run is created and no
official results are overwritten. The next **Run CUDA summaries** click captures
the initial full-player salience backfill. Old formula readers remain accurate
until each player's new version is published.

`daily_salience_bands_v1` now also saves an **independent** daily distribution:
`[0,50], (50,60], (60,70], (70,80], (80,90], (90,100]` on the absolute 0–100
salience scale. Classification compares unrounded raw fractions on CUDA.
Every dated game with calculated salience counts, including games lacking an
accuracy/Stockfish assessment. Each player/local day and each of the seven
bullet/blitz/rapid selections retains all six counts, total games and all tied
modal bands. Missing salience is never assigned to the first band. Version and
timezone are stored separately from the unchanged per-game salience algorithm.
These rows publish atomically with the game scores. GET daily accuracy includes
the separate salience payload with an explicitly different cohort; it does not
recount games or compute bins. Unsaved timezone overrides return unavailable,
never another timezone's data or an implicit CPU/GPU calculation.

The hero replaces its relative-average slider with six absolute-band buttons
(and All), filtering days by any tied winning salience band. Hover shows the
complete salience distribution and its independent game count. Legacy summary
responses can still display the previous slider during upgrades. The new API
reports bins pending until an explicit CUDA run; it never derives bins from the
old daily averages. Existing appearance targets remain valid for saved presets;
the new buttons each have independent editable targets.

Per user direction, integration does not require a separate live-player
comparison/validation run. `tests/check_salience_cuda_integration.py` is an
isolated plumbing check with synthetic records and private PostgreSQL, checking
manual capture, legacy deferral, checkpoints, daily publication and input races.
The six-row device smoke check establishes execution compatibility, not
full-cohort numerical parity or production performance.

After an explicit, idle activation, ingestion and FEN import retain their normal
storage, integrity checks and coverage updates. They do not compute CPU accuracy
or position-score histograms. The existing canonical GPU worker still prepares
coverage, but cannot infer unless a superuser has created a durable summary run.
CPU blunders remain retired. Salience has an explicit manual-CUDA activation
described below; existing results are not changed at activation. Behavioral summaries can now
be explicitly migrated using the metadata activation described below.

The Analyze Positions control is above Local and GCloud. GET status and ordinary
page loads never start calculations. POST /analysis/cuda-summaries/run is superuser
only. It captures pending revisions transactionally, coalesces repeated clicks,
and uses the existing single-job RTX 4060 worker. Restart/Redis-wakeup recovery
continues only previously requested work. Retry stops after three consecutive
failed batches and requires an explicit Retry run action. Old snapshots survive.

The explicitly enabled writers run **blunders, score distribution, accuracy,
behavioral summaries and Unexpected endings**.
Accuracy keeps the existing per-game formula. Manual salience uses the selected
first-15-full-moves, equal-weights formula after explicit salience activation.

`shared_scores_v1` reads bounded, mixed-player game batches using binary COPY.
Each unique game's position occurrences are read once, including partially
scored games needed by the histogram. The same arrays provide contiguous,
verified-source transitions to the unchanged canonical blunder model. Unknown
historical sources retain the existing histogram semantics and unknown mate
distance; they are not silently considered verified blunder inputs. Unscored
positions contribute only to coverage counters, never to the scored matrix.

The established `score_distribution_v2` CUDA kernel calculates CP bins, mate
distance/side and tablebase counts. CUDA also reduces recorded endings and
coverage, then merges compact day partitions. CPU work is transport, metadata
selection, validation, serialization and publication, not a histogram fallback.
Only affected UTC days are reread; games shared by two players are not reread.
Both a changed day's old owner/date and its new owner/date remain covered.

Shared execution no longer treats 32,768 games or 2,097,152 positions as a GPU
batch limit. `batch_budget.py` fills mixed-player batches against currently free
device memory and the worker's cgroup/available host RAM. The 512 MiB device
reserve is for CUDA/runtime overhead, not a desktop on the dedicated RTX 4060.
Peak estimates include per-position temporaries and per-game/day groups; very
short games are also bounded by their metadata cost. Games are never split.
Metadata is keyset-paged from the captured run only. Binary reads use bounded
8,192-game/1,048,576-position chunks into one preallocated host array; SQL/COPY
chunk size is not the GPU calculation size.

`snapshot_v2` finishes the entire captured cohort's shared input preparation
before initializing CUDA. Bounded extraction checkpoints are atomically written,
fsynced, checksummed and recorded in run timings. They contain numeric arrays and
JSON metadata, never pickle. Read-only database connections close before CUDA;
failed/cancelled COPY readers terminate only their owned connection. A retry
reuses completed extraction instead of fetching it again. Files live under
`ALGORITHM_WORK_DIR/../cuda-inputs/<run UUID>` outside research scratch cleanup.
They are run-scoped, retained for review, and not blindly reused across new runs.
The protected disk-space reserve applies before writing each checkpoint.

Prepared chunks compose one memory-sized GPU batch per worker turn. Each raw
position array is uploaded once; blunders, scores and accuracy reuse its device
columns. Allocation pressure retries fewer cached chunks without querying FENs
again. There is no CPU inference fallback. Input and result cursors advance only
with their durable checkpoint; publication retains exact-revision/player guards.
Serialization runs off the event loop, staging uses binary COPY and set-based
writes, and blunder histograms reduce on CUDA once per player at final publication
instead of repeatedly aggregating all player games after every batch. A cancelled
worker records a retryable error while retaining completed input/output checkpoints.
The container defaults to 4 CPUs for transport/packing and a **16 GiB ceiling**
(not a reservation) for inputs, merge/packing buffers and outputs. Override with
`CHESSISM_ALGORITHMS_CPUS`/`CHESSISM_ALGORITHMS_MEMORY`; budgets respect smaller
limits too. Changing Compose does not reconfigure a running worker.

`shared_blunder_execution_v2` invokes the unchanged frozen rating-aware equation
over the full memory-budgeted input rather than the old 2M-row sub-loop. The
frozen `canonical_blunders.compute.infer` remains the reference for existing
model hashes and parity checks, and for legacy blunder-only captured requests.
Model coefficients, threshold, mate rules, flags, counts and source hashes do
not change. Execution version/hash is recorded separately in publication info.
Run timings record batch count, OOM retries and the latest batch's positions,
games, host/device budgets, input-upload bytes and reserved device pool size.
Source preparation (including durable writes), cached local reads, CUDA calls,
publication and finalization have separate timings. Queue/planning/commit overhead
also contributes to wall-clock time; compare end-to-end runs, not GPU utilization.

Roll out this transport upgrade only while CUDA/research work is idle. Update
the algorithm worker and the API's owner-scope capture code; leave producers and
database services running. No schema migration, model activation, automatic
rerun or existing-summary rewrite is required. Retain the previous
worker image for rollback. Validate with `tests/test_cuda_batch_budget.py`, the
disposable `tests/check_accuracy_cuda_postgres.py` integration, and
`tests/check_large_shared_cuda.py` on an idle RTX 4060. The last test chooses a
synthetic batch from real available memory and checks all three calculations,
single-upload reuse without accessing live data. `tests/test_cuda_input_snapshot.py`
checks artifact integrity, merged indexes, subset parity and cancellation cleanup.

For an explicitly requested full-cohort comparison (including already-current
players), `scripts/rerun_cuda_summaries.py --start` is an administrative launcher.
It requires idle producers/GPU work, current player summaries and empty pending
queues. In one transaction it preserves published summaries in a uniquely named
`cuda_compare_<uuid>` schema, captures the previous run, marks cache revisions
for a full rebuild, and requests the normal manual run. It never deletes current
summaries or edits game/FEN sources. The run's `timings.comparison` records the
baseline schema and previous work scope; baseline data is retained for review.
All three metrics are recomputed. If the old run had no pending blunders, the
new run's extra blunder work must be disclosed in performance comparisons.
Requeue advances revisions, so compare blunder counts/flags rather than their
revision-dependent source hashes. Compare integer histograms exactly and
accuracy floats with the established numerical tolerance. This launcher is
covered by `tests/check_cuda_rerun_postgres.py` using private synthetic storage.

`game_accuracy_v1` ports the legacy volatility-weighted/harmonic mean to CUDA,
including the exact 2–8-ply window endpoints, +/-1000 evaluation clamp, +15 CP
opening prior, +1 bonus, mate/tablebase handling and zero-accuracy harmonic rule.
It is not rating-aware: the approved rating-aware model belongs to blunders,
not this unchanged accuracy formula. CP gain/loss, mean/median Win% loss,
mate-position counts and the final CP value are also produced on CUDA because
they share the existing engine-summary row. No CPU blunder count is computed,
copied or overwritten. Fully analyzed contiguous games are required; ratings
are not an accuracy eligibility requirement.

Daily accuracy uses six unrounded ranges: [0,50], (50,60], (60,70], (70,80],
(80,90], (90,100]. CUDA stores counts and **all** tied modal bands for each
player/local day and each of the seven mode combinations. Games need more
than ten analyzed player moves, preserving the chart's eligibility threshold.
The chart labels categories by ranges; these are not average percentages.
Existing game salience is read, not recalculated; its daily sum supports the
same client-side whole-day slider. Unknown salience is not invented as zero.
The normal read path serves saved GPU daily rows. Explicit non-default timezone
requests use a bounded SQL grouping of saved game scalars, never move/FEN reads
or accuracy computation. This preserves the existing timezone API semantics.

Accuracy rows stage with shared input batches and publish under the same
player-state revision lock as score distributions. The first explicit run
backfills players without a v1 accuracy snapshot; subsequent runs replace
changed UTC-day game scalars and reduce the compact daily summaries on CUDA.
Until a first accuracy run is published, the UI labels and retains the old
daily chart. It never substitutes old daily averages for modal categories.

Score scopes capture player revisions, dirty days and pending game revisions.
Metadata planning runs on the worker, not in the HTTP request. Batch staging
and canonical publication commit with the input cursor. A retry cannot double
add counts. Complete per-player snapshots are published only if the captured
revision still matches and no FEN preparation remains. Concurrent changes keep
the old snapshot and remain pending for a new click. Temporary manifests/counts
are removed at completion; compact run/scope audit and timing remain.

public.cuda_summary_control is the durable scheduling policy. The pending table
mirrors canonical game invalidation using statement-level transition triggers.
There is one pending row per metric/game, updated to the latest revision.
Run/item tables freeze a finite scope. Source changes before/during calculation
are superseded, not silently consumed by an old request. Canonical publication
and manual acknowledgement commit together, acknowledging only exact revisions.
New work requires another click. Only metadata/schema are added at activation;
no source scores, model coefficients or existing summary values are rewritten.

Current per-game accuracy/efficiency and score histogram entry points defer in
manual mode, including calls from page-load repair paths. Saved-summary reads and
small groupings of their already-published data remain available. The hero shows
pending metrics; absence of new summary output is not an ingestion failure.
CPU database I/O, coverage maintenance, API rendering and scheduling remain.

Deployment: overlay code on the existing dependency images; deploy the API,
algorithm worker, score-summary worker, ingestion/FEN/local-analysis producers
and cloud controller while idle. Install the schema with manual=false first;
activate only after these consumers/producers contain the policy guard. Do not
restart PostgreSQL, Redis or unrelated jobs. Reinstalling is additive/idempotent.

For the second milestone, install the additive schema, update only the API and
algorithm worker while idle, then run `python -m
chessism_api.operations.cuda_summaries.manage --enable-scores`. This enables the
new writer for future explicit requests; it neither queues a rebuild of current
players nor starts a calculation. Existing captured blunder-only requests retain
their original behavior. Pending player/day state also enables Run when there
is no remaining canonical blunder queue.

Accuracy activation uses `python -m chessism_api.operations.cuda_summaries.manage
--enable-accuracy` after the additive schema and API/GPU-worker update. It enables
future explicit requests and reports players requiring the initial backfill;
it does not start a GPU job or overwrite existing engine-summary values.

Validation: tests/check_manual_cuda_postgres.py uses a disposable, network-isolated
database with synthetic games and a mocked CUDA boundary. It checks coalescing,
concurrent clicks, captured revisions, publication races, retry/history, CPU
deferral and preparation without inference. The browser fixture intercepts every
API call and checks explicit Run, progress, retry, completion retention and History.

## CUDA metadata stage

`behavior_metadata_cuda_v1` produces hours, local-day/hour outcome counts, daily
last ratings, rating distributions, opponents and openings from game metadata.
`unexpected_endings_cuda_v1` produces the four outcomes and three advantage bands
from each game's exact final position, including games whose other positions
are not fully analyzed. The threshold stays 200 CP and missing/mate/tablebase
categories retain their existing rules. Current behavioral snapshots remain
visible until replacement. No page load runs either algorithm.

Source rows are keyset-paged across players into checksummed numeric checkpoints.
All input preparation finishes before feature calculation; prepared chunks merge
into memory-budgeted mixed-player GPU batches. UTC-day invalidation limits future
reads. Identity/timezone encoding, transfer and publication remain CPU work;
grouping/counts and latest-rating selection run on CUDA, with no CPU fallback.
Multi-key grouping uses device lexsort and parallel group boundaries, NOT CuPy's
Python-loop `unique(axis=0)` implementation. A synthetic 1,310,720-row batch on
the RTX 4060 passed in 0.44 s on 2026-10-28 (not an end-to-end database benchmark).

Compact partitions are merged on the GPU. Before initial publication, current
saved behavioral snapshots must match exactly for integer counts and within
1e-9 absolute / 1e-10 relative tolerance for elapsed seconds. The old payloads
are retained in `cuda_metadata_baseline`; stale/missing references are explicitly
reported rather than treated as validation. Any mismatch blocks publication.
Captured behavior and score revisions are checked again under locks; concurrent
changes retain old output and leave new work pending. Retries reuse checkpoints.

Install the additive `metadata_store.install` schema while metadata=false;
update the API, algorithm worker AND player-summary worker while idle, then
activate with `manage --enable-metadata`. The old behavioral writer takes the
policy lock and defers when enabled. Activation does not enqueue work. The next
explicit Run CUDA summaries request captures the initial metadata backfill,
without unnecessarily rerunning already-current position algorithms. Historical
CPU summarizers remain as isolated test references / pre-activation compatibility,
not automatic production writers after the switch.

Tests: `test_cuda_behavior --cuda --large` uses only synthetic inputs on the
RTX 4060. `check_cuda_metadata_postgres` uses private PostgreSQL and a mocked
device boundary to verify old-SQL parity, changed dates, shared batches, manual
deferral, mismatch rejection, publication races and extraction retries.

Still outside CUDA: profile/navigation lookups, game-list filtering/sorting over
saved catalog counts and position-popup ranking. Coverage
totals are included in the new saved payload, but the hero coverage endpoint has
not been switched to it; fresh ingestion coverage remains available immediately.
The position-popup reader still groups all matching occurrence identities before
streaming its short page (`player_score_position_page.ranked_candidates_sql`),
so a small page does not bound that preparation cost. Its next optimization is
a revisioned reusable ranking, not merely a smaller LIMIT. The hero totals reader
(`ask_db.get_player_fen_score_counts`) aggregates saved per-game coverage, not FEN
history. Switching it to manual totals requires explicit stale/pending semantics
so newly imported coverage is not presented as missing until the next CUDA click.

### Full position-summary comparison (2026-10-28)

`compare_cuda_summaries.py` compared run
`8d0e71b3-8187-4269-8b4a-f77510f72ed9` with the retained
`cuda_compare_dc3833a90732456081c28c907412e96e` baseline: 49 score snapshots,
1,053,334 blunder games, 1,006,298 accuracy/engine rows and 207,262 daily rows
had no missing rows or value mismatches. Maximum accuracy difference was
9.947598300641403e-14. Blunder snapshot comparison accounts explicitly for the
37 previously reviewed zero-move corrections using retained before payloads;
revision-dependent hashes are not compared as numerical results.
`tests/check_shared_cuda_postgres.py` compares saved outputs against the old SQL
oracle with synthetic partial/mate/tablebase/unknown-source cases, multiple
batches, shared owners, source races, score-only work, date moves and deletion.
`tests/check_shared_cuda_device.py` checks the actual RTX 4060 against independent
bin/mate oracles and tests device-only aggregation. Neither test uses live data.
`tests/test_cuda_accuracy.py --cuda` compares actual RTX 4060 results against an
independent scalar oracle across window sizes, exact band edges, modes and ties.
`tests/check_accuracy_cuda_postgres.py` checks the exact existing SQL projection,
atomic publication, unchanged retired counters, date moves/deletions, stale
reads, timezone overrides and no automatic work. The daily-band browser fixture
checks range ticks, tied points, hover counts, keyboard and year navigation,
salience filtering, pending legacy display and read-only requests.
