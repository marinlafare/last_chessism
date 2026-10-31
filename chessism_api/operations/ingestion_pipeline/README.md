# Ingestion pipeline

This package is the canonical home of the path from a Chess.com player update
to queryable positions:

```text
jobs.py
  -> chesscom.py       serial, rate-limited monthly downloads
  -> game_importer.py  import use-case and derived-summary handoff
       -> pgn.py              pure clock validation and PGN transformation
       -> game_repository.py  deduplication and atomic game persistence
  -> fen_orchestrator.py
       -> fen_batches.py       compact-storage bounded replay + atomic bulk writes
       -> fen_bulk.py          COPY staging, identity resolution, exact deltas
       -> fen_workers.py       compatibility workers for older storage profiles
       -> fen_core.py          pure chess replay and aggregation
       -> fen_repository.py    claims, move reads, state, scoped recounts
  -> tablebase.py       Syzygy marking (outside this package; shared analysis)
       -> tablebase_batches.py numeric-ID work spool and bulk result updates
       -> tablebase_parallel.py bounded, persistent read-only probe processes
       -> tablebase_probe.py    pure WDL/DTZ/best-move rules
  -> player_salience.py final derived projection (outside this package)
```

Supporting modules:

- `months.py` selects full or update month ranges.
- `stages.py` defines the UI/backend lifecycle vocabulary.
- `steps.py` owns per-worker, per-run substeps, heartbeat and durable completion
  timings. A heartbeat is not presented as newly processed work. Browser reloads
  retain completed steps; missing totals remain unknown instead of borrowing
  another stage's counts.
- `progress.py` owns transient Redis counters and lifecycle snapshots, with a
  shared 24-hour expiry. Parallel counter updates remain atomic Lua operations.
- `timing.py` owns durable run and stage timings.

## Ownership rules

- HTTP routes enqueue work; they do not run ingestion in request handlers.
- `jobs.py` owns the player-level download/import job lifecycle.
- `pgn.py` rejects games without a clock for every played half-move. Such games
  are intentionally unusable for Chessism and must not be silently accepted.
- FEN child workers claim games with `FOR UPDATE SKIP LOCKED`. A game becomes
  `fens_done` only after both the unique FEN rows and all game-position links
  have committed.
- Failed or interrupted claims are released before a retry.
- In compact storage, new associations and their exact occurrence/summary
  deltas commit together. Retrying the same links adds zero to their counters.
  An existing move pointing to a different FEN aborts the whole batch.
  Full recounts remain compatibility/repair paths, not routine compact ingestion.
- Tablebase discovery is scoped to the games completed by the current pass.
- New game ownership rows are recorded in `player_salience_pending_game` in the
  same transaction as ingestion. The drained pipeline queues those players for
  salience refresh; small updates apply exact position-frequency deltas, while
  first runs and large changes use a canonical full rebuild.
- `player_position_frequency` stores only positions appearing in multiple games.
  A missing row means corpus frequency one; this keeps singleton positions from
  duplicating the much larger game/FEN association table.

## Parallelism and batching

Chessism runs multiple FEN worker processes. SAN replay through `python-chess`
is Python CPU work, so each process handles its games sequentially; spawning
hundreds of `to_thread` calls would add overhead without escaping the GIL.
Compact ingestion holds at most 300 games and normally at most 30,000 played
positions per transaction. A single oversized game is kept whole rather than
truncated. Unselected game claims are released without marking them processing. Queue
results contain counts and game IDs, never millions of position dictionaries.
Transaction-local COPY staging disappears on commit/rollback; the durable
game/FEN rows are the checkpoint. Games stay claimed until the coordinator
publishes their coverage. Interrupted jobs can replay committed batches safely.

FEN identity insertion and locking use one textually ordered `INSERT ... ON
CONFLICT DO UPDATE ... WHERE false` pass. Conflicts lock existing rows without
rewriting them or changing scores/reservations; the non-key update lock is
compatible with association foreign-key checks. This avoids a separate snapshot
for existing-row locks followed by new-row insertion. See PostgreSQL's
[INSERT locking/RETURNING contract](https://www.postgresql.org/docs/15/sql-insert.html).
Deadlocks (`40P01`) and serialization failures (`40001`) retry the entire bounded
transaction, including game claims, at most four attempts with short jittered
backoff. Each attempt uses a fresh session after rollback. Returned counters only
advance after commit; step counters rewind after rollback while keeping elapsed
time. Other errors and exhausted retries remain visible failures.

Each batch resolves its locked FEN strings to numeric database IDs once, in a
fresh statement after identity locking. Association COPY rows use batch-local
integer keys rather than repeating the FEN text; linking, metadata and exact
appearance deltas reuse the resolved IDs. Every association must belong to the
batch's locked input set. Per-batch logs separate COPY, insert/lock, identity
resolution, metadata, link and counter timings to identify production contention.

A cheap existence check queues tablebase work; the candidate count remains
unknown until discovery finishes. Candidate IDs are then discovered once,
streamed to an automatically removed temporary spool, and consumed in
1,000-position batches. Read-only probing releases its database session before
starting. Only publication takes ordered FEN locks and rechecks eligibility and
cloud reservations with a fresh snapshot. Results for positions claimed or
scored meanwhile are discarded, not overwritten. Cancellation rolls back the
publication transaction and closes probe workers and the spool.
Exact WDL, DTZ and best-move selection are unchanged. Compact writes use COPY
and one UPDATE rather than one update execution per solved FEN. Coverage still
uses the established retry-safe refresh after each committed batch.

`TABLEBASE_PROBE_WORKERS` defaults to four, capped by CPU affinity and the
container CPU quota (and never above four). Spawned processes open their own
read-only Syzygy handles and persist across batches. Only one bounded batch is
in flight; its output order is stable. Parallel probe errors, cancellation and
120-second batch timeouts terminate and join the pool instead of publishing
unknown results. A one-CPU worker uses one shielded read-only thread. No child
process accesses PostgreSQL or Redis.

Best-move probing reuses one scratch board and skips child DTZ reads when the
child has a worse WDL outcome than the current best, or is a draw (whose ranking
does not use DTZ). It retains UCI-order tie-breaking, missing-distance behavior,
and the caller's board/history. `tests/test_tablebase_selection.py` compares it
with the original selector across all five WDL outcomes, unavailable probes,
missing distances, ties, terminal positions, and probe errors.

On 2026-10-27, an offline one-CPU comparison on 1,000 saved DenLaz endgames
matched every output field in both implementations. Alternating original / new /
new / original runs averaged 9.433 / 8.799 seconds (6.7% less probe time).
This is a bounded sample, not a promised whole-ingestion speedup. A two-pass
WDL-then-DTZ variant did not improve timing and was not retained.

Scoped coverage refreshes analyze their temporary game-ID table; scopes of up
to 10,000 games aggregate through indexed per-game lookups. This prevents small batches from choosing
full scans of all game-position associations and all FENs. Large cloud scopes
and explicit full repairs retain the grouped query; an empty selection is a no-op,
never a request for a global rebuild. Stable game locks and absolute counts
are unchanged, including for FENs shared with other players' games.
COPY inputs and occurrence-delta scratch tables are also analyzed before their
joins; PostgreSQL's autovacuum cannot supply statistics for temporary tables.
Per-batch tablebase refreshes remain in place: this optimization does not defer
correctness work into an in-memory-only end-of-run list.

`INGESTION_CPU_LIMIT` now defaults to four CPUs for the coordinator/tablebase
worker (previously one CPU). It does not alter Stockfish nodes, threads, VM counts,
or the independent cloud controller.

Monthly Chess.com requests remain serialized with a delay. This is deliberate
API politeness, not a missed parallelization opportunity.

## Ingestion optimization — 2026-10-29 UTC

These changes were initially staged without touching the running cloud job,
then deployed at 02:02 UTC after job `a2b7a9da8be6457a821492df678930ca`
completed, including cleanup, and the ingestion queues were verified empty.
Only the ingestion coordinator, three FEN workers and API were recreated.
CUDA summary publication remains manual; neither this change nor tablebase completion
automatically starts CUDA summaries. Salience behavior is unchanged.

The ingestion workers use persistent `chessism-ingestion:runtime`, also tagged
`optimized-20261029` (image prefix `58e9a28ea168`). The coordinator now has four
CPUs and confirms four probe workers; FEN worker concurrency is unchanged.
The API uses `chessism-api:runtime`, also tagged `ingestion-optimized-20261029`
(image prefix `a80c792a7528`), so progress labels reflect the new publication
order. `dev_tools/Dockerfile.ingestion-code` overlays only the tested ingestion
modules on each service's preserved base; it does not install dependencies or
replace unrelated application code. Other workers retain their existing images.

Rollback images remain available as `chessism-ingestion:before-optimized-20261029`
and `chessism-api:before-ingestion-optimized-20261029`. A rollback must retag only
the corresponding runtime aliases and recreate only the affected services when
idle; restore the prior one-CPU coordinator budget if rolling that change back.

Release verification passed 127 worker-image tests, 150 API-image tests and an
actual spawned-pool reuse/cancellation/timeout/failure check. Tests imported the
built images' code, not host application sources. All four workers connected
successfully to the existing database/Redis; the API returned HTTP 200. Source
hashes matched the tested modules. Database, Redis, cloud services, CUDA and all
other worker container identities/start times remained unchanged. No new
ingestion, cloud analysis or CUDA calculation was started for deployment.

Baseline GHANDEEVAM2003 run `20fdf229-69c3-4a22-b6fe-01eff661b24a` completed in
1,916.238s: download/parse/save 232.195s, FEN extraction/persistence 815.344s,
coverage 34.014s, ingestion summaries 8.436s and tablebase processing 825.106s.
Tablebase probing alone took 715.366s for 41,231 positions. Parallel worker
substep timings overlap and must not be summed as sequential elapsed time.

An isolated read-only benchmark on 1,500 deterministic synthetic endgames
matched all WDL/DTZ/score/mate/move outputs and ordering. Alternating 1/2/4/4/2/1
process runs averaged 28.925s / 14.237s / 8.758s for the probe phase (about 3.3x
one-to-four-process throughput). Startup was warmed separately. This is not a
whole-ingestion speedup prediction, and live contention can change the result.

A separate disposable PostgreSQL benchmark compared 40,000 associations against
150,000 seeded FENs. Old/new/new/old writes took 1.497/1.253/1.325/1.259s with
exact identity, occurrence and summary parity. This small rollback fixture does
not establish a reliable production storage speedup. Detailed write timings
are retained for the next real ingestion; writer concurrency remains unchanged.

## DenLaz ingestion verification — 2026-10-27

Run `b11dbec7-26ca-4e36-9f83-efafbe7b77da` completed without a recorded error in
633.067 seconds: 7,994 imported games, 7,649 newly extracted games and 733,906
position occurrences. Main phases: download/parse/save 133.671s; FEN extraction
216.994s; coverage and ingestion summaries 26.194s; tablebase processing 255.386s.
Nested/parallel timing rows must not be added as sequential phases. The
tablebase stage solved all 15,671 candidates; its probe work accounted for
222.311s. Stockfish summary revision 125 was published, with no pending revision
or error at the post-run check.

Worker logs showed no restart or failed/restarted pipeline stage. Navigation
does not control worker execution: request handlers enqueue the chain, and each
worker queues its successor. UI unmount cancels status GETs only; hidden-tab
polling intentionally returns cached status. Returning to the page reconnects
to the running work. The new selector timing above was measured afterward in
an isolated container; it was not part of this live ingestion.

## Safe changes

Coordinator job-lock release uses an atomic owner check so a finishing worker
cannot delete a replacement job's lease. Do not clear claims or Redis globally
as a maintenance shortcut.

The endgame coordinator now uses the same atomic release helper for finished
jobs, stale-job cleanup and reservation-error cleanup. Its progress snapshots
also use `write_job_progress`, preserving the existing fields and 24-hour TTL.
Compact writes return before the legacy writer; no duplicate compact conversion
remains in that unreachable branch. `tests/test_tablebase_lifecycle.py` covers
the progress contract, stale-owner race, queue failure and malformed snapshots.

### Worker-only release — 2026-10-27

The endgame optimization and lifecycle cleanup were deployed at 21:47 UTC as
`chessism-api:ingestion-cleanup-20261027` (image prefix `9c9e1c0dce53`). The
persistent `latest` and `runtime` tags point to this release; the previous
`evaluation-refresh-v3-20261027` image remains available. Only the idle
`pipeline-worker` container was recreated, with its existing one-CPU limit and
validated `relational_v3` profile. Protected API/database/cloud/FEN/game/GPU
worker identities, start times and restart counts were unchanged. No live
evaluations were rewritten and no test ingestion or cloud job was submitted.

Verification: 146 offline backend tests, seven isolated evaluation-write
database tests, compact-ingestion concurrency/rollback/coverage checks, and
16 web progress/polling tests passed. A further 23-test smoke check imported
the built runtime's code rather than mounting source over it. The live module
hash matched the tested source and worker startup connected successfully.
The next new-player ingestion is the full-pipeline performance check; the
6.7% result above is only the offline endgame-probing measurement.

FEN failures go to each worker's container logs (`docker compose logs fen-worker-1`),
rotated at 5 MB with three files per worker. The historical `illegal_fen.txt`
is no longer mounted or appended to and is preserved for manual review.

When adding a stage, update `stages.py`, durable timing, Redis progress, and the
positions-page stage mapping together. Keep every Python module below 1,000
lines. New database work belongs in a repository/query module, not in pure PGN
or FEN transformation functions.

## Verification

`tests/check_ingestion_bulk_postgres.py` runs only in a disposable, network-none
PostgreSQL container with no live data mounted. It covers real COPY writes,
duplicates, concurrent replay, rollback, conflicting associations, retained
scores, cloud-reservation races, candidate spooling and the bounded SAN worker.
The small fixture throughput is not a production-scale speed prediction.

`tests/test_tablebase_parallel.py` covers quota bounds, reuse, ordering, errors,
timeouts and cancellation. `tests/check_tablebase_pool_processes.py` additionally
checks real spawned processes and verifies that failure/cancellation leave no
child processes behind. `tests/benchmark_tablebase_parallel.py` compares real
read-only Syzygy outputs and probe timing. `tests/benchmark_ingestion_writes.py`
compares numeric-ID writes to an explicitly supplied previous implementation
using only the disposable database.

`tests/test_ingestion_steps.py` and the web `pipelineProgress.test.mjs` cover
unknown totals, cumulative batch counts, failure, heartbeat and stale-run
isolation. No core schema migration is needed for this implementation.
