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
Compact ingestion holds at most 300 games of position data per worker. Queue
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

Tablebase candidate IDs are discovered once, streamed to an automatically
removed temporary spool, and consumed in 1,000-position batches. Every batch
takes ordered FEN locks and rechecks cloud reservations with a fresh snapshot.
Exact WDL, DTZ and best-move selection are unchanged. Compact writes use COPY
and one UPDATE rather than one update execution per solved FEN. Coverage still
uses the established retry-safe refresh after each committed batch.

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

`INGESTION_CPU_LIMIT` defaults to one CPU for the coordinator/tablebase worker
(previously half a CPU). It does not alter Stockfish nodes, threads, VM counts,
or the independent cloud controller.

Monthly Chess.com requests remain serialized with a delay. This is deliberate
API politeness, not a missed parallelization opportunity.

## Safe changes

Coordinator job-lock release uses an atomic owner check so a finishing worker
cannot delete a replacement job's lease. Do not clear claims or Redis globally
as a maintenance shortcut.

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

`tests/test_ingestion_steps.py` and the web `pipelineProgress.test.mjs` cover
unknown totals, cumulative batch counts, failure, heartbeat and stale-run
isolation. No core schema migration is needed for this implementation.
