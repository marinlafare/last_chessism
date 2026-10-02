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
       -> fen_workers.py       process-parallel replay and bulk writes
       -> fen_core.py          pure chess replay and aggregation
       -> fen_repository.py    claims, move reads, state, scoped recounts
  -> tablebase.py       Syzygy marking (outside this package; shared analysis)
  -> player_salience.py final derived projection (outside this package)
```

Supporting modules:

- `months.py` selects full or update month ranges.
- `stages.py` defines the UI/backend lifecycle vocabulary.
- `progress.py` owns transient Redis counters.
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
- Position occurrence counts are recomputed only for affected FENs. Full
  database summary scans are fallback repair paths, not the normal algorithm.
- Tablebase discovery is scoped to the games completed by the current pass.

## Parallelism and batching

Chessism runs multiple FEN worker processes. SAN replay through `python-chess`
is Python CPU work, so each process handles its games sequentially; spawning
hundreds of `to_thread` calls would add overhead without escaping the GIL.
Database writes remain bulked into 5,000-row transactions and are distributed
across the existing worker processes.

Monthly Chess.com requests remain serialized with a delay. This is deliberate
API politeness, not a missed parallelization opportunity.

## Safe changes

When adding a stage, update `stages.py`, durable timing, Redis progress, and the
positions-page stage mapping together. Keep every Python module below 1,000
lines. New database work belongs in a repository/query module, not in pure PGN
or FEN transformation functions.
