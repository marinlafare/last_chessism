# Lafareto salience comparison

An explicitly prepared experiment, separate from official salience and manual
CUDA summaries. `/research/salience-comparison` is a superuser-only research UI.
Its API routes also require superuser access independently of the parent router.

## Snapshot and calculations

`build.py` captures metadata, SAN moves, position identities and the existing
baseline under one PostgreSQL REPEATABLE READ READ ONLY transaction. Only
FEN-ready, nonempty games belonging to the selected tracked player are included.
The PostgreSQL snapshot closes before local reference calculations begin.
SQLite artifacts live under `/research-data/salience-comparisons`, not in any
official summary table. A completed artifact is published with an atomic rename;
incomplete `.building` files never appear in the UI. No GET route queues work,
changes data, recalculates scores or needs PostgreSQL.

For each player color separately, frequency is the number of games containing
the FEN. Both sides' move occurrences are included. Modes are pooled; filtering
the displayed list by mode does not change frequency denominators.

- Current: depth-weighted mean of `1/(frequency*occurrence_index)`, with depth
  weight `0.25 + 0.75*min(ply/16,1)`.
- Equal: the same full-history position frequencies, but all depths weight one.
- Opening: only plies 1–30, with frequency counted within those same windows.
  Every included occurrence weighs one; short games use their actual length.

No variants alter the within-game repetition penalty. All scores are raw 0–1,
not daily averages or relative slider values. Histograms use shared axes, ten
bins `[0,.1), …, [.9,1]`, and include every snapshot game. These reference
calculations are intentionally CPU/SQLite experiments, not a CUDA migration.

When the source salience summary is ready, baseline scores must agree within
`1e-9` or the artifact is not published. The report records the input digest,
source transaction snapshot, formula version, count and maximum difference.

## Review and persistence

Game lists are indexed and paginated (25 rows by default, maximum 50). Sorting
and browsing read saved scores only. The shared game viewer accepts an optional
snapshot loader and comparison panel; ordinary hero/game viewers keep their
existing loader and content. Snapshot SAN reconstructs the board without a live
game/FEN request or engine calculation. Per-position contributions are small,
single-game reads from the saved frequencies and sum to each saved game score.

Assessments and notes are stored separately in `<snapshot>.reviews.sqlite`.
Only an explicit authenticated POST writes them; snapshot bytes are unchanged.
The latest superuser assessment is shared, with reviewer and update timestamp.
No assessment changes official scores or selects a winning algorithm.

## Explicit preparation

Build `dev_tools/Dockerfile.salience-comparison`, then run the dedicated service:

```sh
docker compose -f docker-compose.yml -f dev_tools/compose.salience-comparison.yml run --no-deps salience-comparison
```

It is bounded to two CPUs and 3 GB RAM and starts no dependent services. The
default player is lafareto. A rerun creates a new snapshot and preserves prior
snapshots and reviews. There is intentionally no automatic rerun or calculate
action on the read-only browsing page. The legacy official salience-management
page remains at `/research/salience`.

## Verification

`tests/test_salience_comparison.py` covers frequency scope, repetition, window
boundaries, short games, exact baseline checks, contribution sums, pagination,
annotations, snapshot immutability, saved board replay and API access control.
`chessism_web/scripts/test-salience-comparison-browser.mjs` uses mocked APIs only
to test sorting, pagination, shared keyboard highlighting, board inspection,
the opening cutoff and saved assessments. Existing game-review checks must
continue to pass because the viewer extension is opt-in.
