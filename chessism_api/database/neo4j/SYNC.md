# Chess graph synchronization

PostgreSQL is authoritative. A transactional `graph_sync.event` outbox captures
completed games, path corrections, evaluation changes and player changes. Events
contain IDs, not FEN payloads. One `neo4j-sync` consumer publishes memory-bounded
game batches or 1,000 evaluation IDs per batch. New ingestion does not wait for Neo4j. No CUDA or
Stockfish work is started by this service.

Game publication no longer has a 25-game limit. The worker reads the graph's
heap/transaction limits and its own cgroup/available RAM, then budgets by estimated
plies (including the old graph path for deletions/corrections). It reserves memory
for graph state, GC, other requests, source rows, delta maps and Bolt packing.
The original 1-GiB worker / 1-GiB graph heap permitted approximately 23,000
estimated plies. The 2-GiB worker / 2-GiB graph heap permits approximately 46,000,
subject to available memory and the adaptive time bound. Up to 16 adjacent game events
can share a batch; score/player events and delayed retries remain boundaries.
This metadata window does not change the durable 1,000-ID event format.

Batches adapt toward a 15-second duration and shrink on failure/memory pressure.
Offsets advance only after the graph commit; a lost acknowledgement replays
idempotently, including batches spanning several events. A separate heartbeat
keeps the UI live while a larger transaction is running. SIGTERM finishes the
current batch before exit; Compose allows 120 seconds for this, and only the
sync worker needs to restart for worker-code releases. No graph rebuild or
PostgreSQL/Neo4j server restart is needed.

Publication sends game metadata without resending all its moves, skips old-path
lookups/deletes for new games, omits zero deltas, and validates/removes counters
in the write statement instead of repeatedly rematching them. Source parsing
uses one SAN-and-push and prepares each distinct position once per batch.
Structured `Graph batch` logs record source, graph, total duration, games, plies,
positions and budget. Their `query_seconds` map also separates position writes,
game paths, player-position counters, move counters and the other publication
statements. Timings include result consumption, not only query submission; no
FEN payloads or query parameters are logged. Transaction/commit overhead remains
part of `graph_seconds`, so the two measurements need not sum exactly.
`tests.test_neo4j_batches`, `tests.test_neo4j_sync` and the
isolated `tests/benchmark_neo4j_ingestion.py` cover bounds, exact counters,
multi-event progress and lost-acknowledgement recovery. The synthetic 500-game
baseline took 34.88 seconds (20 batches of 25); the first candidate took 20.95
seconds (273 + 227 games) with identical path fingerprints and graph totals.
These are isolated benchmark results, not a guarantee for every live player.

The dedicated graph-worker image also pins `neo4j-rust-ext==6.3.0.0` alongside
`neo4j==6.3.0` for accelerated Bolt encoding/decoding. Publication uses compact
fixed-field wire rows without changing the stored graph schema or fingerprints.
Paths are grouped by game (one game lookup per path), REACHED deltas by player
(one player lookup per group), and position counters are applied in the same
statement as position/evaluation publication. Old-only positions on a shortened
or deleted path are decremented separately without clearing their evaluations.
All changes remain one atomic, retry-safe graph transaction.

The next isolated 500-game benchmark, at the same 1-GiB heap/512-MiB cache and
1-GiB worker limit, measured 17.82 seconds for a warmed original worker versus
11.96 seconds for compact/grouped publication with Rust (about 33% less time).
The cold original run was 35.74 seconds and is not the fair steady-state baseline.
Both warmed runs used batches of 273 and 227 games. Exact node/relationship
counts, per-position and per-player counters, path fingerprints and lost-ack
replay checks passed; correction/deletion integration tests also passed.
No REACHED-key migration or prefetch pipeline is part of this release.

The larger live graph exposed long publication pauses that the small synthetic
fixture did not reproduce. The subsequent memory configuration is a 2-GiB fixed
heap, 4-GiB Neo4j page cache, 8-GiB container allowance (no container swap), and
four CPUs; the sync worker has a 2-GiB allowance. These are limits/reservations,
not a guarantee of higher throughput. Heap/page-cache settings are not dynamic.
To apply them during an ingestion: let the sync worker stop gracefully after a
committed batch, record the outbox offset and graph counts, restart only Neo4j,
verify the same graph counts and new settings, then resume the sync worker.
The existing outbox resumes the same ingestion; never recreate its event queue
or reimport already-acknowledged games. PostgreSQL/cloud workers stay running.

`Player(id)` participates in `Game(id)` through `PLAYED(color)`. A game preserves
every occurrence through `AT_PLY(ply, from_key, uci, san, move_key)` to a shared
`Position(key, fen_id, fen)`. Ordered plies reconstruct the exact game even after
transpositions/repetitions. The standard initial node is shared, and no synthetic
ply-zero occurrence is counted. Standard chess only; archived variants are not
included.

`MOVE(key, uci, san)` joins source and destination positions. `REACHED(color)`
joins players and positions. Counters distinguish occurrences from distinct games;
all counts cover **imported graph games**, not the whole PostgreSQL corpus.
Compact typed evaluations are stored in White's perspective; unknown provenance
remains unknown, and mate/tablebase scores are never treated as centipawns.

Game fingerprints and transactional contribution deltas make duplicate delivery
safe, including a crash after Neo4j commit but before SQL acknowledgement. Failed
events retain their error and retry with bounded backoff; System Status shows
progress and offers immediate retry. Deleting a game removes its contributions.
Unused positions retain zero counters for reuse. Completed outbox records remain
as an audit trail; retention should be chosen after measuring real update volume.

While the backup publication barrier is held, the consumer refreshes its
heartbeat and reports `waiting` with an explicit backup reason. Ingestion and
cloud progress preserve that reason instead of showing stalled work. Completed
work stays complete, and an actual failed event still takes priority as
`retrying`. The reporting heartbeat is stopped before a batch publishes its
completion/failure state, including cancellation paths.

Cloud score events capture their owning job from the live FEN claim in the same
transaction as the score update. The consumer persists matched graph IDs before
publication, giving the UI an honest denominator and retry-safe counts. Positions
not yet in Neo4j are excluded. API polling reads only the saved outbox metadata.
The cloud progress list shows graph synchronization independently of cleanup;
cloud admission and cleanup do not wait for Neo4j. Earlier jobs without ownership
receipts report queue catch-up without fabricated per-job counts. After restoring
an older snapshot, run the explicit installer before starting the newer consumer.

FEN capture compares evaluation columns with `EXCEPT`, not a join of unindexed
OLD/NEW transition tables. Empty ingestion updates can otherwise cache a
quadratic join plan for later large appearance-count updates. Counter-only
updates return before reading claims or the outbox; true evaluation changes
retain transactional cloud ownership and 1,000-ID chunks. An existing deployment
can upgrade only this function, without restarting ingestion or replacing its
trigger, using `python -m chessism_api.database.neo4j.fen_capture`.
The isolated regression is `tests.check_graph_fen_capture_postgres` (requires
the dedicated `fen-capture-test-db`, never live data).

Ingestion progress resolves the matching game/score ID sets once per query,
then aggregates durable outbox offsets. Score rows do not repeatedly scan the
entire ingestion's game list. Duplicate events finish only once all corresponding
updates have been acknowledged; other players, explicit backfills and events
outside the run interval remain excluded. The API retains its three-second
statement timeout and logs read failures without hiding PostgreSQL completion.
`tests.check_graph_ingestion_progress_postgres` checks these semantics and a
17,485-game / 31,202-score / 1.75-million-link synthetic fixture.

Install explicitly (never during API startup):

```
python -m chessism_api.database.neo4j.sync_manage --install --activate
python -m chessism_api.database.neo4j.sync_manage --player lafareto --limit 5000
python -m chessism_api.database.neo4j.sync_manage --verify BACKFILL_UUID
```

Enable capture before scheduling the backfill. The saved game-ID list is fixed.
The verifier checks every selected game path and evaluation, and independently
recounts graph occurrences, distinct games and move/player contributions.

Combined backup takes the graph-publication barrier **before** creating its SQL
snapshot, holds it until the Neo4j dump finishes, then releases it while PostgreSQL
compression continues. The SQL snapshot includes pending graph events, allowing a
restored graph to catch up without losing committed changes. Game ingestion and
cloud analysis continue during this barrier. Restore tests remain isolated.

First live acceptance sequence: 5,000 existing lafareto games, replay verification,
real player update, real newly analyzed FENs, then combined Backup / Test backup.
Do not alter live evaluations to manufacture an unscored test.

## Read-only graph explorer

Positions → Neo4j test loads `/graph-explorer` only when opened.
The superuser-authenticated API exposes player search, frequency rankings,
one-hop neighbors, and matching games. It accepts no arbitrary Cypher and has
no ingestion/write endpoint. Game review still reads the authoritative game
through the existing PostgreSQL API.

Global frequencies use Position/MOVE counters; player frequencies use REACHED.
Player-specific moves and game lists match actual AT_PLY occurrences, including
the move key, rather than fabricating game sequences by walking aggregate edges.
Occurrences and distinct games are separate; the synthetic initial occurrence
is never counted. Evaluations retain White's perspective and typed mate/tablebase
values. All displayed coverage refers only to imported games.

Rankings cache at most 1,001 rows per scope and return pages of 10 (top 1,000);
neighbors return at most six per direction and games return pages of 15, newest
first. Cache lifetime is 30 seconds, bounded to 32 entries. At most two graph
reads run concurrently; transactions have a six-second deadline and requests
have a ten-second deadline including queueing. These are prototype limits for
the sample, not a claim that full-corpus filtered rankings are already optimized.
No graph schema/index migration is necessary for this explorer.

## Earlier force-directed prototype (retained test tooling, not the page default)

The earlier prototype used `/graph-explorer/map`, which
captures the first 30 actual plies of at most 20,000 imported games (lowest game
IDs, explicitly labelled if truncated). It keeps up to 6,000 most-used positions
plus the shared start, and up to 20,000 connections **between retained positions**.
These are visualization limits, not deletions or full-corpus analysis. The first
test has 5,219 lafareto games and 5,000 verified Hikaru games; the cohorts are not
matched by date, rating, or mode and are not complete player histories.

One snapshot contains occurrence and distinct-game counts for all games, each
comparison player, their union, and White/Black subsets. A head-to-head game is
counted once in the union. Node counts group by game before counting games, so
repetitions cannot inflate distinct-game counts. Root starts are not added to
played-position occurrence totals. Position details and games apply the same
30-ply cutoff; if the overview's game cap is reached, details explicitly cover
matching **imported games**, rather than pretending to be only the bounded map.

Snapshots use the existing two-reader gate, a 20-second read transaction and a
25-second total request deadline. Cache: five minutes, at most three map payloads;
explicit Refresh may invalidate after 15 seconds. Other explorer reads retain
their six-/ten-second limits. There is no arbitrary Cypher or graph-write route.

The browser uses a dedicated Web Worker for Louvain communities and a seeded,
120-iteration ForceAtlas2 layout with Barnes–Hut repulsion. The initial hub is
excluded from community detection, but retained in the displayed map. Clustering
and layout use undirected, log-weighted connectivity; displayed move edges retain
their actual directions. Groups are algorithmic communities, not official
opening names. Group size means number of visible positions; a FEN's size means
distinct games; edge thickness means observed move appearances.

Sigma.js/WebGL provides pan/zoom/selection, with an SVG fallback and an accessible
paginated node list. A group opens a detail map (at most 500 positions), with
existing one-hop exploration and game review alongside. Fixed coordinates and
viewport bounds are reused across player/color filters. Layouts are keyed by
snapshot hash and layout version; only three are retained in memory/IndexedDB.
Closing the disclosure aborts requests and terminates an unfinished worker.
Palette swatches, node right-click color selection, backgrounds and controls use
independent page-scoped appearance targets. No Gephi installation, new graph
schema, full-history import, or automatic CUDA work is required.

Checks: `tests.test_graph_explorer`, `tests.test_graph_explorer_http`,
`chessism_web/tests/openingMap.test.mjs`, `test-graph-explorer-browser.mjs` and
`test-common-positions-browser.mjs`. Optional `preview-opening-map.mjs SNAPSHOT`
renders a saved bounded snapshot without live API access.

## Collapsed ply-map summaries (page default)

`collapsed_ply_map_v1` covers **all currently imported games**, not the full
PostgreSQL corpus and not a new game sample. Its display window is 30 plies.
Uniqueness uses `Position.games` across the full imported games: a position
repeated three times in one game is still unique to one game. Visual shared
nodes use `(fen_id, ply)`; canonical Position nodes are unchanged.

Real `AT_PLY` sequences determine connections. Unique runs are compressed but
may reconnect to shared nodes later. Every compressed connection retains its
exact hidden length and per-game expansion reference. Virtual tail nodes bundle
paths by shared parent and reason. `ending` means the actual final game ply is
within the window; `continuation` means the game continues beyond it. Virtual
nodes have no FEN. Initial-position counts mean game starts, not played moves.

The additive PostgreSQL `graph_map` schema holds the four derived summaries:

- `node`: visual identity, FEN reference, ply, kind and game count.
- `connection` / `segment`: endpoints, compression count, ending reason and
  compact references for expansion. No complete move sequences are persisted.
- `player` / `game`: sparse White/Black game memberships and captured source
  hashes. Selected-player unions count a head-to-head game once.
- `snapshot` plus `node.x/y`: versioned view manifest and prepared coordinates.

`graph_map.control` is invalidated by statement-level INSERT triggers on the
existing game/player outbox. Score-only events do not invalidate topology.
Ingestion never calculates the map. **Run CUDA summaries** captures the pending
revision only after graph synchronization is caught up. A restartable input stage
reads 500 games at a time, using a keyset cursor and temporary `graph_map.input`
rows. These inputs are deleted on publication or supersession; failed inputs
remain available for explicit retry.

The worker processes the captured cohort together. Path compression and layout
are host-side structural work; numeric entity/game deduplication and counts use
one bulk CUDA grouping operation on the RTX 4060. It does not use a CPU counting
fallback. A conservative host/device memory guard fails explicitly rather than
silently truncating an oversized cohort. Partitioned preparation is required
before a cohort exceeding that budget can be published.

The grouping implementation is `bulk_lexsort_v2`: device lexicographic sorting
and adjacent-row deduplication, not CuPy's Python-loop `unique(axis=0)` path.
Output semantics and the `collapsed_ply_map_v1` snapshot contract are unchanged,
so previously captured inputs remain reusable. One spawned CUDA process handles
the whole cohort and reports compression, grouping and layout milestones. A
180-second deadline or cancellation terminates that child; worker heartbeats
remain responsive and PostgreSQL input checkpoints stay intact for retry.
The run records grouping/compute/publication timings and input/unique pair counts.

Publication checks the version, outbox and source revision again, then switches
the active snapshot in the same transaction as all summary writes. Changed
inputs retain the previous snapshot and remain pending. Two complete generations
are retained for open tabs. Graph evaluations, FEN strings, profiles and complete
move sequences are not duplicated in the permanent summaries. Standard database
snapshots include these PostgreSQL tables; no additional backup service is needed.

GET `/graph-explorer/ply-map` reads at most 300 saved nodes and the connections
between them. It reports links crossing the current view boundary. The browser
pages moves/positions, preserves fixed coordinates and scroll state while adding
up to eight player highlights, and shows all games neutrally otherwise. Shared
nodes show multiple rings. Each rendered control has a page-scoped style target.
The main map does not query Neo4j or run layout/calculations on demand.

Detail requests fetch ten captured games at a time, newest first, then their
bounded paths from Neo4j. Source hashes must still match; otherwise the API asks
for a refreshed snapshot rather than presenting changed paths as old results.

Install (does not start work):
`python -m chessism_api.database.neo4j.ply_map_manage`

Checks: `tests.test_ply_map` (opt-in `PLY_MAP_TEST_CUDA=1`),
`tests.check_ply_map_postgres`, `tests.test_ply_map_neo4j`,
`tests.test_graph_explorer_http`, `test-ply-map-browser.mjs`, and
`test-common-positions-browser.mjs`. Database/CUDA checks use synthetic inputs
only. The real cohort is first built by the user's manual summary request.
