# Chessism V1 architectural refactoring

**Current checkpoint — 2026-11-19:** Stage A is formally ACCEPTED. The Stage B
cancellation correction is implemented and verified in `7783e54`, pending one
short independent re-review. See `docs/architecture/stage-b-cancellation-correction.md`.
Do not begin Stage C or D.

## Historical Stage A execution plan

**RESUMED — 2026-11-18, fast-track Stage A authorization.** Resume from
`b00885a`; preserve the existing A1/A2 and five A3.1 checkpoints. Source inventory
matches all 1,744 application/test inputs plus the documented handoff-only change.
Stage A remains incomplete and unaccepted.

## Active coordinated conversion — CUDA publication and dependent readers

Continuation authorization: `/home/jon/.codex/attachments/ec6637db-553b-4054-85c1-c8af30164a85/Pasted text.txt`.
Entry is clean `3f03ac3`; all 1,777 frozen export inputs match. Reuse its 197
verified checks, not another export gate. Trace: queued revisions -> immutable
bounded input snapshots -> existing CUDA kernels -> revision-checked staged
publication -> saved chart/research readers. Convert these paths together.

Add required CUDA/blunder/score/salience tables and invalidation to the empty
canonical initializer. Remove runtime schema installation/profile selection from
these consumers. Use native `(game_link,is_white)` persisted identities; any
salience GPU matrix index is run-local, mapped to that key, never a replacement
application identity. Preserve kernels, float64 arithmetic, unknown-vs-zero output,
manual-run policy, revision guards and atomic publication. Use existing guarded
transaction/cleanup boundaries; test rollback, cancellation and changed inputs.

Updated inventory (bounded scan, not another assessment):
- Converted: ingestion, analysis/receipts/recovery, canonical FEN/coverage, core
  analytics, homepage/player metadata/stats, appearance caches, structured export
  sources/lifecycle and restricted analytical reads. No runtime ask_db imports remain.
- Essential A3: coordinated CUDA publication/readers; remaining research/admin
  registry and deletion consumers; graph outbox/PostgreSQL sources; backup-source
  reads preserving the external recovery contract. Resolve actual saved CPU-baseline
  semantics before converting research comparisons; never relabel GPU counts as CPU.
- Removable A4: now-unreferenced ask_db implementation, obsolete profile adapters,
  DBInterface after its ingestion stragglers, historical installers/repair tooling,
  duplicate query branches and runtime DDL. Validate disabled FK triggers at startup.
- Defer: kernel/GPU redesign, optional query tuning, cloud-controller redesign and
  other Stage B/C work. No evidence justifies removing a user-visible feature.

Focused gate: fresh canonical PostgreSQL, revision/supersession and two-side
publication/read parity, invalidation, rollback/cleanup, existing pure calculations
and P1 cases. Short real-CUDA smoke only if locally available and isolated. Reserve
broad suite comparison and independent acceptance for the Stage A boundary.

## Fast-track remaining-work inventory and next interval

Authorization: `/home/jon/.codex/attachments/62531b35-6e44-4a9c-9888-8cb307dd2400/Pasted text.txt`.
This supersedes the pause and the earlier compatibility/procedural requirements.
No historical migration framework or importer. Focused gates after coherent changes;
one broad comparison and independent review at the Stage A boundary.
Process policy updated 2026-11-18: run checks expected under five minutes directly;
only hand off monitor.py for processes expected to exceed five minutes. No further
approval is needed for in-scope implementation/local commits. Public feature removal
or material API changes still require approval. Protect the original DB/checkpoint,
Redis, Neo4j and deployed resources. No Stage B/C, push, merge or deployment.

Bounded source assessment (2026-11-18): `ask_db.py` retains 2,970 lines and 11 runtime
importers; four files mention DBInterface; 76 files still call/import storage_for,
seven compact_for (overlapping counts, not distinct consumers). Already converted:
canonical imports/month accounting, FEN extraction/coverage, analysis selection and
receipts, tablebases/recovery, accuracy refresh, analysis/player/FEN/dashboard reads
and shared mode summaries. Do not repeat those implementations or their last gate.

Remaining groups, in implementation order:
1. Essential application analytics: time-control/player charts and moves/openings,
   result/length/activity queries; remaining FEN/scored-rating projection writers;
   home/player metadata snapshots, hours, distribution/catalog/explorer consumers.
   Preserve global one-game versus player two-side counting and UTC/calendar policy.
2. Required research/export/graph/admin consumers: 21 CUDA and 11 canonical-blunder
   modules use profile-dependent storage, alongside seven research modules. Preserve
   user-facing feature contracts; translate native side identities and add required
   canonical auxiliary tables. The costly duplication is schema selection, source
   queries and cache publication, not evidence that these features are obsolete.
3. A4: retire profile selection/adapters/views, DBInterface, ask_db, legacy startup
   DDL and historical-only migration tooling after caller conversion. Auth and cloud
   state stay. Month accounting/evaluation constraints were corrected in A2; disabled
   FK-trigger validation remains relevant and is still due at canonical startup.

No user-visible feature removal is proposed from this bounded assessment. Historical
repair/migration tools and permanent legacy adapter branches are removal candidates.
Do not delete feature code merely because it uses them. Current code still cannot
be run against the existing database; tests use only an empty disposable canonical DB.

**Completed logical change: canonical time-control analytics (117 checks passed).** Move eleven global/player
read operations from ask_db to `operations/time_control_views.py` and concrete SQL
in `database/postgresql/time_control_reads.py`. Share moves/openings/length/activity
queries where their meaning is identical; preserve distinct histogram and heatmap
response conventions. Extract the unchanged weighted Jenks calculation for its two
real consumers. Operations own one guarded read-only UTC snapshot; adapters require
that transaction. Preserve filters, pagination, result filters, empty/invalid inputs,
rating arithmetic and chart formulas. Set deterministic move/opening/chronology ties.
Hours/saved-snapshot conversion follows with its publisher, not a compatibility shim.
Update only actual callers; remove the old definitions rather than retaining aliases.

Focused verification: actual canonical PostgreSQL fixtures and independent aggregate
oracles for variants/zero games, self-play, rating filters, ties/pages, normalized
openings, UTC boundaries, empty inputs, snapshot consistency, read-only/error and
cancellation cleanup, and real routes. Retain relevant prior player-view and P1 tests.
The old /tmp test dependencies disappeared on restart; reuse an installed image with
verified dependencies, not application startup. Record exact test outcomes and new
runner evidence before a local commit. No broad suite until the stage boundary.

## Completed logical change — canonical scored-rating projection (148 checks passed)

Entry: time-control gate passed (117 checks), committed locally as `d67ad3a`.
Replace the scored-rating builder/reader/publisher in `ask_db.py` with one concrete
canonical adapter and one application operation. Preserve weighted Jenks groups,
position-occurrence buckets, duplicate-range behavior for small rating populations,
chart ordering and overview omission of the large chart column. Initialize the
required singleton in canonical DDL (version 7); reads must never rebuild it.
Refreshes own one guarded transaction: lock the singleton, stream one consistent
source query, calculate, publish, commit. Default READ COMMITTED makes a waiting
refresher read fresh source after the lock; stronger injected isolation may reject
conflicts and must propagate them. No compatibility alias or historical migration.
Update the actual callers/imports and the five existing cache/route tests; replace
only the obsolete request-time bootstrap assertion with missing-row failure and
empty-initializer checks. Fix int32-minimum ABS overflow in the source coverage
aggregate with an explicit bigint cast and regression for both refresh paths.

Focused gate: schema initializer/version/constraints, canonical coverage and new
rating projection PostgreSQL cases (empty/eligibility/rounding/buckets, single
snapshot, serialized refreshes, rollback/failure/cancellation and readonly routes),
existing rating tests, transaction cleanup and all 68 P1 checks. No full-suite run.
Legacy repair-delta conversion remains in the subsequent auxiliary-consumer chunk.

## Completed logical change — canonical global summary publication (194 checks passed)

Entry: scored-rating conversion passed 148 checks and committed as `8993f41`.
Replace remaining database/FEN/pipeline/scored-position refresh SQL in `ask_db.py`
with a concrete `summary_writes.py` adapter and `operations/global_summaries.py`.
Reuse canonical game counts in ingestion and existing canonical pipeline refresh.
Use initialized singletons, explicit operation-owned transactions, ordered database
then scored-summary locks, and common FEN aggregates for full refresh and new-score
deltas. Full FEN refresh publishes both projections from one statement snapshot;
no caller-supplied counts from a separate transaction and no cache-bootstrap fallback.

Move tablebase's post-commit scored-position delta into the score transaction via
its existing evaluation-count boundary, also used by local/cloud Stockfish writes.
This closes a demonstrated code-level refresh/delta double-count race without
changing results, leases, receipts, worker limits or progress. Remove the redundant
post-commit delta and unused legacy snapshot/delta functions. Preserve public count
meanings (database scored_fens is nonzero; scored_positions includes zero), bucket
thresholds, WDL/appearance arithmetic and pipeline stage semantics. Cast before ABS
in FEN ingestion as well, so canonical integer extremes remain usable.

Update actual imports/callers and obsolete internal tests with an explicit mapping
record. Cover real full/delta parity, concurrent refresh/publication/ingestion,
rollback/missing rows/cancellation, exact source counts, empty operations, replay,
filters and all P1 regressions. Include prior canonical evaluation, tablebase, FEN
and read fixtures; focused gate only. No live database or backup operations.

## Completed logical change — canonical homepage snapshot (190 checks passed)

Entry: global summaries passed 194 checks and committed locally as `71f231b`.
Add version-8 canonical home cache/source tables and statement-level structural
invalidation. Preserve official-title filtering, distinct FEN/game unions, two
half-units per game, both self-play sides, stale-cache visibility, live avatars,
all-mode game roster and five-minute queue debounce. Active games remain standard
and nonempty as in the other canonical analytics; score/processing/appearance
updates must not invalidate this structural cache.

Use one JSON player-contribution array in the singleton snapshot rather than a
separately deleted/reinserted roster table. This keeps old totals and contributions
intact after physical profile deletion, without a dangling authoritative player FK
or a cascading partial cache. Numeric IDs join live avatars; cached names/titles
belong to the snapshot. This is a bounded derived cache, not a storage profile or
compatibility layer. Only the homepage consumes the old roster table at runtime.

Move SQL into database/postgresql/home_summary.py; operations/home_summary.py owns
read/write transactions and presentation/queueing. Build the full cache in one SQL
statement snapshot under the existing nonblocking transaction advisory lock.
Publish once; record failure metadata in a separate guarded transaction, only if
its attempt predates no newer completed attempt. No page-time DDL/heavy rebuilds.
Drop the internal migration-only transaction_managed option; update its sole script
caller and explicit-factory test. Leave legacy startup/profile retirement to A4.

Verify old homepage contracts on a fresh canonical PostgreSQL corpus, plus trigger
rollback/column exclusions, eligibility changes, physical deletion retention,
snapshot publication races, failure metadata ordering, missing rows, read-only
requests, concurrency, cancellation/lock cleanup, schema version rejection and P1.
Run one focused gate. Player metadata snapshots/hours remain the next consumer group.

## Completed logical change — canonical player metadata snapshots and hours (193 checks passed)

Entry: homepage gate passed 190 checks; local commit `870c035`.
Keep the existing pure daily/combined calculations and seven snapshot sections.
Canonical version 9 adds ID-owned state, dirty UTC days, partitions and snapshots;
foreign keys cascade derived data on profile deletion. Source game/profile changes
invalidate transactionally, including eligibility, ownership, timezones and opponent
renames; score/FEN work does not. Preserve bounded daily rebuilds and old complete
snapshots until all dirty partitions are processed. Detect concurrent invalidation
at publication under REPEATABLE READ. Shared guarded transactions own all cleanup.

Use one concrete metadata adapter; remove storage-profile branches from this path.
Move hours/latest reads to the operation with canonical fallback inside one guarded
read snapshot. Update the route and catalog snapshot reader; keep public calculations,
feature flag and manual CUDA ownership. No new schema installer: the legacy worker
installer is retired in A4 with the remaining runtime initialization paths.
CUDA/frequency writers still require their separately planned A3.2 canonical conversion;
no compatibility layer will be added for them. Legacy cross-profile parity scripts
are mapping debt; keep their unrelated catalog/distribution contracts for later gates.

Verify canonical section values, bounded incremental repair, timezone/DST/self-play,
correction/delete/eligibility/ownership changes, rename/delete/FK integrity, concurrent
refresh/invalidation, late failure metadata, cancellation/rollback/lock release, readonly
hours/latest routes, manual ownership, schema initialization and relevant P1 checks.
Do not run broad suites or real application services for this focused change.

## Completed logical change — canonical game catalogs and rating distribution (138 checks passed)

Entry: player metadata gate passed 193 checks; local commit `2eec444`.
Replace profile-selected opponent/opening group queries, visible-opponent ratings
and exact own-rating histogram reads with one canonical SQL adapter. Preserve
standard/nonempty eligibility, self-play sides, unknown/invalid ratings, normalized
opening keys, selected-mode/color minimum groups, exact percentage bins, deterministic
ties, search, pagination and public route shapes. Reuse saved metadata sections.

Operations own one guarded readonly snapshot for existence, source counts and
visible-page enrichment; preserve bounded read slots, timeouts and Redis fallback.
Use numeric player identity in live Redis keys as well as saved catalog keys, so
name reuse cannot serve an earlier profile's counts. No account/auth contract changes.
Keep existing calculation/route tests; replace obsolete SQL-rewriter mock assertions
with adapter-call checks plus real canonical PostgreSQL source-oracle coverage.
Move the shared opening SQL expression to its concrete adapter and update the
existing chart membership import. No new schema or compatibility aliases.

Focused gate: catalog/live-vs-saved parity, every outcome-bin boundary, single-search
minimums, metadata eligibility and rating ties, identity/cache/error/cancellation
paths, real routes, prior metadata tests and relevant P1 checks. No full suite.
Explorer/hero/score-based analytics remain subsequent consumer conversions.

## Completed logical change — canonical chart and behavioural reads (156 checks; existing baseline debt)

Entry: catalogs/rating distribution passed 138 checks; local commit `479a062`.
Convert chart metadata drill-down plus behavioural activity, daily ratings and day
observations to one concrete canonical game-side reader. Keep public response shapes,
filter validation, opening normalization, two self-play sides, chart keyset ordering,
local calendar/DST policy, saved/manual snapshot semantics and dense live-rating gaps.
All-mode activity/day includes daily and unknown modes; chart/rating series includes
only bullet/blitz/rapid. Preserve standard/nonempty eligibility. Make same-instant
day-observation ties deterministic (white then black, consistent with last rating).

Files: operations/player_chart_games.py; first three behavioural readers in
operations/player_hero_analytics.py; database/postgresql/player_activity_reads.py;
tests/check_canonical_activity_postgres.py; tracking and canonical persistence docs.
No schema changes or new generic abstraction. Operations own the shared guarded read
transaction, including profile resolution and saved/live selection. Leave the shared
already-canonical profile lookup and remaining engine-dependent hero consumers for
their conversion; do not expand this chunk to rewrite unrelated measures.

Focused gate: independent PostgreSQL records for filters, pagination/ties, chart
membership, timezone windows/DST, sparse saved and dense live ratings, empty/missing
profiles, manual/stale snapshots, consistent reads under concurrent writes, database
errors/timeouts, repeated cancellation, and actual HTTP routes. Keep prior catalog,
metadata and applicable pure/timezone/P1 checks. No broad suite or live services.

Chart/activity verification note: the first run passed all 63 PostgreSQL checks.
Its workflow group includes three existing erroring time-filter methods (eight
error reports). Unchanged HEAD reproduces identical tracebacks and historical
approved identities. Keep those tests and require the exact exception details;
require all new checks and P1 checks to pass. Reuse the 63 passed checks only while
all source hashes match, then complete the remaining 93 checks. Do not repair the
legacy explorer mock in this chart/behavioural conversion.

## Completed logical change — canonical bounded FEN popups (159 checks passed)

Entry: chart/activity gate verified with exact baseline debt, commit `f8ddc09`.
Convert selected-FEN game pages and bounded score-matching examples to canonical
relationships. SQL belongs in database/postgresql/player_position_reads.py; retain
operations/player_position_games.py and player_score_position_page.py for validation,
cursors and output. The get_positions boundary in player_score_distribution.py owns
guarded read cleanup; other histogram/frequency/CUDA paths are unchanged.

Remove the redundant separately stored game-side membership check and nullable-date
cursor workaround: canonical games own both sides and require timestamps. Preserve
rare/common FEN strategies, selected player color/modes, distinct game membership,
date/link pagination and 30-game cap. Examples use native (game_link, n_move, is_white)
occurrence keys, never an invented sequence ID. Version the opaque example cursor
and distinguish a completed game from an actual move-zero association explicitly.
Retain 32-game/512-occurrence scan limits, 10 displayed examples, per-page FEN/owner-
side deduplication, score bounds/perspective and lazy board fetching. No new DDL.

Files: the two operation modules, get_positions boundary, new concrete adapter,
tests/test_chart_popup_queries.py, the two affected cases in
tests/test_player_score_distribution.py, tests/check_canonical_position_popups_postgres.py,
and tracking/docs. Map existing structural unit tests to the canonical adapter and
natural key while retaining every behavior/assertion; the unrelated GPU-query test
is unchanged. Map the legacy isolated position-game script's meaningful assertions
to canonical fixtures; nullable timestamps/missing sides cannot exist canonically.

Verification: rare/common page equality, self-play, repeated occurrences, structural
eligibility and FK constraints, scope-bound cursors, same-move plies and move zero,
sparse/empty batches, score categories/boundaries, cancellation/readonly/timeouts,
snapshot consistency and actual routes. Include prior FEN-view and chart/P1 checks.

Dependency discovered: accuracy/blunder charts consume CUDA-owned publication tables
and active-model state, not just CPU engine summaries. Keep those readers with their
A3.2 publisher conversion; do not replace unknown GPU counts with CPU counts or add
a compatibility layer merely to finish A3.1 first. Saved frequency publication has
the same dependency. No feature removal or new stage authorization is needed.

## Active authorization — Complete Stage A (A2–A4)

Source: `/home/jon/.codex/attachments/babb4ae9-7339-4464-b0fa-baf24184c1f2/Pasted text.txt`.
A1 was independently ACCEPTED WITH DOCUMENTED LIMITATIONS. Its commits are
`22a406c` and `35be4fc`; entry HEAD is `35be4fce19d692914df76904cb638b361351b346`.
Repository and branch are `/home/jon/Desktop/workshop/last_chessism` and
`refactor/p1-analysis-failure-propagation`. The entry working tree is clean and
all 1,699 protected application/test inputs match the accepted evidence.

The user authorizes A2, A3 and A4 as one stage, local commits after focused gates,
and one independent review at the end. No additional batch approvals are needed.
This overrides the skill's repeated sign-off and full-suite-per-chunk rules.
Existing API 3 failures/10 errors/276 skips, worker 7 skips, and four frontend
failures remain separate unresolved debt. No new failures/errors/skips are allowed.

## Ordered implementation chunks

### A2.1 — Canonical game imports and explicit month accounting
Entry: accepted A1, matching baseline and clean working tree (verified).
1. In `canonical/core.sql`, reject unscored timestamps without a source; bump the
   foundation version in `canonical/__init__.py` and update version assertions.
2. Add `database/postgresql/game_imports.py`: concrete bulk ingestion into canonical
   game/player/move/opening tables, exact duplicate/conflict handling, transaction
   guard, shared ingestion lock, and explicit standard/nonempty UTC month counts.
   Accept the caller's PostgreSQL connection; never start/commit/close its transaction.
3. Add an isolated PostgreSQL test fixture shared by schema/import suites and
   `tests/check_canonical_imports_postgres.py`. Cover mixed classifications, retained
   zero-move metadata, self-play once, duplicates/conflicts, rollback/retry,
   cancellation, concurrent importers, UTC boundaries and successfully fetched empty months.
4. Correct A1 synthetic month counts to represent existing active-game semantics.
   Record that these functions are not yet wired to runtime: A2.2/A2.3 integrate the
   complete write unit, including projections. Do not add another storage profile.
5. Run foundation/import/P1 checks on private PostgreSQL/Redis; review evidence,
   then commit locally `refactor: establish canonical game import transactions`.
Exit: all focused checks pass without skips; prior runtime remains unchanged.

### A2.2 — Canonical write relationships and application ingestion
A2.1 completed and verified in local commit `6039b34`.
This chunk has two focused gates: the complete game-import application transaction
(including side-key pending work), then FEN persistence/coverage consumers.
Define required auxiliary write tables in canonical SQL before use;
replace participant surrogate keys with `(game_link,is_white)` in side summaries
and pending work. Convert `ingestion_pipeline/game_importer.py`, `game_repository.py`,
`fen_repository.py`, `fen_batches.py`, `fen_core.py` consumers and corresponding
player/game projection writers. Keep formatter metadata for zero-move games,
explicitly classify analysis-eligible games, and preserve the ingestion public result.
Move shared transaction ownership to the application operation; database functions
accept its session/connection. The first gate adds `canonical/projections.sql`, `game_import_summaries.py`, and
`transactions.py`; switches `game_importer.py` / `jobs.py` / `months.py`; adds
`tests/check_canonical_ingestion_postgres.py` and focused cleanup regressions.
Map existing formatting/archival tests to canonical behavior without weakening them.
Test real complete imports, derived writes, failures,
repeated imports and cancellation. Commit only after the focused gate passes.

The game-import subgate passed all 149 checks and is committed as `4eee021`.
The FEN subgate converts `fen_bulk.py`, `fen_repository.py`, `fen_batches.py` and
`fen_orchestrator.py` to canonical storage. Add `database/postgresql/game_coverage.py`
for caller-owned coverage and pipeline totals; reuse it in `game_import_summaries.py`.
Define the consumed scored-position singleton in `canonical/projections.sql`, bump
schema version to 4, and reject versions 1–3. Remove the legacy orchestration branch;
its worker exports remain until A4's caller audit. Preserve bounded replay/COPY,
ordered locks, exact insertion deltas, evaluation/claim preservation and retry policy.
Application finalization commits coverage, flags and pipeline totals together.
Cancellation drains known workers before releasing claims. Add real PostgreSQL
FEN tests and focused coordinator regressions, retaining existing parser, progress,
automatic scheduling and P1 checks. Record the replaced compatibility coverage.

### A2.3 — Canonical evaluation writes and analysis selection
Depends on A2.2. Convert `fen_results.py`, `fen_summary_recovery.py`, tablebase writes,
`fen_claims.py`, cloud importer/reservation/input preparation, and selectors currently
in `ask_db.py` to cohesive persistence modules. Preserve selection order, fresh
snapshot rechecks, SKIP LOCKED, claim exclusion, receipt replay, and P1 ownership /
cleanup / error propagation. Keep accepted progress/ARQ mechanisms. Tests exercise
actual adapters against canonical PostgreSQL plus existing P1/Redis/ARQ regressions.
Commit verified work; record A2 evidence before A3.

A2.2 completed in `d51cd42` after 198 focused checks. A2.3 begins with the
selection/reservation boundary, followed by evaluation publication and recovery.
Extract selectors plus frozen-scope counts/completion into
`database/postgresql/analysis_selection.py`, removing their `ask_db.py` definitions.
Keep the accepted cleanup algorithm, session handoff, ordering and fresh-snapshot
claim recheck. Replace player-name/side compatibility columns with canonical numeric
relationships; qualify canonical tables in `fen_claims.py`. Reuse the frozen-game
candidate SQL in `cloud_analysis/game_fen_queue.py`; retain durable queue identity,
consumption/claim atomicity, skipped-lock recovery and page bounds. Use the shared
transaction/cleanup helpers for queue construction and failed acquisition.
Update direct imports in analysis/cloud/benchmark callers and corresponding test
patch targets, without changing P1 assertions. Add real PostgreSQL selector/queue
checks, retain all P1 regressions, and run the four existing queue contract tests.
Legacy opt-in database fixtures must be converted/mapped before the final A4 gate;
do not introduce new skips. Evaluation publication remains the following subgate.

Selection/reservation passed 118 checks and is committed as `a0a2af9`.
The next focused gate moves live Stockfish publication to
`database/postgresql/evaluation_results.py`: validate inputs before writes, require
an existing transaction, lock canonical FEN identities in order, update at most
1,000 FENs per statement, replace numeric continuations and increment totals in
that same unit. Remove the now-unused DBInterface evaluation method and injection
parameter; switch local/cloud/reviewed-repair imports and test patch targets.
Keep the backup continuation helper and backup/restore implementation unchanged.
Cloud import uses the shared cancellation-safe transaction owner; retain receipt
identity, replay checks, claim release and counts in the same commit. Add actual
canonical PostgreSQL publication and cloud receipt tests, including rollback,
concurrency, lost acknowledgments and repeated cancellation. Retain all 68 P1
checks and relevant formatting/identity/transaction unit assertions. Do not claim
summary recovery is converted: tablebase and durable outbox conversion follow as
the next A2.3 gate, before A3. No schema change is needed for this publication gate.

Stockfish publication/receipts passed 136 checks and are committed as `853c720`.
Next, convert the tablebase adapter in a separate focused gate. Move candidate
count/admission, streamed numeric-ID discovery, short reads and leases from
`operations/tablebase.py` / `tablebase_batches.py` into
`database/postgresql/tablebase_repository.py`. Keep only the numeric-ID temporary
spool, existing bounded discovery/probe/write sequence and existing ordering;
use canonical tables and a shared candidate predicate. Move the five-piece policy
constant to the pure evaluation contract so SQL and probing share it. Replace
profile-selected publication with the existing bounded COPY/set-based algorithm,
validate all rows, require a caller-owned transaction, verify identities/unscored
state/claims under locks, and reject partial updates. Application code keeps the
score/count commit. Use accepted guarded rollback/closure for leases, including
errors from reporting immediately after acquisition. Retire tablebase_batches
only after updating all callers/tests. Preserve probe algorithms/process budgets,
public ingestion result and current post-commit projection calls. Add actual
canonical PostgreSQL candidate/write/race/cancellation checks, retain tablebase
behavior tests and P1 regressions. Durable projection recovery follows this gate;
no schema change or Stage B work is included here.

Tablebase persistence passed 131 checks and is committed as `809e112`.
The final A2.3 gate defines `canonical/recovery.sql` and schema version 5:
a revisioned per-game recovery queue and statement triggers on relevant FEN,
association and game-participant changes. Preserve cloud claim exclusion, skip
metadata-only FEN writes before association access, retain recovery identities
for deleted games, and use actual game owners instead of triggers on the derived
`game_player` view. Reuse the existing sorted/coalescing queue semantics. Canonical
sources are constrained to stockfish/tablebase (or unscored/unavailable), so there
is no scored unknown-provenance relabeling path; omit the historical source-only
queue branch and its CUDA-only dependency. Preserve recovery report fields with
source_only=0. Add `database/postgresql/summary_refresh.py` for bounded queue reads
and exact-revision acknowledgment/retry updates. Convert recover_batch to these
functions with operation-owned guarded transactions; callback success is required
before acknowledgment. Keep the existing default summary callback until its A3
conversion, clearly marking that end-to-end reader integration remains pending.
Legacy installer/upgrade entry points stay isolated for their remaining A3/A4
callers and are not invoked by canonical initialization or recovery. Verify schema
version/repeat/catalog behavior, all converted writer paths affected by triggers,
metadata fast paths, claim/rollback/owner/deletion behavior, races, backoff and
cancellation. Reuse absolute canonical coverage as the integration callback;
no CUDA, production database, historical relabeling or Stage B work.

A2.3 persistence gates passed; durable recovery passed all 237 checks. Default
summary callback integration remains the first A3 concern.

### A3.1 — Canonical game/player queries and summary readers
Depends on A2.3. Convert routers and operations using game/player queries, navigation,
rankings, pagination, `home_summary.py`, `player_*` summaries and `common_positions.py`.
Derive UTC calendar/ratings explicitly. Consolidate genuinely shared analytics SQL;
keep global/player/historical distinctions. Move cohesive remaining functions from
`ask_db.py`; convert side-specific keys through all callers. Verify ties, stable
pagination, empty output, dates/months, self-play and repeated FENs.

First A3.1 subgate: complete the default durable recovery callback. Define the
side-keyed `game_player_engine_summary` and manual CUDA control singleton in
canonical projections, bump to version 6 and reject versions 1–5. Extract unchanged
accuracy constants/pure functions to `chessism_api/accuracy.py`; existing CPU/GPU
and research consumers import that policy directly. Move the existing CPU SQL
calculation from `operations/player_game_scores.py` to the concrete caller-owned
`database/postgresql/engine_summary.py`, using native boolean sides and canonical
FEN/coverage relationships; preserve every numerical formula. Keep the application
operation as transaction owner, preserve manual deferral and lock its control row
against concurrent activation. Refresh locks games in order before a fresh read.
Move full/affected-FEN coverage refreshes from `ask_db.py` to
`operations/game_analysis_summary.py`, reuse `game_coverage`, preserve absolute
replay and existing coverage-before-player commit boundaries. Remove the obsolete
strict switch: the canonical path always propagates failures. Update all direct
callers/imports/mock targets, including backup refresh imports only; no backup logic
changes. Bind recovery's default callback to its supplied session factory, with
no transaction held across the callback. Preserve injected callback semantics
apart from removal of the redundant strict keyword. Do not convert remaining
readers/GPU publication or delete the old coverage adapter before their A3/A4 gates.

Verify actual default recovery, partial summary failure/backoff, repeated replay,
side identities/self-play/owner changes, eligible/incomplete/zero/variant games,
scoped vs full refresh, empty selections, repeated FENs and numerical equivalence
against the existing independent scalar accuracy oracle. Cover transaction rollback,
query errors, concurrent overlapping refresh, manual deferral/activation, cancellation
and no connection leaks. Re-run affected canonical schema/writer/recovery suites
and accepted P1 regressions; preserve baseline failures and old opt-in fixture
conversion debt. Commit only after the focused gate passes.

The first A3.1 subgate passed all 285 checks; remaining game/player readers follow.

Next A3.1 subgate: canonical analysis pages and game selection previews. Move
`get_scored_game_analysis_overview`, `get_scored_game_analysis_page`,
`get_player_game_analysis_scope`, `preview_player_games_for_analysis` and their
pure fair-month sampler out of `ask_db.py` into `operations/game_analysis_views.py`.
Add `database/postgresql/game_analysis_reads.py` for concrete canonical SQL in
caller-owned transactions. Reuse one player/eligibility predicate for scope and
selection, counting self-play once per game. Preserve canonical normalized-page
ordering (position count then link), page bounds and response fields; calculate
score aggregates only for the visible page. Preserve latest/oldest/fair-range
ordering, date boundaries, occurrence-vs-distinct FEN counts, tablebase eligibility,
and the existing prepared-plan/digest/chunk contract without adding claim exclusion
to ordinary preview counts. Claims are still rechecked at launch; prepared preview
reports claimed counts separately. Keep the fair sampler's numerical algorithm.

Operations own guarded transactions; multi-query views/previews use repeatable
read, and ordinary views are explicitly read-only. Set UTC for reproducible timestamps/month sampling. Prepared preview
uses the existing `prepared_selection.build` within that same owned transaction,
so partial chunks never commit. Update routers and direct imports/mock targets;
no route/result contract changes, legacy aliases or universal query framework.
Verify real canonical overview/pages, ties/pagination/empty output, self-play and
variant/zero exclusions, UTC boundaries, ordering/fair sampling, shared/repeated
FENs, prepared commits/rollback/cancellation, read failures and snapshot consistency.
Include reproducible synthetic page plans, existing route/sampling contracts and
P1 checks. Schema version remains 6; existing schema/writer verification is reused
unless a new change affects it. Older opt-in fixture mappings remain A4 work.

Analysis pages and previews passed all 135 checks; player/global readers remain.

Next A3.1 subgate: player navigation and basic game statistics. Convert the
player coverage/latest-rating, neighbors/title-group navigation, performance,
averages/termination/opening statistics, recent games, game summary, mode games
and mode-stat functions in `ask_db.py` to `operations/player_game_views.py` and
`database/postgresql/player_game_reads.py`. Move the time-control results wrapper
with its existing invalid-mode response. Keep hours/saved snapshots, charts and
other analytics for their following gates. Preserve old public fields, null-vs-zero
behavior and numeric types. A fixed feature-specific side projection derives
rating/result/endings from canonical game/player relationships; no field catalog,
query rewriter, schema views or runtime profile. Filter standard/nonempty games
explicitly because the previous active tables excluded variants/zero-move rows.
Keep player-perspective self-play counts (both sides), distinct from game selection.
Navigation retains tracked/active/title predicates and alphabetical wraparound.

Use a private operation context to share guarded read-only repeatable-read/UTC
setup across these related views; persistence functions require its transaction.
Consolidate duplicate game response formatting. Preserve page bounds; provide
stable date/link/side ordering. Previously unspecified same-timestamp/side ties
in latest-rating and mode ratings use black-before-white, matching recent-game
color ordering; date is always primary. Opening frequency ties use ECO ascending.
Update callers, route imports and reference helpers; no caller aliases or public
endpoint changes. Old opt-in layout fixtures remain scheduled for A4 mapping.

Verify canonical player/profile and game fixtures: empty/missing/deleted/untracked
players, title groups and wrapping, variants/zero games, both self-play sides,
coverage occurrences, rating date-vs-link priority, ties and pagination, UTC dates,
results/modes/unknown modes, raw empty aggregate nulls, concurrent snapshot reads,
read-only/query errors and repeated cancellation cleanup. Compare numeric/result
aggregates to a simple independent in-memory fixture oracle; exercise actual routes.
Retain relevant existing contracts and P1 checks, reuse unchanged schema evidence.
Version remains 6. No hours/snapshot, CUDA, production data or Stage B work.

Player navigation/basic statistics passed all 123 checks; remaining analytics follow.

Next A3.1 subgate: canonical FEN browsing and aggregate reads. Move the six
FEN list/page/count/overview operations from ask_db to `operations/fen_views.py`;
add `database/postgresql/fen_reads.py` for caller-owned SQL. Read initialized
canonical summary singletons directly: HTTP reads never silently rebuild missing
projections. Missing required rows raise; ordinary empty initialized databases
retain zero/null response contracts. Extract unchanged scored-summary arithmetic
into pure `chessism_api/scored_positions.py` for both current refresh and read callers.
Preserve sorting/filter/page bounds and response types; make previously unspecified
frequency ties deterministic by FEN. The top-list route must format unscored
NULL values without a TypeError; scored text remains identical. Widen score arithmetic before ABS/multiplication
because canonical evaluations permit full int32 values. SQL failures propagate.

Convert `fen_explorer.py` sampled reads, examples and distinct-game drilldown to the
same adapter: numeric participants/FENs, boolean sides, unique moves and UTC dates.
Retain three bounded block samples, sample size, account/token/TTL behavior,
10-game keyset pages, 2,000-occurrence cap, verified move examples and timeouts.
Select examples deterministically. Do not add full-table random sorting or change
shared histogram/common-position cache machinery. Share read-only repeatable-read
setup in `transactions.read_transaction`, reusing its unchanged accepted write/
rollback/close mechanism. Replace the existing identical player-view setup with
that helper and repeat its PostgreSQL tests; prepared analysis transactions remain
unchanged. Verify data/public routes, all scored sort modes including zero/mate/
tablebase/int32 edge values, live committed summaries, ties and boundaries, game
identity/participants/UTC, repeated positions/self-play, snapshot races, errors,
read-only enforcement and repeated cancellation. Keep existing explorer/cache
contracts and P1 checks. Schema stays version 6; broader fixture mappings remain A4.

FEN browsing passed all 136 checks; remaining analytics follow.

Next A3.1 subgate: dashboard counts and maintained mode rankings. Move core
database/time-control counts, pipeline overview, remaining-extraction counts and
mode rankings to `operations/dashboard_views.py` and a concrete
`database/postgresql/dashboard_reads.py`. Read initialized singletons without HTTP
rebuilds; missing required rows raise. Main-character game counts count each eligible
game once, including self-play; rankings retain side-perspective totals and active
profile/title filters before LIMIT. Preserve exact public fields and UTC dates.
Pending extraction counts include in-progress games but exclude retained variants,
custom starts and zero-move records, matching canonical analysis eligibility.

Consolidate full/scoped mode-summary refresh with the existing ingestion calculation:
move that SQL from `game_import_summaries.py` into `database/postgresql/mode_summary.py`,
require a caller-owned transaction and serialize with the established ingestion
commit lock. `operations/mode_summary.py` owns standalone refresh transactions at
READ COMMITTED, so waiting on an importer cannot publish an older snapshot. Keep
the A2 calculation, including date/link/white-side rating ties; full scope is None,
empty explicit scope is a no-op. Delete/reinsert and returned counts are atomic;
errors and repeated cancellation finish cleanup. Remove duplicate legacy mode
writers and lazy ensure logic after updating callers. The obsolete ensure call in `main.py` is removed; other startup code only has
its moved refresh import updated. Profile retirement remains A4. Reference tools/test imports move,
with canonical fixture mapping retained as A4 debt; no production operation runs.

Verify full/scoped equivalence with actual ingestion, independent mode/statistics
oracles, ties, profile/title filters, empty/missing singleton behavior, UTC dates,
rollback/failure/repeated cancellation, concurrent refreshes and refresh waiting on
an actual importer. Repeat canonical ingestion and view regressions plus all P1
checks. Other summary writers, home snapshots and analytics remain following gates;
no schema, ingestion response, backup logic, deployment or Stage B change.

### A3.2 — Research, exports, graph, administration and synthetic plans
Depends on A3.1. Define remaining canonical auxiliary structures and convert
`canonical_blunders/`, `cuda_summaries/`, `research_algorithms/`, `exports/`,
graph database consumers, deletion/admin and snapshot consumers. Preserve feature
contracts and existing backup/restore behavior; no real backup or restore operation.
Replace profile-dependent queries with direct canonical SQL and native keys.
Add representative equivalence tests and reproducible query-plan/cardinality checks;
report measured scale honestly. Commit after focused verification and record A3 evidence.

### A4 — Retire old implementations and enable fail-closed startup
Depends on verified A2/A3 consumers. Replace `runtime.py` / `engine.py` storage-profile
selection and startup DDL with explicit canonical version/catalog validation.
Extend the fingerprint to detect disabled FK enforcement before using this startup
boundary, with a focused regression. Remove `DBInterface`, `ask_db.py`, old
`relational_storage.py` / `compact_storage.py` / `relational_reads.py`, compatibility
views and obsolete initialization/migration paths only after dependency searches
show no required runtime caller remains. Preserve supporting auth/cloud structures.
Map removed compatibility tests to canonical behavioral replacements; retain the
coverage and record the mapping. No generic repository/query framework.

Final gate: complete backend/worker suites with exact baseline comparison; canonical
initialization and representative application workflows; transactions, locking,
repeated cancellation and recovery checks; retired-import/DDL/source-size audit;
owned-resource cleanup. Final local commit(s), accurate evidence report, then stop
for ONE independent Stage A acceptance review. No claim of acceptance beforehand.

## Safety and handoff

Only isolated disposable PostgreSQL/Redis resources. Preserve existing database,
recovery checkpoint, ignored/untracked files, Neo4j and deployed services. No push,
merge, deploy, external side effects, Stage B or unrelated repairs. A blocking public
contract change or material scope expansion requires user input, not a workaround.

Every long process runs detached with a working `/tmp/.../monitor.py`, one display
refreshed every 120 seconds, elapsed/start/stage/status, honest ETA, bounded resources
and cleanup. Confirm startup and return immediately; resume only on user completion.
No polling or waiting by the agent. Commit verified changes after evidence review,
not unverified work merely to satisfy the skill's handoff template.

## Historical P1 #1 plan: Analysis selection failures

Current work: the independent review requested two blocking corrections. The
original implementation record below remains historical, not formal acceptance.
See the correction chunks at the end of this plan. No push, merge or deployment.

Source: the approved design in this conversation and the implementation instructions at
`/home/jon/.codex/attachments/2eca9586-fb0c-4f10-9c69-f9a72f07e4d5/Pasted text.txt`.

Created: 2026-11-09. Baseline: `4f77d15df240a06efc7904496af7ec5e7a7b89cb`.

Target: a database selection failure fails the current local analysis attempt,
releases its resources, and cannot be mistaken for successful exhaustion.

## Authorization and execution

The user explicitly authorized implementation of P1 #1 and the attached design,
including removing unnecessary error-suppression compatibility. The ordered
chunks below implement that authorization. Do not address P1 #2 or other findings.

For every long-running verification process, start it detached, record its identity,
give the user one command for the temporary status monitor, and end the turn. It
must refresh in place every 120 seconds, show start time and elapsed time, and remain
open until Ctrl+C, including after the process finishes (user's latest preference).
Give an evidence-based duration estimate where available; never invent an ETA.
Do not ask the user to watch application/system logs. Resume only after the user
reports completion. Do not poll or wait for these jobs. Short inspections and
focused deterministic checks may finish within the tool's initial response.

Use branch `refactor/p1-analysis-failure-propagation`. Preserve unrelated changes.
Commit verified chunks locally. No deployment, schema migration, production data
access, or changes to engine retry limits/timeouts are authorized by this plan.

## Decisions and invariants

- The caller audit found no legitimate consumer of suppressed selection errors.
  Remove `raise_errors` from all three selectors and their callers.
- Keep the existing `fetch_batch(limit)` callback and session/FEN tuple contract.
  `(None, None)` is reserved for successful empty selections, including an empty
  frozen-game input. Empty means no currently eligible unlocked work, not that
  every database position is analyzed.
- Keep query text, ordering, deduplication, limits, `FOR UPDATE SKIP LOCKED`, cloud
  claim exclusion, and the fresh-snapshot eligibility recheck unchanged.
- The selector owns its session until a successful handoff. Failure/cancellation
  must attempt rollback and always attempt close, including rollback failure.
  Cleanup errors must not replace an already active selection error/cancellation.
- The worker owns handed-off sessions. Existing committed batches stay committed;
  uncommitted work is released. Preserve progress count meanings and offsets.
- Acquisition errors escape processing retries. The coordinator cancels and
  awaits siblings, publishes failure best-effort, then re-raises the original
  exception. External cancellation remains cancellation.
- A failed query does not mean a known number of FENs failed. Do not fabricate
  failed-position counts or reset valid progress when reporting failure.
- Final `failed`/`complete` progress survives late Stockfish telemetry. An explicit
  reset permits another attempt; `loop_complete` is not a final terminal phase.
- Preserve API/queue names and payloads, successful return shapes, Redis keys/TTLs,
  cloud pause/recovery, tablebase behavior, and processing-error retry policies.
- ARQ may expose `status=complete` for a retained failed result. Assert
  `result.success == false` and, when available, `progress.phase == failed`.

## Preflight

- [x] Inspect working tree: clean at baseline commit.
- [x] Audit all selector definitions, direct callers, and injected local callbacks.
- [x] Confirm no configured Python lint/type-check commands or configuration.
- [x] Inspect the existing runtime image: includes API test dependencies absent
  from `.venv-cloud` (PyArrow, Neo4j, and pglast).
- [x] Create the working branch and commit these tracking documents.
- [x] Run the complete main Python offline unittest discovery in the corrected
  image, with repository source read-only, no network, and dummy database settings.
- [x] Run batch-worker offline unittest discovery in the same isolated Python 3.11
  environment. Record results separately because the worker requires Python 3.11+.
- [x] Establish available isolated Redis/PostgreSQL verification infrastructure.
  Never borrow application service connections or persistent application volumes.

## Verification commands

### Baseline finding and approved gate exception

The first baseline run finished with 1,364 tests, 3 failures, 18 errors and 276
skips in 52.923 test seconds (56.600 container seconds). No application or test
changes preceded it. This run does not establish a passing baseline.

Eight error reports are attributable to the test environment: two missing dummy
auth settings, one missing worker-test import path, one missing Google dependency,
and four uses of `TestCase.enterContext` unsupported by Python 3.10.

Three assertion failures and ten error reports expose existing test/code
mismatches. Details and test identities are recorded in the manifest and log.
They must not be silently fixed as part of P1 #1.

The user approved this exception with "ok proceed" after reviewing the baseline:
1. Correct the isolated test environment (Python 3.11+, all declared dependencies,
   dummy auth settings, and worker-test fixture path) and rerun the unchanged
   code to establish the exact remaining failures.
2. Keep unrelated source and tests unchanged. Record exact baseline failures;
   do not skip them or describe the full suite as passing.
3. Require every new P1 #1 regression and relevant previously passing test to
   pass. Require the full suites to introduce no new failures/errors and no new
   skips; investigate changes in baseline failures individually.
4. Retain isolated Redis/ARQ and PostgreSQL checks, scope review and all other
   chunk gates. Any unexpected P1-related baseline defect is investigated before
   proceeding; this is not a blanket waiver for analysis failures.

The skill's entry gate says "If not: STOP. Fix or escalate to user." That escalation
has been resolved by the user's explicit approval above; do not request it again.
The approved comparison against the corrected baseline replaces the requirement
for a globally green suite at the entry and chunk gates. Scope remains P1 #1.

### Corrected isolated environment and temporary monitoring

Use the existing `chessism-cloud-controller:latest` image (Python 3.11.17), with
Neo4j 6.3.0 and pglast 5.9 installed into a disposable dependency directory.
The package index no longer serves pglast 5.9, although the existing API image
contains it. Build exactly that version for Python 3.11 from upstream commit
`b3fc527daeb113fb1ee6490ce1000d8e49c29cc4` (release `v5.9`) and its pinned parser
submodule. The temporary dependency build container installs its own compiler
into its disposable filesystem and preserves the resulting wheel for reuse.
Only dependency installation has network access; it mounts no checkout or
credentials. Both test containers have `--network none`, read-only source and
root filesystem, dummy auth/database settings, and writable temporary paths.
Run test containers under the host UID/GID; the dependency builder returns wheel
and dependency ownership to that user so artifacts can be deleted afterward.
No Docker socket or production volumes enter the containers.

The temporary program is `/tmp/chessism-p1-verification/monitor.py`. It provides
a detached `--run` mode, instrumented unittest discovery via `--suite main` or
`--suite worker`, and a default console monitor. It records atomic status updates,
current test, elapsed time and structured results. Monitoring defaults to every
120 seconds, clears the previous terminal display and stays open after completion;
Ctrl+C stops only the monitor. ETA is shown
for the API stage based on its previous 53-second run, with runtime-change caveat;
other stages/overall ETA remain unavailable without evidence.

Monitoring command: `python3 /tmp/chessism-p1-verification/monitor.py`.

The wrapper discovers and executes the same tests as these commands (no filters
or skips added), with working directory `/workspace` for the main suite:

```sh
python -B -m unittest discover -s tests -p 'test*.py' -v
```

Mount this checkout read-only at `/workspace`; set
`PYTHONPATH=/test-deps:/workspace:/workspace/stockfish-batch-worker:/workspace/stockfish-service:/workspace/tests:/workspace/stockfish-batch-worker/tests`,
`DATABASE_URL=postgresql+asyncpg://test:test@127.0.0.1:1/test`, and
`PYTHONDONTWRITEBYTECODE=1`. Hide the checkout's `.env` using a read-only `/dev/null`
mount. Run with `--network none --read-only`, bounded resources and writable tmpfs
at `/tmp`. Do not enable database, Neo4j, CUDA, or engine integration opt-ins in
this offline suite. Any unavailable/opt-in tests must be reported as unverified,
not counted as passing integration checks.

Worker offline suite, from `/workspace/stockfish-batch-worker/`, in the same image:

```sh
python -B -m unittest discover -s tests -p 'test*.py' -v
```

Focused tests during implementation:

```sh
python -B -m unittest discover -s tests -p 'test_analysis_failure*.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_terminal_progress*.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_scheduling.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_batch_limits.py' -v
python -B -m unittest discover -s tests -p 'test_pipeline_helpers.py' -v
```

Use an isolated Redis for Lua and ARQ tests, with uniquely named keys/queues and
automatic cleanup. Explicitly supply that test connection; no fallback to the
application Redis. Use isolated PostgreSQL for existing cloud-claim/selection
checks when available. Record exact launch commands and results in the manifest.

Run the complete relevant offline suites at each chunk gate; run targeted Redis
and PostgreSQL regressions covering changed contracts. Run `git diff --check`
and compile changed Python sources in memory. No Python type checker/linter is
configured; record those checks as not applicable, not as executed passes.

## Chunk 1: Keep terminal progress authoritative

Depends on: preflight meeting the approved baseline-comparison gate.

Files:
- `stockfish-service/routers/analysis.py`
- New `tests/test_analysis_terminal_progress.py`
- New `tests/analysis_failure_test_support.py`, only for isolated test connections
  and loading the Stockfish module without colliding with application routers.
- The three tracking documents.

Steps:
1. Add tests executing the actual Stockfish Lua script against isolated Redis.
   Show that late updates overwrite `failed` and final `complete` before the fix.
2. Move the terminal check ahead of all progress-set and payload mutations. Return
   the existing processed count without altering a terminal payload or its sets.
3. Cover ordinary progress, monotonic counts, intermediate `loop_complete`, and
   an actual `_reset_job_progress` followed by successful new progress.
4. Run focused and complete verification gates, record red/green evidence, and
   review that the engine execution/retry code is untouched.

Exit: terminal updates are atomic no-ops, resets work, existing suites pass.

Commit: `fix: preserve terminal analysis progress against late engine updates`

## Chunk 2: Propagate selection failures and finalize failed jobs

Depends on: verified and committed chunk 1.

Implementation files:
- `chessism_api/database/postgresql/ask_db.py`
- `chessism_api/operations/analysis.py`

Necessary caller updates (remove the obsolete keyword only):
- `chessism_api/operations/cloud_analysis/controller.py`
- `chessism_api/operations/cloud_analysis/input_preparation.py`
- `chessism_api/operations/cloud_analysis/work_units_controller.py`
- `stockfish-batch-worker/benchmark_vm16/large_database.py`
- `tests/test_cloud_analysis_database.py`
- `tests/test_cloud_sequences_database.py`
- `tests/test_cloud_work_units_database.py`
- `tests/check_cloud_game_queue_postgres.py`

Regression files:
- New `tests/test_analysis_failure_selection.py`
- New `tests/test_analysis_failure_jobs.py`
- New `tests/test_analysis_failure_arq.py`
- `tests/analysis_failure_test_support.py`
- `tests/test_analysis_batch_limits.py` only if its callback assertion needs to
  express behavior; keep it unchanged if the callback identity remains valid.
- The three tracking documents.

Steps:
1. Add deterministic regression tests and record failures on the unmodified
   selector/coordinator code. Bound failing-path tests so the known retry defect
   cannot hang the test run.
2. Remove the `raise_errors` parameter and suppression from the three selectors.
   Ensure failure/cancellation cleanup always attempts close even if rollback
   fails; preserve the original active exception. Use explicit try/finally or one
   small shared private cleanup function in `ask_db.py` if required to avoid
   repeating the same cleanup policy. Do not alter SQL or introduce a new layer.
3. Remove the obsolete keyword at every audited caller, including benchmark and
   integration-test callers. Search the repository again for remaining uses.
4. Move acquisition outside the broad processing retry handler in `_worker`.
   Keep successful batch execution, reservation limits and empty behavior intact.
5. Centralize fatal progress publication after sibling cancellation/cleanup in
   `_run_analysis_job`. Relocate the existing engine-request fatal publication
   to that coordinator while keeping its propagation and retry policy unchanged.
   Preserve the original exception if cleanup/progress reporting encounters
   another error. Do not translate external cancellation into database failure.
6. Test real callbacks from global, player, all/player loop, and frozen-game entry
   points. A failed pass must prevent later passes, cooldowns and success finalizers.
7. Test first-batch failure, failure after a committed batch, offsets, selectors
   cancelled before handoff, workers cancelled after handoff, rollback failure,
   progress-write failure, and ordinary empty completion. Use asyncio events for
   concurrency rather than real sleeps. Preserve processing-error behavior.
8. Use real isolated ARQ/Redis execution with mocked database acquisition to assert
   unsuccessful stored results and failed progress. Re-run terminal/reset tests.
9. Run full verification gates and existing isolated PostgreSQL locking/claim
   tests when available. Record any unavailable integration coverage accurately.

Exit: all required failure paths propagate without successful completion, all
owned resources are released, required regressions pass, and invariant checks
show no changes to queries, queue/API contracts, or P1 #2 policies.

Commit: `fix: fail local analysis when database batch selection fails`

## Postflight and handoff

- Run final verification (reuse an unchanged final chunk gate unless new changes
  or unresolved concerns require repeating it).
- Review diffs and compare the result against every required scenario/invariant.
- Update manifest/log, recording commit SHAs, exact results and any limitations.
- Stop after P1 #1. Report root cause, decisions, simplifications, files changed,
  red/green evidence, validation gaps, risks and scope compliance.
- Before ending any turn for a long-running check, record current progress and
  the monitoring command. Do not claim completion while verification is pending.

## Completion verification

- [x] Both implementation chunks meet the approved verification gate.
- [x] Thirty new regressions pass, including real Redis/Lua and ARQ execution.
- [x] Six existing isolated PostgreSQL locking/claim/recheck tests pass with no skips.
- [x] Full API suite: 1,425 tests, exactly the existing three failures and ten
  errors, 276 unchanged skips. No new failure/error/skip identities.
- [x] Worker suite: 287 tests, zero failures/errors, seven unchanged skips.
- [x] Query construction, ordering, locks, public entry points and engine request
  policies verified unchanged; required caller edits only remove `raise_errors`.
- [x] Changed Python sources compile; whitespace checks pass; no new TODO/FIXME.
- [x] Test services stopped. No deployment or production data/schema changes.
- [x] Final full-suite results reused because subsequent edits affect only the
  temporary PostgreSQL harness and tracking documents, not application/tests.

Remaining validation limits: production/GCP and real Stockfish execution were
not exercised. Opt-in tests outside the six selected PostgreSQL checks remain
skipped. The unrelated baseline failures remain as explicitly approved. There is
no configured Python linter/type checker. Redis progress publication remains
best-effort; ARQ's stored unsuccessful result is authoritative if Redis reporting
is unavailable. Engine retry limits/timeouts remain the separate P1 #2 concern.

## Independent-review corrections

Source: `/home/jon/.codex/attachments/c229c4e7-89a8-479c-892b-137a8cb77a95/Pasted text.txt`.
The user explicitly authorized these two focused corrections, in this order,
with separate local commits. This is the implementation plan for that request;
no additional scope or approval gate is being introduced. Keep the existing
baseline exception and long-process monitoring/handoff instructions above.

Entry evidence: unchanged application source since the verified original gate;
the independent review checked matching archived baseline failures and reran
the 20 selector/orchestration tests successfully. Rerun those focused tests at
correction entry. Full API/worker and isolated Redis/ARQ gates follow each chunk.
The six PostgreSQL checks remain part of final correction verification.

### Chunk 3: Give the coordinator sole ownership of cancellation

Depends on: completed chunks 1 and 2 and the approved baseline-comparison gate.

Files: `chessism_api/operations/analysis.py`,
`tests/test_analysis_failure_jobs.py`, and the three tracking documents.

Steps:
1. Add deterministic two-worker cancellation regressions, with staggered closes,
   successful/failed rollback and repeated external cancellation while draining.
   Use events and observed task completion, not timing-dependent sleeps. Record
   failing assertions against the unmodified coordinator.
2. Shield the initial worker gather so external cancellation reaches the
   coordinator, which cancels unfinished workers once. Retain the worker group
   and drain all workers before propagating the original error/cancellation.
   Shield the drain from later cancellation requests and continue awaiting that
   same drain; do not re-cancel workers. Keep this local to `_run_analysis_job`.
3. Keep failure publication exclusive to ordinary fatal exceptions, after
   cleanup. Preserve `CancelledError` and committed counters/results. Do not
   change selection, processing retries or timeouts. Use Python 3.10-compatible
   asyncio APIs, matching the application's Dockerfile.
4. Run focused tests, source compilation and whitespace checks, then the complete
   API/Redis/ARQ and worker suites against the exact approved baseline. Start the
   long gate detached, provide the existing 120-second monitor and end the turn.
5. Review saved results after the user's completion report and commit only this
   correction and its regression/record changes.

Exit: cleanup completes before cancellation escapes, even with failed rollback
and additional cancellation while draining; original P1 regressions stay green;
no new full-suite failure/error/skip identities.

Commit: `fix: drain analysis workers without repeated cancellation`

### Chunk 4: Scope progress updates to an execution attempt

Depends on: verified and committed chunk 3.

Files: `chessism_api/operations/analysis.py`,
`stockfish-service/routers/analysis.py`, `tests/test_analysis_terminal_progress.py`,
`tests/test_analysis_failure_jobs.py`, `tests/test_analysis_failure_arq.py`,
`tests/analysis_failure_test_support.py`; update `tests/test_pipeline_helpers.py`
and `tests/test_analysis_batch_limits.py` only where existing progress request
or mock expectations must express the new attempt contract. Three tracking files.

Steps:
1. Add isolated Redis regressions delivering old-attempt updates immediately
   after reset, after new progress, and after completion. Require zero mutation
   of the active payload, counters, FEN sets and expiry on rejected updates.
   Record failures against the current job-ID-only implementation.
2. Generate a fresh opaque attempt ID at each top-level reset. Atomically install
   that active ID and clear prior progress/deduplication state. Store it under a
   job-scoped Redis attempt key with the existing retention policy; include the
   attempt ID in new progress payloads. No delete-then-establish race.
3. Pass the same attempt explicitly through `_run_analysis_job`, all loop/frozen
   passes, coordinator progress writes, `_call_engine_service`, and Stockfish's
   `AnalysisRequest`/`_write_progress`. Live analysis without progress remains
   supported; progress-enabled requests require both job and attempt identity.
4. Both Lua update scripts must reject missing/mismatched active attempts before
   any mutation; Stockfish must also retain the terminal-phase guard. Never infer
   an incoming request's attempt from the current Redis value. Reset replaces the
   active identity; successful updates preserve its TTL alongside progress.
5. Test valid current-attempt updates, explicit reset uniqueness, current terminal
   protection, loop reuse, stale coordinator writes, missing identity and expired
   attempt state. Failure to publish/reset Redis remains best-effort and cannot
   change the database/ARQ result; later writes must not resurrect old attempts.
6. Rerun focused failure/ARQ/terminal and affected request-contract tests, then
   full API/worker suites and six existing isolated PostgreSQL invariants. Use
   the established monitoring/handoff workflow for long verification.
7. Review diffs for scope and unchanged selection/transaction/retry policies;
   record results, commit locally, and stop for the user's independent review.

Exit: only the current attempt can mutate progress, resets establish identity
atomically, terminal guards and all original P1 guarantees remain verified.

Commit: `fix: isolate analysis progress by execution attempt`

These corrections do not constitute acceptance of P1 #1. Formal acceptance is
reserved for the subsequent focused independent review requested by the user.

### Correction postflight

- [x] Chunk 3 verified and committed separately as `5542c9a`.
- [x] Chunk 4 verified and committed separately as `7f304be`.
- [x] Chunk 4 focused verification: 60 checks passed, including all 47 P1
  regressions (17 added by these corrections); 25 API checks pass on Python 3.10.
- [x] Final API suite: 1,442 tests, exactly three existing failures, ten existing
  errors and 276 unchanged skips. Worker: 287 tests, no failures/errors, seven skips.
- [x] Six isolated PostgreSQL checks pass without skips; test services stopped.
- [x] Final source hashes match the tested implementation. Compilation, whitespace
  and scope checks pass; no new TODO/FIXME. No application edits after the gate.
- [x] Correction implementation complete; no push, merge, deploy or P1 #2 work.
- [ ] Formal acceptance: reserved for the user's focused independent review.


## Final-review corrections A/B (authorized 2026-11-10)

Source: /home/jon/.codex/attachments/b905d848-5fe4-4e7e-85a9-d7f10c531a13/Pasted text.txt.
The user explicitly authorizes these two bounded implementation chunks; this is
not a request for another general plan sign-off. The only repository is
/home/jon/Desktop/workshop/last_chessism, branch refactor/p1-analysis-failure-propagation.
Verified starting HEAD 24e830e and clean worktree. No other repository/worktree,
production data, schemas, engine policies, P1 #2 or P1 #3 may be changed.

### Entry and verification policy

Preserve previous chunks and their evidence as historical implementation records.
The final independent review returned REQUEST CORRECTIONS: cancellation can
interrupt already-running closure, and Positions interprets telemetry/ARQ result
availability as successful execution. Existing four correction commits remain.

Before application changes, recapture the unchanged full API/worker baseline,
existing 60 focused and 25 Python 3.10 checks, six disposable PostgreSQL checks,
and frontend Node tests/architecture/source-size/build baseline. Compare API and
worker failure/error/skip identities against correction2 evidence, including
tracebacks with line numbers normalized. The explicitly retained API baseline is
1,442 entries, 3 failures, 10 errors, 276 skips; worker 287, 0 failures/errors,
7 skips. New tests must all pass; no silent weakening/skipping. Stop if new
baseline problems prevent meaningful verification. No configured Python or plain
JS type checker/linter; use Python syntax compilation, git diff --check and the
existing frontend architecture/source-size checks and Vite build. Build output
and isolated dependency installs stay in the temporary verification directory.

### Chunk 5: Protect analysis-session cleanup already in progress

Depends on: completed chunk 4 and reviewed entry baseline.
Files: chessism_api/database/postgresql/ask_db.py;
chessism_api/operations/analysis.py;
chessism_api/database/postgresql/analysis_cleanup.py (new, only if a small shared
protected-await helper is needed by both existing cleanup owners);
tests/test_analysis_failure_jobs.py; tests/test_analysis_failure_selection.py;
the three tracking documents.

1. Add deterministic event-controlled regressions for worker closure and
   selector closure after rollback failure, each interrupted by external
   cancellation or sibling acquisition failure. Include repeated cancellation,
   cleanup failure preserving the primary exception, and committed results.
   Assert the coordinator remains pending until closure actually finishes.
   Run and record the failures before application changes; no timed sleeps.
2. Protect cleanup before its first awaited rollback/close can be interrupted.
   A small shared helper may retain a cleanup task, shield every await, drain
   after first/repeated cancellation and then propagate the original outcome.
   Bare shield without draining the retained task is insufficient. Do not hide
   genuine cleanup failures when no primary error exists. Always attempt close
   after rollback failure. Keep selector versus worker ownership unchanged.
3. Retain the coordinator's cancellation and drain behavior, acquisition outside
   retries, prior commits, counters, SQL/claim rules and engine request policies.
   Use Python 3.10 APIs; introduce no service layer or generic framework.
4. Run focused regressions on Python 3.10 and the established runtime; run full
   API/Redis/ARQ and worker comparisons plus PostgreSQL invariants. Verify source
   hashes and no unrelated changes. Commit only after the gate is reviewed.

Exit: all four reproduced orderings and repeated cancellation complete cleanup;
original failure/cancellation survives; all prior P1 guarantees remain verified.
Commit: fix: protect analysis session cleanup already in progress

### Chunk 6: Interpret Positions outcomes from authoritative ARQ results

Depends on: verified and committed chunk 5.
Files: chessism_web/src/pages/positions/positionPageSupport.js;
chessism_web/src/pages/positions/positionStatusFreshness.js;
chessism_web/src/pages/positions/usePositionStatusPolling.js;
chessism_web/src/pages/positions/usePositionsPage.js;
chessism_web/tests/positionsProgress.test.mjs;
chessism_web/tests/positionStatusFreshness.test.mjs;
chessism_web/tests/positionJobOutcomes.test.mjs (new);
chessism_web/tests/positionJobPolling.test.mjs (new if needed);
the three tracking documents. A file in this list changes only if needed for the
status interpretation, outcome presentation or polling lifecycle. Shared polling
transport and unrelated pages remain unchanged; do not start P1 #3.

1. Confirm actual jobs API shapes: status=complete denotes result availability;
   result.success is the execution outcome. Retained-history endpoint already
   derives its terminal status from that outcome. No new wire fields or text
   inference. Inspect queued/deferred/running, cancellation and missing-result
   states before defining the minimal Positions-specific interpretation.
2. Add failing cases for failed reset retaining old completed progress, stale
   completion while queued/running, unsuccessful results with completed/missing
   progress, success, temporarily missing progress and unresolved result.
   Exercise polling continuation/termination through the actual shared polling
   primitive and Positions decisions using a deterministic fake clock. Cover
   freshness checks so stale telemetry cannot hide a confirmed result/retry.
3. Give confirmed execution outcomes precedence; never infer success from ARQ
   status=complete alone. Keep jobs with unresolved outcomes active and polling;
   prevent stale terminal progress from finalizing an active queue execution.
   Preserve valid intermediate phases and explicit unsuccessful/cancelled
   outcomes. Render a failure detail from the actual response contract, without
   claiming success or scheduling successful completion dismissal.
4. Adjust existing fixtures only when they falsely equate a complete queue
   status with success; retain their intended assertions and document why.
   No broad polling transport redesign or account/project work.
5. Run all frontend Node tests, relevant polling/progress tests, existing
   architecture/source-size checks and production build into temporary output.
   Run the final backend/Python 3.10/Redis/PostgreSQL preservation gate; compare
   all broader results to entry baseline, record source hashes, then commit.

Exit: actual outcome determines completion/failure, unresolved jobs keep polling,
all required frontend cases pass, and backend P1 guarantees remain unchanged.
Commit: fix: make Positions use confirmed ARQ execution outcomes

### Final handoff

All long jobs use /tmp/chessism-p1-final-corrections/monitor.py with an in-place
120-second display that remains open after completion. Show one watch command,
launch detached, record PID, and immediately end the turn; do not poll or wait.
Preserve /tmp/chessism-p1-verification evidence. Tests use fresh uniquely labelled
containers, no external network, read-only source and explicit synthetic settings;
only isolated frontend dependency preparation may access the package registry.
Stop after two bounded verification-fix attempts for a chunk. Commit verified
chunks separately and record completion with a tracking-only follow-up if needed.
Report findings, exact red/green evidence, compatibility, baseline debt and
remaining production/real-engine limits. Do not declare P1 #1 accepted, push,
merge, deploy or begin another task; independent acceptance review follows.

### Approved frontend baseline exception (2026-11-10)

The user explicitly authorized continuing chunks 5/6 with the four reproduced
frontend assertion failures. The complete baseline is 320 checks, 316 passes,
four failures, zero errors/skips/cancellations. Exact names and failure details
are frozen in /tmp/chessism-p1-final-corrections/frontend-baseline-complete/
failure-details.json and recorded in the manifest/log. These remain unresolved
testing debt, outside P1 #1. Require every new regression to pass and no additional
failures, errors or skips. Do not weaken/delete/skip tests or repair appearance/
navigation behavior. Stop if existing debt prevents meaningful verification.
The backend exception and all scope/safety restrictions remain unchanged.

### Final-review correction postflight (2026-11-10)

- [x] Correction A verified and committed separately as `49edfc5`.
- [x] Correction B verified and committed separately as `499a781`.
- [x] Eight backend regressions reproduced before correction; all 33 cleanup
  checks pass on Python 3.10 and 3.11, with completed-cleanup assertions.
- [x] Twenty-six frontend regressions pass; 21 reproduce old defects and five
  preserve correct behavior. All 34 focused frontend checks pass.
- [x] Complete API/worker results match approved failure/error/skip details;
  six isolated PostgreSQL checks and 68 focused checks pass.
- [x] Full frontend: 346 checks, 342 passes, exactly four approved preexisting
  failures, zero skips; exact failure details retained in the manifest.
- [x] Architecture, source-size and production build pass. Final source hashes
  match; backend source/image checks validate reuse of its complete passed gate.
- [x] Only approved production/test files and tracking documents changed;
  disposable test services removed and original ignored directory preserved.
- [x] No production changes, push, merge, deployment, P1 #2 or P1 #3 work.
- [ ] Formal P1 #1 acceptance: reserved for independent review.

## Completed logical change — canonical research training and binary sources (167 passed)

Entry: bounded FEN popups passed all 159 checks; local commit e4a298a.
Convert rating-aware v1/v2 training selection and the shared verified-evaluation
position query to one concrete canonical reader. Convert canonical binary COPY to
that same native source. Keep whole-game limits, deterministic sampling, date and
player splits, FEN identities, evaluation classifications and immutable inference
unchanged. Explicitly filter standard/nonempty games instead of assuming a native
schema excludes variants. Cast before ABS for valid integer-minimum evaluations.
Training operations own guarded read-only snapshots; binary extraction borrows its
caller's transaction. Remove SQL string replacement used to insert FEN identities.

Update the actual source consumers and focused mocks, without changing model logic.
Verify fresh canonical PostgreSQL selection/extraction, real binary parity, source
bounds, coverage mismatch, missing predecessors, cloud guard, snapshot consistency,
read-only operation, database error propagation and repeated cancellation cleanup.
Retain existing model/packing/sampling checks and accepted P1 regressions. Map the
old miniature legacy/profile training fixtures to these canonical cases; broader
research saved-CPU-reference and CUDA publication conversions remain separate.
No GPU/device execution, source database access or runtime DDL. No feature removal.

## Completed logical change — canonical player profiles and statistics (144 passed)

Entry: research source gate passed 167 checks, committed as a55f25e; clean tree.
Replace operations/players.py DBInterface/ask_db calls with one concrete adapter,
explicit guarded transactions and atomic profile/statistics upserts. Add the existing
useful v3 numeric-owned player_stats columns to canonical DDL (version 10), retaining
response fields and FK cascade. No JSON schema or historical migration framework.
Fetch remote profile/statistics outside database transactions. A failed profile fetch
preserves an existing full/deleted profile or creates a shell. A successful explicit
refresh clears deleted_at and preserves a concurrently curated timezone when remote
input supplies none. Concurrent creates keep one stable numeric player identity.
Stats remain a whole fetched snapshot, with last_updated advanced at publication;
missing remote stats preserve previous data. Database errors propagate to caller.

Keep parsing/public batch results and route contracts. Test real PostgreSQL upserts,
FK/uniqueness, schema version rejection, shell/full/deleted state, timezone races,
concurrent requests, rollback/cancellation cleanup, readonly reads and remote calls
without an open DB transaction. Retain ingestion/account and existing P1 regressions.
No actual external API call, live DB, push or deploy. Other DBInterface callers and
old runtime initialization remain for A4 after conversion; do not remove them early.

## Completed logical change — canonical FEN appearance snapshots (147 passed)

Entry: profile/statistics gate passed 144 checks; local commit af6b7ca.
Add the four existing useful histogram/common-position cache tables and initialized
singletons to canonical DDL (version 11). Preserve stale complete snapshots rather
than giving cached FEN/example identities authoritative FKs. Provide the native
bounded commonness ranking index. Move source/cache SQL into a concrete adapter;
remove legacy move-ID and profile branches. Keep ten-position pages, deterministic
modal labels, verified legal examples, histogram meanings, Redis TTL/debounce and
cursor/public payload contracts. Cache keys use a canonical namespace.

Use guarded read/write transactions, REPEATABLE READ publication, existing advisory
locks and limits. Missing required singleton rows are errors. Record failures only
for their own attempt, so older failures cannot mark a newer snapshot failed.
Verify canonical source values, page ties, legal examples, constraints, missing-row
failure, consistent snapshots, atomic rollback, cancellation, contention, stale error
suppression and existing route/cache tests. No actual Redis/Neo4j or source DB use.
Run this expected-under-five-minute focused gate directly; no user handoff.


## Completed logical change — canonical structured dataset exports (112 passed)

Entry: FEN cache conversion verified (147 checks), committed as 7d6d323.
Use one canonical table map in the strict structured compiler, its preview/file
readers and the analytical view installer. Keep the explicit allowlist and bound
values. Apply approved participant key (game_id,is_white), float64 elapsed_seconds,
int64 joined; advance the structured catalog version to identify these changes.
Keep standard-game scope, including stored zero-move standard games: the actual old
export source had no nonempty-game predicate. Do not import unrelated analytics
eligibility rules. Preserve self-play sides, repeated FEN occurrences, score/NULL
representations, stable ordering, bounded streams and Parquet metadata.

Use guarded read transactions and drain active file threads/streams on cancellation.
Verify every source/column on canonical PostgreSQL, preview and file parity, empty
output, native identities, fractions/large timestamps, numeric extrema, injection
rejection, snapshot races, failure and repeated cancellation. Existing credential,
job lifecycle and restricted analytical-reader conversion will follow together;
this chunk does not claim those tables have been initialized or tested end to end.
No live installation, role changes, network, GPU, real database or checkpoint access.


## Completed logical change — canonical export lifecycle and analytical reader (197 verified)

Entry: structured-source gate passed 112 checks; local commit 17d91de.
Initialize existing export_client/export_job columns, native ownership FK and
idempotency uniqueness in canonical version 12. Keep immutable requests/manifests
as bounded files, no JSON-table framework. All application metadata units own
explicit guarded transactions. Drain active construction before releasing the
worker lock or deleting artifacts, including repeated cancellation and timeout.
Preserve request admission, credential scopes, revocation, owner-only status/file
access, individual cancellation and restart/expiry maintenance.

Port existing isolated export and analytical lifecycle fixtures to freshly
initialized canonical databases. Exercise real restricted roles/queries only on
the disposable server; retain all meaningful security and SQL/matrix scenarios.
The role installer owns only its security views/roles, not public-table repairs.
Guard analytical transaction closure; preserve projection/selection calculations.
No production backup/restore behavior, actual credentials, deploy or external DB.
Focused schema, lifecycle, source, security and accepted P1 checks; no broad suite.

## Approved historical research retirement
Remove five saved-CPU comparison launch/dispatch paths and their now-dead source
and compute implementations. Keep saved reports, exploration exclusions, fixed-rule
oracles, current training/evaluation and CUDA publication. Retained full-development
metadata uses canonical player/revision tables and explicit read/write transactions.
Reject stale queued jobs and saved-definition replay before numerical or file work.
Verify current calculations, API rejection, saved reports and history preservation.

## Approved FEN-only restore compatibility
Schema16 supports saved scores with NULL analysis_source, preserving a known or
missing timestamp, without fabricating attribution. Keep existing WDL/integer/FK
constraints and known-source rules. Native typed restore locks existing FEN keys,
updates values/continuations atomically, retains absent-FEN accounting and newest
record wins. Export NULL-timestamp scores conservatively on every delta. Port
existing archive/restore regressions to canonical fixtures and verify old archive
bytes, unknown metadata, rollback/cancellation, snapshot/late commits and malformed
inputs. This resolves the final known live compact-storage backup consumer.


## Completed implementation — A4 obsolete storage retirement (acceptance pending)
Remove unused DBInterface/ask_db/profile adapters and historical migration/repair
commands after dependency inventory. Keep old archive validators read-only and
AST-identical. Keep model adoption/rollback and active bounded ingestion. Port
current query/transaction tests; map retired migration-specific identities in
storage-retirement-tests.json. Verify import closure, native persistence and P1
checks; reserve one broader comparison for the Stage A boundary.


## Stage A boundary — implementation verified, independent review pending
A2 native writes, A3 required reads/feature consumers and A4 runtime retirement are
implemented. Schema17 is the sole runtime contract. Supported backup validators
remain; no original database or recovery archive was read or changed. Final broad
comparison and focused follow-ups are recorded in the manifest and acceptance
packet. Existing unrelated test debt remains explicit. No Stage B/C work authorized
by this completion record. Independent acceptance has not occurred.


## Authorized correction — Stage A score arithmetic bounds (2026-11-19)
Entry: reviewed e02d47a; clean worktree; single numeric blocker, no new stage.
Verify White-relative CP, mate and tablebase conventions. Add actual PostgreSQL
regressions for page aggregation and engine refresh, full INTEGER bounds, both
perspectives, raw-score preservation and unchanged bounded accuracy formulas.
Reproduce overflow, then widen score operands to bigint before ABS/negation, as
in existing matrix readers. No schema, classification, formula or API changes.
Gate: focused new PG checks plus affected reads/recovery/matrix/scoring checks;
syntax and scope review. No broad-suite rerun. Preserve original data/checkpoint.
Exit: evidence and local commit; stop for brief independent numeric re-review.

Numeric correction gate completed: red reproduced both overflow paths; green
64/64 (49 PostgreSQL), no failures/errors/skips. Only SQL operand widths changed.
See docs/architecture/stage-a-numeric-correction.md and archived evidence.
Next: local checkpoint and brief independent numeric re-review; stop implementation.


## Authorized Stage B — bounded execution and cloud dependencies (2026-11-19)

Stage A formally ACCEPTED in the final focused review at 7d9fe8e. Current
authorization: da40c3b8-f712-44b7-bcd7-f885c7b44b66/Pasted text.txt. Prior Stage A
stop instructions are historical. Implement B1–B3 together; no C/D or live data.

B1: replace indefinite HTTP calls and batch reselection retries with one finite
transport policy shared by queued and immediate callers. At most three HTTP
attempts, identified transient errors only, cancellation-aware bounded backoff
and overall deadline derived from the existing 300s engine stall allowance and
2.1s/FEN scheduling estimate. Keep row locks across attempts; fail the job after
exhaustion/permanent/malformed/database errors, drain siblings, preserve commits.
Remove the service's nested search retry, bound pool waiting/engine startup and
execution, and dispose interrupted engines before re-leasing. Preserve immediate
response shape, score serialization and attempt-scoped progress. Inspect relevant
cloud worker bounds without expanding into a new scheduling framework.

B2: move save_run/save_job, unchanged in behavior, from coordinator into a small
shared durable transition module. Update backend/recovery imports and their tests;
preserve row locks, claims/receipts, cancellation guards and verification stamps.
Check dependency direction including deferred imports, not just module importability.

B3: focused deterministic engine/service/cancellation regressions; isolated real
PostgreSQL selection/commit/cleanup and Redis/ARQ attempts; cloud transition tests.
Capture relevant accepted-baseline identities first, compare near completion.
Reuse unchanged Stage A evidence. One relevant broader gate near completion, no
frontend work. Short isolated real-engine check if cached environment supports it.

Each meaningful verified change gets a local commit; no push/merge/deploy. Tests
expected under five minutes run directly; longer separate gates get the established
120-second single-screen monitor and return control. Stop after complete Stage B
evidence and request one independent acceptance review.

Stage B completed and verified 2026-11-19. B1 local commit `b8c5973`; B2 local
commit `f073914`. Integrated gate: 255 passed (145 PostgreSQL); final focused:
29 passed; real Stockfish: 1 passed. Relevant broader comparison: 209 checks,
same baseline 1 failure/1 error, zero skips or new failures/errors. All verification
resources removed. See the Stage B handoff and archived evidence. Next action:
one independent B1–B3 acceptance review; stop implementation until authorized.

## Authorized Stage B cancellation-race correction (2026-11-19)

Entry: clean reviewed 62563a6, expected repository/branch. Authorization:
81f85ee1-b698-4f51-926d-6afe2536930a/Pasted text.txt. One focused chunk:

1. Add deterministic tests before production changes: response/slot completion
   racing external cancellation, startup/search completion, repeated cancellation
   and cleanup failure, concurrent slot ownership, real timeout distinction.
   Capture red results on available Python 3.10 and 3.11 cached runtimes.
2. Replace the five Stage B wait_for boundaries (HTTP, startup, queue, search,
   quit) with a small shared cancellation-preserving deadline operation. The
   caller owns any successfully acquired resource, even when cancellation wins
   its handoff. Reclaim that pool slot explicitly. Preserve primary cancellation
   through endpoint/initialization cleanup if repeat cancellation arrives.
3. Verify new races plus existing client/service lifecycle checks on both Python
   versions; reuse the isolated PostgreSQL/Redis Stage B execution and P1 checks.
   A short real-engine smoke checks the changed process lifecycle. No broad suite.
4. Record evidence, source integrity and unchanged cloud/database/scoring/retry
   contracts; create local correction/evidence commits. No push or deployment.

Exit: focused tests pass with no added failures/errors/skips; owned resources
removed; stop for one short independent re-review. Stage B remains unaccepted.

Correction gate complete: 36 checks passed on each Python 3.10/3.11 runtime;
61 P1/PostgreSQL/Redis/ARQ checks passed; real Stockfish smoke passed. Final new
regressions also tested against immutable reviewed code: 12/13 fail on both
runtimes, including a real post-cancellation database commit. No green failures,
errors or skips. Local implementation commit `7783e54`; all owned resources removed.
Stop here for the focused independent re-review.
