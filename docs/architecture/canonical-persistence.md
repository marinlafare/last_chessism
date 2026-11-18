# Canonical PostgreSQL foundation — Stage A, Batch 1

Status: Batch 1 independently accepted with documented limitations at `35be4fc`.
Stage A2–A4 are now authorized and in progress. This remains **not a runtime-ready
replacement database**. Game-import and bounded FEN ingestion now use canonical persistence; remaining
consumers and runtime startup conversion are still pending. Entry code checkpoint
for the foundation: `0fde829`. No historical data is required.

## Scope and sources

The initializer creates 98 application tables, one semantic view, and a version
record in one empty PostgreSQL 15 database. Ten core tables define the normalized
chess relationships. `support.sql` preserves 78 existing account/session,
appearance, ingestion/timing and cloud tables, including the typed cloud document
children. Seven projection tables support game imports and bounded FEN persistence; one
revisioned queue retains durable per-game summary refresh requests. The supporting
declarations were rendered from the checkpoint's SQLAlchemy metadata;
initialization imports neither that metadata nor the old runtime. Large typed
cloud definitions are retained because their checksums, packed arrays, receipts
and cancellation plans are live recovery contracts, not historical compatibility.

This is deliberately not a claim that all 197 backed-up tables are dispensable.
Derived analytics/summary tables, research definitions/artifacts, export jobs, and
PostgreSQL graph projections/outbox are not activated by this foundation. A2/A3
must add their needed canonical definitions alongside conversion of their writers
and readers, before A4 removes old runtime support. Existing code and declarations
remain intact. No optional feature is silently retired.

## Canonical relationships and concrete simplifications

| Area | Foundation | Replaces in the target runtime |
| --- | --- | --- |
| Players | Numeric `player.id`, unique normalized external name; profile fields retained | Name-keyed physical writes and adapter translation |
| Games | Existing provider `link` identity, two player FKs, optional numeric mode, UTC timestamp, rules/setup, generated standard classification | Separate standard/archive identity domains, duplicated calendar/average fields |
| Participation | `game_player` is a `UNION ALL` projection of the two game sides | Duplicate writable participant rows and synthetic participant ID |
| Moves/openings | `(link,n_move)` moves; shared opening string with `(link,n_moves)` reference | Redundant move identities and repeated opening strings |
| Zero-move games | `game.n_moves=0`; still one global game identity | Separate zero-move tombstone identity table |
| Positions | Numeric `fen_id`, unique canonical four-field FEN string | Competing physical/view types; string keys in occurrence writes |
| Occurrences | `(game_link,n_move,is_white)` PK, numeric FEN FK | Redundant occurrence ID and weaker uniqueness including the FEN |
| Continuations | `(fen_id,rank)` PK, integer score | Surrogate continuation ID and float score adapter |
| Imported months | `(player_id,year,month)`, nonnegative count, explicit empty month | Surrogate month ID and player-name conversion |
| Accounts/sessions | Existing identifiers, role enum, password/token hashes, activity/expiry/revocation and cascade semantics | No redesign of authentication |
| Cloud state | Existing typed contracts, receipts, cancellation and lifecycle fields | No speculative replacement of durable recovery structures |

The participant view has application meaning; it is not a legacy-shape adapter.
Self-play produces two sides, not one deduplicated player. A2 must replace child
summary references to old `game_player.id` with `(game_link,is_white)`, preserving
side-specific calculations. This is a concrete conversion dependency identified
while implementing the schema, not an automatic authorization to change callers.

A2 must retain provider metadata for zero-move imports instead of emitting only a
tombstone. Old data need not be converted. Global duplicate prevention must include
standard, variant and zero-move games. Identical repeats do not increment ledgers;
conflicting payloads for the same link are errors, not blind overwrite/upsert.
Calendar and average-rating payloads will be derived from the canonical timestamp
and ratings; existing UTC filtering/API semantics must be characterized in A3.

Nonstandard games retain metadata and moves but cannot acquire extraction flags
or FEN associations. The generated standard predicate matches the current
`standard_game`: rules `chess` with no setup or the normal starting position.
Changing a referenced game's classification is rejected. This is checked in both
write directions; row locks serialize association creation with game changes.

## Evaluation contract

Keep the current four-field FEN identity. Six-field engine input counters do not
create new stored positions. PostgreSQL checks the key shape, not chess legality;
the existing parser owns legality and canonicalization.

Use integer centipawn/mate encoding and the existing `evaluations.py` interpretation:
zero is a scored draw; NULL is unscored; mate values retain their current sentinel
meaning and perspective. No new mate-distance or score-kind column is invented.
Fresh scored rows require source and timestamp. WDL is absent or three integers
in 0..1000 summing to 1000. Tablebase rows require WDL -2..2 and optional DTZ;
Stockfish rows cannot contain tablebase metadata. `tablebase_unavailable` remains
unscored and eligible for Stockfish. Validation must reject fractional inputs
before parameter binding: PostgreSQL casts alone do not guarantee no rounding.
Reuse `validate_result`/`validate_batch`, not a second domain validation framework.

## Persistence contracts and transaction ownership

These contracts define A2 conversion; this batch does not add unused repository
classes or connect new functions to current operations.

| Boundary | Owner and behavior |
| --- | --- |
| Schema initialization | Explicit developer command owns its initialization transaction; caller closes connection. No nested transaction, reset, migration or data deletion |
| Validation/startup | Read-only `validate(connection)`; missing/incompatible/drifted schema raises. Startup must never initialize automatically |
| Import bundle | Operation owns one transaction spanning players, game/moves/openings, month ledger, stale projections and ingestion accounting. Low-level functions accept session, never commit, and return actual inserted identities/counts |
| Analysis selection | Preserve `fetch_batch(limit)` and `(session,fens)` handoff; `(None,None)` only successful exhaustion. Selector cleans up on failure; worker owns handed-off session |
| Evaluation writes | Caller transaction includes scores, continuations, summary deltas, receipt insertion and claim release. Validate complete batch first; lock rows and recheck unscored state. No helper commit |
| Cloud reservation | Lock selected FEN rows and publish claims in the same transaction; unique FEN claim. Competing local/cloud workers follow the same locking order |
| Receipt replay | `(run_id,task_index,batch_index)` identifies a persisted batch. Existing receipt checksum/content must match before treating retry as acknowledged; uniqueness alone is not an idempotency proof |
| Cancellation | Durable cleanup intent is separate from successful analysis. Preserve existing coordinator transitions, cancellation-safe cleanup helper, and primary-error propagation |
| Accounts | Identity is separate from a tracked chess player. Active account, role, expiry/revocation and secret verification remain application policy; FK existence does not grant access |
| Queries | Short read sessions may be locally owned; helpers participating in a larger transaction receive its session. Read functions never perform hidden commits |

Selection retains `READ COMMITTED`, `FOR UPDATE SKIP LOCKED`, descending game
frequency, deduplication and the separate fresh-snapshot claim/score recheck.
Locks remain held until the owning transaction commits/rolls back. The row-lock
and fresh-recheck checks are distinct tests. No lease framework or retry policy
change is introduced. P1 #1 cleanup, committed results, ARQ propagation, attempt
identities and terminal-progress guards remain untouched and regression-tested.

## Initializer and version validation

Run from the repository root with declared dependencies available:

```sh
python -m scripts.init_canonical_db --socket /tmp/chessism-canonical-EXAMPLE/pg --database chessism_canonical_example --user test
python -m scripts.init_canonical_db --socket /tmp/chessism-canonical-EXAMPLE/pg --database chessism_canonical_example --user test --validate-only
```

These are command templates, not permission to use an application database.
The CLI requires an explicit private Unix socket and development database name;
it never loads `.env`, application settings or a default database URL. The runner
creates the server/database. The initializer itself does not create/drop databases.

Initialization serializes with a transaction advisory lock. An empty database is
created atomically; any error rolls back tables, enum, functions and marker. A
repeat validates the existing artifact and catalog then returns `False` without
resetting data. Populated unmarked databases are rejected. The marker contains the
foundation contract, version, DDL checksum, catalog checksum and initial timestamp.
Validation performs reads only, rejects an incompatible version/artifact or catalog
drift, and never repairs it. It works inside a read-only transaction.

Version 1 is the accepted Batch 1 foundation. Version 2 closes the nullable CHECK
loophole: unscored rows with no source cannot carry an `analyzed_at` timestamp.
Version 3 adds the six ingestion projection tables and initialized singleton
totals. None of these versions implies whole-application readiness. Changes
to these artifacts or additions in A2/A3 require an explicit new version; for these
disposable databases reinitialization uses a newly created database, not automatic
migration. No runtime storage profile or release switch is installed by A1.

## Verification and baseline

The results below are the accepted A1 gate. A2 evidence is recorded separately in
the manifest; the A1 gate is not evidence for newly added runtime persistence.

`tests/canonical_schema_fixture.py` supplies synthetic standard/variant/custom-start,
zero-move and self-play games, repeated positions, two accounts, empty/populated
month ledgers and two cloud runs. No real user data or production credentials.
Account fixtures cannot authenticate until a test explicitly installs a valid hash.
There is no default admin account in an initialized database.

`tests/test_canonical_schema.py` runs offline safety checks. Explicit
`tests.check_canonical_schema_postgres` runs real integration tests; missing isolated
configuration is an error, not a skip. Each method owns a disposable database.
Checks include atomic initialization/repeat/concurrency/failure, incompatible
versions/catalog drift, relationships/uniqueness, evaluation encodings, account
hash/session fields, original typed-cloud-codec round trips, receipts with results,
rollback, cancellation, lock exclusion and a real observed row-lock/recheck race.

The existing 68 focused P1 checks passed with a fresh private Redis. Complete suites
are reused against the matching 1,699 input hashes; none of their existing files
were changed. Known debt remains: API 3 failures/10 errors/276 skips; worker 7
skips; frontend 4 failures. Full details remain in the historical manifest. All new
and selected focused checks must pass with zero skips. No Python linter/typechecker
is configured. No production, GCP, browser or real-engine behavior is newly claimed.

Reviewed gate: **99 tests passed**, zero failures/errors/skips (5 offline + 26
PostgreSQL + 68 P1). Total supervisor time: 16.47 seconds; foundation suite 7.16s,
P1 suite 4.53s. All four network-disabled disposable containers were removed;
a separate filtered Docker inspection confirmed no containers for this run remain.
All 1,707 frozen assessment/test inputs matched on review, including the 1,699
protected baseline files. After review, one extra blank EOF line was removed from `support.sql`; all SQL
statements are identical. The five offline checks were rerun for the final artifact.
Other post-gate changes are documentation/tracking only.

Evidence: `/tmp/chessism-canonical-a1-20261110-01/` contains structured results,
logs, exact container commands, image identities and source hashes. Result digest:
`791e9d61e9e9c720ba510f2096f630816840fedd712ceffd850b9648e45c2c00`.
Tested DDL digest: `dccf484b7f38d8935d2c9c849ac521440cdb02fc182511c877905182034990e1`.
Committed DDL digest (EOF whitespace only): `364339bf98861abdf6fe591940ca0cd23848755f386b184e4ef332bcd6f68e62`.
The three historical full-suite results above are reused evidence, not newly
executed suites and not globally passing results.

## Review risks and next batches

- A1 proves database contracts, not that current callers work on this schema.
- Derived projections and feature-specific auxiliary schemas must be added in A2/A3.
- Side-key, zero-move and derived-date conversion require deliberate caller updates.
- Live authentication/role provisioning remains a separately approved cutover task.
- Small fixtures do not prove historical-scale query performance; A3 needs seeded
  cardinality comparisons and index/query-plan checks before removing tuned paths.
- Existing cloud lifecycle policies remain in their current coordinator; B will
  remove dependency cycles without weakening receipts, cleanup or recovery.
- Backup/restore behavior, Neo4j/GCP services and the verified checkpoint are unchanged.
- A2–A4 are authorized as one stage; independent acceptance follows their final gate.

## A2.1 canonical import boundary

`database/postgresql/game_imports.py` accepts an existing PostgreSQL transaction.
`ingest_games` validates complete game/move/opening bundles, stages bounded groups,
and takes the existing ingestion transaction lock. Identical repeats return no new
identities. Conflicting metadata, moves or openings raise; they never silently
overwrite or acknowledge different content. Extraction flags on existing games are
preserved. All classifications, including zero-move metadata, share one identity.

`sync_month_counts` records explicitly fetched months, including empty ones. It
counts only `is_standard AND n_moves > 0`, once per game even in self-play. UTC
timestamp boundaries replace calendar columns; session timezone does not redefine
an imported month. Variants, custom starts and zero-move games remain stored but
excluded from this established active-game count.

These functions do not begin, commit, roll back or close their caller's transaction.
The infrastructure chunk introduced no new storage profile or runtime switch.

## A2.2 game-import application transaction

`ingestion_pipeline/game_importer.py` now owns the transaction spanning games,
players, moves, openings, all successfully fetched month ledgers, game/mode totals,
and pending salience work. It uses `transactions.write_transaction`; rollback and
closure finish through repeated cancellation using the accepted P1 helper unchanged.
Summary failure rolls back the import. No post-commit refresh can turn a failed
summary update into a partially acknowledged import.

Formatting no longer writes player shells or discards existing identities before
checking their contents. Identical downloads are parsed and checked at persistence;
conflicting retries fail. The public success string and already-imported message
remain strings with the established wording. Counts describe actual inserted rows.
Zero-move game metadata is preserved; no tombstone or archive option is needed.

`player_salience_pending_game` is keyed by `(game_link,is_white)` with a cascading
game FK. Both sides always exist in `game_player`, including self-play, so ownership
is derived rather than duplicated. Mode summaries retain their prior perspective
semantics (both sides in self-play); month/global game totals count games once.
Global game/pipeline totals preserve the active standard/nonempty population from
the deployed relational_v3 behavior. Position counters are untouched by game imports.

Version 3 defines `player_salience_summary`, `player_salience_pending_game`,
`main_character_mode_summary`, `game_analysis_coverage`, `database_summary` and
`fen_pipeline_summary`. The latter two start with zero singleton totals. Other
projection/feature definitions remain for the subsequent A2/A3 work. Historical import adapters have no new runtime callers and await A4
retirement; no additional selectable storage profile was introduced.


## A2.2 bounded FEN persistence

Version 4 adds the consumed `scored_position_summary` singleton with existing
score-bin, appearance and aggregate fields. Initialization seeds zero totals;
versions 1–3 are rejected, never silently upgraded. No existing database is changed.

`fen_batches` owns each bounded replay/persistence transaction. `fen_repository`
selects only standard, nonempty, incomplete, unclaimed games in link order using
`FOR UPDATE SKIP LOCKED`. Replay remains pure. `fen_bulk` borrows the active driver
transaction for COPY and resolves numeric identities once. Its existing ordered
FEN locks, conflict rejection and inserted-link counter deltas remain unchanged.
Repeated positions count occurrences; duplicate retries add nothing. Scores,
continuations and cloud claims are preserved. Missing required summary rows fail
and roll back the batch. Commit/rollback/closure use the accepted cleanup helper.

The coordinator joins every known child before releasing claims, including after
external/repeated cancellation or an enqueue/worker error. After workers commit,
one application transaction writes absolute coverage, final game flags and pipeline
totals. Failure here retains committed FEN/link/count data, rolls back finalization,
and permits exact retries. Progress is published after the corresponding commit.
`game_coverage` contains canonical SQL and never commits; `None` denotes intentional
whole-corpus repair while an empty scope does nothing. Small scopes retain indexed
per-game aggregation and sorted game locks followed by fresh snapshot reads.

The duplicate legacy orchestration branch is removed. Old worker exports and the
profile-based coverage module remain only for callers awaiting A2.3/A4 retirement.
Rating, tablebase and player-summary consumers are not yet converted; this is still
an intermediate Stage A checkout, not a deployment-ready runtime. The focused gate
uses actual PostgreSQL and synthetic child scheduling; it does not exercise live
Chess.com, Stockfish, GCP or production-scale throughput.


## A2.3 analysis selection and reservation boundary

`analysis_selection.py` owns global, player and frozen-game acquisition, plus the
frozen-scope remaining/completion reads. Their definitions are removed from
`ask_db.py`; direct local/cloud callers and test patch targets move together.
Successful empty selection returns `(None, None)`. A nonempty lease transfers an
open transaction to its caller. Acquisition errors propagate after cleanup; cleanup
failure is never successful exhaustion. The accepted P1 cleanup function body is
unchanged, and the three duplicated acquisition lifecycles now share one function.

Global/player frequency ordering is preserved. Player membership uses numeric
participants and EXISTS, so repeated occurrences and self-play do not consume the
batch limit. Frozen-game selection and durable cloud queue construction share the
same candidate SQL, retaining independent minimum game/move priorities and the
FEN-text tie-break. Queries use native `is_white`; no layout/profile is selected.

Cloud queue identity and page limits are unchanged. Build checkpoints commit in
their existing two stages; reservation hands off candidate removals with the FEN
locks, to be committed atomically with claims by the application. Skipped locked
candidates remain available. Failed/cancelled acquisition uses the same guarded
cleanup as local selection, including rollback failure and repeated cancellation.
Live claim subtraction still uses a separate, bounded fresh-snapshot statement.

The focused selection gate includes actual PostgreSQL leases/queues and all P1
regressions. It reuses the completed 198-check ingestion/schema gate for unchanged
inputs, rather than repeating that workload. Existing opt-in legacy database
fixtures are still pending the final canonical integration mapping; none are
silently skipped or treated as canonical verification. Evaluation publication,
receipts, tablebase and durable projection recovery remain the next A2.3 subgate.


## A2.3 Stockfish publication boundary

`evaluation_results.py` stages canonical integer scores, exact nullable WDL,
provenance, timestamps and numeric continuations in the caller's transaction.
It validates the full input before writes, locks existing FENs in FEN-text order,
and refuses missing identities or already-scored positions for live publication.
Reviewed replacements retain their separate aggregate-adjustment responsibility.
Typed-array updates are bounded to 1,000 FENs per statement, including all-NULL
columns; alternatives use bounded COPY. No profile choice, generic repository,
hidden commit or implicit transaction is introduced. Empty input is a no-op.

Local analysis keeps its accepted lease/commit/cleanup/progress ordering. Cloud
publication uses the existing cancellation-safe transaction owner for scores,
claim release, job/run counts and immutable receipt identity. Optional timing is
still recorded after the authoritative commit; a lost acknowledgment is resolved
by the existing receipt replay check. Durable projection recovery and tablebase
publication are still pending the next gate; this is not a runtime-ready checkout.

The unused DBInterface evaluation method and its injectable instance are removed.
The old `operations/fen_results.py` now serves only the unchanged backup
continuation helper. Backup/restore code and behavior are untouched. Reviewed
repair and old opt-in integration fixtures have their writer imports updated;
their layout/setup conversion remains explicitly required before the A4 gate.
New canonical integration checks cover their live-publication atomicity, bounded
updates, receipt replay, counters and rollback behavior against the actual schema.

The publication gate passed 136 checks: 25 offline/PostgreSQL, 43 related unit
contracts and all 68 P1 regressions, with no failures, errors or skips. Synthetic
cloud payloads were validated by the real checkpoint validator. This verifies
canonical persistence and receipt behavior without live Stockfish or GCP access.


## A2.3 tablebase persistence boundary

`tablebase_repository.py` owns the shared canonical candidate predicate, numeric-ID
spool, admission/counts, short reads, locked acquisition and bounded publication.
The five-piece policy is shared with the unchanged pure probe algorithm through
`evaluations.py`. Discovery streams 5,000 IDs at a time to temporary disk and
closes its cursor/session; probing holds no database session or FEN locks. Global
frequency order, scoped first-move order and SKIP LOCKED semantics are preserved.
Explicit candidate-ID publication waits for the ordered locks and then performs
the accepted fresh-snapshot score/claim recheck.

The application owns the acquired write transaction, including exact new-score
counts. Canonical publication validates the full bounded batch, refuses missing,
scored or claimed FENs, and uses the existing COPY/set-based update without a
storage-profile branch. Unavailable positions stay unscored and remain eligible
for Stockfish. Partial updates and missing totals fail the whole write unit.
Cleanup covers failures after lease acquisition as well as save/commit failures;
rollback and closure use the accepted P1 shielded cleanup mechanism.

`operations/tablebase_batches.py` is retired after its callers move to the concrete
adapter. Probe algorithms, process limits, result contracts and post-commit
projection calls are unchanged. Durable summary recovery is still the next gate;
this intermediate runtime is not ready for deployment. New integration evidence
will use synthetic probes and an isolated PostgreSQL instance, not actual Syzygy
files, Stockfish, production data or GCP.

The tablebase gate passed 131 checks: 21 offline/PostgreSQL, 42 related contracts
and all 68 P1 regressions, with zero failures/errors/skips. It verifies actual
canonical persistence with synthetic probes; real Syzygy files are not exercised.


## A2.3 durable summary recovery (verified persistence gate)

Version 5 adds `recovery.sql`; versions 1–4 are rejected without automatic
migration. The initializer installs the queue and triggers atomically and catalog
validation covers their definitions. `fen_summary_refresh` has one row per game,
a monotonically allocated revision, retry timing and failure details. It has no
game FK because deleting a game must retain work to remove its derived summaries.

Statement triggers coalesce affected identities in game order. FEN triggers compare
score and tablebase-kind changes before touching associations; metadata, no-op and
empty writes do no association lookup. Cloud-claimed writes rely on their existing
receipt/projection recovery path. Association changes and eligible game ownership
changes retain recovery even when no FEN score changes. Canonical participants
are observed on game, not on the derived game_player view.

Canonical constraints prohibit scored unknown/historical provenance labels. The
old provenance-only routing branch is therefore unnecessary here: unavailable
unscored labels leave evaluation semantics unchanged, while score or tablebase
kind changes require the full refresh. No refresh_engine flag, extra queue or
CUDA dependency is added. Existing report keys remain, with source_only=0.

`summary_refresh.py` contains bounded canonical queue reads and exact-revision
acknowledgment/backoff SQL. `recover_batch` owns guarded transactions, closes its
read session before the callback, requests strict absolute refresh, and only then
acknowledges its captured revisions. A newer change survives success or failure
of an older refresh. Cancellation/acknowledgment errors leave work retryable;
committed scores and projection work survive. No queue code recalculates deltas.

Integration verification uses the actual canonical coverage writer as the callback.
The default player/analytics refresh callback still awaits A3 conversion; complete
reader/GPU integration is not yet claimed. Legacy installer/upgrade entry points
remain only for their A3/A4 caller conversion and are not used by canonical
initialization or recovery. No runtime readiness or production-scale performance
claim follows from this intermediate gate.

Recovery verification: 237 checks passed without failures/errors/skips, including
all canonical writer/schema suites, 20 recovery cases and 68 P1 regressions.
Evidence: `/tmp/chessism-canonical-a2-recovery-20261110-01`. All 1,726 frozen inputs
match and all five disposable containers were removed. The synthetic metadata
UPDATE used 2,005 FENs/4,006 associations: 31.949ms total, 0.989ms queue trigger.
Default player-summary integration still requires the A3 gate described above.


## A3.1 canonical coverage and player accuracy (verified)

Version 6 adds the consumed per-game accuracy projection and manual-run control;
versions 1–5 are rejected without automatic migration. Accuracy keys are
`(game_link,is_white)`, with a cascading game FK; ownership is derived from game,
so a second player name/id cannot contradict that relationship. The existing
manual switch defaults false, matching the previous absent-switch behavior.
Other CUDA publication/invalidation structures remain A3.2 work; no GPU run or
manual activation is performed by initialization.

`operations/game_analysis_summary.py` owns coverage commits and calls the player
accuracy operation after commit, preserving the existing two-stage workflow.
Both use guarded application transactions. The default recovery callback uses
that same supplied session factory and acknowledges only after successful return.
The strict switch and legacy suppression branch are removed: refresh failures
always propagate. Affected-FEN refreshes read absolute counts, never replay deltas.

`database/postgresql/engine_summary.py` owns the canonical SQL calculation without
committing/closing its caller's transaction. It shares/locks the required manual
control singleton against activation, then locks game identities in order before
reading and replacing selected summaries. Manual mode does not touch saved CPU
accuracy. Pure shared formula constants/functions moved unchanged into
`chessism_api/accuracy.py`; CPU/GPU/research readers import them there. The SQL
window, opening prior, score kinds and harmonic/weighted accuracy are preserved.

The focused gate includes 17 actual PostgreSQL cases, including default recovery,
failed accuracy publication after coverage commit, repeated cancellation, manual
mode/activation, self-play/owner changes, repeated FENs and comparison to the
existing independent scalar oracle across 13 synthetic games. Other page readers,
manual CUDA durable publication and legacy fixture adapters remain pending in A3/A4.
No runtime-ready or production-scale claim is made for this intermediate gate.

Summary gate evidence: `/tmp/chessism-canonical-a3-summaries-20261110-01`.
All 285 checks passed without failures/errors/skips, including 68 accepted P1
regressions. All 1,730 frozen inputs match and five owned containers were removed.
Numerical parity covered 13 games/842 positions (maximum error 1.4211e-14);
the scoped SELECT measured 42.795ms. This verifies the default CPU recovery
callback; remaining manual CUDA invalidation/publication is an A3.2 dependency.


## A3.1 analysis coverage pages and previews (verified)

`operations/game_analysis_views.py` owns read/preparation transactions and response
shapes. `database/postgresql/game_analysis_reads.py` accepts that transaction and
queries canonical tables directly, sharing the exact player/eligibility predicate
between scope and selection. Self-play is one selected game; position occurrences
and distinct FEN counts remain separate. The normalized layout's page ordering,
latest/oldest ordering and unchanged fair-month sampler are retained. UTC timestamps
make boundaries reproducible. Score aggregates are computed only for visible rows.

Multi-query pages and previews use repeatable-read snapshots so concurrent deletes
or score changes cannot mix a count/selection from one snapshot with data from
another. Ordinary views are read-only. Prepared previews call the unchanged
prepared-selection builder inside the application-owned transaction; expiry cleanup,
plan metadata and chunks commit or roll back together. Plain previews still count
missing claimed FENs, and prepared previews report their claimed subset separately;
launch remains responsible for its live claim recheck. Route/result fields remain.
The prepared path's existing bounded numeric chunks/digest/TTL are unchanged.

Schema version stays 6. The focused gate adds 18 actual PostgreSQL cases, including
real router serialization, snapshot races, UTC month/date boundaries, duplicate
occurrences, query errors and preparation rollback/cancellation. Existing selectors,
route/sampling/transaction contracts and P1 regressions are retained. Previous
285-check schema/summary evidence is reused for untouched inputs; final full-suite
comparison and older opt-in fixture mappings remain required at A4.

Views gate: `/tmp/chessism-canonical-a3-views-20261110-01`, 135 checks passed,
no failures/errors/skips. All 1,733 frozen source hashes match and five containers
were removed. The synthetic 1,002-game/5,006-association plan aggregated only the
two visible games (0.458ms). This is not historical-scale performance evidence.


## A3.1 player navigation and basic statistics (verified)

`operations/player_game_views.py` owns one guarded, read-only repeatable-read/UTC
snapshot per response through the shared `transactions.read_transaction`. `database/postgresql/player_game_reads.py` contains fixed canonical
queries and requires that transaction. A narrow, feature-specific side projection
replaces the legacy field catalog/query rewrite; no new schema/view/profile exists.
Repeated game-list response formatting is consolidated.

Statistics explicitly select standard nonempty games, preserving the old active
layout's exclusion of variants/custom starts/zero-move metadata. Both self-play
sides count in player statistics and coverage occurrences. Navigation separately
requires tracked, nondeleted profiles, with the existing official-title policy and
alphabetic wrapping. Reading recorded game history does not silently drop rows
merely because their player's profile is untracked/deleted.

Existing public fields, empty aggregate nulls, Decimal averages, invalid-mode
responses and out-of-range page behavior are preserved. Ordering is date, game
identity and side; ties now consistently choose black before white (the existing
recent-page color ordering). This resolves previously unspecified latest/mode
rating ties without allowing a larger old-game ID to displace a newer timestamp.
Opening-frequency ties use ECO ascending. Canonical timestamps cannot be NULL.

Verification maps the prior coverage unit's three SQL-shape checks to stronger
real-data tests: chronological date priority, larger-ID older games, same-date
identity/side ties and the schema's nonnull timestamp constraint. All original
coverage payload assertions remain; the unit now also checks transaction closure.
The new PostgreSQL suite uses an independent in-memory side oracle for statistics,
ratings, ordering and numeric types, and covers snapshots, query failures, repeated
cancellation, profile filtering and actual route serialization. Older opt-in
reference helpers have updated call locations; their fixture conversion remains A4.
Hours/saved metadata snapshots, charts, rankings and other analytics remain the
following A3 gates. Schema version stays 6; no full-runtime readiness claim.

Focused gate: 123 checks passed (40 foundation/PG, 15 related contracts, 68 P1),
including 17 new PostgreSQL cases, without failures/errors/skips. All 1,736 frozen
inputs match; the five isolated containers were removed and independently checked.
Evidence: `/tmp/chessism-canonical-a3-player-views-20261110-01`.

## A3.1 FEN browsing and aggregate reads (verified)

`fen_reads.py` owns concrete canonical SQL; `fen_views.py` and `fen_explorer.py`
own read transactions and response/sampling policy. Explorer queries resolve
participants by numeric IDs, sides from booleans and calendar dates in UTC. Unique
move keys remove duplicate-selection branches. Example selection is deterministic;
its actual replay still has to reach the requested FEN. Frequency lists now break
ties by FEN, matching the canonical pending index. Existing page/filter bounds,
zero-score inclusion, null WDL fields and public scalar types are retained. The
text top-list endpoint formats an unscored NULL without the previous TypeError.

HTTP aggregate reads use the initialized summary singletons and fail if a required
row is missing; they do not silently rebuild projections. Initialized empty data
still returns valid zeros/nulls. Summary arithmetic moved unchanged to the pure
`scored_positions.py` module, used by current refresh callers and read operations.
The page query widens int32 scores before ABS and uses numeric multiplication for
impact, so valid extreme evaluations and large occurrence counts cannot overflow.

`read_transaction` consolidates actual duplicated snapshot setup and delegates
rollback/closure to the unchanged accepted transaction functions. Existing player
views now use it as well. Counts/pages and explorer game/occurrence reads share a
repeatable-read snapshot. Writes are prohibited; errors and cancellation propagate
only after guarded cleanup. The explorer retains three bounded block samples,
100 sampled positions, account/token/TTL behavior, ten-game keyset pages, the
2,000-occurrence cap, per-query timeouts and cached/thumbnail response behavior.
Sampling remains exploratory and may return fewer or no rows for sparse scopes.

Seventeen new PostgreSQL cases cover independent page/statistics oracles, types,
sort/filter boundaries, ties, int32/numeric extremes, actual summary publication,
missing singleton errors, empty data, UTC/numeric participant joins, duplicate
positions/self-play, occurrence caps, replayed labels, snapshot races, read-only
rules, query failures, repeated cancellation and real routes. The old pending SQL
assertion maps to deterministic FEN tie ordering; explorer scope-shape assertions
map to actual PostgreSQL scope membership. No payload or cache assertions removed.
Prior player and analysis view suites repeat; schema version remains 6. Other
summary writers/caches and full baseline comparison remain later A3/A4 work.

Focused gate: all 136 checks passed (57 foundation/PG, 11 related contracts,
68 P1), including 17 new PostgreSQL cases, with no failures/errors/skips. All
1,740 frozen inputs match; five owned containers removed, independently confirmed.
Evidence: `/tmp/chessism-canonical-a3-fen-views-20261110-01`.

## A3.1 dashboard counts and mode rankings (focused verification passed; Stage A paused)

`dashboard_reads.py` contains canonical SQL and requires an application-owned
transaction. `dashboard_views.py` owns guarded read-only snapshots and response
formatting. Maintained totals are read directly; missing required singleton rows
raise, while initialized empty data returns zero counts. Ranking reads no longer
perform lazy rebuilds; the obsolete startup ensure call is removed. The remaining
startup/storage-profile conversion is still A4, not claimed complete here.

Main-character game counts use canonical participant IDs and count each eligible
game once. Ranking projections retain both self-play perspectives, normalized mode
filters and the exact A2 rating/statistics calculation. Ranked rosters require
active tracked profiles; official-title filtering happens before LIMIT, with stable
rating/game-count/name ordering. Game participation counts retain their existing
joined-nonzero policy even for deleted profiles; hiding a profile does not erase
recorded history. UTC output, null-rating defaults and existing public fields remain.
Extraction counts include currently processing eligible games but exclude retained
variants, custom starts and zero-move records.

`database/postgresql/mode_summary.py` now contains the one calculation used by
both ingestion and explicit full/scoped refresh. It accepts the caller's driver
connection and never commits or closes it. None means all players; an explicitly
empty scope is a no-op. `operations/mode_summary.py` owns standalone transactions.
The shared ingestion advisory lock prevents import/rebuild overlap; standalone
operations explicitly use READ COMMITTED so lock waiters see committed source
changes. Delete/reinsert and returned counts form one atomic unit. The accepted
rollback/close mechanism remains unchanged. Original A2 date/link/white-side rating
ties are retained (the basic recent-game reader uses its separately documented
black-side tie). These policies should be considered in the final stage review.

Seventeen new real PostgreSQL cases cover independent side-record oracles, full vs
scoped publication, normalization, self-play, title/deletion/unknown-profile filters,
empty projections and missing singletons, dates/ties, guarded rollback after delete,
concurrent refreshes, a refresh waiting for an actual importer to commit, repeated
cancellation, explicit transaction ownership, read-only failures and actual routes.
Existing explicit-factory tests retain their global-DB prohibition, now targeting
the moved operation. Historical reference tools have updated imports/factory binds;
their full canonical fixture mapping remains A4. Schema version remains 6. No
backup/restore body, ingestion public result, production database or service changed.

The completed dashboard gate passed 153 checks (66 foundation/PG, 19 related
contracts, 68 P1) with no failures/errors/skips. All 1,745 frozen inputs match;
the five disposable containers were removed, independently confirmed. Evidence:
`/tmp/chessism-canonical-a3-dashboard-20261110-01`, also preserved in the safe-stop
archive. No new test or implementation stage was started after the user's stop
request. See `stage-a-resume.md`; Stage A remains incomplete and unaccepted.

## A3 scored-rating projection (canonical version 7)

`operations/scored_rating_summary.py` owns refresh/read transactions and chart
calculation. The concrete adapter reads `game_analysis_coverage` joined to canonical
standard, nonempty games, deriving average Elo from both integer ratings with
numeric arithmetic. It streams one statement snapshot in rating/link order.

Refresh acquires the initialized singleton row lock before reading source, computes
the unchanged weighted Jenks partitions and occurrence counts, then publishes both
JSON payloads/counts in that transaction. Concurrent default READ COMMITTED refreshes
serialize and read fresh source after acquiring the lock. Stronger injected isolation
may raise a serialization failure, which propagates. Rollback/session closure uses
the shared P1 cleanup. Readers see the last committed projection; overview reads
never select the chart column. Missing required rows fail, with no request-time DDL
or rebuild. Empty initialization retains three empty public groups; repeated ranges
for fewer than three distinct ratings retain their existing counting semantics.

Version 7 adds only the required singleton and basic identity/count/JSON constraints;
versions 1–6 are rejected, with no automatic mutation of existing databases. The
source coverage bucket aggregate casts scores to bigint before ABS, supporting the
full canonical integer range. Refresh timing remains unchanged at existing callers;
a committed source change may precede projection refresh as before. Incremental
legacy repair-delta conversion is still pending with the auxiliary consumers.

## A3 global summary publication

`operations/global_summaries.py` owns full/game-count/pipeline/scored-position
refresh transactions; `database/postgresql/summary_writes.py` owns concrete SQL.
Canonical game counts are shared with ingestion, and pipeline refresh reuses the
A2 coverage adapter. Operations require initialized rows and never invent zero
success for missing caches. Database score counts retain the existing nonzero
meaning; scored-position counts include zero evaluations. Bucket, appearance,
side-balance (inclusive 50-cp balance) and WDL formulas are unchanged.

Full FEN refresh locks database_summary then scored_position_summary, reads one
aggregate snapshot, and publishes both caches before committing. New-score
publication uses the same lock order/aggregate definitions inside the existing
Stockfish/tablebase evaluation transaction. Source FEN locks precede the summary
locks; refresh uses MVCC reads and never waits for FEN row locks. A refresher sees
either the committed transition or its later atomic delta, so tablebase no longer
has a separate post-commit scored-position increment that can double count a
concurrent rebuild. Incremental source queries remain bounded to the evaluated
FEN keys. Ingestion keeps its exact new-row/occurrence deltas under the same locks.
Read requests still use the existing read-only adapters. No new DDL/version bump.

The unused legacy snapshot/delta/bootstrap methods were removed with the old SQL.
The pure legacy `fen_ingestion_summary_delta` test is mapped to public position/
appearance arithmetic; actual retry-safe insertion and occurrence deltas remain
covered by canonical FEN PostgreSQL tests, now including integer-minimum scores
and concurrent refresh. Tests of explicit injected factories now verify operation
transactions and reject unowned adapter calls, replacing the obsolete requirement
to supply cached counts from a separate transaction. The profile-wide relational
fixture still awaits A4 retirement/mapping; only its import targets changed here.

## A3 homepage snapshot (canonical version 8)

`canonical/home.sql` initializes the source revision and singleton cache. Eight
statement triggers invalidate structural player/game/FEN/association changes in
the writer's transaction; game eligibility changes are included. Scores, occurrence
counts, processing flags, avatars and joined status do not change the cached union.
Missing revision state fails structural writes rather than losing invalidations.

The cache stores counts and its bounded player-contribution JSON array together.
This replaces the separate homepage roster table: deleting a source profile cannot
cascade away part of a retained snapshot or leave its contribution totals unequal.
Numeric source IDs join live avatars, while names/titles/counts in the JSON belong
to the saved revision. It is a derived snapshot, not authoritative ownership data.

The concrete adapter computes official-title profiles, standard/nonempty games,
distinct FENs and half-unit contributions in one SQL statement snapshot. The
operation keeps the nonblocking transaction advisory lock and existing resource
limits, validates the contribution invariant, and publishes once. Default READ
COMMITTED permits harmless failure-metadata writes from an earlier attempt during
a later build; timestamp-conditional failure recording cannot overwrite newer
published metadata. Source changes after the statement leave a valid older cache
with a visibly stale revision. All sessions use the accepted P1 cleanup mechanism.

Page requests use a read-only UTC snapshot. Live all-mode counts use indexed
canonical ownership plus game eligibility; canonical ownership is a view over game,
so unlike the historical materialized ownership table it necessarily reads that
relation. Requests do not scan moves/FENs/associations or rebuild the cache. Existing
response fields, both self-play contributions, distinct unions, zero-game titled
profiles, live avatars, mode-summary bars, queue identity and five-minute debounce
remain. Legacy profile/startup installers and ORM cache models await A4 retirement.

The old ad-hoc homepage test is replaced by 19 canonical isolated PostgreSQL checks
covering its contracts and new concurrency/deletion/error/cleanup cases; it no longer
uses constants.CONN_STRING. The private historical finalization script consumes the
new JSON roster and no longer needs the migration-only transaction_managed option.
Version validation rejects 1–7 without modifying an existing database. No historical
import/backup restore or deployed resources were used to implement this change.

## A3 player metadata snapshots and hours (canonical version 9)

`canonical/player_analytics.sql` owns state, dirty UTC dates, daily partitions and
seven section snapshots by canonical player ID, with cascading derived-data foreign
keys. No nullable-date migration bucket is needed: canonical game dates are required.
The existing pure aggregation remains unchanged, including both self-play sides,
local timezone/DST handling, last-rating ties, catalog contributions and rounding.

Game transition triggers queue distinct affected owner/day pairs, including old and
new ownership, dates, standard/nonempty eligibility, ratings/results and mode changes.
Profile changes queue full rebuilds; renames also invalidate opponent catalogs.
Game truncation invalidates retained partitions. Score/FEN/processing/avatar/title
updates and duplicate inserts do not dirty metadata. Physical profile deletion
cascades its derived data; demotion/soft deletion hides snapshots and queues removal.

`player_metadata.py` contains canonical SQL. `player_analytics_summary.py` owns the
shared cancellation-safe transaction boundary, calculation and bounded publication.
A batch reads a REPEATABLE READ snapshot, calculates before taking producer locks,
then locks source state before dirty dates, matching ingestion's order. Concurrent
invalidation raises a serialization error before publication. Source changes after
that lock wait and leave a new dirty revision after commit. Partial daily batches
retain old complete sections; the final batch publishes all seven atomically.
Failure metadata cannot overwrite a newer revision or successful publication.

Hours/latest routes use one read-only transaction for cache selection and canonical
fallback; query/database failures propagate. The catalog reader consumes one saved
snapshot, eliminating its metadata/payload read race. Redis ranking keys include
canonical player ID and revision, so deleting/recreating a public name cannot reuse
its old counts. Existing public fields, summary feature flag, empty behavior and
manual CUDA ownership remain. CPU refreshes hold the existing manual-control share
lock when present; no CUDA jobs are launched by this conversion.

Remaining legacy dependency is explicit: the old date/source helpers still used by
Stockfish partitions move unchanged into that consumer, not a compatibility alias
in the canonical metadata operation. Its historical CUDA test imports follow that
move. Stockfish/CUDA/frequency publishers and historical schema installers require
their already planned conversions before A4 retirement. Historical multi-profile
parity scripts remain mapping debt; they are not claimed to pass on canonical V9.
The new focused PostgreSQL suite covers metadata/hours behavior; broader catalog,
distribution and CUDA parity belongs with those consumer conversions.
