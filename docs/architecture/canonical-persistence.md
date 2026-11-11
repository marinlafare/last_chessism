# Canonical PostgreSQL foundation — Stage A, Batch 1

Status: Batch 1 independently accepted with documented limitations at `35be4fc`.
Stage A2–A4 are now authorized and in progress. This remains **not a runtime-ready
replacement database**. Game-import and bounded FEN ingestion now use canonical persistence; remaining
consumers and runtime startup conversion are still pending. Entry code checkpoint
for the foundation: `0fde829`. No historical data is required.

## Scope and sources

The initializer creates 95 application tables, one semantic view, and a version
record in one empty PostgreSQL 15 database. Ten core tables define the normalized
chess relationships. `support.sql` preserves 78 existing account/session,
appearance, ingestion/timing and cloud tables, including the typed cloud document
children. Seven projection tables support game imports and bounded FEN persistence. The supporting
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
