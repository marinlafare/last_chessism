# Numeric game and player FEN references

## Expanded game/player refactor: baseline first (2026-10-12)

### Live deployment status — completed 2026-10-12 (local time)

The separately approved live migration committed on attempt 002 in 1,289.1
seconds, with 16 exact row checks and eight summary checks passing. Original
tables were initially retained in `chessism_legacy`; nonstandard games remain
in `chessism_archive`. The separately approved legacy cleanup is recorded below.

The normalized backup `20261012-035736F_20261013-015318I` passed its restore
test. Backup plus verification took 616.2 seconds. The API/UI and all workers
are now running; all 12 database consumers use the persisted `relational_v2`
profile and the rebuilt images. Nine read-route smoke checks passed, production
authentication remains enforced, and the work queues were empty at activation.
Cloud controller startup was verified without submitting a new cloud job.
Startup checks were followed by user-confirmed successful live ingestion,
cloud analysis, and the requested follow-up test. Startup checks alone are not
end-to-end production job tests.

Local evidence is in `.local-migrations/live-game-player-activation-001.json`,
`.local-migrations/db-refactor/live-migration-002/`, and
`.local-migrations/db-refactor/post-migration-backup-001/`. The database still
uses strict synchronous WAL archiving through the maintenance Compose override;
base Compose retains the original archive wrapper. Worker activation did not
restart PostgreSQL or change that archival policy. Review this difference
before the next database recreation. Old images and verified backups remain
available; the obsolete live tables were removed in the approved cleanup below.

### Approved post-validation cleanup — completed 2026-10-12 (local time)

The newer backup `20261012-035736F_20261013-024445I` passed its restore test in
291.97 seconds. With separate user approval, cleanup removed 13 obsolete tables
and their four owned sequences, nine migration-only comparison views, and the
now-empty `chessism_legacy` and `refactor_reference_v1` schemas. Exact identity
guards and `RESTRICT` drops were rehearsed with rollback before committing.
All 4,318 protected catalog entries outside those schemas remained unchanged.

Ten explicitly allowlisted, stopped rehearsal directories were removed from
the dedicated restore-test volume under its existing workspace lock. The
volume and lock remain available for future restore tests. Real backup files,
migration reports, application images, and the two archive tables (9,464
archived games) were retained. Removed legacy data remains recoverable from
the verified backup; the discarded rehearsal copies need to be recreated if
another rehearsal is required.

The live database decreased from approximately 77 GiB to 42.61 GiB. Measured
main-SSD usage decreased from 64% to 49%, reclaiming approximately 143.86 GiB
across the table and workspace cleanup. Rehearsal `du` totals are not physical
space estimates because Btrfs copies share extents. The real backup disk was
not cleaned. Runtime-schema, database-catalog/row-preview, summary, and HTTP
checks passed without stopping or restarting application services. No cloud
job was submitted. WAL archiving reported zero failures.

Local cleanup evidence is in `.local-migrations/cleanup-game-player-001.json`,
with the exact one-off SQL and workspace allowlists alongside it. These scripts
record the completed operation, not a reusable cleanup command; their original
targets no longer exist. No new backup was launched by this cleanup.

### Live deployment runbook — future attempts require separate approval

The full snapshot rehearsal passed in 1,799.6 seconds: 16 exact row checks,
including 63,200,195 game/FEN links; eight summary checks; injected failure
rollback; and canonical runtime/restart checks. It did **not** alter production.
The broad unit-test timeout was reproduced by `asyncio.run(asyncio.to_thread(...))`
in the restricted execution sandbox. Outside that sandbox, the same 159 tests
passed. No application workaround or weakened assertion was needed.

`scripts/migrate_game_player.py` is the expanded migration entry point. Do not
use `migrate_fen_identity.py --reference-tables` for this expanded scope.
The CLI has two distinct commands:

- `prepare`: local evidence/code fingerprint only; no database, Redis, Docker,
  cloud call, service change, or live authorization.
- `apply`: separately approved maintenance only. Exact cluster/database/path,
  stopped-client, fresh-tested-backup and disk/WAL safeguards are mandatory.

The apply transaction locks the old source tables before its repeatable-read
snapshot, rebuilds every candidate, verifies rows, preserves consumed sequence
IDs, switches tables, refreshes summaries, and approves the canonical release
in one commit. No stale shadows are reused or deleted automatically. The old
tables remain in `chessism_legacy`. Application startup does not run this CLI.
An observer reports real database activity and index counters; a five-second
watchdog cancels work if disk reserve or the backup reservation is lost.

The first live attempt rolled back in final summary validation because the live
PostgreSQL container had a 64-MiB `/dev/shm`, while the accepted whole-database
rehearsal had 256 MiB. `db.shm_size` now explicitly matches that 256-MiB setting.
The live preflight checks the PostgreSQL server's POSIX shared-memory mount via
its own `/proc/self/mountinfo`, not the maintenance runner's filesystem. It
refuses insufficient or unverifiable capacity before copying tables. Increasing
only the migration runner's shared memory does not fix the database limit.

#### Before approving downtime

1. Finish/drain all cloud, ingestion, salience, analysis, research and backup
   work. Do not enqueue more. Inspect stopped/manual clients and queued jobs;
   never flush Redis. Cloud work must be finished, not merely have its local
   controller stopped. Confirm exact database/cluster identity from PostgreSQL.
2. Preserve the currently running image IDs and pin them with rollback tags
   **before** building replacement tags. Record the current database profile
   and `SHOW archive_command`. Save this information locally, without exporting
   environment secrets. Build the API/worker, backup-worker and cloud-controller
   images from the reviewed source; do not recreate running services yet.
3. The current archive wrapper intentionally returns success on archive errors
   to prioritize availability. It is unsuitable for this high-WAL migration.
   With separate maintenance approval, use the following strict command:

   ```sql
   pgbackrest --stanza=chessism --archive-async=n archive-push %p
   ```

   Inspect `pg_settings.source` first: this deployment supplies archive_command
   on the PostgreSQL command line, which overrides ALTER SYSTEM. After stopping
   application clients, use a reviewed Compose override of the complete `db`
   command and gracefully recreate only `db`, keeping its exact existing image,
   PGDATA, backup and socket mounts. Verify cluster identity, health and
   `SHOW archive_command` after restart. Keep that override in subsequent
   deployment commands until the new backup is tested. Do not assume a reload
   changed a startup override.

   This direct synchronous command propagates failures instead of deliberately
   discarding WAL. Do not disable archiving or remove an archive-gap marker to
   bypass a check. Verify successful archiving and healthy disk space. Keep
   strict archiving until the post-migration backup/restore test succeeds.
4. With no new application work, make a **fresh backup and Test backup** using
   the existing UI before downtime, or the existing backup-worker job functions
   during approved maintenance with every other client stopped. Record its exact
   recovery-point ID. The CLI requires the
   latest point to be successfully tested, at most 24 hours old, and bound by
   pgBackRest metadata to the approved cluster system ID. If a prior archive
   gap requires a full backup, let the normal backup pipeline create it.
5. Verify local capacity for candidate tables/indexes and WAL: the CLI reserves
   at least max(80 GB, twice backup database bytes) on the database filesystem,
   plus the configured backup free-floor and 20 GB on backup storage. These are
   conservative free-space gates, not predictions of final storage usage.
   Do not delete backups or retained test copies automatically to make room.
6. Confirm the actual database container has at least 256 MiB of shared memory
   after its graceful recreation. Keep the pinned PostgreSQL image, data mounts
   and strict archive command unchanged. Preserve failed attempt reports; any
   approved retry uses a new frozen plan, report directory and container name.

#### Prepare a frozen local plan (safe before downtime)

```sh
.venv-cloud/bin/python scripts/migrate_game_player.py prepare \
  --rehearsal .local-migrations/db-refactor/migration-sequence-001 \
  --plan .local-migrations/live-game-player-plan-001.json
```

Preparing does not approve live work. Application/migration source, dependency
or Compose changes invalidate the plan; review changes and create a new plan.
Rehearsal paths are repository-relative so the plan also verifies inside the
read-only `/candidate` container mount. Do not edit source during execution.

#### Approved maintenance window ONLY

Stop all application clients, keeping **db and redis running**. This causes UI
downtime. Do not use `down`, remove volumes, or kill PostgreSQL. The service
list below matches this repository; verify it again if Compose changes:

```sh
docker compose stop chessism-web chessism-api cloud-controller cloud-billing \
  worker-analysis analysis-telemetry worker-games worker-research worker-algorithms \
  worker-salience worker-backup fen-worker-1 fen-worker-2 fen-worker-3 pipeline-worker \
  stockfish-service
```

The command below is a **template, not something to run during preparation**.
Replace the recovery-point/cluster values with those explicitly approved. The
maintenance Compose file is never loaded by ordinary `docker compose up`. Its
pinned tested image mounts source, PGDATA, backups and archive spool read-only;
only reports and temporary files are writable. It has no Docker socket or
cloud credentials and starts no dependencies. Network/volume names are pinned
to this checkout's `last_chessism` deployment; review for any other deployment.

```sh
docker compose -f docker-compose.migration.yml --profile maintenance run -d \
  --no-deps --name chessism-game-player-live-001 game-player-migration apply \
  --approve-live-migration --confirm-writers-stopped \
  --plan /candidate/.local-migrations/live-game-player-plan-001.json \
  --backup-id APPROVED_TESTED_BACKUP_ID \
  --expect-database chessism_db --expect-system-id APPROVED_CLUSTER_SYSTEM_ID \
  --expect-pgdata /var/lib/postgresql/data --expect-port 5432 \
  --output /reports/live-migration-001
```

```sh
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/live-migration-001/status.json'
```

The full rehearsal took about 30 minutes under 2-vCPU/4-GiB runner limits. Live
data size, a single long transaction, synchronous archival, and disk contention
can increase downtime. The apply timeout is four hours; statement limits are
two hours. Keep the host powered on. No app restart is automatic on success.

#### Deployment after a confirmed commit

1. Check successful report, container exit code, canonical release row, all
   validations, archiving and remaining space. On uncertainty, stay stopped.
2. Persist `CHESSISM_DATABASE_PROFILE=relational_v2` in the deployment `.env`
   without printing secrets. All 12 database consumers share this setting.
   Do not use only a transient shell export that a later restart would lose.
3. Start the rebuilt API and frontend first (with their required dependencies),
   keeping all queue workers and cloud-controller stopped. Check authenticated
   read routes, player/game/FEN counts and the database explorer. Compare with
   active/archive counts in the migration report, not old unfiltered totals.
4. Start only the rebuilt backup worker next, create a normalized backup and
   pass its restore test. This protects the new schema before new work begins.
5. Start the rebuilt workers/controllers with the same explicit profile. Run
   small ingestion, analysis-import and salience checks before admitting large
   jobs. Confirm the same profile survives a coordinated application restart.
6. Review restoring the previously recorded archive command after backup proof;
   never guess or blindly overwrite a concurrently changed setting. Keep old
   images, old tables, tested backups and reports until separately approved
   cleanup. There is no automatic legacy-table deletion.

#### Failure and rollback

- **Before commit:** an error/cancellation rolls back the entire build and
  switch. Verify `rollback_verified`, old public columns and absent canonical
  release marker before restarting the pinned old images with profile `legacy`.
  If the connection/report was lost, inspect the database; do not assume failure
  means rollback or retry blindly. Existing shadows/canonical names block retry.
- **After commit:** do not rename old tables back. Summary caches and shared FEN
  data may have changed, and later writes would be lost. Stop/keep all writers
  stopped, preserve the failed migrated cluster, and use the explicitly tested
  pre-migration recovery point through the manual recovery procedure in
  `docs/DATABASE_BACKUPS.md`. Prefer restoration into a separate empty directory
  and validate it before switching; never restore over the sole current copy.
  Restore the matching matrix/algorithm artifacts if required, and use the
  pinned old images/profile. Recovery drops changes after that backup boundary;
  assess them and reconcile Redis jobs explicitly before restarting workers.

The prepared plan is not live approval. Ask for the maintenance window and
target/backup confirmation before changing archiving, stopping services or
running `apply`.

### Whole migration sequence rehearsal

`scripts/run_db_refactor_migration_sequence.py` joins the previously tested
stages into one independently cloned rehearsal. It starts from the **original
legacy baseline snapshot**, not an already canonical database. This is a fresh
build from an older recovery point, not a fresh backup of the current live DB.

The supervised launch is:

```sh
python3 scripts/launch_db_refactor_http_backup.py --name migration-sequence-001
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/migration-sequence-001/status.json'
```

Use a new numbered name if that report already exists. The supervisor reserves
the backup lane for up to six hours and releases only its own reservation. The
actual runner has no network, live database/socket, backup-repository mount,
Docker socket, or cloud credentials. It has a four-hour overall timeout and
two-hour statement limits. The original stopped source remains unchanged.

Steps performed in the new owned copy:

1. Remove the four earlier experimental schemas **only in the clone**, in a
   transaction that verifies outside catalog objects remain unchanged. An
   unexpected dependent view/constraint aborts and rolls this reset back.
2. Rebuild the archive, numeric identities, normalized tables and tuned indexes;
   validate all copied rows, including the active moves and month ledger. Reject
   unknown game classifications rather than silently dropping games.
3. Exercise injected and completed table-switch rollback.
4. Inject a summary failure after the table switch and check that both the
   namespace and summary caches return to their prior state.
5. Commit the table switch, summary refreshes, validation and private release
   approval in **one transaction**. Keep all original tables in the legacy
   schema; do not reclaim their storage.
6. Restart private PostgreSQL and recheck the runtime contract and summaries.

The watcher exposes elapsed phase time, passed checks, PostgreSQL activity,
index/statistics block counters where available, and generated WAL. It does not
invent an overall percentage for INSERT/validation operations. Reports and the
stopped copy remain available for review. Avoid editing application/migration
source or starting a backup while it runs; source changes invalidate acceptance.

This runner deliberately has **no production target option**. Success still
requires review of its evidence, a current backup with a passed restore test,
and explicit approval of the live maintenance/deployment plan. No production
table deletion, service restart, cloud job or automatic live migration occurs.

The user has selected a broader next migration: archive nonstandard games out
of the active corpus, introduce numeric player/mode/opening identities, reduce
`game_player` to a player/game/side association, reference its identity from
salience, replace the two remaining large text FEN references, and remove the
five unwanted score aggregates while preserving analysis coverage. Keep game
calendar fields, `avg_elo`, association row IDs, salience status, and Stockfish
results. Unrelated audit findings and CUDA work are deferred.

**The implementation below still covers only the two FEN-reference tables. It
is not an approved live runner for this expanded scope.** Do not start the old
live migration or rebuild production services while preparing the candidate.

The first new stage is `scripts/run_db_refactor_baseline.py`. Run it detached in
the pinned current backup-worker image, mounting the candidate repository at
`/candidate:ro` (not over the image's `/app`), backup storage read-only, the
restore workspace volume, and a dedicated writable report directory. Do not
mount production PGDATA, its socket, cloud credentials, or the host Docker
socket. The image's original analytics SQL is frozen into the manifest using
AST extraction without importing/executing that application module. Source
SQL, fixed inputs, schema/index definitions, storage sizes and PostgreSQL
settings accompany repeated timings, output fingerprints and execution plans.

Only the backup-lane reservation uses the shared Redis service; no work is
enqueued. A kernel workspace lock protects the restore lifecycle. PostgreSQL
listens only on a private Unix socket, the connection validates its data
directory and port, and all measurements use read-only transactions. Recovery
must finish naturally before queries begin. The CLI has no database URL option
and forces incidental application DSNs to an unreachable loopback endpoint.

The baseline uses the same tested recovery point for large, medium and small
player histories. It covers game lists/date filters, coverage, salience reads,
position frequency, missing-FEN selection, ordered game positions/moves,
opening/rating filters, and the deployed engine/playing-pattern analytics SQL.
This is **read-query baseline preparation**, not a complete acceptance test.
Write throughput/idempotency, archive/referential validation, full/incremental
salience calculation, application/API behavior, candidate timings and new
backup/restore tests remain mandatory before live cutover. Archived nonstandard
data is an expected active-corpus change, so candidate comparisons must account
for that explicitly, never silently discard mismatching results.

First-observation latency is not claimed to be cold-cache latency. Five warmed
samples and their sample p95 do not establish production tail latency. Before
and after runs must use the same resource limits, settings, inputs and recovery
point; avoid competing local heavy jobs and never flush the host's caches.
Timeouts remain visible and cannot be counted as acceptance passes.

Reports and sampled query inputs remain local under `.local-migrations/` (not
committed). The private database is stopped afterward and retained under a
`db-refactor-baseline-*` directory, outside regular `rehearsal-*` cleanup, for
the later candidate comparison. This uses local disk until explicit cleanup.
No data is archived, table changed, cloud job launched or live app restarted by
the baseline. Its completion never automatically launches a migration.

Monitor with `scripts/watch_db_refactor.py --report REPORT/status.json`. Keep
the accepted baseline manifest/results and pinned image IDs. A current tested
backup must be rechecked before the eventual maintenance window; a baseline
recovery point is not automatically a sufficiently fresh cutover backup.

### Candidate read/schema rehearsal (not a deployable migration)

`scripts/run_db_refactor_candidate.py` continues the stopped baseline workspace
without restoring again. It verifies the successful baseline report, workload
fingerprint, workspace path, private data directory/socket and resource profile.
It refuses existing candidate schemas rather than dropping/reusing their data.
Use the same pinned backup-worker image, 2 vCPU / 4 GiB / 256 MiB shm limits,
mounts and backup-lane/workspace coordination as the baseline. No live PostgreSQL
mounts, application init, cloud credentials or Docker socket are allowed.

The added `medium_verified` cohort is the player closest to 10,000 games among
players with at least 1,000 games and nonzero `player.joined`, excluding the
original three samples. Its old-schema cases run first. Then all 49 cases run
against a standard-game-only reference and the candidate. The original 38
baseline cases remain unchanged and their reports are retained.

Private schemas:

- `refactor_reference_v1`: standard-game-filtered legacy views, for an apples-to-
  apples comparison after archiving exceptions.
- `refactor_candidate_v1`: numeric players, game modes, game ownership, compact
  game-player associations, numeric FEN references, normalized salience/engine
  ownership, opening dictionary and reduced game analysis coverage.
- `refactor_archive_v1`: typed copies of nonstandard game and move records.
- `refactor_read_v1`: read-only-use legacy-shaped projections. Dates/ratings and
  names are joined, not duplicated physically. The FEN workload is explicitly
  adapted to numeric joins; it does not join the full FEN back to itself.

Public tables, source sequences and Stockfish results are not modified. Moves
retain their current physical source table; the read adapter excludes archived
games. Numeric continuation/FEN parent tables are shared read-only by both
versions. All candidate copying/indexing/validation occurs in one transaction;
failure rolls back candidate/archive objects. The reference views are retained
for inspection. Exact value/multiplicity comparisons validate the copied fields;
the large FEN-association comparison uses its row PK plus counts to avoid two
full-width external sorts. Unexpected archived-game FEN contributions abort the
rehearsal because they require a different salience-cache migration.

This phase does NOT complete production API/ORM writer migration, nonstandard
ingestion routing, corpus-summary rebuilding, saved matrix compatibility,
portable backup support or live deployment. Those remain explicit gates.
In particular, read adapters are not an authorization to make existing writers
insert into views. Existing game/player summary caches are retained as baseline
evidence; their corpus counts need rebuilding before the active-game scope can
change in production.

`comparison.json` contains output matches, before/after timings and slowdown
flags. A flag (>20% and >5ms median increase) prioritizes investigation; it does
not define an acceptable regression allowance. All differences/timeouts need
review, and noisy small changes need repeat measurements. `needs_review` means
the rehearsal completed but its comparisons require attention, not that live
data changed. Even `succeeded` is NOT production migration acceptance.

The generic watcher now accepts either baseline or candidate `status.json`.
The database is stopped and all report/source/candidate data retained afterward;
no migration or cleanup starts automatically. The rehearsal has a six-hour
overall limit and two-hour DDL statement limits; query limits/repetitions match
the baseline. PostgreSQL fixture tests cover exact copying, archive scope, ID
high-water preservation, wrong-target guards and transactional interruption.

#### Retuning the completed candidate

`--retune-from /reports/candidate-001` explicitly reuses that stopped, fully
validated candidate instead of rebuilding it. Use a new `--output` directory.
The runner binds the previous backup/workspace, resource profile, all 49 fixed
queries and completed 13 row validations to the baseline. The first report is
never overwritten. The large-table row validations are inherited, not claimed
as newly rerun; `validation_reused_from` identifies the source.

`scripts/db_refactor_tuning.py` first reproduces the original result hashes and
proves whether **all** four recent-games differences are exclusively the
`game.time_elapsed` integer versus `game_player.time_elapsed` floating-point
return type. Different values, ordering or other fields abort. The new SQL
casts this field explicitly; comparisons remain strict and are not weakened.

Retuning adds indexes only in the private candidate schema. Native numeric
queries select bounded game pages before enriching them with player names,
join salience by `game_player_id`, and rank frequency groups before fetching
FEN text. `WITH TIES` preserves the complete boundary group for the original
lexical-FEN tie break; it is not a hard 100-FEN fetch guarantee. Both side
branches apply eligibility filters before LIMIT and retain self-play rows.

Focused correctness checks precede a fresh 49-query reference/candidate timing
comparison with the original resource limits, repetitions and timeouts. The
retune still stops private PostgreSQL and releases the backup reservation,
and still does **not** authorize or complete production migration.

#### Isolated writer contracts and physical backup round-trip

After a successful read comparison, `scripts/run_db_refactor_write_validation.py`
accepts `--baseline /reports/baseline-001 --candidate /reports/candidate-002
--output /reports/write-validation-001`. Use the same pinned backup-worker image,
source/report/restore mounts, resource limits and backup-lane/workspace locks.
It does not modify production source bindings, restart app services, contact
Google, or write to the existing backup catalog/repository.

The candidate store explicitly maps parsed game bundles to numeric players,
modes, game owners and opening dictionaries; variants and their original setup,
rules and move data go to the typed archive. A caller transaction and the private
PGDATA/socket guard are mandatory. Tests use the real bundle formatter and FEN
extractor, retry duplicate bundles/associations, and inject a failure to prove
rollback. A temporary candidate moves table is used for probe inserts because
the read rehearsal deliberately shared the public moves source. It disappears
with the outer rollback; no writer inserts into a compatibility view.

The existing shared local/cloud FEN-result writer is tested for zero scores,
WDL, replacement continuations and retry-safe coverage. The established full
and incremental salience calculations use player-scoped temporary legacy-shaped
working tables and an explicit numeric sink; their results are compared against
independent occurrence math. These are correctness contracts, not yet optimized
bulk-write or production orchestration acceptance. All probe game/data writes
are rolled back. As with ordinary rolled-back PostgreSQL inserts, private
identity sequence counters may advance; stored production data is never touched.

Four existing cloud-import integration tests also run against an empty private
`chessism_cloud_test` database, with generated test schemas and no cloud calls.
They cover both backend checkpoint layouts, atomic receipts, duplicate replay,
and a failure before commit. They test the unchanged import core, not its future
normalized game-summary wiring.

The source private server is then **stopped**. An isolated pgBackRest config
creates a new local repository under `db-refactor-acceptance-*`, takes an offline
full backup, verifies it, and restores into a new `db-refactor-baseline-verify-*`
workspace. Counts, columns, validated constraints and indexes for every candidate
and archive table must match; the writer probes run again after restore. Nothing
is overwritten or automatically deleted. The source and restored servers stop
on completion/failure, before the backup reservation is released. A six-hour
bound applies. Keep sufficient disk for both the repository and restored copy.

`scripts/watch_db_refactor.py` shows phase, check counts and the copy percentage
reported by pgBackRest. `acceptance.json` explicitly retains pending gates:
application/worker/controller integration, global/engine summaries, deletion,
matrix contracts, month ledgers/concurrency/bulk throughput, online WAL archiving,
the backup UI/manifest orchestration, and a fresh tested pre-cutover backup.
Passing this offline round-trip is **not** acceptance of the live backup pipeline
or authorization to deploy the new database schema.

## Scope and deployment boundary

This is the second explicit migration, after numeric continuation references.
It changes **both** `game_fen_association.fen_fen` and
`player_position_frequency.fen_fen` to `fen_id BIGINT REFERENCES fen(fen_id)`.
The parent keeps the full portable FEN text. Association row IDs, game links,
move numbers/colors, player names/colors, both frequency counters and all engine
results are preserved. No Stockfish recalculation is needed.

The candidate API, DB workers, backup worker and cloud-controller must NOT be
deployed against the old tables. Startup refuses a text/partially migrated
reference schema before `create_all`. The already-running image is unaffected
by preparing source changes. Do not run `docker compose up --build` until the
maintenance cutover is coordinated.

Ingestion still transports portable `fen_fen` strings between its jobs. At the
DB boundary it resolves unique FEN keys in batches of at most 1,000 lookups,
then inserts numeric associations. Missing/conflicting IDs fail the transaction;
retrying identical game/move/color links does not create duplicates. Cloud
files, portable FEN backups and browser payloads retain FEN text, never local
numeric IDs. Stored matrix definitions contain catalog column keys, not SQL;
their compiler now joins numeric references. Existing matrix arrays do not
need rewriting. Salience staging and stored frequency lookups both use IDs.

## Validation before live cutover

1. Keep a successful whole-database backup plus its passing isolated restore
   test, dependency chain, and matching current application images/source.
   A portable FEN-only backup is not enough. Use the newest tested recovery
   point; create/test a new one if data has changed since the last backup.
2. Run isolated PostgreSQL regressions: migration, full/incremental salience,
   idempotent ingestion, cloud imports/game drafts, summaries, explorer, matrix
   queries and portable backup roundtrips. No tests should target production.
3. Rehearse on a complete isolated restore using the backup-worker environment
   and candidate source mounted at `/app`. Supply a dedicated writable report
   folder and pass `--reference-tables`:

   ```sh
   python scripts/rehearse_fen_identity.py \
     --reference-tables \
     --backup-id VERIFIED_RECOVERY_POINT \
     --report /reports/status.json
   ```

   Do not run that command directly in the live database container. The runner
   uses its private restore cluster/socket, reserves the shared backup lane and
   requires backup database bytes plus 80 GB of free rehearsal space. It stops
   and removes only its restored workspace after success; failures retain it
   for diagnosis. Phase timings, index progress and heartbeat are written to the
   report without application records or credentials.
   After PostgreSQL accepts connections, the runner explicitly waits for
   recovery to finish and the connection to become writable before starting
   WAL progress queries or migration work. This readiness check has a
   five-minute timeout and never forces promotion. Each retry gets its own
   workspace and backup-lane owner token; a failed restored copy is preserved.
4. Monitor the host report from the repository root:

   ```sh
   watch -n 5 'python3 scripts/watch_fen_migration.py --report .local-migrations/fen-references/rehearsal/status.json'
   ```

   The isolated rehearsal is not a live migration. Compare measured sizes and
   timings before scheduling downtime; no storage/time savings are guaranteed
   from the small regression fixtures.

## Live cutover (separately authorized)

Drain ingestion, imports, local/cloud analysis, salience, backup and other DB
jobs. Stop the API and all DB-writing clients, including cloud-controller.
Preserve current image IDs and verify DB/archive disk headroom. The live runner
requires no other DB client connections and no active cloud jobs:

```sh
python scripts/migrate_fen_identity.py \
  --apply --reference-tables \
  --backup-id VERIFIED_RECOVERY_POINT \
  --expect-database chessism_db \
  --rehearsal-report /reports/rehearsal/status.json \
  --report /reports/live/status.json
```

The runner rejects a continuation-only rehearsal for this migration. Both new
tables are copied, indexed, foreign-key-validated and compared row-for-row
against the originals. Every game association and every frequency counter
must match, and counts must match. Original and numeric FEN identity sequences
are not reallocated. The association sequence preserves its prior high-water
mark, including sequence values used by rolled-back/duplicate inserts.

Both tables switch in **one transaction**. Failure at any stage before commit
leaves both originals active. Known homepage invalidation triggers move to the
new association table, now watching `fen_id`; unexpected dependencies fail
closed. Retained originals have their outgoing foreign keys and homepage
triggers detached so they neither block subsequent game/player deletion nor
change through cascades. This does not disable any constraint on active data.

Deploy matching API, all DB writers, backup worker and rebuilt cloud-controller
together. Independently validate the numeric schema, summaries, a bounded
ingestion/import path and application health before reopening normal work.

## Backups and retirement

New whole-database manifests use schema version 6 and record both
`fen_identity_version: numeric_v2` and `fen_reference_version: numeric_v1`.
Restore validation checks the new references, keys, deletion rules and sequence.
Earlier backup manifests remain testable with the original reference layout;
restoring them for app use needs matching old images or this migration.

The migration never deletes these retained snapshots:

- `game_fen_association_legacy_text`
- `player_position_frequency_legacy_text`

They are not dual-written rollback mirrors. Do not swap them back after new
work has been ingested. Keep recovery backups until a post-migration backup
passes its restore test. Then, with explicit approval and a fresh dependency
check, retire only those exact old tables using `DROP ... RESTRICT` (never
`CASCADE`). Make and test a cleaned-layout backup afterward. Account for the
two-full-chain retention policy; do not expire the protected recovery point.

## Expanded game/player refactor: application integration (not deployed)

The larger game/player normalization is a separate rollout from the two-table
commands above. **Do not use those commands to deploy this expanded candidate.**
The existing read and writer/backup rehearsal reports remain historical results,
not approval of the new application integration.

Application repositories now have an explicit `RelationalStorage` session
contract. Default sessions keep their existing behavior. The optional
`init_db(..., relational_storage=...)` path verifies the normalized layout and
skips all legacy startup DDL. Canonical `public` storage also requires a
`relational_storage_release` approval marker; nothing creates that marker at
startup or in these tests. Compose services have not been changed or rebuilt.

Integrated paths:

- Typed game ingestion, numeric players/modes, compact player/game ownership,
  opening dictionary and nonstandard-game archive. Inputs are copied into
  bounded temporary stages; retries skip already committed children. A single
  transaction advisory lock serializes the ingestion commit (not downloads or
  analysis) to keep cross-schema routing and month counts exact.
- Extraction claims/state and numeric FEN associations; shared local/cloud
  result persistence; absolute coverage refreshes and numeric engine summaries.
  Coverage locks affected games in order and recounts committed results, so
  overlapping retries cannot add scores twice.
- Native full/incremental salience sinks, summary/queue state, player deletion,
  projection repair and explicit numeric homepage-invalidation triggers.
- Saved matrix `average_abs_score` remains available, calculated only for a
  selected bounded output, without a stored per-game aggregate. Scored-game
  pages likewise compute legacy response fields for visible rows only;
  fully-scored rows now tie-break on game ID rather than maximum score.
- Version-7 backup manifests describe the relational columns, constraints,
  indexes and read views; restore validation rejects mismatches. Older backup
  versions retain their previous validation paths.

`tests/check_relational_application_postgres.py` exercises actual application
entry points in a disposable PostgreSQL 15 container with **no network** and
**no production/backup-volume mounts**. It checks overlapping ingestion,
rollback, archived-game deduplication, month counts, disjoint extraction claims,
zero-score/replayed coverage, engine summaries, full/incremental salience,
matrix queries, global summaries, deletion protection, a 2,001-game batching
boundary, four import/receipt core tests, and an offline physical pgBackRest
roundtrip through the application's version-7 manifest validator.

The fixture does not establish production-sized throughput or online recovery.
Latest integration validation (2026-10-12): all fixture stages above passed,
including the physical backup/restore. The targeted regression run completed
210 tests: 181 passed, 29 skipped, zero failures. Four real cloud-import core
tests were additionally run against an empty private database by the fixture.
The bounded 2,001-game / 8,004-move insertion plus replay took 0.376 seconds in
the last disposable run; this is not a production throughput estimate. All
private databases stopped, and no live service was rebuilt or restarted.

Before live cutover we still need:

1. A production-sized rehearsal of these **application** entry points, including
   latency checks for read-view consumers versus the tuned native benchmark
   queries. The earlier 49-query report does not prove all application calls
   have the same execution plans.
2. Online WAL archiving and complete UI/worker backup orchestration validation,
   including matrix/algorithm companion artifacts and retention behavior.
3. A complete expanded migration/cutover runner, canonical read views, explicit
   coordinated API/worker/controller configuration, fresh tested recovery point,
   and separately approved live downtime. No old table or backup retirement is
   authorized by the integration fixture.

### Production-sized application and online-backup rehearsal

`scripts/run_db_refactor_application.py` uses the **separate restored copy**
from `write-validation-001`, never the original candidate workspace or live
database. It reserves the backup lane and shared rehearsal workspace, requires
the same 2-vCPU / 4-GiB resource profile, and refuses reused report directories
or an already-used application copy. Existing recovery backups are not mounted.

The runner completes private physical moves and the month ledger, including
its own sequence and foreign key to the candidate player table. It exercises
36 actual application read calls across the four recorded player cohorts,
interleaving reference/candidate warmups and three measured samples. Reference
engine/pattern analytics use the deployed image's literal text-FEN SQL; candidate
analytics use current numeric-FEN SQL. Payloads and array order are compared
strictly. A >20% and >5-ms latency increase is flagged for review, not accepted
as a permissible regression. This is not an HTTP/network benchmark.

Committed write probes run only in that disposable copy: 2,001 games with two
concurrent submissions, typed variant archival/retry, concurrent recounts for
100 existing games, and real-player salience. Canonical FEN scores are not
overwritten. Reports record each stage rather than hiding incomplete work.

The backup stage runs the actual application backup worker functions with a
fresh private repository/configuration and private status/catalog paths. It
creates a full and incremental chain while committed probe writes continue.
After the incremental backup, a known extra commit is archived, followed by a
later commit. Recovery targets the time between them and must include the first
but exclude the second. The version-7 schema validator and matrix-definition,
algorithm and matrix-file validators run against the restored database.
The existing two-full-chain policy is retained; this run does not exercise
old-chain expiry. Live backup retention is untouched.

Worker Redis transport is substituted inside this isolated harness; the real
backup-lane reservation is held separately for the whole run. Thus this checks
worker orchestration, NOT the full HTTP/ARQ/UI path. Canonical-schema promotion,
trigger/dependency relocation, coordinated service configuration and live
downtime remain separate gates. `production_ready` always remains false.

The no-network disposable online fixture passed full/incremental backup,
post-backup WAL replay, recovery boundary, a real matrix companion file,
saved matrix instructions and algorithm results. The targeted rehearsal and
backup unit suite passed 48 checks. These do not predeclare the outcome of
the production-sized run. Source hashes are recorded and checked at completion;
avoid editing backend/scripts during the run.

Run monitoring locally (no database or cloud request):

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/application-001/status.json'
```

### Read-only application retune after the online rehearsal

The first application rehearsal passed the write and online-backup/WAL checks,
but reported 16 response differences and nine latency flags. The four chart
responses include a fresh top-level `generated_at` on each call. The comparator
now validates that timestamp's timezone-aware syntax and excludes only that
top-level metadata field. Nested timestamps, numeric types, list ordering and
every other value are still compared strictly. Reports include raw hashes,
comparable hashes, difference paths and within-side variations.

Normalized player-scoped readers now query the compact ownership association
directly and join only the game fields/dimensions they require. Count queries
do not join the game table at all. Termination aggregates no longer expand
the entire compatibility view. Ownership and both self-play perspectives are
preserved. Explicit side and mode tie-breakers make tied list/rating results
deterministic in both storage paths. Production sessions remain on legacy
storage; this change does not activate the migration.

`scripts/run_db_refactor_read_retune.py` reuses the **untouched original read
candidate**, not the application copy modified by committed write probes. It
requires the accepted candidate report and the completed application's passed
write/backup checks, reserves the backup lane/workspace, and starts private
socket-only PostgreSQL. All measured sessions are read-only; it does not perform
DDL, ingestion, backups or live deployment. It retains the resource profile,
alternating warmups/three timed samples and the >20% AND >5-ms slowdown gate.
EXPLAIN plans are collected outside timing, without executing a second query.
Source fingerprints must remain unchanged throughout the run.

The `--focused` option checks the seven retuned readers on the three smaller
cohorts. The first focused pass (`read-focus-001`) passed 21/21 comparisons with
zero slowdown flags. The full pass must still check all 36 calls, including
the largest player and engine/pattern analytics. The previous online-backup
report remains separate; a read-only pass does not rerun or replace it, and
`production_ready` remains false pending the cutover gates above.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/read-retune-001/status.json'
```

#### Rating-history query retest

The full `read-retune-001` pass completed 36/36 calls in 5m12s: all results
matched, with no errors and one remaining latency flag (Magnus rating history,
61 ms reference versus 87 ms candidate). Its original report is preserved.

The rating reader now uses the existing game-side player/mode/date indexes to
select narrow rows before checking the compact ownership association. It
preserves both self-play perspectives, filters before daily selection, computes
the same local date, selects the latest `(played_at, link, side)` per mode/day,
then decodes mode names. No persistent summary, index or schema change is needed.
Legacy sessions retain the original query and response-building code.

`--ratings-only --repeats 5` tests 20 cases: four player cohorts, all/bullet/blitz/
rapid histories and a calendar-year window. `--analyze-plans` optionally captures
actual EXPLAIN ANALYZE/BUFFERS plans outside the timed samples using read-only
connections. Unlike default EXPLAIN, this option **executes the SELECT again**.
Strict result equality and the original slowdown threshold still apply. The
first experimental pass (`ratings-001`) identified additional filtered-query
slowdowns; it remains recorded rather than being overwritten or accepted.

The disposable fixture covers empty/archived-only players, both sides, self-play
ties, null dates/modes, local midnight, daylight-saving dates, one-sided date
bounds and mode filters. Keep Docker running and avoid backups while the private
production-sized test holds the shared backup/restore lane.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/ratings-002/status.json'
```

`ratings-002` completed in 38.7 seconds: **20/20 exact matches, zero errors and
zero slowdown flags**, using five interleaved timed samples per side. Magnus's
all-time rating history measured 80 ms reference / 55 ms candidate; the largest
player measured 1,281 / 551 ms. These are medians from this run, not guaranteed
HTTP latencies. The original 61 / 87 ms failure is retained in `read-retune-001`.
Private PostgreSQL stopped afterward. This resolves the rating-query gate; it
does not authorize live cutover or replace the earlier backup acceptance report.

### Canonical namespace switch and rollback rehearsal

`scripts/run_db_refactor_cutover_preflight.py` inspects the **accepted online-
restored application copy**, not the original baseline or live database. Its
default mode is read-only metadata collection. `--rehearse-switch` additionally
exercises the switch in private transactions that **always roll back**, including
the successful path. There is no commit, live-URL or release-approval option.
It uses the same backup-lane/workspace reservations and stops private PostgreSQL
before releasing them. New report directories preserve all earlier evidence.

The snapshot inventory (`cutover-preflight-001`) found 42 relevant relations,
43 foreign keys, eight custom triggers, 34 view-dependency edges, 13 owned
sequences and one stored function. Only two external FKs need reattachment:
`player_stats.player_name` and `main_character_mode_summary.player_name` to the
replacement player table. Retained original FEN-association/frequency tables
must be detached from active `fen` FKs so they cannot block or follow new data
deletion. Their own internal relationships remain in the retained snapshot.
Unknown triggers, external FKs, public views, functions, ACLs or RLS policies
fail closed for explicit review; dynamic SQL cannot be proven safe by catalogs
alone. The eventual live runner must inspect the live dependencies again.

The switch rehearsal moves original tables to `chessism_legacy`, promotes the
already-built private candidate tables to `public`, and renames archive/read
schemas to `chessism_archive` / `chessism_reads`. It reattaches and validates
the two external FKs, installs eight numeric homepage-invalidation triggers,
checks the canonical storage contract and ensures no active FK or canonical
read view still points to retained legacy tables. It does not create a release
marker, seed/reset sequences, copy large tables, rebuild summaries, start app
services, or commit any of those changes.

An injected failure occurs after moving the old tables but before promoting
the replacements. After that failure and after the complete switch, rollback
must restore exact relation/constraint/trigger identities, definitions and
sequence states. The disposable PostgreSQL fixture passed. The production-sized
`cutover-switch-001` rehearsal passed all nine checks in about one second. That
is **metadata-switch time only**, not an estimate of full live migration/downtime.
Both transactions rolled back, the private server stopped, and live services
and existing backups were untouched.

Preparing this stage also exposed an advisory-lock collision: normalized
ingestion/projection repair used the cloud controller's lifetime lock. They now
share the dedicated transaction lock `731946237` instead. A private two-writer
test confirms it is independent of controller lock `731946219`, still enforces
mutual exclusion between ingestion and projection repair, and releases at the
transaction boundary. Cloud/schema startup locks were not changed.

Remaining before a live cutover: production copy/validation/sequence-high-water
runner, coordinated API/worker/controller startup configuration, canonical
application/restart/HTTP-ARQ backup acceptance, active-corpus summary refresh,
a fresh tested recovery backup and explicit maintenance-window approval.
No retained originals or recovery backups have been retired. Passing this
rollback-only stage is not production readiness or permission to deploy.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/cutover-switch-001/status.json'
```

### Explicit service profile and committed private restart rehearsal

All 12 Compose database consumers now receive the same
`CHESSISM_DATABASE_PROFILE` setting, defaulting to **legacy**. API, ARQ workers,
algorithm worker, salience CLI, controller, and matrix-storage CLI share the
runtime adapter. `relational_v2` selects only `public` / `chessism_archive` /
`chessism_reads`; it requires the complete normalized schema and approved
`relational_storage_release` marker. Startup does not create that marker or
perform migration DDL. Unknown profiles and mismatched legacy/normalized
schemas are rejected. Controller validation happens before cloud-client
construction, retaining the caller's SSL options. Nothing has been activated
by this source change: do not rebuild/restart the production stack yet.

The private table-copy preparation now preserves move and month sequence
high-water marks, including IDs already consumed by deleted/rolled-back rows.
Canonical rehearsal also checks the copied association sequence and keeps an
already-higher target value. Original source sequences are never modified.

`run_db_refactor_release.py --application REPORT --output NEW_REPORT` clones
the accepted, stopped online-restored database into a **new private workspace**.
It holds the backup/restore reservation, requires sufficient disk space, rejects
external tablespaces/symlinks, and uses an independent copy (never hard links).
It promotes the clone, tests refusal while the private release marker is false,
enables that marker on the clone only, and checks both shared runtime initializer
paths. It stops/restarts private PostgreSQL and repeats the checks in a fresh
Python process, comparing the typed backup-schema contract. No actual API/ARQ
consumers or cloud clients are started. The clone and reports are retained,
and the private server is stopped before releasing the backup lane.

The disposable fixture passed committed promotion, approval/profile rejection,
sequence preservation, bounded read/write transactions, and PostgreSQL restart.
This is a **startup/restart gate**, not a repetition of the complete read
benchmark or a full production deployment. The runner deliberately still has
no live target or commit-to-production option. A live copy/validation runner,
active-corpus summary refresh, real HTTP/ARQ backup acceptance, a fresh tested
backup and maintenance-window approval remain required before live cutover.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/canonical-release-001/status.json'
```

`canonical-release-001` succeeded with 16 checks in 6.5 seconds on the independent
production-sized copy. That is copy/promotion/startup/restart time using the
already-prepared candidate, **not full migration or application/backup test
time**. The source remained stopped and unchanged; the rehearsal server stopped
afterward. The accompanying offline suite passed 47 tests, and the disposable
real-PostgreSQL copy/sequence/switch/restart fixtures passed. Full service
lifespans, queue execution, summary rebuilds and the live runner remain gated.

### Real HTTP / Redis / ARQ backup acceptance

`scripts/launch_db_refactor_http_backup.py --name http-backup-NNN` starts a
detached host supervisor. It takes an owner-checked reservation of the normal
backup lane, then launches the test runner with **Docker network=none** and a
throwaway Redis sharing only its loopback namespace. No ports are published;
the runner has no Docker socket, production PostgreSQL data/socket, production
backup repository or cloud credential mounts. Redis persistence is disabled.
Only the supervisor accesses Docker and the live backup reservation key.

The runner clones the accepted canonical-release database into a new private
workspace, preserving the accepted source. It starts PostgreSQL with private
WAL/backup paths, the current `main:app` with its real lifespan, and the actual
`worker.WorkerSettings` on an empty private backup queue. Environment variables
are allowlisted, dotenv loading is disabled and authentication uses a new
throwaway admin created through the actual gate/signup endpoints. No auth,
Redis, queue, database, backup or restore dependency is mocked.

The test checks unauthorized rejection and representative authenticated API
reads, submits a full backup and incremental backup through HTTP, waits for
the real ARQ job results, checks the v7 canonical schema manifest, then submits
the real restore-test endpoint. That worker validates FEN identity, the
normalized schema, summary invariants, matrix recipes/files and algorithms,
and removes only its own disposable restore copy after success. Finally the
API/worker restart and must retain authentication and the successful restore
status; the private backup reservation must be released.

Restore workers allocate a short, unpredictable `/tmp/chessism-pg-*` socket
directory with mode 0700, independent of the data workspace depth and `TMPDIR`.
They validate the full PostgreSQL socket filename against the 107-byte Linux
limit before restoring any data. TCP remains disabled. The empty socket
directory is removed only after the owned cluster is confirmed stopped; an
unresolved postmaster PID preserves the workspace and socket for inspection.
The first HTTP acceptance attempt (`http-backup-001`) completed both backups
but could not start restored PostgreSQL because its nested socket path was too
long. It did not validate the restored database; retain its report as failed.

Only backup queue consumers run: automatic API work can enqueue into private
analysis queues but is never executed. No cloud controller is started. This
tests backend HTTP behavior, not browser rendering or a fresh full migration.
Active-corpus summary refresh and the final production copy/cutover runner
remain separate gates.

The test keeps its migrated clone, private backup repository and logs for
review. API/worker/private PostgreSQL stop on exit; the host supervisor stops
and removes only its ephemeral Redis container and releases only its own
backup reservation. The runner container remains available for logs. Do not
start another backup/restore or edit application code during this test.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/http-backup-002/status.json'
```

### Active-corpus summary / rollback acceptance

`python3 scripts/launch_db_refactor_http_backup.py --name corpus-finalization-001`
uses the same owner-checked backup reservation and resource-limited,
network-none runner, but starts **no Redis, HTTP, queue or cloud processes**.
It requires successful `http-backup-002` evidence for the exact accepted
canonical source. A new independent copy is made; the accepted source, original
tables and previous reports remain intact.

`scripts/db_refactor_corpus.py` reuses application summary functions with an
explicit session factory. Sessions join one REPEATABLE READ transaction via
savepoints, so helper commits cannot publish a partial migration. Database,
pipeline, scored-position, tracked-player mode, salience totals, rating-group
and homepage summaries are refreshed and checked against typed base tables.
Archived games must have no FEN associations: otherwise this stage refuses to
preserve the existing frequency/per-game analyses and requires a full rebuild.
Pending/missing salience work remains stale, never automatically certified as
ready. FENs, scores, games and retained original tables are not changed here.

The runner blocks the private release before work, proves an injected failure
rolls back the cache fingerprints, then commits all validated summaries and
the **private-only** approval flag together. PostgreSQL restarts; runtime
checks, counts, cache fingerprints and the accepted schema contract must still
match. A tiny disposable fixture additionally rolls back the entire successful
refresh and tests the archive-with-FENs refusal. Temporary socket directories
are short/private and removed only after their owned PostgreSQL has stopped.

This stage does **not** execute or authorize the live migration. The final
fresh-source copy/cutover orchestration remains to be built/reviewed after
these results. The live sequence must: quiesce all writers/queues/controllers;
require a fresh, successfully restore-tested backup; verify source/dependency
and disk-space preconditions; copy/validate the normalized and archive rows;
preserve sequence high-water marks; switch namespaces with the release blocked;
finalize/validate caches; and enable the coordinated relational runtime only
after acceptance. Retained tables are not permission for an unsafe hot rollback:
before reopening writers, rollback can restore the quiescent recovery point;
after new writes, recovery needs explicit WAL/data reconciliation. No legacy
tables or existing recovery chains are retired by this step.

```bash
watch -n 5 'python3 scripts/watch_db_refactor.py --report .local-migrations/db-refactor/corpus-finalization-001/status.json'
```
