# Numeric FEN references for continuations

The subsequent game-association/player-frequency migration is described in
[FEN_REFERENCE_MIGRATION.md](FEN_REFERENCE_MIGRATION.md). It has a separate
rehearsal, explicit `--reference-tables` switch and coordinated deployment.

This migration preserves `fen.fen` as the portable unique position key and
primary key. It adds a generated, unique BIGINT `fen.fen_id`. Only
`fen_continuation` changes its reference from full FEN text to that numeric ID.
Game/FEN associations, analysis scores, engine settings and cloud file formats
do not change. Local/cloud writes use the same persistence function; identifiers
are resolved in batches (cloud import reuses identifiers it already loaded).

## Required order

1. Create a whole-database backup and pass **Test backup**. Preserve that exact
   recovery point, its full dependency chain, and the matching application
   source/container versions. A portable FEN export alone is not a rollback.
2. Run the isolated migration and backup/import tests. Run
   `scripts/rehearse_fen_identity.py` inside a one-off backup-worker container
   against an isolated restore of that recovery point. Give it a dedicated
   writable report directory; it never connects to the live PostgreSQL server.
3. The rehearsal requires the uncompressed database size plus 80 GB free for
   rewritten relations, indexes, WAL and temporary work. It exclusively reserves
   the normal restore workspace and backup lane. Its temporary cluster listens
   only on a private Unix socket. It checks every continuation and performs a
   portable export/restore roundtrip, then removes only its disposable workspace.
4. After the rehearsal passes, drain analysis/import/ingestion/backup jobs,
   stop the API and all database-writing workers, and check free space on both
   the database disk and WAL archive disk. Do not start new jobs during cutover.
5. Use `scripts/migrate_fen_identity.py --apply` with the exact backup ID,
   expected database name, passing rehearsal report, and a new live report
   path. It refuses other client connections and active cloud jobs. **Do not
   use application startup to migrate:** startup refuses the old/partial schema.
6. Deploy the matching API and worker images together, **including a rebuilt
   cloud-controller** (it embeds the API model and writes imported results
   directly, so merely restarting its old image is insufficient). Check a bounded local
   persistence/backup roundtrip and cloud importer regressions. No Stockfish
   recalculation is needed and no cloud job is required for this migration.
7. Create and restore-test a post-migration database backup before retiring the
   legacy table. Preserve the pre-migration backup chain until acceptance.
   Existing retention keeps two full chains, not unlimited rollback points;
   do not create additional full chains that could expire the protected one.

## Data and recovery guarantees

Adding the parent identity/unique constraint is one transaction. Creating,
copying, indexing, validating and swapping the numeric continuation table is
another. Copy validation compares all row IDs, FEN relationships, ranks, moves,
and scores and checks identical counts. A failure before swap rolls that entire
transaction back, leaving the text continuation table active. The additive
parent identity may remain and is reused on retry.

The old table is renamed to `fen_continuation_legacy_text`, not dropped. It is
a **cutover snapshot**, not a live mirror: after new writes begin, restoring
that table alone would discard later changes. Full rollback requires the
preserved database recovery point and matching old application version, or a
separately reviewed reverse migration that retains new results. Neither script
automatically rolls back a completed cutover or deletes the old table.

The swap refuses external views, inbound foreign keys, user triggers, explicit
grants or row-security policies on the old table rather than silently stranding
those dependencies. It preserves existing continuation IDs and advances the
new continuation sequence; parent numeric IDs have a validated unique key and
foreign-key relationship.

## Backup compatibility

Portable FEN backups retain the existing FEN-text format. Numeric IDs are never
exported. Restore resolves the FEN string to IDs in the destination database;
old export files and databases with independently assigned IDs remain usable.
Export keeps its consistent snapshot and existing incremental checkpoint rules.

Whole-database backups record `fen_identity_version: numeric_v2`. Restore tests
validate the identity column, validated constraints and both identifier sequences.
Pre-migration backups remain testable as `text_v1`; restoring one for actual use
requires its matching old application or this explicit migration before starting
the new application. Backups of a partially migrated schema are refused.

Progress reports contain phases, durations, counts and relation sizes, never
credentials or application rows. Rehearsal/live reports are separate. A
`succeeded` rehearsal does not imply the live schema has been changed.

## Live progress display

From the repository root:

```bash
watch -n 2 'python3 scripts/watch_fen_migration.py'
```

The live runner publishes a heartbeat every five seconds, stage durations,
read-only PostgreSQL activity and supported index/statistics progress counters.
Percentages apply only to the named database substage, never the whole job.
Unsupported statements show activity and elapsed time rather than a fabricated
percentage. Rehearsal timings are a reference, not a completion deadline.
WAL bytes show database activity, not completed rows. The watcher reads only a
local JSON file; it does not query Google Cloud or PostgreSQL.

`succeeded` means the schema migration passed; the app must stay paused until
the matching images are deployed and checked. A failed or stale report requires
inspection, not an automatic restart with either application version.
After deployment is independently verified, `deployment.json` in the migration
directory (alongside `live/` and `rehearsal/`) records
that outcome; the watcher then shows that the app has reopened and reminds the
operator to create and test a post-migration backup.
