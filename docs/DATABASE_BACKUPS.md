# Chessism database backups

Chessism keeps these recovery files on the backup disk whose UUID
is `1ebffac6-4b7a-4906-9e07-9586060d3825`:

- `/main-monitor-db-backups/chessism/database/` is a pgBackRest repository for
  complete PostgreSQL recovery.
- `/main-monitor-db-backups/chessism/fen-analysis/` contains the portable,
  compressed FEN-analysis export.
- `/main-monitor-db-backups/chessism/research/matrices/` contains separate copies
  of legacy completed snapshots, synchronized by the manual database backup.

New matrix definitions are small instruction records in PostgreSQL's
`matrix_definition` table. They are included directly in physical full and
incremental database backups. Live previews write no files and are never
backed up. Schema-version-4 recovery manifests record a definition count and
SHA-256 fingerprint; the manual **Test backup** compares the restored
instructions against it. No matrix materialization is triggered by a backup.
Older recovery points remain testable, but report definition verification as
not recorded. A database backup must be made after saving a definition to
protect that new definition; this remains a manual operation.

Legacy working matrix files are in the project's ignored `research_data/matrices/`
folder, not in PostgreSQL. A physical database backup by itself only contains
their metadata. New recovery-point manifests include a `matrices` list of UUIDs
and checksums. The worker copies only new snapshots, reuses unchanged copies,
and publishes no successful combined backup if matrix copying fails. Temporary
failed copies are removed; previously valid copies are never overwritten or
pruned. Deleting a legacy snapshot only deletes that working copy. Deleting
instructions only removes the definition row; existing snapshots and previous
database backups are unaffected.

During backup, recipe saves/deletes and legacy snapshot completion/deletion are protected by a database
advisory lock until pgBackRest has finished. Existing completed matrices remain
readable; Stockfish and game ingestion are unaffected. The `saving_matrices`
phase reports snapshot counts separately from the database byte progress.

The existing `/main-monitor-db-backups/timeshift/` directory belongs only to
Timeshift. Chessism never creates, edits, moves, or removes anything inside it.

## Storage policy

- Chessism allocation: 200 GB.
- Protected filesystem free-space floor: 150 GB.
- First database backup: full.
- Storage format: pgBackRest bundled block-incremental backups.
- Normal subsequent backup: block incremental.
- New full chain: after 10 incrementals, after 7 days, or after interrupted
  WAL archiving.
- Retention: two complete full-backup chains.
- Trigger: manual only, from the Backups page.
- The age and incremental-count rules only select the type after the superuser
  clicks `Backup database`; they never schedule or start work themselves.
- Storage reporting separates backup-chain files, continuously archived WAL,
  FEN exports, research artifacts, and other metadata. The cached measurement
  refreshes at most once per minute while the Backups page is open.
- Validation: pgBackRest verifies the selected recovery point and required WAL
  before Chessism publishes a successful status.
- Restore rehearsal: manual only, from the superuser-only `Test backup` button.

While a backup runs, its status file and Redis job progress contain pgBackRest's
factual completed and total byte counters. The Backups page uses those counters
for the transfer bar and a rolling 30-second throughput estimate for the
transfer ETA. Verification is shown as a separate final phase because
pgBackRest does not expose a byte-total percentage for that step. Elapsed time
continues across every phase and is reconstructed from the persisted start
timestamp after a page refresh.

## Block-incremental migration

Repositories created before block-incremental storage was enabled remain intact
while Chessism creates and verifies one new full backup containing the block
maps needed by future incrementals. Only after that verification succeeds does
Chessism expire the superseded legacy full and all incrementals that depend on
it. Later button presses return to incremental backups and store changed blocks
instead of recopied multi-gigabyte relation files.

The catalog records the storage mode per backup. If the block-enabled full fails,
the legacy chain remains available and the next manual attempt still requests a
full backup. Superseded chains are expired through pgBackRest, never by deleting
repository files directly. Normal block-enabled chains continue to use the
two-full-chain retention policy.

If the external volume marker is absent, the API reports the repository as
unavailable and does not fall back to the system disk.

## Initial host setup

Review and run `scripts/setup_backup_volume.sh` as root. It mounts the ext4
partition by UUID, verifies the existing Timeshift directory, creates separate
application directories, and copies the existing FEN-analysis files. It saves
a timestamped copy of `/etc/fstab` before adding the mount.

## Recovery boundary

The pgBackRest repository restores the complete PostgreSQL 15 cluster. It does
not restore Redis queues, source code, `.env` secrets, Syzygy files, or logs.
The FEN-analysis export remains independently restorable into regenerated FEN
rows.

After PostgreSQL recovery, matrix working files can be restored separately:

```sh
docker compose exec -T chessism-api python -m chessism_api.operations.matrix_constructor.storage_cli restore --backup-id RECOVERY_POINT_ID
```

This verifies and copies the matrix UUIDs referenced by the restored database,
then rebases their metadata to portable relative paths. Retain the matrix
backup directory alongside the PostgreSQL backup chains. Old database backups
without a matrix companion manifest cannot claim matrix-file recovery coverage.

Full database restoration is intentionally not exposed in the UI. It must be
performed while the database is stopped and should first be rehearsed in an
isolated PostgreSQL 15 container.

## Disposable restore test

`Test backup` does not replace the live database. It serializes through the
same backup queue, selects the latest verified recovery point, and restores it
into the local Docker volume `database_restore_test`. The worker is limited to
one CPU, starts the restored PostgreSQL 15 cluster only on a private Unix
socket, validates the required schema, durable summary rows, and FEN primary
key, then stops PostgreSQL and removes the restored files.

For new recovery points, the rehearsal also compares completed matrix UUIDs in
the restored database with the recovery-point manifest and hashes every matrix
backup file. This is read-only and does not replace working matrices. The
additional validation remains manual, as part of `Test backup`.

The backup worker also removes any leftover `rehearsal-*` workspace when it
starts. This recovers the temporary disk space if Docker or the host was
stopped before the job's normal success/failure cleanup could run. Unrelated
files in the restore volume are never selected by startup cleanup.

The test requires the uncompressed database size plus 15 GB of free local
space. It never runs on a timer or as a side effect of creating a backup; a
superuser must click the button. The pgBackRest repository is used only as the
restore source. Test status is recorded independently in
`restore-test-status.json`, and the exact catalog row and manifest are marked
passed or failed. A failed rehearsal does not delete its backup.

## Restore procedure

Restoration replaces the PostgreSQL cluster, so first preserve the current
`db_data` directory and keep the application stopped. On a recovery host with
the repository mounted at `/main-monitor-db-backups`, use the same
`chessism-postgres:15-backup` image and configuration:

1. Confirm the requested recovery point with `pgbackrest --stanza=chessism info`.
2. Stop every Chessism container that accesses PostgreSQL.
3. Move the current `db_data` directory to a dated recovery location; never
   delete it before the restored database has been validated.
4. Create an empty `db_data` directory owned by PostgreSQL UID/GID 999.
5. Run `pgbackrest --stanza=chessism --set=<backup-id> restore` with the empty
   data directory, repository, and pgBackRest configuration mounted in the
   container.
6. Start only PostgreSQL, run `pg_isready`, then compare critical row counts
   and application migrations before starting the API and workers.

The Backups page reports `Restore tested: not yet` until this disposable
cluster rehearsal passes. A successful backup or repository verification is
not falsely presented as a restore test.
