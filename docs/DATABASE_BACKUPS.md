# Chessism database backups

Chessism keeps two independent backup formats on the recovery disk whose UUID
is `1ebffac6-4b7a-4906-9e07-9586060d3825`:

- `/main-monitor-db-backups/chessism/database/` is a pgBackRest repository for
  complete PostgreSQL recovery.
- `/main-monitor-db-backups/chessism/fen-analysis/` contains the portable,
  compressed FEN-analysis export.

The existing `/main-monitor-db-backups/timeshift/` directory belongs only to
Timeshift. Chessism never creates, edits, moves, or removes anything inside it.

## Storage policy

- Chessism allocation: 200 GB.
- Protected filesystem free-space floor: 150 GB.
- First database backup: full.
- Normal subsequent backup: incremental.
- New full chain: after 30 incrementals, after 30 days, or after interrupted
  WAL archiving.
- Retention: two complete full-backup chains.
- Trigger: manual only, from the Backups page.
- Validation: pgBackRest verifies the selected recovery point and required WAL
  before Chessism publishes a successful status.

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

Full database restoration is intentionally not exposed in the UI. It must be
performed while the database is stopped and should first be rehearsed in an
isolated PostgreSQL 15 container.

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

The Backups page intentionally reports `Restore tested: not yet` until a
separate disposable-cluster rehearsal has been completed. A successful backup
or repository verification is not falsely presented as a restore test.
