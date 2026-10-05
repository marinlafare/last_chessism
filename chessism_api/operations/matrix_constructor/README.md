# Matrix constructor

The constructor now saves **definitions**, not datasets. `matrix_definition`
contains a name, row unit, selected fields/roles, filters, row cap, deterministic
ordering and encoding instructions. Saving never counts chess records, creates
files, calls Redis, or starts a worker. A future algorithm runner can materialize
a definition when needed; that runner is not implemented by this change.

Existing `matrix_artifact` rows and files are retained as legacy snapshots. On
the first creation of the definition table, completed snapshots' configurations
are imported under the same UUIDs. The import is transactional with table
creation and runs only once, so deleted definitions do not reappear on restart.

## Definitions and live previews

- `definitions.py` normalizes recipes and estimates counts up to 10,001 matching
  rows (or the recipe cap plus one). Larger scopes are labeled as lower bounds.
- `live_preview.py` reads an ordered prefix of 25/50/100 rows plus one lookahead,
  under a read-only transaction and a 15-second statement timeout. No count query,
  files, or full dictionaries are needed. Aggregate row types may still need to
  aggregate the filtered source data; users should narrow expensive scopes.
- `definition_backups.py` fingerprints instructions protected by the normal
  PostgreSQL backup. The manual restore rehearsal compares the restored records
  with that fingerprint. Previews are transient and never included in backups.

The UI labels samples **Live preview—not a saved snapshot**. Category values
remain source strings, not temporary codes that could be mistaken for stable
GPU inputs. Missing source values are null / N/A. Numeric column types describe
the future materialized representation. Saving an empty scope is valid.

Superuser endpoints under `/research/matrices`:

- `POST /`: save instructions only (201); `GET /`: paginated definitions.
- `POST /preview`: bounded preview of an unsaved recipe.
- `POST /estimate`: optional bounded live count; does not gate saving.
- `GET /definitions/{id}` and `GET /definitions/{id}/preview`: saved instructions/sample.
- `DELETE /definitions/{id}`: removes only the instructions.
- `GET /snapshots` and `/snapshots/{id}/preview`: retained legacy files.
- `DELETE /snapshots/{id}`: removes only legacy working files, never a definition or backup.

The old single-UUID snapshot read/delete routes remain compatibility aliases;
there is no HTTP endpoint that queues new snapshot construction. Definition
saves/deletes use the same short advisory-lock operation as the backup catalog;
while a backup holds that lock, writes return a retryable conflict. Previewing
and reading definitions remain available.

## Shared query and legacy materialization infrastructure

- `catalog.py` owns the allowlisted row units, source fields, encodings, and SQL
  expressions exposed to the UI.
- `queries.py` validates requests, caps preview
  counts at the requested row limit, and protects the local disk reserve.
- `sql_plan.py` adds only the joins needed by selected fields and filters. For
  non-aggregate exports, it limits the source rows before optional enrichment.
- `arrays.py` writes typed column batches, missing masks, dictionaries, and
  numerically stable profiles. Fractional averages remain floats; integer
  columns reject truncation/overflow instead of silently changing values.
- `jobs.py` streams one repeatable-read PostgreSQL snapshot into NumPy arrays on
  the research worker.
- `storage.py` owns working paths and the completed-snapshot catalog lock.
- `artifact_files.py` verifies manifests and copies immutable files safely.
- `storage_cli.py` migrates or restores the local working copies. The sibling
  `operations/matrix_backups.py` integrates them with database recovery points.
- `preview.py` reads small DataFrame-style pages from the immutable numeric
  files. It never rebuilds a matrix or queries the original chess data.

Each completed version-2 artifact groups columns into typed NumPy arrays such as
`features_float32.npy`, `features_int32.npy`, and `features_int64.npy`. A
manifest maps every requested column back to its array and column offset.
Artifacts also contain missing-value masks, optional label arrays, compressed
row keys, category dictionaries, column profiles, and file checksums.
PostgreSQL stores only job and artifact metadata in `matrix_artifact`; the
matrix itself remains outside the database.

## Legacy working storage and backups

Working snapshots live in `research_data/matrices/<UUID>/` inside the project
directory, excluded from Git and Docker build contexts. The API and research
worker mount this as `/research-data/matrices`; the backup worker mounts the
same data read-only. The local reserve is 20 GB by default, configurable with
`MATRIX_FREE_FLOOR_BYTES`. `MATRIX_ARTIFACT_DISPLAY_DIR` sets the host-facing path.

The manual **Backup database** operation copies new completed snapshots into
`/main-monitor-db-backups/chessism/research/matrices/<UUID>/`. Existing copies
are reused after checking their manifest digest and file sizes; new copies are
checksum-verified before publication. The external 150 GB reserve and 200 GB
Chessism quota still apply. Backups are not silently deleted when a working
matrix is deleted, nor when an older PostgreSQL backup chain expires.

The recovery-point manifest records the exact completed UUIDs and manifest
checksums. A database advisory lock freezes completion/deletion through both
the matrix copy and PostgreSQL backup; it does not block Stockfish or ingestion.
Construction may finish writing while a backup runs, but its completion waits
to be published. Metadata paths are relative; API paths are derived from the
current storage configuration, not the machine that created a backup.

The one-time migration is copy-and-verify, never a move. Build the updated
images and stop the old API, research, and backup services first, then run:

```sh
docker compose run --rm --no-deps chessism-api python -m chessism_api.operations.matrix_constructor.storage_cli migrate
```

Restart those three services after all copies have been verified. The external
matrix root must be writable by the backup worker (UID/GID 999); the host setup
script creates it with that ownership. Do not recursively change other storage.
After restoring PostgreSQL, restore the companion working files using:

```sh
docker compose exec -T chessism-api python -m chessism_api.operations.matrix_constructor.storage_cli restore --backup-id RECOVERY_POINT_ID
```

This command checks the recovery-point snapshot list against the restored
database and never overwrites a conflicting UUID. **Test backup** also verifies
all matrix checksums against restored metadata, without copying them into live
working storage. Legacy recovery points without a companion list are explicitly
reported as not covering matrices. Matrix recovery does not require a GPU.

## Legacy DataFrame viewer

The small grid icon beside a completed artifact opens a read-only table with
feature/label selection and 25, 50, or 100 rows per page. The superuser-only
`GET /research/matrices/{UUID}/preview` accepts `offset`, `limit` (maximum 100),
`role` (`all`, `features`, or `labels`), and `slice_index` (default zero).
The API checks completion with one metadata lookup, validates file paths and
sizes, and memory-maps only the requested numeric array ranges. It does not
load the full matrix, dictionaries, or compressed row-key stream to page data.

The table uses a zero-based numeric index. Missing masks become `null` / N/A;
category values remain their original dictionary codes, labeled as codes.
Integers outside JavaScript's exact range are serialized as decimal strings.
All current constructor snapshots are 2D, including ones with three columns.
The reader can also display 3D arrays one slice at a time using the convention
`[slice, row, column]` with matching masks. Producing such tensors is not yet a
constructor option. The browser is a DataFrame-style view, without requiring
Pandas or loading a full DataFrame on the server.

The constructor preserves raw source values. Normalization, data splitting,
feature transformations, and algorithms belong to the future algorithm layer,
so one immutable snapshot can be reused by different experiments without
silently changing its meaning.

The constructor intentionally does not accept SQL, joins, or expressions from
the browser. Add a new source column by defining it in `catalog.py`, then cover
its validation and generated query in `tests/test_matrix_constructor.py`.

Revision 3 keeps the version-2 file layout. Existing snapshots are immutable
and are not rewritten. A new snapshot is needed to use the corrected fractional
average-rating encoding and exclusion of empty final half-moves.

Important semantics:

- Row limits select a deterministic ordered prefix, **not** a random sample;
  the manifest records that ordering.
- Move/appearance player filters refer to the mover. Salience frequencies are
  relative to that player's color-specific corpus, including all positions in
  those games, not merely the rows selected for export.
- Raw FEN CP scores retain their database (White) perspective; game-player
  engine-summary fields already use the player's perspective.
- Stale/queued/running salience is missing, not zero or an old valid value.
- Monthly grouping and timestamps use UTC consistently.
- Numeric buffers are disk-backed and batches bounded. Category dictionaries
  grow with distinct values: use a smaller cap when selecting FEN/SAN identities.
  Disk estimates include a conservative dictionary allowance, not an exact size.
- Final checksums run outside the database transaction and off the worker's
  event loop. Cancellation removes the incomplete snapshot; completed folders
  are never overwritten.

Validation:

```sh
python -m unittest tests.test_matrix_constructor tests.test_matrix_preview tests.test_matrix_definitions
# Optional PostgreSQL checks: temporary tables roll back; preview reads <=5 rows.
CHESSISM_DB_INTEGRATION=1 python -m unittest tests.test_matrix_definitions_database tests.test_matrix_storage_database
# Against the existing schema, with a disposable MATRIX_ARTIFACT_DIR and an
# appropriate MATRIX_FREE_FLOOR_BYTES; no migrations or production writes:
python -m tests.matrix_constructor_smoke
python -m tests.validate_matrix_artifact /path/to/manifest.json
```
