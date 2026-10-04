# Matrix constructor

This package turns the normalized Chessism schema into immutable research
snapshots without adding redundant feature tables to PostgreSQL.

- `catalog.py` owns the allowlisted row units, source fields, encodings, and SQL
  expressions exposed to the UI.
- `queries.py` validates requests, caps preview
  counts at the requested row limit, and protects the shared disk reserve.
- `sql_plan.py` adds only the joins needed by selected fields and filters. For
  non-aggregate exports, it limits the source rows before optional enrichment.
- `arrays.py` writes typed column batches, missing masks, dictionaries, and
  numerically stable profiles. Fractional averages remain floats; integer
  columns reject truncation/overflow instead of silently changing values.
- `jobs.py` streams one repeatable-read PostgreSQL snapshot into NumPy arrays on
  the research worker.

Each completed version-2 artifact groups columns into typed NumPy arrays such as
`features_float32.npy`, `features_int32.npy`, and `features_int64.npy`. A
manifest maps every requested column back to its array and column offset.
Artifacts also contain missing-value masks, optional label arrays, compressed
row keys, category dictionaries, column profiles, and file checksums.
PostgreSQL stores only job and artifact metadata in `matrix_artifact`; the
matrix itself remains outside the database.

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
python -m unittest tests.test_matrix_constructor
# Against the existing schema, with a disposable MATRIX_ARTIFACT_DIR and an
# appropriate MATRIX_FREE_FLOOR_BYTES; no migrations or production writes:
python -m tests.matrix_constructor_smoke
python -m tests.validate_matrix_artifact /path/to/manifest.json
```
