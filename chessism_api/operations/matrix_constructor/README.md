# Matrix constructor

The constructor saves **reusable column formulas**, not datasets. New
`matrix_definition` entries use `definition_kind: "template"` and contain a name,
row unit, selected fields/roles, dynamic move-column instructions, deterministic
ordering and encoding instructions. They do **not** store a selected player,
date range, game count, minimum game length or fixed move range. Saving never counts chess records, creates
files, calls Redis, or starts a worker. A future algorithm runner can materialize
a definition when needed; that runner is not implemented by this change.

Existing `matrix_artifact` rows and files are retained as legacy snapshots. On
the first creation of the definition table, completed snapshots' configurations
are imported under the same UUIDs. The import is transactional with table
creation and runs only once, so deleted definitions do not reappear on restart.

## Game move-score recipe and algorithm inputs

`/research/matrices` is the formula editor. `/research/algorithms` contains the
formula browser and execution editor. The former `/research/matrices/collection`
URL redirects there, preserving preview query parameters. Its page and Research
hub card have been removed; no formulas, datasets or results are deleted by this
change. Saving a formula never overwrites a previous definition and requires no
count or preview request.

### Built-in defaults versus custom formulas

The algorithm input browser's collapsed **Default** section shows formulas
from `defaults.py`, including **Game move scores**, score distribution, blunder
inputs, and **Accuracy — ordered game evaluations, v1**. Built-ins are
versioned code-owned templates, not auto-created database rows; they cannot be
edited or deleted through definition endpoints. They require per-use parameters
and use the same bounded preview implementation as user-created templates.

The constructor excludes the specialized `move_scores` template from its row-unit
selector and starts with the generic `game` row type for custom column selection.
The full API catalog retains `move_scores` for preview parameters, existing
definitions and backwards compatibility. Previously saved move-score formulas
remain in their custom/foreign groups, unchanged. Old constructor links to those
formulas explain the move to Default instead of silently changing a saved record.

`GET /research/matrices/defaults` lists metadata only, with stable `builtin_key`
and `builtin_version` values. `POST /research/matrices/defaults/{key}/preview`
accepts the same `MatrixParameters` body and `limit`/`role` query options as a
saved-template preview. Both inherit the existing superuser authorization. No
default dataset is generated simply by expanding the Default section.

#### Accuracy input v1 (review only)

The constructor's Default matrices section exposes `default:accuracy` with four
columns: `game` (dictionary-encoded identity), `ply` (int32), `score` (float64),
and `score_kind` (category). Game/ply are structural keys. Each fully scored game
appears once, including when both tracked participants are selected. All White
and Black plies remain in order, scores stay in White's perspective, and repeated
positions remain separate occurrences. Eligible owners have `joined > 0` and
are not deleted. Modes are bullet, blitz and rapid; runtime filters restrict the
population without changing the formula.

Coverage flags are checked against actual scores and contiguous unique plies;
incomplete or inconsistent games are excluded whole. A runtime row budget rounds
down to a prefix of whole games. Only the bounded preview display can truncate
that sequence. Score kinds use the shared verified evaluation contract:
`cp`, `mate`, `tablebase`, or `unknown`; missing provenance is not invented and
unknown scored evaluations remain visible for review.

The +15 CP starting evaluation belongs to the future algorithm configuration,
not a repeated input column. Per-game ownership/date/mode metadata and the shared
GPU extraction stage are separate future work. This formula does not calculate
accuracy, queue GPU work, change summaries, or replace the current CPU accuracy
implementation. Loading its metadata never reads chess records.

### Custom and foreign collections; retired snapshots

User-created formulas default to `collection: "custom"`. Imported or integration-
related formulas can use `collection: "foreign"`; this is an explicit metadata
tag in the existing instruction document, not a name heuristic or a change to
their query. Algorithm inputs group formula metadata into **Default**, **Your
formulas** and **Foreign formulas**. Saved inputs use one paginated metadata
request; default inputs are loaded separately and do not affect the saved-list
offset. Opening groups does not fetch game records.
`GET /research/matrices?collection=custom|foreign|all` retains filtering and
defaults to `all` for compatibility with existing consumers.

The five carried-forward October 4–5 validation/test recipes were placed in the
foreign collection with `origin: "legacy_snapshot_import"`. They are not evidence
of export-API-created recipes: that API does not create these definition rows.
Their five old materialized working snapshots were explicitly retired, including
the UUID folders and `matrix_artifact` records, while retaining all definitions
and leaving backup copies untouched. The Legacy UI component has been removed.
There is no HTTP endpoint for creating new legacy snapshots. Compatibility reads,
guarded deletion and backup-restore handling remain for historical backups.

### Formula versus runtime parameters

The constructor chooses the row type, columns and name only. In the algorithm
input browser, the **eye** opens the sample viewer directly without changing
the selected input or opening data controls. For templates, the frontend
supplies transient sample parameters: all players/modes, no date or minimum-
length filter, and a server-bounded sample (50 displayed rows by default, at
most 100). Move-score samples use fully scored games and all played moves.
The viewer labels this as an automatic sample, not an algorithm's chosen scope.
Nothing queries chess records until preview or an explicit algorithm action.
Selecting a formula (its columns for a default, **Use in algorithm** for a saved
formula) opens the data-selection form in the algorithm page. **Apply data
selection** calls `POST /research/algorithms/input` to resolve the columns and
parameters in memory only; it does not query game records or write data.
The body identifies either `matrix_definition_id` or `builtin_matrix_key`, plus
`matrix_parameters` using the same scope schema as a preview. The response is
`{config, materialized: false}`.

Builder validation, save, sample and preflight accept those same source fields.
Saving freezes the resolved recipe and `matrix_source` metadata in the algorithm;
the underlying formula is unchanged. Built-ins use a null matrix-definition
foreign key, not a synthetic database row. Reopening an algorithm retains its
frozen input, including when the original formula was deleted. Existing bounded
CPU limits remain: 12 input columns, up to 100,000 source rows for a pipeline,
an explicit input row cap, and a fixed move range. All-moves/unlimited selections
remain preview-only. Run and sample calculations still require explicit actions.

`definitions.resolve_definition()` combines the saved shape with validated
per-use parameters in memory. It does not modify the database definition. A
fixed runtime move range expands into its numbered feature columns; all moves
uses the bounded sample-width discovery below. The response reports
`applied_parameters`. A template without runtime parameters is rejected rather
than interpreted as a query of all games.

For a saved template use:

```http
POST /research/matrices/definitions/{id}/preview?limit=50&role=all
```

```json
{
  "filters": {
    "players": ["hikaru"], "modes": ["bullet"], "max_rows": null,
    "min_moves": 0, "analyzed_only": true
  },
  "move_scores": {
    "all_moves": false, "first_move": 1, "last_move": 40,
    "score_perspective": "player"
  }
}
```

Previously saved scoped formulas are not migrated or rewritten. Their existing
GET previews retain their original instructions. Loading one in the constructor
copies its column structure into a **new** reusable formula, leaving the source
unchanged. Legacy snapshot and CPU calculation paths reject unresolved templates
before queueing; the algorithm input step now resolves templates explicitly.

The `move_scores` row unit supplies `player`, `game` (exact int64 ID), `result`
(`w`, `d`, `l`, or null for unknown/inconsistent outcomes), and
`move_score_<number>` columns. Runtime `move_scores` options choose `first_move`,
`last_move` (1–500), and `score_perspective` (`player` by default,
or `white`). Each score is the stored evaluation AFTER that player's own move;
mate-encoded scores are not ordinary centipawns. This is not an accuracy input
containing both players' complete half-move sequence.

The row limit counts player/game pairs, not raw moves or distinct games across
multiple selected players. Ordering is date, game ID, color. `analyzed_only`
requires complete game coverage, checks actual scores for all played moves, and
requires every requested own move before the row cap. When disabled, missing
moves/scores remain null; zero stays zero. The bounded preview selects only its
small player/game prefix before pivoting; it never constructs the full saved cap.
Cheap candidate filters and ordering sit inside an `OFFSET 0` subquery barrier
before the expensive per-game score checks. The outer limit can stop after enough
qualifying games instead of checking all moves in the player's history before
sorting. There is no pre-limit that could discard later qualifying games.
Live preview transactions also disable JIT locally and retain the 15-second
statement timeout; full execution and export settings are unchanged.

`move_scores.py` owns the parameterized pivot and options. The standard 48-column
limit is unchanged for other recipes; this one allows its three identity/result
columns plus at most 500 scores. Definitions use the existing database table and
backup contract. No CUDA work, Stockfish run, export job, or schema migration is
started by creating or previewing this formula.

### All matching games and all available moves

`filters.max_rows: null` means all matching rows, with no hidden numeric cap.
Omitting the field keeps the existing default; numeric limits remain supported.
Other filters still apply, and a game with both selected participants still
produces two player/game rows. `min_moves: 0` means any game length, not a move
column selection. The UI distinguishes this minimum-length filter from the
move range.

`move_scores.all_moves: true` stores a dynamic recipe with `first_move: 1` and
`last_move: null`. Supply only identity/result columns in `feature_columns`;
numbered score columns are generated automatically as features. Fully scored
selection still checks every actually played move, but no longer requires all
games to reach the same fixed move number. Shorter rows are padded with null.
Explicit-range recipes remain backward compatible (`all_moves` defaults false).

All-moves previews first select at most 100 qualifying player/game rows (plus
one lookahead), then fetch their moves in the same read-only statement. They
expand columns through the longest **displayed sample** game, not the entire
requested dataset, and return `column_scope: "sample_moves"`. They do not pivot
the full scope or compute its total. Duplicates or a sampled move above the
500-column preview guard fail clearly; no moves are silently dropped. Estimates
still scan at most 10,001 matches even when there is no row limit.

This is a formula/preview feature, not full matrix execution. Existing CPU
algorithms require a fixed-column recipe with an explicit row limit. Dynamic
all-move or unlimited-row execution belongs to the later calculation pipeline;
it is rejected before queueing in the existing fixed-column runners. Legacy
snapshot materialization requires an explicit row limit. Saving an all-data
definition does not launch an unlimited query or allocate a complete matrix.

## Definitions and live previews

- `config.py` owns pure allowlist validation and shared limits; it has no worker,
  NumPy, disk-space, or database dependencies.
- `definitions.py` normalizes recipes and estimates counts up to 10,001 matching
  rows (or the recipe cap plus one). Larger scopes are labeled as lower bounds.
- `live_preview.py` reads an ordered prefix of 25/50/100 rows plus one lookahead,
  under a read-only transaction and a 15-second statement timeout. No count query,
  files, or full dictionaries are needed. Feature/label selection also narrows
  the SQL projection and its enrichment joins, not just the returned JSON.
  Aggregate row types may still need to
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
- `POST /preview`: bounded preview of an unsaved scoped recipe.
- `POST /estimate`: optional bounded live count; does not gate saving.
- `GET /definitions/{id}` and `GET /definitions/{id}/preview`: saved instructions/sample.
- `POST /definitions/{id}/preview`: saved column template + per-use parameters;
  bounded sample, no mutation or dataset materialization.
- `DELETE /definitions/{id}`: removes only the instructions.
- `GET /snapshots` and `/snapshots/{id}/preview`: retained legacy files.
- `DELETE /snapshots/{id}`: removes only legacy working files, never a definition or backup.

The old single-UUID snapshot read/delete routes remain compatibility aliases;
there is no HTTP endpoint that queues new snapshot construction. Definition
saves/deletes use the same short advisory-lock operation as the backup catalog;
while a backup holds that lock, writes return a retryable conflict. Previewing
and reading definitions remain available.

## Page-owned code map

`routers/research_matrices.py` preserves the public route surface and assembles
`routers/matrices/definitions.py`, `legacy_snapshots.py`, and `schemas.py`.
Superuser authorization is inherited from the research router's mounting in
`main.py`. Legacy snapshot reads connect to Redis only for queued/running work;
completed snapshots remain readable without Redis. The legacy NumPy reader is
imported only when its preview endpoint is used. Package imports never load the
legacy construction worker implicitly.

`chessism_web/src/pages/MatricesConstructor.jsx` owns page layout. Everything
specific to this page is in `pages/research/matrices/`:

- `MatrixDefinitionForm.jsx`: the custom formula's three form sections, without
  request logic. `MatrixParametersForm.jsx` is mounted only in the algorithm page.
- `matrixForm.js`: pure, tested form conversions; copies never mutate recipes.
- `useMatrixConstructor.js`: form actions, saved/draft preview selection, and
  abortable estimates. Changes and navigation discard stale responses.
- `useMatrixDefinitions.js`: list pagination and metadata updates. Repeated
  load-more clicks cannot request one page twice. Save/delete update the list
  from their successful response, avoiding a redundant follow-up GET.
- `matrixApi.js`: all page-owned HTTP calls, including cancellation signals.
- `MatrixDefinitionList.jsx`: saved instructions and selectable default columns.
  `algorithms/MatrixFormulaBrowser.jsx` groups default/custom/foreign formulas;
  `useAlgorithms.js` owns selection, pagination and per-algorithm scope.
- `MatrixDataFrameModal.jsx`, `MatrixPreviewButton.jsx`, and the two stylesheets:
  the bounded live/legacy viewer and presentation.

Legacy materialization remains for existing jobs, validation utilities, and
future algorithm reuse; it is not reachable from the constructor's save button.
The retired local snapshots and their preserved formulas are described above.
Backup formats and historical restore compatibility remain unchanged.

## Shared query and legacy materialization infrastructure

- `catalog.py` owns the allowlisted row units, source fields, encodings, and SQL
  expressions exposed to the UI.
- `queries.py` builds parameterized selection/filter queries only.
- `legacy_estimates.py` estimates disk usage and checks the local reserve for
  legacy materialization; it is never imported by definition-only requests.
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
python -m unittest tests.test_matrix_constructor tests.test_matrix_preview tests.test_matrix_definitions tests.test_matrix_cleanup tests.test_matrix_storage
# Optional PostgreSQL checks: temporary tables roll back; preview reads <=5 rows.
CHESSISM_DB_INTEGRATION=1 python -m unittest tests.test_matrix_definitions_database tests.test_matrix_storage_database
# Against the existing schema, with a disposable MATRIX_ARTIFACT_DIR and an
# appropriate MATRIX_FREE_FLOOR_BYTES; no migrations or production writes:
python -m tests.matrix_constructor_smoke
python -m tests.validate_matrix_artifact /path/to/manifest.json
```

Frontend checks, from `chessism_web/`:

```sh
npm run test:matrices
npm run build
# Optional: host Firefox + Node with WebSocket support, and the local Vite server.
# Uses a temporary browser profile and mocked APIs; no production records change.
npm run test:matrices:browser
```

The browser test defaults to `http://localhost:6789`; override `MATRIX_TEST_URL`
and `MATRIX_BROWSER_PORT` if needed. It exercises the editor, preview, pagination,
and delayed responses after edits/deletes. The fixture is not part of the built
application. Python regression tests cover import isolation, public URLs,
column-specific SQL joins, and preserved snapshot/backup compatibility.
