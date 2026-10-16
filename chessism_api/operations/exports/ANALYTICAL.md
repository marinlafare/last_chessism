# Analytical export integration contract

Base URL, Docker network and bearer authentication remain as in [README](README.md).
Only server-side clients explicitly enabled for analytics can submit these requests.
The existing structured `POST /exports/v1` and preview are unchanged. Analytical
routes return the same job identity and reuse status, cancellation, manifest,
Parquet download, checksums, idempotency and 24-hour expiry.

## Routes and finished-result semantics

| Method and path | Contract |
|---|---|
| `GET /exports/v1/analytical-catalog` | Versioned relations, fields, types, keys, relationships, SQL subset, score semantics and limits |
| `POST /exports/v1/sql` | Restricted SQL specification; HTTP 202 with export ID |
| `POST /exports/v1/move-matrix` | Select games first and build a complete player-move matrix; HTTP 202 |
| `POST /exports/v1/analytical-preview` | Same SQL/move-matrix design inside a preview envelope; HTTP 200 with up to 100 final rows, schema and `has_more` |
| `GET /exports/v1/{id}` | Status, phase, rows written, `error_code` and safe error message |
| `GET /exports/v1/{id}/manifest` | Final schema, actual row count, snapshot, request, limits, semantics, checksum |
| `GET /exports/v1/{id}/file` | Completed Parquet file only |
| `POST /exports/v1/{id}/cancel` | Cancellation request; effective at the next checkpoint or query timeout |
| `DELETE /exports/v1/{id}` | Delete a terminal export's temporary files |

Both submission routes require `Authorization: Bearer ...`, `Content-Type:
application/json` and a stable `Idempotency-Key`. Submit the same body/key after
a transport failure. A different body with the same key returns 409. Failed jobs
need a new key. Credentials and source grants are checked server-side and again
when the worker starts the query.

For SQL designs, `max_result_rows` is a **failure guard on the final result**,
not an inserted SQL LIMIT. If one extra row exists, the job fails and all partial
files are removed. It defaults to 100,000 and cannot exceed the token's limit.
Move matrices instead derive their row bound from `max_games × selected sides`;
callers do not need a separate row-limit or null-column setting. Output has at most
128 uniquely named columns. File-size, execution, type or query-cost failures
also publish no downloadable dataset. An empty result is a valid typed file.

## Preview a design before saving

`POST /exports/v1/analytical-preview` requires the same bearer token and analytical
source permissions as saving, plus `Content-Type: application/json`. It does
**not** require an `Idempotency-Key`: no job, file or saved design is created.
The existing structured-source `POST /exports/v1/preview` remains unchanged.

The envelope contains `design` (the exact body you would POST to `/sql` or
`/move-matrix`, including its `mode`) and `preview_rows` (integer 1–100, default
100). `preview_mode` defaults to `design`, preserving the behavior below; use
`sample` for a fast move-matrix sample without full-selection validation or totals.
In the default `design` mode, do not reduce the game selection to obtain a preview: the server
executes its selection, calculations, joins, grouping and pivot before sampling
the **final** rows. A 1,000-game matrix still selects 1,000 games for a 10-row
preview, and duplicate moves anywhere in that selection remain errors.

```json
{
  "design": {
    "mode": "move_matrix",
    "name": "Hikaru bullet scores, moves 1-40",
    "player": "hikaru",
    "game_mode": "bullet",
    "max_games": 1000,
    "players": "requested",
    "first_move": 1,
    "last_move": 40,
    "score_perspective": "player",
    "require_complete_scores": true,
    "format": "parquet"
  },
  "preview_rows": 10
}
```

For SQL, supply the normal `mode: sql` design instead. This runnable small example
demonstrates that a NULL column is removed even though its second row is non-null:

```json
{
  "design": {
    "mode": "sql",
    "sql": "SELECT 0::integer AS score, NULL::integer AS missing UNION ALL SELECT 12, 7 ORDER BY score",
    "max_result_rows": 2,
    "exclude_null_columns": true
  },
  "preview_rows": 1
}
```

HTTP 200 response (snapshot time is illustrative):

```json
{
  "kind": "analytical_preview",
  "mode": "sql",
  "snapshot_at": "2026-10-15T00:00:00+00:00",
  "preview_rows": 1,
  "columns": [{"name": "score", "type": "int32", "nullable": false}],
  "rows": [[0]],
  "row_count": 1,
  "has_more": true,
  "total_row_count": 2,
  "entire_result_checked": true,
  "column_selection": {
    "exclude_null_columns": true,
    "checked_scope": "entire_result",
    "dropped_columns": ["missing"]
  },
  "selection": {
    "kind": "explicit SQL selection; limits inside SQL are intentional",
    "selected_games": null
  },
  "score_semantics": {
    "note": "Final expressions are caller-defined; source score_cp is White perspective. See the design SQL and catalog semantics."
  },
  "json_integer_encoding": "decimal string when outside JavaScript safe range"
}
```

Rows are positional arrays aligned with `columns`, whose types use Arrow names
(for example `int64`, `int32`, `string`, `date32[day]`, `timestamp[us, tz=UTC]`).
Dates/timestamps use ISO strings; timestamps without a timezone stay without one.
Integers outside JavaScript's exact range ±(2^53−1) are **decimal strings** in JSON;
use the column's type to reconstruct them, never a floating-point intermediary.
Parquet retains actual integer types. SQL NULL is JSON null and zero stays zero.
Non-finite floats use strings (`NaN`/`Infinity` are not valid JSON numbers; the
actual Python encodings are `nan`, `inf`, `-inf`).

`has_more` means additional final-result rows exist beyond the returned sample,
not additional moves within a game. Without null-column filtering, the reader
normally fetches just the sample plus one row. `total_row_count` is then null and
`entire_result_checked` false unless EOF was reached. A successful small preview
does not certify that the full export fits all resource/row limits. If the result
is small enough to fully check, its exact count is returned (including zero).

### Fast move-matrix sample (recommended for design editing)

Use the same `POST /exports/v1/analytical-preview` endpoint and token, with
**`preview_mode: "sample"`** outside the unchanged design. No Idempotency-Key
is required. For example:

```json
{
  "preview_mode": "sample",
  "preview_rows": 10,
  "design": {
    "mode": "move_matrix",
    "player": "hikaru",
    "game_mode": "bullet",
    "max_games": 1000,
    "players": "requested",
    "first_move": 1,
    "last_move": 40,
    "score_perspective": "player",
    "require_complete_scores": true,
    "format": "parquet"
  }
}
```

The server uses the **same eligibility predicate and score transformation** as
saving, but stops after selecting enough qualifying games for the small sample.
It does **not** select all 1,000 games, count the full selection, or fetch an extra
game just to answer `has_more`. The bounded sample still checks duplicate moves
and complete scores, and retains **all requested columns**. It starts at a random
populated year, expands forward then backward only until the sample is filled,
and sorts the final rows by `played_at ASC NULLS LAST, game_id ASC`.
This is not a uniform random/statistically representative sample of games.
A preview chooses a fresh starting year, so its rows may differ from a saved
export even though both use identical eligibility and year-expansion rules.

Response fields are the normal typed columns and positional rows, with:

```json
{
  "kind": "analytical_sample",
  "preview_mode": "sample",
  "mode": "move_matrix",
  "preview_rows": 10,
  "row_count": 10,
  "has_more": null,
  "total_row_count": null,
  "entire_result_checked": false,
  "column_selection": {
    "exclude_null_columns": false,
    "checked_scope": "not_checked",
    "dropped_columns": []
  },
  "selection": {
    "max_games": 1000,
    "result_row_bound": 1000,
    "result_row_bound_source": "max_games_times_selected_sides",
    "selected_games": null,
    "selected_participants": null,
    "players": "requested",
    "game_order": "played_at ASC NULLS LAST, game_id ASC",
    "first_move": 1,
    "last_move": 40,
    "require_complete_scores": true,
    "complete_score_scope": "whole_game",
    "game_limit_applies_to": "qualifying_games",
    "eligibility": "Every played move of the entire game has a stored score for both players, and the requested move range exists for every selected participant.",
    "counts_scope": "sample_only",
    "sample_game_limit": 10,
    "returned_games": 10,
    "returned_participants": 10,
    "sampling": "random starting year, forward then backward until the sample is filled; final rows sorted by date; not a uniform random sample of games",
    "insufficient_games": null,
    "missing_games": null,
    "available_games": null,
    "message": null
  }
}
```

This excerpt omits `columns`, `rows`, `snapshot_at`, `score_semantics`, and
`json_integer_encoding`, which remain in the real response. Return counts vary
with available data. All full-result totals and `has_more` remain **null**, even
for empty samples; clients must not display null as zero or false. No exhaustive
count or full-export validation is promised.

- `preview_rows` remains 1–100. Recommend 10–20 for interactive design editing;
  send it explicitly (the backwards-compatible default remains 100).
- The internal sample-game bound is `min(max_games, ceil(preview_rows / sides))`.
  The saved design's row bound remains `max_games × sides`; it is not reduced by
  the preview. `selection.result_row_bound` reports that full design bound.
  `sides` is 1 for `requested`, 2 for `both`. Complete scoring always covers both
  players throughout each sampled game, regardless of output sides. An odd preview row
  count may show only the White row of the final sampled game; it never drops
  move columns or weakens either player's eligibility.
- Without `require_complete_scores`, sample mode retains missing scores as null,
  exactly as saving would. The complete-score flag must stay true to require
  complete move-score rows.
- `exclude_null_columns: true` is rejected for move matrices in both preview
  modes and saving (HTTP 422). Use complete-score eligibility instead. SQL
  design previews still support full-dataset null-column selection.
- Sample mode is move-matrix-only. SQL with `preview_mode: sample` returns 422;
  its existing `design` preview remains available.
- The sample cannot discover errors in unexamined games or validate that the
  whole saved dataset fits execution/file limits. Saving still performs full
  duplicate/completeness/resource checks and can fail independently. A sample
  is not an export or a certificate that an export will succeed.
- Sampling does not modify the supplied design or persist artifacts. Save the
  **original `design` object**, without the preview envelope, to `/move-matrix`;
  it still selects up to the original `max_games` **qualifying** games. No sample
  limit enters the saved request or idempotency fingerprint.

Performance target is roughly one second for typical small samples, not a
guarantee for sparse or expensive selections. The sample path uses 3-second
statement and 5-second overall execution deadlines, with the same planner,
authorization, reader-isolation and response-size guards. If eligibility cannot
be satisfied within the budget, it returns `504 PREVIEW_TIMEOUT` rather than
relaxing the requirements or presenting an unchecked partial sample. Connection
cleanup can add a few seconds. Each preview/export still uses a separate snapshot.

### Exclude columns containing any NULL

SQL analytical **designs** accept `exclude_null_columns: true` (default false).
New move-matrix designs always retain all requested columns; use
`require_complete_scores: true` to select complete games instead.
Place the flag inside `design` when previewing and keep it in the normal body
when saving. It is a column-selection operation over the **entire selected final
dataset**, never only the preview and never the unselected database corpus.

- Drop a column if **any** selected row contains SQL NULL in it.
- Do not remove rows, fill missing scores, change zeros, average duplicates or
  alter game selection. Zero, false and an empty string are not NULL.
- Return the surviving columns in their original order; `dropped_columns` names
  every exclusion. Verified surviving columns have `nullable: false`.
- This can remove calculated columns from a SQL result. New move-matrix exports
  do not use column projection; their requested move columns always remain.
- If **all** columns would disappear, fail with `NO_NON_NULL_COLUMNS`; do not
  return an unusable zero-column dataset. An empty result keeps its typed columns
  because there are no NULL values, and reports `row_count: 0`.
- Preview scans the whole selected result in bounded batches and retains only
  the requested sample. If it cannot finish checking within its limits, it
  **fails**, rather than asserting that unexamined columns are null-free.
- Saved exports apply the identical rule. They stream to a bounded temporary
  Parquet spool, then project the surviving columns in batches after releasing
  the database snapshot. They do not rerun the query or load the whole dataset
  into memory. The manifest reports the final schema and `column_selection`.

Each preview/export has its own read-only repeatable-read snapshot. The same
design against unchanged data produces the same column selection, but a preview
does **not** freeze the database for a later export. New games/scores between the
two requests may change rows/columns; treat the saved manifest as authoritative.

Preview limits: one preview per integration (shared with the original preview
route), 100 returned rows, 2 MiB JSON response, 64 MiB scanned JSON-equivalent
values, 12-second statements, 15-second overall execution deadline (connection
cleanup may add a few seconds). Existing token row caps, planner/type limits and
the dedicated reader's two-connection ceiling also apply. Whole-result checking
never bypasses them. For larger designs, submit a saved asynchronous export.

Runtime failures return an HTTP error, never a partial preview:

```json
{
  "detail": {
    "code": "NO_NON_NULL_COLUMNS",
    "message": "Every result column contains NULL; no columns satisfy exclude_null_columns. Change the design or disable the flag."
  }
}
```

| HTTP | Meaning |
|---|---|
| 401 / 403 | Missing/expired token or missing analytics/source grants (existing auth error format) |
| 422 | Invalid request/blocked SQL (FastAPI validation list), token row-cap error (string), or execution error `{code,message}` such as `NO_NON_NULL_COLUMNS`, `DUPLICATE_MOVE`, `RESULT_ROWS_EXCEEDED`, `UNSUPPORTED_RESULT_TYPE`, `QUERY_PLAN_LIMIT`, `PREVIEW_SIZE_EXCEEDED`, `QUERY_RESOURCE_LIMIT`, `QUERY_ERROR` |
| 429 | `PREVIEW_BUSY`; retry after the `Retry-After` header |
| 503 | `ANALYTICS_UNAVAILABLE`, `UNSAFE_QUERY_ROLE`, `ANALYTICS_BUSY`, `PREVIEW_UNAVAILABLE` |
| 504 | `PREVIEW_TIMEOUT`; narrow the design or request a saved export |

## Complete Hikaru matrix

POST `/exports/v1/move-matrix`:

```json
{
  "mode": "move_matrix",
  "name": "Hikaru bullet scores, moves 1-40",
  "player": "hikaru",
  "game_mode": "bullet",
  "max_games": 1000,
  "players": "requested",
  "first_move": 1,
  "last_move": 40,
  "score_perspective": "player",
  "format": "parquet"
}
```

Output: `game_id` (int64), `player_name` (string), `color` (`white`/`black`),
`played_at` (nullable UTC timestamp), `move_1` … `move_40` (nullable int32).
Game IDs remain exact integers.

`played_at` is the stored game **start** date/time, not export time or game end.
It is included automatically in previews and new Parquet exports without adding
any request field. JSON preview values are ISO 8601 UTC strings, for example
`"2024-08-17T19:24:30+00:00"`; preview column metadata describes the Arrow type
`timestamp[us, tz=UTC]`. Parquet retains the timezone-aware timestamp type, not
a string. Missing dates remain null, even with `require_complete_scores: true`
(that flag controls scores, not dates). Both participants share the same timestamp
when `players: both`. The response's `columns` and each positional row include
`played_at` immediately after `color`, before the move columns.

Older already-submitted exports keep their original schema. Submit a new
Idempotency-Key to receive the added date column; no request-body change is needed.

The server chooses up to 1,000 distinct matching games using the random-year
expansion below, then sorts the final rows by
`played_at ASC NULLS LAST, game_id ASC`.
`players: requested` selects only the named player's
perspective; `both` selects both participants and may produce 2,000 result rows.
The row bound is derived automatically: 1,000 for `requested`, 2,000 for `both`.
Names match exactly.
Fewer matching games are allowed and reported, never padded or fabricated.

Selection and date ordering are automatic: **do not send a year or ordering
parameter**. Both preview modes and saved exports follow this rule:

1. Pick a random populated year from this player's history for the requested
   game mode, up to the current UTC year. This light lookup does not scan scores.
2. Collect qualifying games from that year, oldest first within the year.
   Apply `player_win`, whole-game completeness and requested move-range rules
   before any game counts toward the limit.
3. If necessary, continue through later years up to the current year.
4. If still short, start at the year immediately before the original random
   year and work backward. Empty years are skipped. Already-selected games are
   not discarded or duplicated when expanding into another year.
5. Stop at `max_games`. If all years are exhausted, use qualifying undated
   games as a final fallback, then return the games available with shortfall
   metadata. Future-dated games are not included.
6. Sort the **selected result** chronologically, oldest first. Selection priority
   is by the year-search rule above; final row order is by date.

Missing dates come last; game ID breaks date ties.
With `players: both`, White's row precedes Black's within each game. The same
`played_at` date used for ordering is included as a default output column.
The analytical catalog describes this under `move_matrix_ordering`, and preview
and manifest metadata report `selection.game_order`. The catalog also describes
`move_matrix_selection`. The response/manifest `selection` includes:

- `strategy`: `random_year_forward_then_backward`.
- `start_year`: the randomly chosen year, or null if no dated games exist.
- `year_search_order`: populated years in search order (not a claim that every
  year was scanned). `selected_years` lists years contributing selected games.
- `undated_games`: selected games without a date, used only as a last fallback.
- `insufficient_games`: true if the full selection found fewer than `max_games`.
- `missing_games`: requested minus selected games, or zero when filled.
- `available_games`: exact qualifying total when insufficient, otherwise null
  because no separate full-total query is run.
- `message`: a shortfall explanation, or null when the request was filled.

For example, a request for 10,000 games with only 4,921 qualifying games across
all years succeeds with those 4,921 games and reports `insufficient_games: true`,
`missing_games: 5079`, and `available_games: 4921`. Resource/time-limit failures
still fail explicitly; they are **not** reported as an ordinary game shortfall.
With `players: both`, these are game counts, not result-row counts.

Sample previews use the same expansion but stop at their small sample limit;
`insufficient_games`, `missing_games`, and `available_games` remain null because
the full request has not been evaluated. Normal design previews and saved
exports evaluate the requested `max_games`. An export seeds the random choice
from its durable export identity, so idempotent retrieval keeps the same choice.

Already-submitted exports retain their original game-ID ordering. Use a **new
Idempotency-Key** for the new year-selection dataset; reusing a pre-change key
returns HTTP 409 rather than silently returning a differently ordered selection.
Previously completed files and manifests are not rewritten.

Move numbers are full-move numbers for the given side: `move_1` is that player's
first move. Each value is the score of the position **after** that move. Missing
scores, missing positions and moves not played remain NULL; zero remains zero.
Unexpected duplicate game/side/move records fail with `DUPLICATE_MOVE` before
pivoting. MAX is used only after uniqueness is checked, never to merge duplicates.

Stored `score_cp` is White-perspective centipawns. `score_perspective: white`
(default) keeps it; `player` negates scores for Black so positive favors that
row's player. Existing Stockfish mate scores use `score(mate_score=10000)`, encoded
near +/-10,000, not ordinary centipawn values. The stored scalar cannot provide
a separate reliable mate flag. Exports use existing analysis; they do not run
Stockfish or certify every stored score has identical provenance.

Explicit move ranges only; up to 124 move columns plus three identity columns
and the default `played_at` column (128 columns total). Older submitted requests
retain their previous 125-move-column allowance and three identity columns.
“All moves” is deliberately unsupported rather than silently dropping columns.

The manifest's `selection` reports `max_games`, actual `selected_games`, actual
`selected_participants`, `result_row_bound`, `result_row_bound_source`, move bounds
and ordering. Its `row_count` describes the
finished matrix; `complete_result` is true only when all requested rows fit.

### Filter by the named player's recorded result

Move-matrix designs accept `player_win`: `any` (default), `win`, `loss`, or `draw`.
For example, add `"player_win": "win"` to the export body, or to the identical
`design` object in a `preview_mode: sample` preview. Other strings are HTTP 422.

The filter uses recorded game points, not Stockfish evaluations: 1 for a win,
0 for a loss, and 0.5 for a draw, interpreted relative to `player` regardless
of color. Both sides must have consistent final game points; missing, unfinished,
unknown or inconsistent results qualify only for `any`. The filter is applied
before `max_games`, together with the existing game-mode and completeness rules.
With `players: both`, it still filters by the named player's outcome and then
returns both participants' rows; it does not require both players to win.

The analytical catalog advertises the values under `move_matrix_options.player_win`.
Preview `selection.player_win`, the manifest's `selection.player_win`, and the
saved `source_definition.player_win` identify the applied filter, including `any`.
Omitting the field and explicitly sending `any` are equivalent within the same
matrix contract. Already-submitted requests are not rewritten
and default to `any`; changing a result filter requires a new Idempotency-Key.
No extra columns are added to the matrix and the Parquet download is unchanged.

### Simplified move-matrix inputs and compatibility

Send **`max_games`** and **`require_complete_scores`**, without `max_result_rows`
or `exclude_null_columns`. This applies equally to saving and both preview modes.

- `max_games` defaults to 1,000, with a hard maximum of 100,000 games.
- The row bound is `max_games` for `players: requested` and `2 × max_games` for
  `players: both`. No need to repeat or calculate it in the caller.
- Token limits still apply to the **derived** bound; requests exceeding it return
  HTTP 422 before querying. For a 100,000-row token, that is at most 100,000 games
  for `requested` or 50,000 for `both`. The catalog publishes these bounds as
  `move_matrix_options.max_games.max_requested` and `.max_both`.
- `require_complete_scores` defaults to false: missing scores remain NULL.
  When true, the entire game must be scored for both players, including moves
  outside the output range, before the game limit is applied. Every requested
  move column is retained in **both** cases.
- Older callers may temporarily send `exclude_null_columns: false` and an integer
  `max_result_rows` **at least** the derived bound. These deprecated fields are
  omitted from new stored designs; a larger supplied bound does not override the
  derived one. True null-column exclusion or a smaller guard returns HTTP 422
  with instructions to remove the obsolete field. They are never silently changed.
- Already-submitted exports keep their immutable original guard/projection and
  request checksum. Replaying an equivalent older key returns that original job;
  a genuinely different design remains HTTP 409. Old jobs whose explicit guard
  is smaller, or which drop columns, are not equivalent to a new design.
- **Whole-game semantic update:** previously submitted `require_complete_scores:
  true` jobs retain their old requested-range rule. Submit a **new Idempotency-Key**
  to obtain the new whole-game behavior; reusing an old range-only key returns
  HTTP 409 rather than mislabeling an old dataset. Jobs are versioned internally;
  callers must not send a version or another score-selection option. Request
  bodies remain unchanged. Response/manifest `selection.complete_score_scope`
  is `whole_game`, `none`, or `requested_moves` for old jobs executed after this
  update. Previously completed manifests are not rewritten and may lack this field.
- The manifest's top-level `max_result_rows` and job's `row_limit` remain **output
  metadata** for compatibility; for new matrices they contain the derived bound.
  `selection.result_row_bound_source` is `max_games_times_selected_sides` for new
  designs and `legacy_explicit` for previously stored explicit-guard designs.

SQL exports retain their independent `max_result_rows` and `exclude_null_columns`
options unchanged. Structured-source exports are also unchanged.

### Select games with complete scores, keeping every move column

Move-matrix designs now accept **`require_complete_scores: true`** (strict
boolean, default false). This filters **games**, unlike SQL's `exclude_null_columns`,
which drops **columns**. **True means the entire game, for both players, is
scored. False means no score-completeness requirement.** It does not start
analysis or fill missing scores. There is no new `score_requirement` input.

The updated request to `POST /exports/v1/move-matrix` is:

```json
{
  "mode": "move_matrix",
  "name": "Hikaru complete bullet scores, moves 1-40",
  "player": "hikaru",
  "game_mode": "bullet",
  "max_games": 1000,
  "players": "requested",
  "first_move": 1,
  "last_move": 40,
  "score_perspective": "player",
  "require_complete_scores": true,
  "format": "parquet"
}
```

The server applies this order **inside the database, in one snapshot**:

1. Match player and game mode.
2. Keep games where **every played move in the entire game, for both players**,
   has an associated position and a non-NULL stored score.
3. Require every requested move to exist for each output participant.
4. Fill up to `max_games` using a random starting year, later years, then earlier
   years as described above; sort the selected games by
   `played_at ASC NULLS LAST, game_id ASC` (oldest first).
5. Build one complete row per requested participant with **all** columns from
   `move_<first_move>` through `move_<last_move>`, inclusive.

There is no preliminary candidate-game LIMIT and no post-LIMIT dropping of
incomplete rows. If the earliest games are incomplete, later qualifying games
fill the requested allowance. The existing query/resource deadlines still
apply; reaching one fails explicitly rather than returning a partially searched
result as complete. Planner estimates may reject a costly selection even with a
small final game limit.

With `players: requested`, only the named player's row is exported, but **the
opponent's scores and moves outside the selected range still affect eligibility**.
For example, requesting moves 1–20 rejects a game with an unscored move 35, or
an unscored opponent move. We check current stored scores in the same snapshot,
not just a potentially stale summary flag. The entire game need only be checked
for each sampled candidate; preview does not complete the full `max_games` search.
With `players: both`, the requested range must exist for both players and each
qualifying game produces two rows: for
1,000 games, the server derives a 2,000-row bound. It never truncates the second
player's row to fit an independent caller-specified cap.

An unplayed final Black half-move is not a missing score: a game ending after
White's move can still be fully scored. However, if that unplayed Black move is
part of the requested output range with `players: both`, the game cannot fill
the requested matrix and is excluded.

Zero is a valid complete score. Stored mate encodings are also non-NULL scores
and retain the documented score semantics. Missing moves (including games ending
before the requested move), missing positions or NULL scores are ineligible;
duplicate move rows cannot fill holes, and duplicates in selected games still
fail with `DUPLICATE_MOVE` rather than being averaged or silently skipped.

Fewer than `max_games` qualifying games is a successful smaller dataset. If none
qualify, the result is an empty typed matrix retaining every requested column.
All move columns are marked `nullable: false` in this mode. Identity columns
retain their existing types. No columns are dropped.

`exclude_null_columns: true` is rejected for new move-matrix requests, regardless
of the complete-score setting. Omit it. Previously admitted jobs retain their
stored instructions as described in the compatibility section above.

Preview uses **the exact same design and eligibility rule**:

```python
# design is the JSON object above, unchanged.
requests.post(
    base_url + "/analytical-preview",
    headers={"Authorization": "Bearer " + token},
    json={"design": design, "preview_rows": 10},
    timeout=25,
)
```

`preview_rows` limits only returned final rows; it does not change `max_games` or
eligibility. `entire_result_checked` still reports whether the preview cursor
read every final row, not whether the eligibility predicate was applied: that
predicate always applies to every selected game. Preview `selection` and the
export manifest include:

```json
{
  "max_games": 1000,
  "selected_games": 1000,
  "selected_participants": 1000,
  "players": "requested",
  "game_order": "played_at ASC NULLS LAST, game_id ASC",
  "first_move": 1,
  "last_move": 40,
  "require_complete_scores": true,
  "complete_score_scope": "whole_game",
  "game_limit_applies_to": "qualifying_games",
  "eligibility": "Every played move of the entire game has a stored score for both players, and the requested move range exists for every selected participant."
}
```

Actual selected counts can be lower. Preview/save remain separate snapshots;
intervening score updates can change which games qualify. Raw SQL designs do not
accept this matrix-specific flag: express the eligibility predicate before LIMIT
in the SQL itself. The analytical catalog advertises this option and its rules.

## General SQL

POST `/exports/v1/sql`, for example:

```json
{
  "mode": "sql",
  "name": "Bullet game counts by player",
  "sql": "SELECT player, COUNT(*) AS games, AVG(player_rating)::double precision AS average_rating FROM exports_data_v1.player_games WHERE mode = $1 GROUP BY player ORDER BY games DESC, player",
  "parameters": [{"type": "string", "value": "bullet"}],
  "max_result_rows": 100000,
  "format": "parquet"
}
```

The [working analytical example](example-analytical.json) selects Hikaru's first
1,000 bullet games in a materialized CTE, then joins moves, groups, calculates an
average and applies a window function. Its final row bound counts **games**, not
individual moves. The [matrix request](example-move-matrix.json) provides the
strict duplicate-checked 40-column pivot without generating SQL in the caller.

For game-scoped analyses, use the example's player-ID lookup, materialized game
selection and correlated `LATERAL` move lookup (`OFFSET 0` retains the lookup
boundary). A broad join through security-barrier views can otherwise plan a full
corpus scan even for a small final result. The server rejects excessive estimated
cost; a small result-row cap alone does not make a query inexpensive.

Use the versioned relations in the catalog:
`exports_data_v1.games`, `.players`, `.positions`, `.game_positions`, `.moves`,
`.player_games`. Their columns mirror the existing structured catalog. Always
qualify relation names; use aliases for columns. No internal/public tables,
system catalogs, metadata, credentials or arbitrary views are exposed.

One PostgreSQL 15 SELECT is allowed, with non-recursive read-only CTEs, subqueries,
inner/outer/cross joins, aggregates, GROUP BY/HAVING, CASE, COALESCE/NULLIF,
GREATEST/LEAST, window functions, DISTINCT, ordering and read-only set operations.
Joins/aggregations in raw SQL have exactly their explicit SQL meaning: authors
must handle intentional fan-out and choose deterministic ordering themselves.
The API cannot infer a generic query's “number of games”; that count is null in
the SQL manifest, whose `source_definition` retains explicit selection limits
and bound values. Matrix requests report verified game and participant counts.

Only the catalog's allowlisted functions/operators/casts are accepted. Supported
functions include count/sum/avg/min/max, standard deviations, abs/round/trunc,
selected string/date functions and row_number/rank/lag/lead. Functions and ordinary
operators are normalized to `pg_catalog`; search_path excludes public/temp schemas.
Unsupported AST nodes fail closed. No writes anywhere in the syntax tree, SELECT
INTO, row locking, session commands, recursion, arbitrary/table functions,
extensions, custom operators/types/collations, arrays or Python are accepted.

Parameters use `$1`, `$2`, etc., plus typed values. Types: `int16`, `int32`, `int64`,
`float64`, `string`, `bool`, `timestamp[us, UTC]`. Values are bound, never substituted
into SQL text; timestamps require a timezone. Typed NULL is supported. Every
parameter must be used. All comparisons retain PostgreSQL NULL semantics.

Output types: bool, int16/int32/int64, float32/float64, text/varchar, date,
timestamp and timestamptz. The schema is read from PostgreSQL before streaming,
so empty/all-NULL results remain typed. Unbounded `numeric`/decimal, arrays and
composites are rejected, not silently rounded: cast numeric aggregates explicitly
to a suitable scalar, for example `AVG(x)::double precision`, or integer when
appropriate. Integer overflow remains an error. Use pandas' Arrow backend:

```python
df = pandas.read_parquet(path, dtype_backend="pyarrow")
```

## Security, resources and reporting database assessment

SQL is parsed with the PostgreSQL-15-compatible parser and completely traversed.
Execution uses a separate `chessism_export_reader` login with SELECT on approved
views only. It cannot read base/metadata tables, own/create objects, create temp
tables, write, administer roles, inherit roles or switch into the application
role. Views have a separate NOLOGIN owner with read access to their backing
tables. Privilege drift is checked before admission and execution. Metadata
writes still use the internal application connection; SQL never runs on it.
Database credentials stay inside Chessism; never mount `export-reader.json` into
the consuming app. Continue using only `local-dataframe.token` there.

Defaults: one background export globally; two reader connections; read-only
repeatable-read transactions; 60-second statements; 15-minute export deadline;
16 MiB work_mem per operation; 256 MiB temporary-file budget; no parallel query
workers; 2 GiB output; 20 GB free-disk reserve. An EXPLAIN cost above 250,000 rejects
the query before execution (`CHESSISM_EXPORT_MAX_PLAN_COST` is server-controlled).
SQL is at most 32 KiB, 8,000 AST nodes, depth 60, 12 CTEs per WITH, 200 parameters.

These limits and planner estimates **do not guarantee CPU/I/O isolation or a
whole-query memory cap**. The first implementation shares the existing database
and is intended for bounded selections. Move sustained large scans to a separate
reporting PostgreSQL instance with these same versioned views and read-only role;
configure its private reader credential in Chessism and expose snapshot/refresh
staleness to consumers. No reporting copy, external service or cloud cost is
created by this installation. Do not bypass admission guards to run huge joins
on the live ingestion database.

## Errors

Invalid syntax/blocked operations/invalid parameters: HTTP 422 before queuing.
Token without analytics/source grants: 403. Unsafe/unavailable reader: 503.
Existing lifecycle HTTP errors remain unchanged. Accepted jobs can finish with
`status: failed`, `error_code`, and a safe `error`:

- `RESULT_ROWS_EXCEEDED`, `OUTPUT_COLUMNS_EXCEEDED`, `FILE_SIZE_EXCEEDED`
- `DUPLICATE_MOVE`, `DUPLICATE_COLUMNS`, `INCOMPLETE_MATRIX`
- `UNSUPPORTED_RESULT_TYPE`, `QUERY_ERROR`
- `NO_NON_NULL_COLUMNS` when opt-in null-column filtering leaves no columns
- `QUERY_PLAN_LIMIT`, `QUERY_TIMEOUT`, `QUERY_RESOURCE_LIMIT`, `EXPORT_TIMEOUT`
- `UNSAFE_QUERY_ROLE`, `ANALYTICS_UNAVAILABLE`, or general `EXPORT_FAILED`

Failed exports never expose partial Parquet files. Do not label a failed/limited
result complete in the lessons UI. Download, checksum and persist the successful
file/manifest before requesting its deletion. The lessons app may then register
its local file as `from dataframes import hikaru_bullet_scores`.

## Administrator deployment and backup compatibility

```sh
docker compose build chessism-api worker-backup
docker compose run --rm --no-deps chessism-api python -m chessism_api.operations.exports.cli install-analytics
docker compose up -d --no-deps chessism-api worker-exports worker-backup
docker compose exec -T chessism-api python -m chessism_api.operations.exports.cli enable-analytics --id CLIENT_ID
```

The API image installs the parser from this directory's `requirements.txt` in
addition to the repository's shared dependencies; backup-only workers do not
need it. When running API tests outside Docker, install both requirement files.
Installation adds views, roles and two small metadata columns; no chess rows are
copied. It revokes PUBLIC TEMP on this database (the existing administrator is
unaffected) and refuses that change if other non-admin logins require review.

Backups explicitly record the two supported role identities without passwords.
The private restore test creates them NOLOGIN before restoring ownership/ACLs.
Older single-owner backups remain supported. After an actual disaster restore,
run install-analytics to provision the query credential/settings before enabling
the service; snapshots do not contain query credentials or temporary export files.

Security references: [PostgreSQL views and ownership](https://www.postgresql.org/docs/15/sql-createview.html),
[transaction/resource settings](https://www.postgresql.org/docs/15/runtime-config-client.html),
[PostgreSQL AST parser](https://pglast.readthedocs.io/en/v5/usage.html).
