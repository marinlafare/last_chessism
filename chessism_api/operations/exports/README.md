# Request-driven DataFrame exports

This integration reads Chessism's validated compact (`relational_v3`) database.
The original structured endpoint never accepts SQL, arbitrary joins, filesystem paths, Python code, or access
to authentication, billing, backup, cloud-control or integration tables.
Saved matrix definitions are not required and their existing APIs are unchanged.

For finished analytical datasets, including restricted SQL and player move-score
matrices, see [the analytical integration contract](ANALYTICAL.md). Those are
separate opt-in routes; the original structured API remains compatible.

## Connection and authentication

The internal base URL is `http://chessism-api:8000/exports/v1` on the
`chessism_exports` Docker network. Only the API is attached to this additional
internal bridge; PostgreSQL and Redis remain on Chessism's private network.
The other application's Compose file can join it without changing its normal
network or receiving database credentials:

```yaml
services:
  your-app:
    networks: [default, chessism_exports]
    secrets: [chessism_export_token]
    environment:
      CHESSISM_EXPORT_URL: http://chessism-api:8000/exports/v1
      CHESSISM_EXPORT_TOKEN_FILE: /run/secrets/chessism_export_token
secrets:
  chessism_export_token:
    file: /absolute/host/path/to/local-dataframe.token
networks:
  chessism_exports:
    external: true
    name: chessism_exports
```

Ensure the consuming container's user can read its secret; do not make the host
credential world-readable. Keep its own default network for any internet access.
From the host, the existing published API is `http://127.0.0.1:8003/exports/v1`.
`localhost` inside the other container refers to that container, not Chessism.
Use TLS rather than plain HTTP if connecting beyond this trusted same-host bridge.

Every route requires `Authorization: Bearer <token>`. Browser session cookies
and admin passwords do not authenticate here. Tokens are stored as SHA-256
hashes and expire; each is restricted to its allowed sources and row cap.
Job status, manifests, downloads, cancellation and deletion are owner-scoped.
Revocation stops an active export at its next checkpoint (or query timeout).

## Request contract

`GET /catalog` lists permitted datasets, fields, types, row keys and operators.
The six sources are `games`, `players`, `positions`, `game_positions`, `moves`,
and `player_games`. `positions` has one row per distinct FEN;
`game_positions` retains each occurrence. `moves` has one row per **played
half-move**, not one row per physical `moves` record. Its `player` is the mover.
To include both sides of a player's games, filter `white_player OR black_player`.
No deduplication or score-perspective conversion is implicit.

`POST` to the base URL submits a JSON specification:

```json
{
  "name": "Hikaru blitz games",
  "source": "games",
  "select": ["game_id", "played_at", "white_player", "black_player", "white_rating", "black_rating"],
  "where": {
    "all": [
      {"any": [
        {"field": "white_player", "op": "eq", "value": "hikaru"},
        {"field": "black_player", "op": "eq", "value": "hikaru"}
      ]},
      {"field": "mode", "op": "eq", "value": "blitz"},
      {"field": "played_at", "op": "gte", "value": "2026-01-01T00:00:00Z"}
    ]
  },
  "order_by": [{"field": "played_at", "direction": "asc"}],
  "limit": 100000,
  "format": "parquet"
}
```

Send an `Idempotency-Key` header (8–128 letters/digits/`.`/`_`/`:`/`-`).
Identical retries return the same ID; a different request using that key is 409.
Failed/expired requests require a new key. A replay does not take a new snapshot.
POST returns 202 and the job's `id`, status, phase and counters, not the dataset.

Selections are ordered prefixes, not random samples. Unique source keys are
appended to ordering automatically, without adding columns to the requested
output. All NULLs sort last. Names/strings compare exactly and case-sensitively.
Supported operators: `eq`, `ne`, `in`, `not_in`, `gt`, `gte`, `lt`, `lte`,
`is_null`, `is_not_null`; NULL operators have no `value`. Ordinary comparisons
and `not_in` exclude NULLs (SQL semantics); use an explicit `any`/`is_null` to
include them. Timestamp filters require a timezone. Unknown keys/operators,
wrong types, SQL expressions, and excessive requests are rejected, not ignored.

Limits: 48 columns; 5,000,000 rows maximum and the token's possibly lower cap;
128 KiB request; filter depth 6, 100 nodes, 50 conditions/group, 1,000 IN values.
Default row cap is 100,000. The catalog reports the token cap.

## Routes

All paths below are relative to `/exports/v1`:

| Method/path | Result |
|---|---|
| `GET /catalog` | Permitted data contract |
| `POST /preview` | Same specification, at most 100 rows, no artifact |
| `POST` base URL | Submit export, requires Idempotency-Key |
| `GET /{id}` | State, phase, actual rows written, size, expiry and safe error |
| `GET /{id}/manifest` | Completed provenance and checksum |
| `GET /{id}/file` | Completed Parquet download |
| `POST /{id}/cancel` | Request cancellation; poll until terminal |
| `DELETE /{id}` | Delete a terminal export's temporary files |

FastAPI's `/docs` and `/openapi.json` document the schemas and `ExportToken`
security scheme. 401 = invalid credential; 403 = source denied; 404 = unknown
or another client's job; 409 = not ready/key conflict/still running; 410 = files
expired/deleted; 413 = oversized body; 422 = invalid query; 429 = capacity busy;
503 = temporarily unavailable/query limit exceeded.

Preview JSON includes `columns`, positional `rows`, `has_more`, and snapshot
time. Integers outside JavaScript's safe range are decimal strings **in the
preview only**. Parquet retains native integers, including 64-bit identities.
A preview and a later export are separate snapshots. Separate exports also do
not promise a shared snapshot; use one dataset request when consistency matters.

## Execution, integrity and cleanup

The dedicated `worker-exports` runs one job at a time, including a cross-worker
PostgreSQL advisory lock. Admission permits two unfinished jobs/client and
eight globally. A queue outage cannot lose committed jobs; the worker repairs
queued admission on startup and every 30 seconds. Interrupted running jobs fail
closed and remove incomplete files; they are not resumed against a new snapshot.

Rows stream through one read-only repeatable-read snapshot in 2,048-row batches.
Only selected/filter/order fields' many-to-one joins are included. No full
COUNT is run. A single lookahead row sets `limit_reached` when the output omits
additional matching rows. Progress shows actual rows written, not a fake
percentage of the requested cap. Empty exports are valid typed Parquet files.

Data uses Zstandard Parquet with an explicit Arrow schema: no float32 matrix
conversion or category codes. Timestamps retain microseconds and UTC; integers,
NULLs, booleans and float64 values are preserved. The manifest records source
semantics, requested fields/filters, effective order, schema/catalog version,
snapshot identity/time, actual rows, cap/truncation, file bytes and SHA-256.
Completion is published only after closing the snapshot, finishing the file,
hashing it and writing the manifest. Partial files are never downloadable.

Defaults: 15-minute export deadline; 60-second SQL statement limit; two-second
lock wait; 2 GiB file limit; 20 GB filesystem free reserve; 24-hour retention
after completion. `CHESSISM_EXPORT_TIMEOUT_SECONDS`, `CHESSISM_EXPORT_MAX_BYTES`,
`CHESSISM_EXPORT_FREE_FLOOR_BYTES`, `CHESSISM_EXPORT_RETENTION_SECONDS` and
`CHESSISM_EXPORT_ROOT` configure these bounds (set consistently on API/worker).
Preview has a 15-second deadline and one concurrent preview per token.

Files live in `research_data/exports/<id>/`, excluded from Git and database
backups. Small normal-column PostgreSQL tables `export_client` and `export_job`
track credentials/jobs; **no result rows or JSON documents are stored in them**.
Requests and manifests are files. A database restore does not restore temporary
downloads; missing files return 410. Keep the receiving app's verified copy and
manifest for reproducibility, then optionally DELETE the server copy. A failed
download does not delete it, and downloads can be retried before expiry.

## Installation and credentials (local administrator only)

Build the API image, then explicitly install only the two additive metadata
tables; no chess table migration or full-database copy is involved:

```sh
docker compose build chessism-api
docker compose run --rm --no-deps chessism-api python -m chessism_api.operations.exports.cli install
docker compose up -d --no-deps chessism-api worker-exports
docker compose exec -T chessism-api python -m chessism_api.operations.exports.cli create-client \
  --name local-dataframe --sources games players positions game_positions moves player_games \
  --max-rows 100000 --days 90 --output /research-data/integrations/local-dataframe.token
```

The token is written once to a private mode-0600 file, never printed. Mount that
file as a secret in the receiving app; do not put it in Git, logs or a URL.
`list-clients` shows identifiers, grants and expiry without credential hashes.
Use `revoke-client --id CLIENT_ID` and create a replacement file to rotate keys.

## Example client and validation

`scripts/fetch_dataframe_export.py` uses only Python's standard library. It
submits, polls, downloads in chunks, verifies SHA-256/size, and saves the manifest.
It refuses to overwrite existing files. Reuse its printed request key after a
connection failure to avoid submitting duplicate work.

```sh
python scripts/fetch_dataframe_export.py \
  --base-url http://127.0.0.1:8003/exports/v1 \
  --token-file research_data/integrations/local-dataframe.token \
  --request chessism_api/operations/exports/example-request.json \
  --output research_data/export-sample.parquet
```

In the receiving application (install pandas and pyarrow there):

```python
import pandas as pd
df = pd.read_parquet('export-sample.parquet', dtype_backend='pyarrow')
```

Large files still require enough RAM when loaded into a whole DataFrame. Use
`pyarrow.parquet.ParquetFile(...).iter_batches()` for bounded client processing.
No GPU, Google Cloud job, or cloud billing service is involved in this API.

Tests: `python -m unittest tests.test_exports`; isolated PostgreSQL lifecycle
tests in `tests/test_exports_database.py` require `EXPORT_TEST_DATABASE=1` and
a disposable database named exactly `chessism_export_test`.
