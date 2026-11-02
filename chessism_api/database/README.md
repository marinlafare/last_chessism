# Database backends

```
database/
  postgresql/  # authoritative data, ORM, storage profiles, existing migrations
  neo4j/       # optional derived position graph, connection/configuration tools
```

PostgreSQL remains authoritative for games, positions, evaluations, summaries,
accounts, appearance, jobs, and backups. Its code moved; its database service,
tables, storage paths, migrations, and SQL behavior did not change. Use explicit
imports such as `chessism_api.database.postgresql.engine`.

Neo4j is a separate Docker service, with separate named `neo4j_data` and
`neo4j_logs` volumes. The `neo4j-sync` service maintains a derived chess graph
from a transactional PostgreSQL change queue. See [synchronization](neo4j/SYNC.md)
for the schema, bounded backfill and acceptance sequence. PostgreSQL/API startup
does not depend on Neo4j. No optional graph plugin (APOC/GDS) is installed.

## Local Neo4j setup

From the repository root:

```sh
python -m chessism_api.database.neo4j.manage init-secret
docker compose up -d --no-deps neo4j
docker compose exec -T chessism-api python -m chessism_api.database.neo4j.manage check
```

The first command creates `.local-secrets/neo4j-auth` (mode 0600) with a random
password. It refuses to overwrite an existing secret. The directory is excluded
from Git and Docker build contexts. Docker mounts the file into Neo4j and the
API; passwords are not stored in Compose, returned by health checks, or logged.
An explicit root bootstrap reads the owner-only bind-mounted secret before
delegating to the official entrypoint, which runs the database as `neo4j`.
The initial account is `neo4j`. To use Browser, read the local secret yourself;
the password is the portion after the first `/`. Do not paste it into chat.

- Browser: http://localhost:7474
- Host Bolt: `bolt://localhost:7687`
- Internal API connection: `bolt://neo4j:7687`, database `neo4j`
- Published ports bind only to 127.0.0.1; no public reverse-proxy route is added.
- Budget: 2 CPUs, 3 GiB container limit, 1 GiB maximum heap, 512 MiB page cache.
- Usage telemetry, Fleet management/discovery, and forwarded HTTP headers are
  disabled. The Python driver also disables telemetry.

The container health check checks HTTP availability. The explicit backend
`check` command also verifies authentication and executes a read-only query.
The API has no graph startup connection requirement. System Status reads saved
PostgreSQL synchronization status; the independent graph worker owns its driver
lifecycle. There is no public arbitrary-Cypher endpoint.

Changing the initial secret does not change an existing Neo4j database password.
Use Neo4j's password-change workflow and update the mounted secret together.
Never delete the data volume to reset a password or use `docker compose down -v`.

## Deployment boundary

The folder reorganization requires a complete backend-code build, not an older
partial-file overlay. Root `Dockerfile` already copies the complete package.
For the initial API-only release, `dev_tools/Dockerfile.database-services`
preserves the verified dependency filesystem, flattens the old image's deep
layer chain, adds the driver, copies the full backend, and removes legacy module
paths. Older historical partial overlays must not be used for this relocation.

Already-running workers retain their existing code and need no restart: their
SQL schema and job payload contracts are unchanged. Rebuild whole backend code
when deploying their next change. Do not hot-copy half of this package move into
a running service.

## Chess schema

Shared `Position` nodes use canonical PostgreSQL FEN IDs and preserve side to
move, castling and en-passant rights. `Game` nodes retain every ply, including
repetitions; `Player` nodes connect to games and positions. One standard initial
position is shared. Typed evaluations are copied for indexed graph filtering.
Occurrences and distinct-game counts cover imported games only. PostgreSQL
remains authoritative; the graph is rebuildable and independent of CUDA scheduling.

## Backup and Test backup

The existing **Backup** button now creates one cataloged recovery bundle:

- A consistent PostgreSQL custom-format compressed snapshot, plus the existing
  external matrix files and research-definition integrity manifests.
- Native Neo4j dumps of **both `neo4j` and `system`**, stored under
  `database/neo4j/<snapshot-id>/`, each with its own byte count and SHA-256.
- The exact Neo4j image digest, so rehearsal cannot silently upgrade the store.

Neo4j Community requires an offline dump. Only Neo4j pauses while those two
archives are written, and it is restarted before the PostgreSQL dump proceeds.
PostgreSQL, ingestion, CUDA, and cloud workers are never stopped. A publication
barrier freezes the derived graph before acquiring the PostgreSQL snapshot and
releases it after the graph dump. Pending synchronization events are included in
the SQL snapshot; replay safely catches up after restoration. This is a
coordinated recoverable projection, not a distributed transaction between engines.

**Test backup** restores PostgreSQL to its existing private socket, validates it,
and removes that disposable copy. It then checks both graph archive checksums,
loads them into a fresh disposable Neo4j volume, runs native consistency checks
(graph, properties, indexes, counts), starts the isolated database, waits for its
indexes, and queries graph counts, schema and system users. No published ports,
network connectivity, live data volume, or production credentials are given to
the restore container. Authentication is disabled **only in that isolated copy**.
The temporary containers/volume are removed before success is published.

A new bundle is marked restore-tested only after **both engines pass**. Previous
verified backups are retained until then; retention also rechecks graph hashes.
Legacy backups remain PostgreSQL-only and report Neo4j as `not_included`; testing
one cannot invent a Neo4j backup or retire a graph-containing recovery point.

`neo4j-backup` is a dedicated controller, accessed over a private file mailbox
mounted only into the PostgreSQL backup worker. It has no network, cloud secrets,
API routes or Redis connection. Docker socket access is **host-privileged** even
when mounted read-only; do not expose the controller or add arbitrary commands
to its protocol. It accepts only validated snapshot IDs and dump/test operations,
selects the exact Compose project's Neo4j service, and cleans only its labeled
temporary objects. The API and PostgreSQL worker do not receive Docker access.
The PostgreSQL worker still runs as `postgres`.

The controller records a durable recovery journal before stopping Neo4j. On
failure/restart it releases temporary dump locks and restarts that same service.
A replaced service ID requires inspection instead of guessing. Expiring worker
leases cancel abandoned requests. Never delete a recovery journal to bypass a
failed restart; inspect `docker compose logs neo4j-backup neo4j` first.

Keep `.local-secrets/neo4j-auth` in separate protected secret storage. `system.dump`
contains authentication records/hashes, not the plaintext login password. Dump
files are private (0600). Store snapshots and their manifests together. Disaster
restoration should load both dumps with the recorded image into a **new** volume,
never over the live volume. Use Neo4j's documented offline dump/load procedure.

Isolated regression check: `tests/check_neo4j_backup_roundtrip.py` provisions a
synthetic graph with a relationship/constraint and runs the actual PostgreSQL
snapshot plus Neo4j dump/load/check/query flow. It requires a disposable host
directory prefixed `/tmp/chessism-neo4j-backup-test.`, and never uses live mounts.
