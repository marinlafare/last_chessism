# Stage D progress — source restore running

Stage D is in progress; it is not complete or accepted. Starting point:
`refactor/p1-analysis-failure-propagation`, accepted HEAD `5e12a21`.

## Current operation

On 2026-11-19 the isolated historical source restore started successfully at
18:26 CST. Archive identity, size and SHA256 match the verified checkpoint.
Seven restore-supervisor safety tests pass with no failures, errors or skips.
The operation is currently loading historical tables; final restore success and
all 197 exact table counts have **not** yet been verified.

- Workspace: `/tmp/chessism-canonical-stage-d-20261119`
- Container: `chessism-stage-d-source-restore-20261119`
- Source volume: `chessism_stage_d_source_20261119`
- Initial supervisor host PID: `996540`
- Evidence: workspace `evidence/restore-status.json`, `restore-commands.log`,
  `checkpoint-validation.json`, and eventually `source-ready.json`.
- Monitor: `python3 /tmp/chessism-canonical-stage-d-20261119/monitor.py`

The monitor replaces its display every 120 seconds; Ctrl+C stops only the viewer.
The detached supervisor remains running. Limits: 4 CPUs, 8 GiB RAM, two restore
workers, 150 GiB database size, 80 GiB minimum free space, two-hour operation
budget. The checkpoint is mounted read-only; the container has no network or
published ports. It writes only its new source volume and isolated evidence.
It stops PostgreSQL cleanly after validation or failure and retains the volume.
Do not remove or reuse an incomplete source volume without inspecting its state.

## Resume after the user reports completion

Read the live status and evidence first. Confirm a successful terminal status,
`source-ready.json`, matching counts across all 197 tables, and clean PostgreSQL
shutdown before treating the source as ready. On failure, inspect the saved error
and partial counts; preserve the original checkpoint and existing resources.

Next inspect the restored schema and actual historical relationships. Implement
explicit, bounded mappings into a distinct canonical target, starting with the
required representative sample and focused tests. No canonical target or importer
has been created yet. Full selected-data import, D2 real local application/browser
integration, and D3 PostgreSQL-only backup/restore and final comparison remain.

Accepted A/B/C source is unchanged. Original databases, existing Redis/Neo4j,
recovery checkpoint and GCP services remain untouched. No cutover, push, merge or
deployment is authorized. Stage D independent acceptance is still required.
