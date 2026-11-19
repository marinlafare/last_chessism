# Stage A implementation handoff — independent acceptance pending

Branch: `refactor/p1-analysis-failure-propagation`.
Scope: authorized Stage A only; no push, merge, deployment or live database cutover.

## Result

The runtime now requires canonical PostgreSQL schema version 17. Initialization
creates an empty database deterministically, owns its transaction and advisory
lock, and validates the schema identity, DDL digest and catalog constraints on
repeat use. Runtime startup validates; it does not install or migrate tables.

A2 native ingestion/evaluation writes and A3 required readers now use canonical
player IDs, game/side composite identities and four-field FEN identities. Game
variants remain representable; feature-specific analysis eligibility remains
explicit. Authentication, account ownership, ingestion accounting and cloud
claims/receipts/cancellation state remain part of the initialized contract.

Required analytics, exports, CUDA publication, research, graph-outbox, player
administration and FEN backup consumers have been converted. SQL resides in
concrete persistence modules where useful, with operation-owned transactions;
no generic replacement for DBInterface was introduced. The accepted P1 cleanup,
error propagation, attempt isolation and authoritative job outcomes are preserved.

A4 removes DBInterface, ask_db, competing storage adapters, historical runtime
installers and their maintenance launchers. Active bounded ingestion remains.
Historical-only tests have explicit dispositions in storage-retirement-tests.json;
important lifecycle checks now use canonical PostgreSQL. Read-only validation of
old backup archives remains, including the unchanged compact validator logic.

## Approved contract decisions

- Exports use `(game_id, is_white)`, float64 elapsed time and int64 join timestamps.
- Historical saved-CPU comparison launch paths are retired; saved reports, current
  model training/evaluation and CUDA publication remain.
- Whole-database manifest format 9 carries canonical identity/checksums. Old
  archive validation remains supported; archive operations/locations are unchanged.
- Unattributed saved FEN scores may retain NULL source and known or NULL timestamps.
  No source/time is invented. Other integer, WDL, source, uniqueness and foreign-key
  constraints remain. NULL-timestamp scores appear conservatively in every delta.

## Verification

| Gate | Result |
|---|---|
| API final comparison | 1,279 effective checks; 1 existing failure, 2 existing errors, 257 offline opt-in skips |
| Worker, supported Python 3.11 | 287; no failures/errors, same 7 skips |
| Accepted P1 checks, isolated Redis | 68 passed; no failures/errors/skips |
| Frontend | 346; 342 passed, same 4 failures; no skips/cancellations |
| Frontend architecture, size, build | Passed |
| Canonical cloud database fixtures | 112 distinct checks passed on disposable PostgreSQL |
| Canonical worker checkpoint lifecycle | 3 passed, including rollback/retry and cancellation |

The API result combines one broad 1,276-case run, 37 affected fixture checks and
four cloud SDK checks; import placeholders are replaced by actual discovered
cases. It is not a second full-suite run. Unsupported worker/runtime and incomplete
frontend environment attempts remain recorded, without being counted as passes.
The supported worker suite was rerun unchanged. Frontend failure blocks match the
baseline after normalizing only timing, numbering and temporary source paths.

The three remaining API defects reproduce independently on unchanged baseline
`0fde829d9477b3f73714b52e820a67bad49dad77`:

- `test_cloud_auto_sequences.AutoSequenceTests.test_split_total_instead_of_multiplying_or_balancing`: expected ValueError not raised.
- `test_cloud_analysis.ControllerGateTests.test_real_cleanup_writes_tmp_plan_and_can_recreate_it_after_restart`: fixture lacks `job_id`.
- `test_pipeline_helpers.AnalysisJobsApiTests.test_analysis_jobs_lists_running_and_queued_from_server_queue`: fixture produces NULL progress.

Earlier baseline: 1,450 API tests, 3 failures, 10 errors, 276 skips. Exact discovery
differences are saved: 61 added/renamed and 232 removed/renamed identities. Obsolete
profile/migration checks were retired; current behavior was ported. Existing
matrix and time-filter mock mismatches were corrected with their conversions.
Offline PG/Redis/GPU skips remain explicit. Affected FEN restore, matrix and cloud
cases passed their separate isolated gates; P1 Redis tests passed separately.

Other focused evidence is retained and reused: 197 export lifecycle, 164 CUDA
checks plus 12 overlapping real RTX 4060 cases, 78 graph/admin checks plus two real
CUDA map checks, 44 backup checks with synthetic dump/restore, 65 FEN backup/restore,
92 matrix, 44 quality, and 50 research/admin checks. Counts overlap and must not be
summed as a unique total. The final source comparison confirms application code
was unchanged after the broad run; only affected fixture and tracking corrections
followed it. No redundant export full gate was run.

Evidence:

- `evidence/stage-a-final-comparison-2026-11-18.tar.gz`
  SHA256 `70dd8c92e1dab50abc0a8d7aff77a781da9204f5f42661a61163c4929f501738`.
- `evidence/stage-a-storage-retirement-2026-11-18.tar.gz`
  SHA256 `2073b45b4e0c59fd24b1b3e04d3cfdebdf333ade6b7f010633d5eac516afbaf1`.
- Earlier focused archives/checksums: `refactor-manifest.json`, Stage A fast-track records.

## Limits and next action

The original database, verified recovery checkpoint, Neo4j, Redis and GCP resources
were untouched. All verification-owned containers were removed. The original
database has not been converted: canonical startup deliberately rejects its old
schema. Deployment or data cutover requires separate authorization and planning.

No production-scale performance claim follows from synthetic fixtures. No real
GCP or Neo4j integration was run. Real CUDA coverage was selected, not a full
production workload. The existing salience SQLite report format supports one side
per game and explicitly rejects self-play before publication. Numerical formulas
and saved reports remain unchanged. Unrelated API/frontend testing debt remains.

The inventory initially missed matrix/quality/research/game-information consumers
and the existing coefficient-experiment table; these were converted/initialized
before retirement. This explains the additional work beyond the earlier estimate.

Next: independent acceptance review of Stage A's actual code and saved evidence.
Stage A is implemented but **not formally accepted**. Do not start Stage B or C
from this handoff.
