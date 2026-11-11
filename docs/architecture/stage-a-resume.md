# Chessism Stage A — safe-stop handoff

**PAUSED at the user's request on 2026-11-10, America/Mexico_City.**
Do not resume implementation, tests, services or refactoring until the user explicitly
authorizes it. Stage A is **incomplete and not accepted**. No A3/A4 completion claim.

## Git checkpoint and scope

- Repository: `/home/jon/Desktop/workshop/last_chessism` only.
- Branch: `refactor/p1-analysis-failure-propagation`.
- HEAD before checkpoint: `c74864ff4b4a7628262f1dedf96aa482187cca45`.
- Saved code checkpoint: **`05f0bf3bc059028f22c8bd5a693079cbef76522e`**, titled
  `wip: pause partial Stage A after verified dashboard checkpoint`.
- Current HEAD includes the following documentation-only handoff finalization.
  Resolve that HEAD with `git log -1 --format=%H -- docs/architecture/stage-a-resume.md`.
  The final chat response also records the exact HEAD; no application edits follow the WIP.
- The checkpoint is recoverable work, not integration or stage acceptance. No push.
- Before checkpointing: 17 modified files and five new Python files, all task-related.
  These are saved together with the handoff and evidence archive. No user files were
  discarded, reset or cleaned; ignored files were untouched.
- Continuing authorization comes from the Stage A plan, but today's explicit stop
  takes precedence. Preserve database/backups, stay in this repository, and do not
  push, merge, deploy, access production or begin Stage B.

Completed local commits before this WIP (oldest first):

- `22a406c` — feat: establish canonical PostgreSQL foundation
- `35be4fc` — docs: record Stage A1 verification and review handoff
- `6039b34` — refactor: establish canonical game import transactions
- `4eee021` — refactor: commit canonical game imports and projections atomically
- `d51cd42` — refactor: use canonical bounded FEN persistence and coverage
- `a0a2af9` — refactor: isolate canonical analysis selection and cloud queues
- `853c720` — refactor: publish canonical evaluations with atomic cloud receipts
- `809e112` — refactor: isolate canonical tablebase persistence and cleanup
- `9432d16` — refactor: establish canonical durable summary recovery
- `eac036c` — refactor: convert coverage and accuracy refreshes to canonical storage
- `3d2136f` — refactor: convert analysis pages and previews to canonical reads
- `59a305c` — refactor: convert player navigation and game statistics to canonical reads
- `c74864f` — refactor: use canonical snapshots for FEN browsing

## Completed implementation and evidence

A1 was independently accepted with documented limitations. Current schema version
is **6**. Runtime startup and all consumers have not yet been fully converted.

**A2 completed:** canonical atomic game imports and duplicate/conflict handling;
standard/nonstandard/custom/zero-move retention; explicit month accounting;
side-key pending work and projections in the import transaction; bounded canonical
FEN persistence and coverage; analysis selectors/cloud queues with locking and
claim rechecks; validated Stockfish writes and atomic cloud receipts; canonical
tablebase discovery/publication/lease cleanup; revisioned durable summary recovery
with exact acknowledgments, retry/backoff and cancellation safety. Public ingestion
results and accepted P1 ownership/progress/error guarantees remain preserved.

**A3.1 completed subgates:** default coverage/player-accuracy recovery callback;
canonical analysis overview/pages and frozen previews; player navigation and basic
game statistics; canonical FEN browsing/explorer/summary reads; dashboard counts
and shared full/scoped mode ranking refresh (the latter is saved in this WIP).
Application operations own transactions; persistence modules require them.
Accepted rollback/closure and P1 behavior are reused.

Focused verification already passed (counts overlap; do not sum as unique tests):

| Gate | Checks | Evidence directory under `/tmp/` |
| --- | ---: | --- |
| A2 imports | 117 | chessism-canonical-a2-imports-20261110-01 |
| A2 game workflow | 149 | chessism-canonical-a2-gameflow-20261110-01 |
| A2 FEN workflow | 198 | chessism-canonical-a2-fens-20261110-01 |
| A2 selection | 118 | chessism-canonical-a2-selection-20261110-03 |
| A2 evaluations/receipts | 136 | chessism-canonical-a2-evaluations-20261110-02 |
| A2 tablebase | 131 | chessism-canonical-a2-tablebase-20261110-01 |
| A2 recovery | 237 | chessism-canonical-a2-recovery-20261110-01 |
| A3 coverage/accuracy | 285 | chessism-canonical-a3-summaries-20261110-01 |
| A3 analysis views | 135 | chessism-canonical-a3-views-20261110-01 |
| A3 player views | 123 | chessism-canonical-a3-player-views-20261110-01 |
| A3 FEN views | 136 | chessism-canonical-a3-fen-views-20261110-01 |
| A3 dashboard/rankings | 153 | chessism-canonical-a3-dashboard-20261110-01 |

The latest run passed **66 foundation/PostgreSQL + 19 related contracts + 68 P1**,
including 17 new PostgreSQL cases, in 26.12 seconds. No failures, errors or skips.
Raw logs and structured results agree. All 1,745 frozen input hashes matched at
the start of the stop. Saving the handoff subsequently changed only the frozen
`docs/architecture/canonical-persistence.md` evidence/status text. The other 1,744
inputs still match; no application or test input changed. Both expected document
hashes are recorded in `stage_a_completion.safe_stop.post_stop_integrity`.
The retired module remains absent. Startup compilation and five offline checks
had already passed. Nothing was rerun after the stop request.

The dashboard gate preserves the A2 mode calculation, shares it with ingestion,
serializes refreshes with its advisory lock, and explicitly selects READ COMMITTED
for standalone refreshes. Tests exercise actual import/refresh blocking, full/scoped
publication, failure after deletion, repeated cancellation, filters, canonical
eligibility, empty/missing projections and real routes. Reads no longer rebuild
mode projections, and `main.py` no longer calls the obsolete ensure function.

## Remaining A3 and A4 work; known risks

- **A3.1 unfinished:** remaining dashboard/FEN/scored-rating projection writers;
  global and player time-control analytics, charts, rankings/summary consumers;
  home snapshots, player hours/saved analytics, common-position and related caches.
  Continue from the concrete plan and caller inventory, not from an assumption
  that moving a few functions completed the runtime conversion.
- **A3.2 not started:** remaining auxiliary canonical structures and consumers for
  CUDA/blunders, research, exports, graph, administration/deletion and snapshots;
  feature equivalence and representative synthetic query plans.
- **A4 not started:** fail-closed canonical startup/version/catalog validation;
  specifically detect **disabled FK enforcement** in the schema fingerprint;
  retire storage profiles, DBInterface, ask_db, old adapters/views/initializers
  after the caller audit; map old layout fixtures; final full API/worker baseline
  comparison, source/dependency/resource audit and one independent stage review.
- Mixed old/new consumers remain. Do not point this partial implementation at the
  existing database or assume application-wide canonical readiness.
- Historical managed-migration tools still need A4 mapping: the moved standalone
  mode refresh owns its transaction; no old savepoint/RR compatibility was added.
- Existing tie policies remain distinct: mode projections use the A2 white-side
  tie, while basic recent-game reads use their documented black-side tie. This is
  recorded for stage review, not silently changed at this checkpoint.
- Synthetic verification does not establish historical-scale performance, actual
  Stockfish/GPU execution, GCP behavior or deployment readiness.
- No blocker or failing check was found in the latest focused gate. No full suite
  has yet been run on the completed Stage A implementation, because it is unfinished.
- Unresolved baseline debt remains: API **1,450 tests, 3 failures, 10 errors,
  276 skips**; worker **287 tests, no failures/errors, 7 skips**; frontend **346
  tests, 4 failures, no skips**. Exact prior details remain in the manifest/log.
  API failure identities include `test_cloud_auto_sequences.AutoSequenceTests.test_split_total_instead_of_multiplying_or_balancing`,
  `test_database_runtime.ProfileTests.test_all_compose_database_consumers_share_one_default_legacy_switch`,
  and `test_matrix_cleanup.MatrixBoundariesTests.test_empty_column_projection_is_valid_for_every_row_type`.
  Do not silently skip/weaken tests or repair unrelated debt. Final A4 must show
  no unexplained additional failures/errors/skips and document retired-contract mappings.

## Files in the paused dashboard checkpoint

These changes have focused verification but remain part of incomplete Stage A:

- `chessism_api/database/postgresql/ask_db.py`
- `chessism_api/database/postgresql/dashboard_reads.py`
- `chessism_api/database/postgresql/engine.py`
- `chessism_api/database/postgresql/game_import_summaries.py`
- `chessism_api/database/postgresql/mode_summary.py`
- `chessism_api/operations/dashboard_views.py`
- `chessism_api/operations/mode_summary.py`
- `chessism_api/operations/player_deletion.py`
- `chessism_api/operations/players.py`
- `chessism_api/routers/fens.py`
- `chessism_api/routers/games.py`
- `docs/architecture/canonical-persistence.md`
- `main.py`
- `refactor-log.md`
- `refactor-manifest.json`
- `refactor-plan.md`
- `scripts/compact_db_application_checks.py`
- `scripts/db_refactor_corpus.py`
- `tests/check_canonical_dashboard_postgres.py`
- `tests/check_home_rankings_postgres.py`
- `tests/check_relational_application_postgres.py`
- `tests/test_db_refactor_corpus.py`

The checkpoint additionally contains this handoff and the durable evidence archive.
WIP `05f0bf3` preserves these files; the following commit only finalizes this
handoff and stop metadata. The source/hash inventories in the latest run distinguish changes from
all 1,699 originally protected baseline files.

## Processes, resources and database safety

- Latest supervisor **2682953** has exited, confirmed in the host process namespace.
- Run ID: `01e4a3a922864e89954effa32a5cf521`.
- Containers `chessism-canonical-a3-dashboard-01e4a3a922-{postgres,redis,foundation,workflow,p1}`
  were removed by the completed supervisor. The exact ownership-label query and
  a query for canonical test containers both return none. No named volumes or
  exposed network services were created; PostgreSQL data used disposable tmpfs.
- No task-owned verification writer or active transaction remains. No shutdown
  signal, force kill or extra resource deletion was needed during this stop.
- User monitor **2698835** was still open at inspection. It is a read-only display,
  refreshes every 120 seconds, and can be closed with Ctrl+C. It cannot modify the
  repository, database or saved verification results. It was left untouched.
- Existing Chessism database, verified PostgreSQL recovery checkpoint, Neo4j,
  Redis queues and production/GCP resources were not accessed or changed by this
  stop. No backup/restore, deployment, push or merge was performed.

## Durable evidence and tomorrow's first action

Archive: `docs/architecture/evidence/stage-a-stop-2026-11-10.tar.gz`.
SHA256: `e6db7951dc795f1a894ea0dd319fbf0026d9f2a7fa06579bd42f02ddc1f83c47`.
It preserves 350 top-level text artifacts from 16 Stage A runs, including failed
attempts, logs, result reports, source hashes, commands, runners and monitors.
`index.json` records original paths and per-file hashes; archive contents were
read back and verified. No database/socket/volume contents are included. Original
`/tmp` evidence remains intact; if it disappears, read the archive without running
its scripts or creating services.

**First action after explicit renewed authorization:** read this handoff, then
`refactor-manifest.json`'s `stage_a_completion`, the active portion of
`refactor-plan.md`, and the recent `refactor-log.md`. Verify repository/branch/HEAD
and working-tree state against this checkpoint; compare source hashes with the
latest saved inventory before changing anything, allowing the explicitly recorded
documentation-only stop update. Do not repeat the already passed
153-check gate unless intervening source changes justify it. Identify the next
cohesive remaining A3.1 summary/analytics conversion from the plan before editing.

All future long operations still require a unique `monitor.py`, one display
refreshed every 120 seconds, bounded isolated resources and cleanup. Confirm startup
and return control immediately; the user reports completion. Stop at the stage
boundary for independent review. **No resumption is authorized by this handoff.**
