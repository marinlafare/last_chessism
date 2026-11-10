# P1 #1 refactor log

## 2026-11-09 — Planning and preflight

- Read the attached implementation instructions and architectural-refactor skill.
- Confirmed clean baseline `4f77d15` and no existing refactor tracking documents.
- Audited all three selectors and every repository caller. No caller requires
  suppressed errors; the parameter will be removed, as requested in the attachment.
- Confirmed that enabling propagation alone would feed the worker's broad retry
  handler. Acquisition must escape that handler and fail at the coordinator.
- Confirmed the separate Stockfish Lua producer can overwrite terminal phases.
- Prepared two ordered, independently verifiable chunks and a manifest.
- Local Python lacks PyArrow, Neo4j and pglast. The existing API runtime Docker
  image has these dependencies; the main baseline will use a temporary isolated
  container. No production services, data or images have been modified.
- No production source or test files have been modified. Baseline checks pending.
- User requires handoff for long processes: provide a monitoring command, then
  wait for a new user message rather than polling.

## 2026-11-09 — Baseline handoff

- Created branch `refactor/p1-analysis-failure-propagation`.
- Started main offline tests in detached container
  `chessism-p1-baseline-20261109` using the existing API runtime image. The checkout
  is read-only, `.env` is hidden, database settings are dummy, network is disabled,
  and no production volumes are mounted. Exact launch command is in the manifest.
- Monitor: `docker logs --follow chessism-p1-baseline-20261109`.
- Result: `docker inspect --format 'status={{.State.Status}} exit={{.State.ExitCode}}' chessism-p1-baseline-20261109`.
- No polling or waiting performed. Results remain unverified until the user
  reports completion. No application source or tests have been changed.
- Session ended. Current state: 0 of 2 chunks complete, baseline verification
  pending. Next: inspect this baseline result, finish remaining preflight checks,
  then begin chunk 1 if the verification gate passes.

## 2026-11-09 — Completed baseline diagnosis and gate decision

- User reported the process finished and requested continuation. Read its
  completed status and output; did not wait or poll a running process.
- Container exited 1. UTC start `2026-11-10T00:14:35.585254193Z`; finish
  `2026-11-10T00:15:32.18567682Z`. Tests took 52.923 seconds; container elapsed
  56.600 seconds. Result: 1,364 tests, 3 failures, 18 errors, 276 skips.
- Full output retained in the container and copied to
  `/tmp/chessism-p1-baseline-20261109.log` for diagnosis.
- Eight error reports come from the chosen environment: missing dummy auth
  variables (two import failures), missing worker-test path (one), absent Google
  dependencies (one), and Python 3.10 lacking unittest `enterContext` (four).
  These are setup defects, not evidence of application regressions.
- Inspected existing alternative images: controller has Python 3.11.17, Google
  dependencies, PyArrow, ARQ and NumPy, but lacks Neo4j and pglast. Batch test
  image has Python 3.11.17 and Google dependencies but lacks API dependencies.
  No dependency installation or additional long process has been started.
- Existing assertion mismatches:
  - `test_cloud_auto_sequences.AutoSequenceTests.test_split_total_instead_of_multiplying_or_balancing`:
    test rejects 200,001 Cloud Run positions; current schema permits 6,000,000.
  - `test_database_runtime.ProfileTests.test_all_compose_database_consumers_share_one_default_legacy_switch`:
    test expects 16 consumers, current Compose has 17.
  - `test_matrix_cleanup.MatrixBoundariesTests.test_empty_column_projection_is_valid_for_every_row_type`:
    test forbids `row_key,` anywhere in SQL; current score-distribution CTE contains
    that text while the final projection selects only `input.row_key`.
- Existing fixture mismatches:
  - Cloud cleanup fixture omits `run.job_id`, now used for timing publication.
  - Jobs API fixture supplies two Redis responses; progress now also reads
    `chessism:job_started`, exhausting that mock sequence and returning no progress.
  - Time/result explorer fixture supplies four execute responses; production now
    makes five calls, including three transaction/session settings. This yields
    eight error reports across tests/subtests.
- Existing balanced scheduling tests and inspected engine retry, loop, frozen-game
  and selection-query tests passed. Batch-limit module did not load because of
  missing dummy auth settings; its coverage remains pending.
- Recorded a concrete proposed verification-gate exception in the plan. It is
  not approved: first correct setup and establish exact baseline failures, then
  require passing P1 #1 regressions and no new full-suite failures without editing
  unrelated tests/source. The requested skill explicitly requires escalation
  when the entry test gate fails.
- Session ended. Current state: 0 of 2 chunks complete, application/test source
  unchanged, no running verification process. Next: obtain the gate decision,
  correct the isolated environment, and complete baseline checks.
