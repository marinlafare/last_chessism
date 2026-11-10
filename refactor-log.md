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

## 2026-11-09 — Approved gate and temporary monitor handoff

- User explicitly approved proceeding under the proposed baseline-comparison
  gate. Updated the plan/manifest; no further approval of that exception is needed.
- User requested a removable console program showing elapsed time, current stage,
  estimated finish when supportable, and status. Created
  `/tmp/chessism-p1-verification/monitor.py`, outside the repository. Checked its
  syntax and a completed-result display without starting a monitoring wait.
- The default monitor checks every 120 seconds, displays start/elapsed/stage/test
  and result counts, and exits when finished. Ctrl+C closes only the monitor.
  It does not print application or system logs. Overall ETA is unavailable;
  the main-stage estimate uses the previous 53-second test duration and explains
  that the runtime has changed.
- Corrected verification setup uses Python 3.11.17 from the existing controller
  image, missing Neo4j/pglast dependencies in a disposable overlay, dummy auth
  values, and the worker-test fixture path. Both suites run without network or
  integration opt-ins. Dependency downloads alone have network access, with no
  checkout/credentials mounted. Containers run under the host UID/GID for cleanup.
- Started detached runner PID `1657631`. It installs dependencies, runs main
  unittest discovery, then worker unittest discovery, recording separate logs,
  structured reports, exact Docker commands and atomic progress in the temporary
  directory. Existing failures do not prevent the worker baseline from running.
- Monitoring command: `python3 /tmp/chessism-p1-verification/monitor.py`.
- Session ended. Current state: 0 of 2 chunks complete; corrected baseline process
  launched, results pending. No application or test source changed. No polling or
  waiting performed. Next: after the user reports completion, inspect both results
  and apply the approved comparison gate before starting chunk 1.

## 2026-11-09 — Live monitor correction

- User requested that the monitor remain open and report continuously instead of
  appearing as a snapshot. Changed the temporary program's default refresh from
  120 seconds to one second, with terminal output redrawn in place.
- The monitor now remains open even after verification finishes, displays its
  final status, and closes on Ctrl+C. It still follows the same status file and
  does not restart or interrupt verification. `--once` remains an explicit option.
- Checked changing stage/test output and continued rendering after completion
  with simulated state updates and no real-time wait.
- Session ended. Original verification handoff remains in effect; no new long
  process was launched, no running result was polled, and no application/test
  source changed. Await the user's completion report before continuing P1 #1.

## 2026-11-09 — Exact-version dependency build and monitor interval

- User reported failed setup. Read completed output: pip could not obtain
  `pglast==5.9`; setup exited after 2.62 seconds, before either suite started.
- Confirmed that the existing API image actually has pglast 5.9 with PostgreSQL
  parser 15.1 and Neo4j 6.3.0. The package index's version endpoint returned 404;
  the exact upstream release remains at https://github.com/lelit/pglast/releases/tag/v5.9.
- Inspected upstream setup and submodule configuration. Preserve the dependency
  version by building commit `b3fc527daeb113fb1ee6490ce1000d8e49c29cc4` and its
  pinned libpg_query submodule for Python 3.11. The temporary builder installs
  compiler/Git and Cython<3, verifies source/version, builds a reusable wheel,
  installs the overlay, and verifies imports plus parsing `SELECT 1`.
- The dependency container has a disposable writable filesystem and network;
  only temporary verification files are mounted. Test containers retain no
  network, read-only source/root filesystem, and host-user ownership. The builder
  returns dependency/wheel artifact ownership to the host user, even on failure.
- User changed the monitor preference back to 120-second refresh, clearing the
  previous display so only one record remains. Updated the program and plan;
  it stays open until Ctrl+C and now reports the dependency build's current step.
- Archived failed setup output under
  `/tmp/chessism-p1-verification/failed-setup-1/`. Restarted the detached pipeline
  as PID `1712083` using new `chessism-p1-corrected-baseline-v2-*` container names.
- Monitoring command remains `python3 /tmp/chessism-p1-verification/monitor.py`.
  Restart an already-open monitor to load the updated 120-second display behavior.
- Session ended. Current state: 0 of 2 chunks complete, corrected baseline run
  restarted and results pending. No application/test source changes, no dependency
  version changes in the project, and no polling/waiting on the new long process.
  Next: inspect results after the user's completion report, then proceed under
  the approved baseline-comparison gate.
