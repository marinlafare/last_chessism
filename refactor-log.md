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

## 2026-11-09 — Corrected baseline accepted; starting chunk 1

- User reported completion. Dependency build succeeded with exact pglast 5.9
  (PostgreSQL 15.1 parser) and Neo4j 6.3.0. Retained its reusable wheel/dependencies.
- API baseline: 1,395 tests in 52.58 seconds, 3 failures, 10 errors, 276 skips.
  Every failure/error matches the previously diagnosed unrelated baseline issues;
  all eight test-environment error reports are resolved. Worker baseline: 287
  tests in 26.41 seconds, zero failures/errors, seven opt-in skips.
- Accepted preflight under the user's approved comparison gate. Archived exact
  results, logs and commands in `/tmp/chessism-p1-verification/baseline/` and stored
  failure/error/skip identities and dependency versions in the manifest.
- Confirmed local isolated Redis 7 and PostgreSQL 15 images are available.
- Starting chunk 1: added eight regressions exercising the actual Stockfish Lua
  and coordinator reset against explicitly isolated Redis. They cover terminal
  payloads, sets and absolute expiration times, normal counters/deduplication,
  intermediate loop completion and reset after both final phases. Production
  source is still unchanged; red verification is next.

## 2026-11-09 — Chunk 1 red/green evidence

- Red run on unchanged production code: eight isolated Redis tests, three failures.
  Late updates changed both final phases to `analyzing` and recreated progress
  sets after a terminal state. Normal progress and reset scenarios already passed.
- Corrected the new test cleanup to explicitly await Redis key deletion; repeated
  red verification confirmed the same three failures without cleanup warnings.
  Authoritative red output: `/tmp/chessism-p1-verification/terminal-red2.log`.
- Added the atomic early guard in Stockfish's existing Lua script, before any
  set mutation or expiry update. Reuses decoded progress for normal monotonic
  counters; no changes to engine execution, retry or timeout policy.
- Green run: all eight tests passed in 0.43 seconds using actual Redis/Lua.
  Output: `/tmp/chessism-p1-verification/terminal-green.log`. Changed Python
  sources compile in memory and `git diff --check` passes.
- Prepared a detached full-suite gate that reuses dependencies (no installation),
  runs the new Redis tests with the full API suite, then the worker suite, compares
  exact failures/errors/skips and expected test counts against the saved baseline,
  and stops the isolated Redis afterward. Expected combined duration is roughly
  82 seconds based on previous suite timing and container startup.

## 2026-11-09 — Chunk 1 verification handoff

- Started detached full-suite gate PID `1778308`. Monitor with
  `python3 /tmp/chessism-p1-verification/monitor.py`; it clears the previous display
  every 120 seconds, remains open until Ctrl+C, and shows the estimated finish.
- Session ended. Current state: chunk 1 implemented with focused red/green
  evidence, full gate pending; chunk 2 not started. Production/test changes remain
  uncommitted until this gate is reviewed; only tracking documents are committed
  at this handoff. No polling or waiting performed on the long verification.
- Next: when the user reports completion, inspect `chunk1-gate-result.json`, review
  the diff, commit chunk 1 if its comparison gate passes, then begin chunk 2.

## 2026-11-09 — Chunk 1 verified

- User reported completion. Full comparison gate passed in 87.30 seconds.
- API suite: 1,403 tests, the same 3 baseline failures and 10 errors, 276 skips.
  All eight added Redis regressions ran; no additional failures/errors/skips.
- Worker suite: 287 tests, zero failures/errors, seven unchanged opt-in skips.
  Failure/error identities and messages, skips and dependency versions match
  baseline. The runner stopped the isolated Redis successfully.
- Reviewed the production diff: only the existing Stockfish progress Lua changes.
  Committing chunk 1 as planned; its terminal/reset behavior is verified.

## 2026-11-09 — Chunk 2 implementation and focused verification

- Chunk 1 committed as `f165434`. Started chunk 2 only after its full gate passed.
- Added 20 selector/coordinator regression tests with deterministic event-based
  concurrency and bounded hang guards. Red run produced 52 failed assertions
  (including subtests) and 11 errors: suppressed selection failures, cleanup
  masking original errors, cancellation leaks and successful finalization.
  Output: `/tmp/chessism-p1-verification/failure-red.log`.
- Added two real ARQ/Redis tests. Before the fix, the failure case demonstrated
  `result.success == True` after a database exception; genuine empty success
  already passed. Output: `/tmp/chessism-p1-verification/arq-red.log`.
- Removed `raise_errors` from all three selectors and all audited callers.
  Selectors now propagate failures/cancellation and release sessions they have
  not handed off. A small private cleanup helper always attempts close if
  rollback fails; active selection errors remain the propagated exception.
- Moved acquisition outside processing retries. Worker cancellation/permanent
  engine errors preserve their original exceptions through rollback/close errors.
  Coordinator awaits sibling cancellation/cleanup, publishes failure best-effort,
  and rethrows. External cancellation is not published as a database failure.
- Accounted for acknowledged commits before awaiting summary refresh: a cancelled
  sibling cannot lose its committed batch from the final progress counters.
- Focused green run: all 22 tests passed in 1.52 seconds, including actual global,
  player, all/player loop and frozen-game callbacks, empty success, ARQ failed
  results, offsets, sibling cleanup ordering, reporting failures, cancellation,
  and existing no-result/invalid-result/write-error retry behavior.
  Output: `/tmp/chessism-p1-verification/failure-green.log`.
- AST checks confirm all selection query constructions and the player SQL builder
  are unchanged. Every cloud/benchmark/integration-test caller change only removes
  the obsolete keyword. Engine HTTP retry/timeout handling and all four public
  entry-point function bodies are unchanged. Compilation/diff whitespace checks
  passed; no configured Python lint/type-check commands exist.
- Prepared full API/worker baseline comparison plus six existing PostgreSQL tests
  for locks, disjoint/cloud reservations, fresh eligibility and stale statistics.
  All services use isolated Unix sockets, temporary data and no network during
  tests. API suite should run 1,425 tests (30 new across both chunks); worker 287.
  PostgreSQL duration has no established estimate, so overall ETA is unavailable.

## 2026-11-09 — Chunk 2 long verification handoff

- Started detached runner PID `1858391`. The existing monitor command remains
  `python3 /tmp/chessism-p1-verification/monitor.py`, refreshing one display every
  120 seconds. It shows the current suite/test, elapsed time and stage estimate
  where available. No polling or waiting performed on this long process.
- Session ended. Current state: chunk 1 verified/committed; chunk 2 implemented
  with passing focused regressions, full suites/PostgreSQL gate pending. Chunk 2
  source/test changes remain uncommitted until gate review; tracking files are
  committed for handoff. Next: inspect all saved results after the user's report,
  resolve any actual regressions, then commit chunk 2 and complete postflight.

## 2026-11-09 — Full suites pass comparison; PostgreSQL setup correction

- User reported the failed gate. Both complete suites passed the approved
  comparison: API 1,425 tests in 52.24 seconds with the exact 3 known failures,
  10 known errors and 276 skips; worker 287 tests in 26.52 seconds with zero
  failures/errors and seven unchanged skips. All 30 added regressions ran.
- Failure occurred before PostgreSQL tests started. The temporary server used
  `/pgsocket`, while the official image's initialization function explicitly clears
  PGHOST and makes psql use `/var/run/postgresql`. Confirmed this by inspecting
  the installed entrypoint. Its bootstrap also defaults to port 5432 unless
  PGPORT is set, even when the final server command sets port 55432.
- Corrected only the temporary harness: mount the private socket directory at
  the image's default socket path and set PGPORT=55432 for initialization. Kept
  no network, read-only filesystem, host UID/GID and temporary PostgreSQL data.
  Removed the conflicting tmpfs for the socket directory; the host-owned bind
  directory also resolves its nonfatal chmod warning.
- Preserved failed setup evidence under
  `/tmp/chessism-p1-verification/postgres-startup-attempt1/`. Application/test
  source is unchanged. The next run retries only the six PostgreSQL tests and
  retains the verified full-suite results, avoiding an unnecessary rerun.

## 2026-11-09 — PostgreSQL-only retry handoff

- Started detached PostgreSQL retry PID `1913071`; monitor command remains
  `python3 /tmp/chessism-p1-verification/monitor.py`. It clears the prior display
  every 120 seconds and shows current stage/test. Overall ETA remains unavailable.
- Session ended. Chunk 1 committed; chunk 2 focused and full-suite comparisons
  passed, isolated PostgreSQL check pending. No new application changes or
  polling/waiting. Next: inspect the PostgreSQL result after the user's completion
  report, then finish verification/commit/postflight if all required checks pass.

## 2026-11-09 — Chunk 2 verified and final scope review

- User reported exit 0. All six existing isolated PostgreSQL tests passed in
  8.91 seconds, with no skips: local locks versus cloud claims, disjoint cloud
  reservations, fresh recheck of scored positions, frozen-sequence claims,
  work-unit claims, and bounded rechecking with stale statistics/generic plans.
  PostgreSQL-only retry including setup/cleanup took 11.38 seconds.
- Combined final gate passed. Retained unchanged API/worker results: API 1,425
  tests, exactly the known 3 failures/10 errors and 276 skips; worker 287 tests,
  zero failures/errors and 7 skips. All 30 new tests ran and passed. Isolated
  Redis and PostgreSQL test services were stopped successfully.
- Final scope/source compilation/whitespace checks passed. No new TODO/FIXME.
  No application/test edits occurred after the verified full-suite run; only
  temporary PostgreSQL setup and tracking documents changed afterward.
- Root cause resolved: selectors used the empty result for operational failures,
  while the worker's processing retry handler also intercepted propagated
  acquisition failures. Strict selection and coordinator failure handling now
  preserve failure through to an unsuccessful ARQ result.
- Clean Architecture responsibilities are explicit: selection owns cleanup until
  session handoff; workers own returned transactions; coordinator owns failure
  finalization; Stockfish telemetry respects final job state. Kent Beck-style
  simplification removes the suppression switch everywhere and retains existing
  callbacks/return tuples, adding only one small shared selection cleanup helper.
- Production implementation: `chessism_api/database/postgresql/ask_db.py`,
  `chessism_api/operations/analysis.py`, `stockfish-service/routers/analysis.py`.
  Keyword-only caller edits: cloud `controller.py`, `input_preparation.py`,
  `work_units_controller.py`; `stockfish-batch-worker/benchmark_vm16/large_database.py`;
  `tests/test_cloud_analysis_database.py`, `test_cloud_sequences_database.py`,
  `test_cloud_work_units_database.py`, `check_cloud_game_queue_postgres.py`.
- New regression files: `tests/test_analysis_terminal_progress.py` (8),
  `test_analysis_failure_selection.py` (8), `test_analysis_failure_jobs.py` (12),
  `test_analysis_failure_arq.py` (2), with `analysis_failure_test_support.py`
  providing explicit isolated Redis access and collision-free service import.
- Known limits: unrelated baseline failures and other opt-in integration skips
  remain; no production/GCP or real Stockfish run; no configured Python lint/type
  checker. Redis reporting remains best-effort, with ARQ failure authoritative.
- No design deviation in application behavior. Verification adjustments were
  the explicitly approved baseline-comparison gate and isolated environment fixes.
  P1 #2, schema migrations, production data, deployment and unrelated refactors
  were excluded. Preparing the planned chunk 2 implementation commit.

## 2026-11-09 — Completed

- Chunk 2 committed as `244a6a4cba30bc4b14944f2fc264343db842e06a`:
  `fix: fail local analysis when database batch selection fails`.
- Chunk 1 commit: `f165434e3eb617f28e96081c60d62c0316225224`:
  `fix: preserve terminal analysis progress against late engine updates`.
- Both chunks and postflight completed under the approved baseline gate. The
  implementation worktree was clean after its commit. No required work remains
  for P1 #1; the unrelated baseline issues and P1 #2 remain outside scope.
- Commits are local on `refactor/p1-analysis-failure-propagation`. The earlier
  pre-refactor checkpoint had already been pushed; no new deployment was made.
- Temporary monitor/results remain at `/tmp/chessism-p1-verification/` for review
  and later deletion. All isolated test services have stopped. Updated the
  monitor's final display to indicate completed implementation and commit IDs.

## 2026-11-09 — Independent review corrections authorized

- Independent review reproduced two blocking defects: redundant cancellation can
  interrupt sibling close, and reset accepts stale Stockfish updates because
  progress has no attempt identity. The original selection-error fix remains valid.
- User explicitly requested these two corrections, separately verified/committed
  on the existing branch. No push, merge, deployment, P1 #2 or unrelated changes.
  P1 #1 acceptance remains pending a subsequent independent review.
- Extended the existing plan/manifest with chunks 3 and 4, preserving the original
  history and approved comparison gate. Working tree was clean at `b81b6ea`.
- Application Dockerfile uses Python 3.10. Cancellation will be owned by the
  coordinator through a shielded initial gather and protected drain; no new task
  APIs, cancellation framework or runtime upgrade are needed.
- Entry checks rerun the 20 existing selector/orchestration tests. The full saved
  gate remains applicable to unchanged source; full suites will run after each
  correction. Retain the user's 120-second in-place monitor and handoff policy.

## 2026-11-09 — Chunk 3 focused correction and regression evidence

- Entry: all 20 existing selector/orchestration tests passed. Started chunk 3.
- Added two event-synchronized tests, each with successful and failed rollback:
  two workers close at different times, and a repeated external cancellation
  arrives while the coordinator is draining. Tests verify completed close before
  coordinator exit, preserved cancellation, no commit and no failed/complete
  publication. A scheduled callback provides an event-loop checkpoint, not a
  timing sleep; a wrapper observes entry into the actual gather drain.
- Red: both final tests against the original coordinator from `b81b6ea` fail
  in all four subcases because sibling close receives another cancellation.
  Evidence: `/tmp/chessism-p1-verification/correction1-red-final.log`.
- The coordinator now shields its initial gather, owns worker cancellation once,
  and shields/awaits the same cleanup gather through later cancellation requests.
  It observes the original gather and then re-raises the original exception.
  Failure publication, transaction handling and processing policy are unchanged.
- Green: all 22 selector/orchestration tests pass locally, and all 22 also pass
  in the existing isolated Python 3.10 API runtime (1.097 test seconds). The first
  Python 3.10 run exposed only a test assumption about cancellation message
  propagation; replaced the message assertion with the actual cancelled task
  state. All cleanup assertions remain. No application workaround was added.
- Python 3.10 syntax/source compilation and `git diff --check` pass. Prepared
  the full API/Redis/ARQ and worker gate (expected 1,427 and 287 tests) using the
  same environment and exact baseline failure/error/skip comparison. Existing
  terminal regressions remain part of that gate. No chunk 4 code changes yet.
- An extra in-memory red replay initially copied the module globals, bypassing
  test patches and hanging the temporary harness; interrupted it with Ctrl+C.
  Corrected the harness to bind the unchanged baseline function to the actual
  module globals, with a hard process guard. Final replay confirms four expected
  red subcases followed by all 22 current tests passing. No source rollback or
  application change was involved in this harness correction.

## 2026-11-09 — Correction 1 full-gate handoff

- Started detached verification runner PID `2448487`. Monitor:
  `python3 /tmp/chessism-p1-verification/monitor.py`. It refreshes one
  in-place display every 120 seconds and stays open until Ctrl+C.
- Session ended. Chunks 1/2 complete; chunk 3 implemented with red/green
  and Python 3.10 focused evidence, full comparison gate pending.
  Chunk 4 pending. Source/tests remain uncommitted until gate review;
  tracking documents committed for handoff. No polling or waiting.
- Next: after user reports completion, inspect `correction1-gate-result.json`,
  resolve any genuine regressions, commit chunk 3, then begin chunk 4.

## 2026-11-09 — Chunk 3 full verification passed

- User reported completion. Reviewed saved gate: passed in 78.06 seconds. API
  1,427 tests with exactly the established three failures, ten errors and 276
  skips; worker 287 tests with zero failures/errors and seven unchanged skips.
  Full failure/error traces (ignoring line numbers), skips, dependency versions
  and Python version match baseline. All 32 P1 regression tests ran.
- Isolated Redis stopped successfully. Final whitespace check passed. Preparing
  the separate local cancellation correction commit; no push or merge.

## 2026-11-09 — Chunk 4 attempt isolation and focused verification

- Chunk 3 committed locally as `5542c9ad00d5df547ccf16d14f657399dd96feef`.
  Started chunk 4 using that verified state. No unrelated working-tree changes.
- Red: added real isolated Redis tests for old Stockfish updates after reset,
  after new progress and after completion. On the original progress code, 11
  tests ran with two failures: old updates recreated cleared state and polluted
  the new attempt. The existing terminal guard already protected completion.
  Evidence: `/tmp/chessism-p1-verification/correction2-red.log`.
- Reset now generates a UUID and atomically sets the job's active attempt key
  while deleting the prior payload/FEN sets. The same ID is explicitly passed
  through global/player, loop and frozen-game batches, all coordinator reports,
  and the Stockfish HTTP payload. Both progress Lua scripts reject absent/stale
  attempt IDs before mutation. Successful updates renew the attempt's existing
  24-hour retention; rejected/terminal engine updates change no TTL or state.
- Progress-enabled Stockfish requests require both job and attempt IDs. Live
  requests without progress remain valid. There is no legacy permissive path,
  new layer, repository, result class or custom exception. Sub-passes require
  their parent's explicit identity rather than inventing one or reading Redis.
- Added coverage for stale coordinator writes; missing/mismatched/expired IDs;
  atomic reset; identity retention; all entry-point/pass propagation; current
  terminal protection; reset/report outages preserving the original database
  exception; wire validation and ARQ stored outcomes. There are now 47 P1
  regressions total, including 17 added across these two corrections.
- Focused green: 60 tests passed in 3.271 seconds, including all 47 P1 regressions
  plus existing scheduling/batch-limit checks, with real isolated Redis and ARQ.
  First focused run had one test-spy failure: AsyncMock wrapping Redis's ordinary
  method returned its coroutine without awaiting it. Corrected the spy to record
  the ordinary call while allowing the real await; no application change needed.
- Application Python 3.10: all 25 selector/orchestration tests also pass. Syntax,
  in-memory compilation and whitespace checks pass. AST comparison confirms the
  Stockfish endpoint's engine execution and API HTTP/error/timeout policy are
  unchanged; selection, lock/recheck, result staging and tablebase files untouched.
- Existing Redis failure behavior stays best-effort: an unavailable reset cannot
  authorize new writers under the old ID. The new attempt then has no reportable
  progress until a successful reset; retained old progress may remain visible.
  ARQ/database outcomes are unaffected. Expired identities cannot be resurrected
  by either writer. No deployment, production data, push, merge or P1 #2 changes.
- Prepared full API/worker comparison and six isolated PostgreSQL checks. Expected
  counts: 1,442 API, 287 worker and six PostgreSQL tests. Formal acceptance remains
  pending the user's focused independent review after implementation verification.

## 2026-11-09 — Correction 2 full-gate handoff

- Started detached verification runner PID `2495389`. Monitor:
  `python3 /tmp/chessism-p1-verification/monitor.py`. One in-place display
  every 120 seconds; remains open until Ctrl+C. Estimated total ~92s.
- Session ended. Chunk 3 verified and committed as `5542c9a`; chunk 4
  implemented with passing focused Redis/ARQ and Python 3.10 checks.
  Full API/worker/PostgreSQL comparison pending. No polling or waiting.
  Implementation/tests remain uncommitted until result review; tracking
  files are committed for handoff. No formal acceptance is claimed.
- Next: inspect `correction2-gate-result.json` after the user reports
  completion, verify recorded source hashes, resolve any regressions,
  commit chunk 4 and complete the correction record. Stop for independent
  review; do not push, merge, deploy or begin P1 #2.

## 2026-11-09 — Chunk 4 verified and correction postflight complete

- User reported completion. Final gate passed in 91.60 seconds: API 1,442 tests
  with the exact three baseline failures, ten errors and 276 skips; worker 287
  tests with no failures/errors and seven unchanged skips. Independently compared
  saved full failure/error traces (normalizing line numbers), skip identities,
  Python and dependency versions: all match the approved baseline.
- All 47 P1 regressions ran in the full API suite, including the 17 added across
  these corrections. All six isolated PostgreSQL lock/claim/recheck tests passed,
  without skips. Isolated Redis and PostgreSQL services stopped successfully.
- Verified all five recorded source hashes match the files tested by the final
  gate. Final compilation, Python 3.10 syntax, whitespace and scope checks passed;
  no new TODO/FIXME. Reuse this unchanged final gate for postflight; only tracking
  documents are modified afterward.
- Correction 1 gives the coordinator sole cancellation ownership and awaits
  protected cleanup. Correction 2 establishes an attempt atomically and requires
  it in both progress writers. No extra architectural layer or compatibility
  fallback. SQL/locking, transaction ownership, commits, ARQ failure propagation,
  empty selection, engine policy and tablebase behavior remain unchanged.
- Implementation files for the corrections: `chessism_api/operations/analysis.py`
  and `stockfish-service/routers/analysis.py`. Regression changes:
  `tests/test_analysis_failure_jobs.py`, `tests/test_analysis_failure_arq.py`, and
  `tests/test_analysis_terminal_progress.py`. Other changes are tracking records.
- Remaining limits: existing baseline failures/skips; no production/GCP or real
  Stockfish execution; database cancellation fault paths use fake sessions.
  Redis progress remains best-effort: if a reset cannot be applied, the new
  attempt cannot report progress and old progress may remain visible. A lost
  response after Redis applies reset does not invalidate the generated token.
- Both requested corrections are implemented and verified. Formal acceptance
  remains pending independent review. Preparing the separate chunk 4 local
  implementation commit. No push, merge, deployment or work on another finding.

## 2026-11-09 — Corrections committed; independent review pending

- Correction 1: `5542c9ad00d5df547ccf16d14f657399dd96feef`,
  `fix: drain analysis workers without repeated cancellation`.
- Correction 2: `7f304be35da9fc5b10ac6a36055ad86c83710249`,
  `fix: isolate analysis progress by execution attempt`.
- Both commits are local on `refactor/p1-analysis-failure-propagation`. Working
  tree was clean after the implementation commit. Recording final commit IDs in
  tracking documents; application/test code remains identical to verified code.
- No pending verification process. Temporary monitor/evidence are retained at
  `/tmp/chessism-p1-verification/`; all isolated test services are stopped.
  Monitor now reports implemented corrections with independent review pending.
- Stop here. P1 #1 has not been formally accepted; the next step belongs to the
  focused independent review requested by the user. Do not proceed to P1 #2.

## 2026-11-10 — Final acceptance returned REQUEST CORRECTIONS; A/B authorized

Verified repository /home/jon/Desktop/workshop/last_chessism, branch refactor/p1-analysis-failure-propagation, HEAD 24e830e, clean worktree before any change. User source: /home/jon/.codex/attachments/b905d848-5fe4-4e7e-85a9-d7f10c531a13/Pasted text.txt. Work stays exclusively in Chessism.

Independent review reproduced four cancellation orderings on Python 3.10.22 where an already-running close is interrupted; existing cancellation tests start cleanup after cancellation. It also executed the actual Positions status helpers and demonstrated stale or absent progress can be mistaken for successful completion despite active/unsuccessful ARQ state. The existing backend selection propagation and successful-reset attempt isolation remain correct.

Added two explicit chunks (5 protected cleanup; 6 authoritative Positions outcome handling), preserving prior history and baseline exceptions. Preparing a fresh isolated baseline before tests-first implementation. No application/test source changed yet. Existing evidence at /tmp/chessism-p1-verification is preserved; new runtime is /tmp/chessism-p1-final-corrections. No other repository, production, schema, engine policy, P1 #2/#3, deployment or push/merge work.

Prepared /tmp/chessism-p1-final-corrections/monitor.py and baseline.py. The runner uses fresh labelled Redis/PostgreSQL services, private Unix sockets, no test-container network, read-only repository mounts, and the exact previous Python/dependency images. It compares full failure/error/skip signatures to correction2, runs the 60/25/6 checks, all 64 frontend Node test files and existing architecture/size checks, then installs locked frontend dependencies in temporary storage and builds an ephemeral source copy. No repository node_modules or build artifacts are created. Hashed 1,386 source/config files; runner scripts parse successfully. Baseline not yet launched at this entry.

Final-corrections entry baseline launched after planning commit b8fbe43; supervisor PID 1584755. Returning control immediately under the established monitor handoff. No application or test changes and no polling.

## 2026-11-10 — Entry baseline runner collision diagnosed

User reported the stopped process. Independently compared API 1,442 entries (3 existing failures, 10 existing errors, 276 skips) and worker 287 entries (0 failures/errors, 7 skips) with correction2, including normalized full traces, skip identities, dependencies and Python: exact matches. All 1,386 protected source/config hashes remain unchanged. The runner failed before PostgreSQL tests because it reused the database-service container name for its test container. No PostgreSQL test failed or was skipped. All four owned containers were removed.

Archived the entire first-attempt root evidence in /tmp/chessism-p1-final-corrections/entry-attempt-1. Corrected only the temporary runner: distinct test role, fresh private sockets, explicit PostgreSQL readiness, and improved subprocess stderr capture. Resume only pending baseline stages, reusing the passing unchanged API/worker evidence with recorded digests. No test assertions, application code or dependencies changed to obtain passing results. No implementation has begun.

Baseline continuation launched after tracking commit 3eb7dde; supervisor PID 1598974. Reusing verified API/worker results; remaining stages pending. Immediate user handoff, no polling and no implementation yet.

User requested removal of raw CalledProcessError/Docker command text from the monitor display. Changed only temporary monitor rendering to show a short saved-evidence notice on failure; retained raw diagnostic evidence. Synthetic running/failed display checks pass, and the suite execution function is byte-for-byte equivalent at AST level. No live verification status inspected, no process restarted, and no application/test files changed. The open viewer must be restarted to load this display change; Ctrl+C affects only the viewer.

## 2026-11-10 — Backend entry verified; complete frontend environment next

User reported the continuation stopped. Six PostgreSQL checks, 60 focused checks (47 P1 regressions), and 25 Python 3.10 checks passed. API/worker matching evidence was reused as planned. All protected inputs remain unchanged and every owned container was removed. The frontend run reported 318 entries: 313 passed, four assertion failures in unchanged appearance/navigation tests, and one module-load failure because graphology was absent. My runner scheduled dependency installation too late; this is a setup defect, not application regression evidence.

The four assertions concern target-scope registration, independent-control seeding (29 versus 22), game-review border properties, and presence of the existing ignored hegel_.md file. Their source inputs are unchanged; preserve that local file. No exception has been approved for these frontend failures and none is assumed. First finish verification with locked dependencies installed, all source inputs preserved, and independent architecture/size/build checks recorded. The temporary frontend runner copies exact hashed inputs into disposable container tmpfs and mounts dependencies read-only. No source checkout, repo node_modules, or production files are created or changed. Reuse passing backend evidence; do not rerun it. No application/test implementation yet.

Frontend baseline completion launched after tracking commit fc83b7c; supervisor PID 1613731. Only pending frontend environment/tests/checks/build are running. Existing backend passes retained, frontend assertion failures not waived. Immediate handoff without polling.

## 2026-11-10 — Frontend runner layout corrected before baseline acceptance

User reported the 20-second frontend job stopped. Locked dependency installation succeeded, as did architecture and source-size checks. The temporary runner mounted node_modules as /deps, breaking Node transitive dependency resolution: opening-map tests could not resolve graphology-utils/defaults and Vite could not resolve esbuild. These were runner setup errors. The source copy also omitted the existing ignored hegel_.md path because it is a directory, not a file; this incorrectly hid an existing navigation assertion failure. The earlier log description of it as a file was inaccurate. The original directory has not been changed or removed.

Archived the complete prior reports and runner under /tmp/chessism-p1-final-corrections/frontend-attempt-1. Corrected only the temporary runner: keep the dependency layout at /deps/node_modules, preserve the existing directory's existence in the isolated source, check both CommonJS and ESM dependency-load errors, and reuse the successful locked dependency installation. A short network-disabled container probe imported both actual Vite and opening-map modules successfully; all source hashes and directory conditions matched, and the probe container was removed. The frontend suite/build must finish with this valid environment before the frontend baseline is accepted. Existing assertions have not been waived, weakened, or repaired. Backend evidence will be reused. No application/test implementation has begun.

Corrected frontend baseline launched after tracking commit e562343; supervisor PID 1628900. Locked dependencies and passing backend evidence reused. Monitor remains an in-place 120-second viewer without raw exception output. Immediate handoff without polling; implementation remains pending.

## 2026-11-10 — Complete unchanged baseline reviewed; frontend exception pending

The corrected frontend run completed in 14.12 seconds with no runner or dependency-load errors. All 64 test files ran: 320 checks, 316 passed, four assertion failures, zero cancelled/skipped/todo. Architecture (18 routes, 14 API modules), source-size (354 files) and Vite production build passed. Every owned container was removed. Independently rehashed all 1,386 protected source/config files and 1,696 frontend input files: unchanged. The existing ignored hegel_.md directory remains intact. Diff from starting HEAD 24e830e contains only the three tracking documents.

Exact preexisting frontend debt:
- appearancePages.test.mjs:66, target annotations: positions/FenIngestionPanel.jsx uses positions/ingestion.player without the expected registration.
- appearancePages.test.mjs:192 (assertion line 199), independent home control seeding: 29 rows versus expected 22.
- appearancePages.test.mjs:257 (assertion line 271), game-review target properties: home/gameReview.chart.title/borderColor absent.
- navigationAppearance.test.mjs:28 (assertion line 30), removed Hegel path check: existing ignored hegel_.md directory makes existsSync true. It was preserved, not removed to obtain a pass.

Complete evidence and exact failure details are frozen in /tmp/chessism-p1-final-corrections/frontend-baseline-complete. Backend baseline remains verified: API 1,442 with precisely the prior 3 failures/10 errors/276 skips; worker 287 with 0 failures/errors/7 skips; 60 focused, 25 Python 3.10 and six PostgreSQL checks passed. No new source failures have been introduced, no assertions weakened, and no test was skipped to mask a result.

The explicit retained exception enumerated backend debt; these four frontend failures were not included. architectural-refactor's entry rule says “If not: STOP. Fix or escalate to user.” Request a bounded exception for these exact preexisting frontend failures, with all new P1 regressions required to pass and no additional failures/errors/skips. Do not repair unrelated appearance/navigation behavior. Chunks 5 and 6 remain pending; no application/test implementation and no active process. Session ended at the baseline decision, with prior chunks 1–4 preserved.

## 2026-11-10 — Frontend baseline exception explicitly authorized; chunk 5 starts

The user authorized the four exact preexisting frontend failures as unresolved
testing debt, with no additional failures/errors/skips and every new regression
required to pass. Rechecked all 1,386 protected source/config and 1,696 frontend
input hashes against unchanged baseline; all match. Confirmed the saved four
failure identities and full assertion/stack details match the completed report.
The ignored hegel_.md directory is preserved. Only tracking differs from 24e830e.
No existing failure prevents event-controlled cancellation or Positions outcome
verification. No appearance/navigation repairs are authorized or planned.

Starting chunk 5: add regression tests before application edits for cleanup
already underway, external/sibling-triggered cancellation, repeated cancellation,
rollback failure and preservation of committed work/primary exceptions.

Chunk 5 red evidence: both Python 3.10.22 and 3.11.17 ran 33 checks. All 25 prior checks passed; exactly the eight new methods failed (36 assertion failures across event-controlled subcases), with zero errors/skips. Every assertion failure reported interrupted cleanup. Tests cover all four reported close orderings, worker rollback already underway, each selector's rollback/close with empty/error selections, repeated cancellations, secondary cleanup failures, preserved original exceptions and previously committed work. Saved exact reports/source hashes under chunk5-red-* before application edits.

Implemented the bounded cleanup helper in database/postgresql/analysis_cleanup.py: retain the operation task, shield every await through repeated cancellation, observe its result, and propagate the first cancellation without replacing it with a secondary cleanup failure. Existing selector/worker owners invoke it around their rollback/close operations. SQL, transaction ownership, commit points, retries, timeouts and coordinator policy remain unchanged. Focused green verification pending.

Chunk 5 first green run exposed a test observation issue on Python 3.10: cleanup completed, but wait_for retrieving a cancelled Task discarded its cancellation-message arguments. A standard-library-only probe in both cached images demonstrated original/repeated/original messages inside the coroutine, but empty arguments at the Python 3.10 Task boundary (3.11 retained them). Archived the complete first green evidence in chunk5-green-attempt-1. Kept the message assertion and observed it as it leaves the coroutine; task-cancelled, cleanup-finished, no-interruption, original-error and commit assertions remain. No application correction was needed.

Reran the final test version against exact 24e830e application files mounted read-only over the disposable container source: both runtimes again report precisely eight new failing methods/36 subtest failures, zero errors/skips, and all 25 prior methods passing. Original implementation hashes are in chunk5-original-implementation/sha256.json; final red evidence is chunk5-red-v2-results.json. With the correction, all 33 checks pass on both Python 3.10.22 (2.46s) and 3.11.17 (4.10s). Green report and immutable source snapshot are retained. AST comparison confirms every original test method is unchanged. Removing only the new cleanup wrappers/imports makes ask_db.py and analysis.py structurally identical to 24e830e: SQL, coordinator, progress, commit, retry and timeout behavior are otherwise unchanged.

Prepared chunk5_gate.py for the longer isolated gate: API expected 1,450 checks with the exact 3 failures/10 errors/276 skips; worker 287 with zero failures/errors and seven unchanged skips; six PostgreSQL invariants; 68 focused checks including 55 P1 regressions and real isolated Redis/ARQ. Full failure/error/skip details must match baseline, not just counts. Reuse the passed 33 Python 3.10 checks and frontend baseline only while their source inputs remain unchanged. All fixtures are newly labelled, private, disposable and network-disabled. No production access or application commit yet. Tracking-only checkpoint before launch; chunk 6 remains pending.

Chunk 5 full verification launched after tracking commit 5afdff7; supervisor PID 1672978. Immediate user handoff without polling. Application/test changes remain uncommitted pending evidence review; frontend correction has not begun.

## 2026-11-10 — Chunk 5 complete verification reviewed

User reported the 94.75-second gate finished. Independently rechecked saved results against the unchanged entry baseline: API 1,450 checks (eight new) with exactly the existing three failures, ten errors and 276 skips; worker 287 with zero failures/errors and seven skips. Full normalized failure/error traces, skip identities, dependency versions and Python versions match, not only totals. Six isolated PostgreSQL checks and all 68 focused checks (55 P1 regressions) passed without skips. The prior 33 Python 3.10 checks remain valid for the identical hashed source. All six owned containers were removed. Source hashes still match; no code edits after the gate.

The four authorized frontend failures remain unresolved debt; frontend inputs are unchanged in this backend-only chunk. No production/GCP/real-Stockfish behavior is claimed by these tests. Correction A is ready for its separate local commit. Correction B remains pending; P1 #1 is not accepted.

Correction A committed locally as 49edfc5 (fix: protect analysis session cleanup already in progress). Working tree was clean immediately afterward. Starting chunk 6 with frontend regressions before implementation.

Verified actual jobs API: /jobs/{job_id} returns independent status, result and progress reads; result.success is a serialized boolean and failures carry result.result.{type,message}. ARQ status complete means result availability, not success. /jobs/analysis/history already derives its terminal status from result.success. Relevant ingestion/tablebase workers re-raise failures after recording progress. The Positions freshness guard currently compares Redis timestamps even when an incoming response contains the definitive execution result. Tests will cover authoritative outcomes, cached completion after failed reset, unchanged active phases, explicit cancellation errors and polling with the existing shared watchStatus primitive. No shared polling transport changes or API contract changes are planned.

## 2026-11-10 — Chunk 6 red/green evidence; final gate prepared

Added 26 frontend regression checks in positionJobOutcomes.test.mjs and positionJobPolling.test.mjs. The final version, run against exact 49edfc5 Positions files in read-only disposable overlays, reports 34 checks: 21 new failures reproducing defects, five new preservation checks passing, and all eight prior focused checks passing. No cancellations/skips. Exact red details and original source hashes are retained under chunk6-red-v2-* and chunk6-original-implementation.

Implemented only three existing Positions files. Boolean result.success determines successful/unsuccessful terminal states; ARQ complete without a boolean outcome keeps polling. Terminal progress/cached UI phases cannot finalize unresolved work. The freshness guard accepts confirmed outcomes independently of progress timestamps and compares telemetry only within the same queue state/attempt; it still protects confirmed success from delayed active discovery. Failure/cancellation details come from the verified serialized result contract. Completion sounds require success=true. Active/failed status updates and restored non-successful records clear stale completion/fading flags. Successful completion remains intact. No API, shared polling transport, polling hook, request interval, timeout or unrelated page changes.

A related cached-state regression reproduced that a falsely completed job could retain fading/completedAt flags after restoration. The test proves failed/unresolved jobs become visible and preserve their status payload, while confirmed success retains its valid completion state. Two existing successful-completion fixtures now supply result.success=true, as the real API does; no intended assertions were weakened.

Focused green: all 34 checks pass in Node 22 (26 new, eight existing), zero failures/errors/cancellations/skips. Polling tests exercise the existing watchStatus transport plus actual Positions decision/freshness helpers with deterministic timers, covering continued requests, terminal success/failure/cancellation, missing progress, HTTP failure and explicit missing-job responses. They are synthetic workflow tests; no mounted React/browser, real Stockfish or production/GCP verification is claimed.

Prepared the final gate: all 66 frontend test files must produce exactly 346 checks, 342 passes and the four approved existing failures, with identical failure details (only TAP numbering/duration normalized) and zero new skips/errors. Architecture, source-size and production build run independently. Source files are copied byte-for-byte into disposable tmpfs with the original hegel_.md directory condition preserved; locked dependencies and output stay in /tmp. The complete backend gate just passed after the last backend edit; final preservation verification reuses it only after checking every backend source hash, exception/skip comparison and cached image identity. A short read-only preflight confirmed all of those match. Avoid rerunning unchanged passing backend suites. Final result/commit remains pending; no acceptance declared.

Final frontend verification launched after tracking commit 174ea70; supervisor PID 1715176. Immediate user handoff without polling. Backend correction is committed at 49edfc5; frontend changes remain uncommitted pending final evidence review.

## 2026-11-10 — Final frontend gate reviewed

User reported the 13.49-second gate finished. Independently compared the final frontend failure blocks with the unchanged baseline: all four exact test identities, assertion details and stacks match (only TAP numbering/duration normalized). All 66 files ran: 346 checks, 342 passed, four authorized existing failures, zero cancellations/skips/todo and no module-load errors. All 26 new regressions pass. Architecture, source-size and production Vite build passed. Exact original failure blocks are now also embedded in refactor-manifest.json so this testing-debt record survives removal of temporary runner files.

All 1,699 final source-input hashes still match the tested snapshot. The original ignored hegel_.md directory remains intact. All four owned frontend containers were removed. Backend source and cached runtime image identities match the complete passed chunk 5 gate: API 1,450 with the same 3 failures/10 errors/276 skips; worker 287 with 0 failures/errors/7 skips; 68 focused checks, 55 P1 regressions, 33 Python 3.10 cleanup checks and six PostgreSQL invariants passed. Backend results were verified and reused after the frontend-only edits, not rerun or represented as rerun. No additional failures, errors or skips compared with the verified baselines.

Correction B is ready for its separate local commit. P1 #1 remains pending independent acceptance review.

## 2026-11-10 — Implementation handoff for independent acceptance review

Correction B committed locally as 499a781 (fix: make Positions use confirmed ARQ execution outcomes). Correction A is 49edfc5 (fix: protect analysis session cleanup already in progress). Both are on refactor/p1-analysis-failure-propagation in /home/jon/Desktop/workshop/last_chessism. Application/test bytes are unchanged from the passed verification snapshots. This final tracking checkpoint records completion of implementation, not acceptance of P1 #1.

Root causes and design:
- Cancellation could enter while a session was already awaiting rollback/close, before the coordinator's shielded drain. A small persistence cleanup helper now retains and drains each operation through repeated cancellation, then propagates the original cancellation. Existing selectors and workers retain ownership and primary-error handling. No new service layer, interface or general-purpose framework was introduced.
- Positions gave progress.phase priority and treated ARQ result availability as success; its freshness filter could also reject an authoritative result using unrelated Redis timestamps. Presentation helpers now use the actual result.success boolean, keep unresolved jobs active, preserve explicit unsuccessful/cancelled results, and prevent stale completion/fading state from finalizing a retry. Progress timing remains a presentation concern; the backend remains the execution authority. Shared polling code was not refactored.

Production files modified:
- chessism_api/database/postgresql/analysis_cleanup.py (new bounded cleanup helper)
- chessism_api/database/postgresql/ask_db.py (selector cleanup calls)
- chessism_api/operations/analysis.py (worker cleanup calls)
- chessism_web/src/pages/positions/positionPageSupport.js (outcomes, details and restored state)
- chessism_web/src/pages/positions/positionStatusFreshness.js (outcome-aware freshness)
- chessism_web/src/pages/positions/usePositionsPage.js (failure display and completion lifecycle)

Test files modified/added:
- tests/test_analysis_failure_jobs.py
- tests/test_analysis_failure_selection.py
- chessism_web/tests/positionJobOutcomes.test.mjs (new)
- chessism_web/tests/positionJobPolling.test.mjs (new)
- chessism_web/tests/positionsProgress.test.mjs (realistic success fixture)
- chessism_web/tests/positionStatusFreshness.test.mjs (realistic success fixture)
The remaining changes are refactor-plan.md, refactor-manifest.json and this log.

Preserved contracts/invariants: database selection errors propagate; (None, None) remains successful exhaustion; acquisition remains outside processing retries; committed results and transaction ownership remain intact; SQL selection/locking/recheck rules are unchanged; ARQ records unsuccessful analysis; attempt identities and terminal Redis guards remain unchanged and verified. The jobs API schema, processing/engine retry and timeout policy, shared polling transport and page polling intervals are unchanged.

Final comparison: API 1,442 → 1,450 tests with the same 3 failures/10 errors/276 skips; worker 287 → 287 with 0 failures/errors/7 skips; frontend 320 → 346 with the same four failures and zero skips. All new regressions pass. Full failure/error/skip identity and detail comparisons are recorded, and the four frontend baseline failure blocks are preserved in the tracked manifest as unresolved testing debt. Nothing was weakened, deleted or skipped to obtain the result; the two fixture updates express the verified successful-result contract.

Verification limits: frontend workflow tests use real status-polling/decision modules with fake timers, not a mounted React/browser end-to-end session. Real Stockfish and production/GCP execution were not exercised. Failed Redis resets can still leave old progress counters visible; they no longer determine completion or stop polling. Cooperative repeated cancellation was tested; hard process termination cannot be covered by an in-process cleanup guarantee.

No production data/schema/deployment changes, push, merge or other-repository work. P1 #2 and P1 #3 were not started. All disposable test containers were removed, and the original ignored hegel_.md directory is unchanged. No verification process remains active. Temporary monitor and evidence remain in /tmp/chessism-p1-final-corrections for review. Stop here; the next step is the user's independent acceptance review.

## 2026-11-10 — Stage A Batch 1 authorized

Repository/branch/HEAD verified, clean at 0fde829. All 1,699 saved input hashes match. P1 #1 acceptance reconciled from the user’s explicit acceptance statement; old entries remain historical. Applying clean-architecture, kent-beck-style, and architectural-refactor. Only the new canonical foundation, contracts, initializer, fixtures/tests and tracking may change. Existing full-suite evidence and exact approved debt are reused; focused foundation and preservation checks will run in isolated resources. No other stage, production action, push, merge, or backup change is authorized.

## 2026-11-10 — A1 foundation prepared for isolated verification

Added independent core/support SQL (10 core + 78 preserved account/ingestion/appearance/cloud tables), one semantic participation view, atomic initializer/version/catalog validation, explicit private-target CLI, contracts, synthetic fixtures, five offline checks and 26 PostgreSQL checks. Support declarations were rendered once from checkpoint metadata in a short network-disabled disposable container, which was removed; initialization has no legacy-model dependency. No runtime caller/profile or existing test changed. All 1,699 protected original hashes still match.

Five new offline tests pass with zero failures/errors/skips; syntax and diff checks pass. Prepared the detached gate in /tmp/chessism-canonical-a1-20261110-01: 31 foundation checks (5 offline + 26 real PostgreSQL), then 68 existing focused P1 checks with real private Redis/ARQ. Source is read-only, network disabled, data lives in disposable tmpfs, no application volumes/credentials. Commands/results and owned cleanup are recorded. Full suites reused under the approved input-hash rule.

Concrete design dependencies for review: participant projection replaces writable game_player rows; zero-move game metadata moves into the common game identity; redundant calendar/average fields and occurrence IDs are omitted. A2 must convert those writers and side-specific summary keys. Derived analytics/research/export/graph tables remain for A2/A3 alongside their caller conversions; this foundation is not a runnable application replacement. Existing cloud typed shapes/receipt/cancellation contracts retained.

Verification and local commit are pending. No independent acceptance is claimed. The user's commit-after-passing instruction takes precedence over the skill's unconditional handoff commit; these additive changes and tracking remain uncommitted until evidence review. Stop after A1 review; no later batch is authorized.

A1 gate launched; supervisor PID 2175427, startup confirmed. Monitor: `python3 /tmp/chessism-canonical-a1-20261110-01/monitor.py`. Returning control without waiting or polling. Next: after the user reports completion, inspect structured evidence/cleanup, correct any demonstrated batch defects, then create the authorized local commit only if verification passes.

## 2026-11-10 — A1 evidence reviewed after user completion report

Gate passed: 31 foundation tests (5 offline safety + 26 PostgreSQL) and 68 existing focused P1 tests; all 99 passed with zero failures/errors/skips. Supervisor 16.47s, foundation 7.16s, P1 4.53s. Reviewed actual logs and structured reports, not only monitor status. All 1,707 frozen inputs still matched, including all 1,699 protected baseline files. Verified four owned containers removed via a separate Docker query filtered by run label; no persistent test database remains. Existing ignored hegel_.md directory is intact.

Preserved full-suite evidence is reused, not rerun or called globally green. Exact API 3 failures/10 errors/276 skips, worker 7 skips and frontend 4 failures remain unresolved debt. No source/test assertions weakened, removed or skipped. Saved results/checksums, image identities, SQL digest and new executable-source hashes recorded in manifest. Only documentation/tracking updated after passing tests. No production database, backup, runtime profile, API contract or deployed service changed.

Clean Architecture: schema construction and validation have explicit connection/transaction ownership; no runtime or legacy-model dependency. Contracts preserve selector handoff, locking and evaluation/receipt atomicity. Kent Beck: one game identity, derived participants, native composite keys and integer evaluation storage remove duplicate representations; unchanged typed cloud shapes retained for actual recovery requirements. No generic repository framework added. One verification cycle passed without correction.

Limitations: runtime consumers are not switched; auxiliary projections/research/export/graph structures remain for A2/A3; small synthetic fixtures do not prove historical-scale performance. Authentication fields and hash verification tested, not a browser login workflow; cloud codec round-trips are not live GCP verification. Independent Batch 1 acceptance is pending. Next action is the authorized local commit followed by review handoff; do not start A2.

The final staged whitespace check (including newly added files) found one extra blank EOF line in support.sql. Removed that byte only; exact SQL equality after trimming final newlines verified. No SQL statement changed. Final DDL/source digests recorded separately from immutable test digests; reran the five offline safety checks. This does not justify repeating the PostgreSQL/P1 gate.

A1 implementation committed locally as 22a406c60c84b3cc57e46e10c742094b5b4ff4d1 (`feat: establish canonical PostgreSQL foundation`). This handoff records the commit and completion; no further application changes. All 99 gate checks passed, final five offline checks and staged whitespace check passed. Stage A1 is ready for independent review, not formally accepted. A2–A4 remain unauthorized pending that review. No push, merge or deployment.

## Stage A completion — authorization and entry checks

A1 acceptance confirmed from the independent review and explicit user authorization.
Repository, branch, HEAD 35be4fc and ancestry verified; working tree clean. All 1,699
protected baseline inputs match. Applying architectural-refactor, clean-architecture
and kent-beck-style. A2–A4 are authorized as one stage with focused gates/local
commits, then complete backend/worker baseline comparison and independent review.
User direction supersedes repeated approval/full-suite-per-change skill steps.

Recorded concrete conversion chunks in the plan. Starting A2.1: canonical bulk
imports/month accounting and the evaluation CHECK correction. Runtime wiring waits
for the complete write unit and required projection definitions in A2.2/A2.3; no new
storage profile. Deferred FK enforcement validation will be corrected before the
A4 startup gate. No production access, backup modification or other-repository work.

## A2.1 prepared for isolated verification

Implemented `game_imports.py`: bounded COPY staging, explicit caller transaction,
existing ingestion lock, complete move/parent validation, exact duplicate handling,
metadata/child conflict rejection, one game identity and standard/nonempty UTC month
counts. No hidden commit or new storage profile. Runtime wiring is pending A2.2/A2.3.
Updated canonical schema to version 2 to reject unexplained unscored timestamps;
version 1 is rejected rather than silently reinterpreted. Corrected the synthetic
month ledger from five total classifications to two eligible games.

Added 16 real PostgreSQL import regressions and two schema regressions; factored
only the repeated isolated-database test setup/cleanup. Five offline tests and syntax
checks pass. All 1,699 baseline inputs remain unchanged. Prepared 117-check gate
(5 offline + 28 schema + 16 imports + 68 P1) in
/tmp/chessism-canonical-a2-imports-20261110-01. Same pinned image IDs as accepted A1;
no network/ports, read-only source, masked .env, tmpfs PG data, bounded resources,
label-checked cleanup. No implementation acceptance or passing integration claim yet.

A2.1 gate launched; supervisor PID 2254139. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-imports-20261110-01/monitor.py`. Returning control immediately.
Next: after user completion, review results/hashes/cleanup, correct demonstrated
defects if necessary, then commit verified A2.1 work locally and continue A2.2.

## A2.1 evidence reviewed

117 checks passed (49 foundation/import + 68 P1), zero failures/errors/skips.
Supervisor time 26.60s; PostgreSQL/offline 14.22s; P1 5.64s. Reviewed structured
results and logs after user handoff. All 1,710 frozen inputs match, including 1,699
protected baseline files. Separate run-label Docker inspection confirms no owned
containers remain. No integration correction was needed; source remains exactly
as tested. Recording result digests and preparing the authorized local commit.

Clean Architecture: application-owned transaction required explicitly, with no
hidden commit/connection lifecycle in persistence helpers. Kent Beck: concrete
bulk functions replace profile-dependent representations; exact retries and active
month counts are directly tested. This verified infrastructure will be connected
in A2.2, not claimed as complete Stage A or independent acceptance.

A2.1 committed locally as 6039b34; nothing pushed. Starting A2.2. The game import
unit and FEN extraction unit will have separate focused gates inside this chunk.
The first gate wires game import/month accounting/projection invalidation into the
application operation, preserves zero-move metadata, and introduces required
canonical summary definitions. Remaining FEN work follows its verification.

## A2.2 game-import transaction prepared for verification

Game importer now owns one transaction for canonical rows, all fetched month
ledgers, game/mode totals and side-key salience invalidation. Pure formatting no
longer commits player shells or skips conflict validation for existing identities.
No post-commit summary refresh. Public result strings remain strings with the
established success/already-present wording; actual inserted identities drive counts.
Repeated cancellation completes rollback/close using the unchanged P1 helper.

Added six canonical projection tables in schema version 3; pending work uses
(game_link,is_white), ownership remains derived. Mode statistics preserve side
perspectives; global/month totals preserve standard/nonempty game counts. Month
reader queries use numeric owner keys. Historical game_repository/relational paths
are no longer called by the game import entry point; removal remains A4 work.

Ten new real PostgreSQL application checks and six cleanup checks prepared. Existing
12 formatting and four relational tests are included with documented replacement
of the archive/tombstone assertion, not dropped coverage. Five offline checks and
syntax pass. Only six protected baseline inputs changed, all within the bounded
conversion; the other 1,693 are unchanged. The gate freezes 1,715 inputs and runs
149 checks (59 schema/import/application + 22 workflow + 68 P1). FEN writes/coverage
remain the next A2.2 gate. No passing integration claim before evidence review.
Evidence: /tmp/chessism-canonical-a2-gameflow-20261110-01. No production access,
external network, database deletion, push, merge, deployment or Stage B work.

A2.2 game-import gate launched; supervisor PID 2297601. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-gameflow-20261110-01/monitor.py`. Returning control immediately.
After user completion: inspect results, source hashes and cleanup; commit this
verified application transaction if passing, then continue A2.2 FEN persistence.

## A2.2 game-import evidence reviewed

149 checks passed (59 foundation/import/application, 22 workflow, 68 P1), zero
failures/errors/skips. Supervisor 24.38s. All 1,715 frozen inputs still match.
All five owned containers removed; independently confirmed by Docker run label.
Reviewed raw logs and structured results. No source correction was needed.
Recording and committing the game application transaction before continuing FEN
persistence/coverage. Full A2.2 and Stage A remain in progress, not accepted.

A2.2 game-import work committed locally as 4eee021. Working tree was clean.
Starting the planned canonical FEN subgate: bounded persistence/claims, coverage,
required scored-position projection, and application orchestration. The existing
coverage algorithm is retained with direct canonical tables; its old profile-based
module remains temporarily for unconverted evaluation callers until A2.3/A4.
No new approval is needed within the authorized Stage A conversion.

## A2.2 canonical FEN workflow prepared for verification

Converted bounded FEN selection/replay/persistence and finalization to canonical
SQL. The legacy orchestration branch is removed; old registered worker exports
remain for A4 retirement. Profile-based coverage remains temporarily for callers
awaiting A2.3 conversion. Schema version 4 adds the actual scored-position singleton;
versions 1–3 fail validation. Game imports reuse canonical pipeline-total SQL.

Batch ownership includes claims, COPY, numeric references and exact occurrence
increments. Coverage, completion flags and pipeline totals share a final application
transaction. Failed finalization preserves committed positions and permits exact
retry. Known child writers are joined through repeated cancellation before claim
release. Accepted P1 cleanup/progress mechanisms are unchanged.

Added 18 mandatory PostgreSQL cases and eight coordinator regressions. Existing
parser/aggregation, automatic scheduling, progress, transaction and P1 checks are
included. Five offline schema checks pass; syntax and whitespace checks pass.
Prepared 198-check gate (77 foundation/PostgreSQL, 53 workflow, 68 P1), with 1,718
frozen inputs. Ten original baseline inputs are intentionally converted; the other
1,689 remain unchanged. Prior baseline debt is unchanged and not claimed resolved.
No full-suite rerun, production access or deployment. Evidence directory:
/tmp/chessism-canonical-a2-fens-20261110-01. No passing integration claim yet.

A2.2 FEN gate launched; supervisor PID 2332736. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-fens-20261110-01/monitor.py`.
Returning control without waiting or polling. After the user's completion report,
review structured results, source hashes and owned cleanup; correct any demonstrated
conversion defects before committing. Stage A remains in progress, not accepted.

## A2.2 FEN evidence reviewed

All 198 checks passed: 77 foundation/PostgreSQL, 53 workflow, 68 accepted P1 checks;
zero failures/errors/skips. Supervisor 27.81s. Read raw test logs and structured
reports; all 1,718 frozen inputs still match. All five containers were removed,
independently confirmed using the unique Docker run label. No implementation
correction was required. Committing the verified FEN conversion locally.

A2.2 ingestion units are complete; remaining evaluation writers belong to A2.3,
and derived feature readers/writers and obsolete adapters remain for A3/A4.
Stage A is still incomplete and has not received independent acceptance. No push,
merge, deployment, production access or changes to the recovery checkpoint.

A2.2 committed locally as d51cd42. Starting A2.3 selection/reservation conversion.
The canonical selectors will replace ask_db definitions; direct callers and tests
move together. Accepted P1 cleanup semantics and existing outcome/progress code
remain unchanged. Cloud queue construction uses the same canonical game ordering.
Evaluation writes/receipt and projection recovery are the next focused subgate.

## A2.3 selection and queue gate prepared

Extracted global/player/frozen-game selectors and scope counters/completion from
ask_db into analysis_selection. Three acquisition implementations share one lease
lifecycle; the accepted cleanup body is unchanged except its function name. Direct
analysis/cloud/benchmark consumers use the new module. No compatibility re-export.
Player queries use numeric owners; frozen selection and durable queue share native
is_white ordering SQL. Claim rechecks explicitly address the canonical public tables.

Queue build checkpoints retain their two transactions. Candidate consumption is
still handed off with FEN locks for atomic claim publication; rollback restores it.
Acquisition now uses the accepted cleanup helper, and construction uses the existing
explicit transaction helper. No cloud side effects or evaluation publication changed.

Eighteen mandatory PostgreSQL regressions exercise actual selectors/queue state,
concurrency, fresh claim snapshots, rollback/restart and cancellation. Two additional
queue cleanup unit regressions cover rollback failure and repeated cancellation.
All P1 selection/job assertions are byte-identical after reversing the module rename.
The query-shape and queue-profile assertion mapping is recorded in the manifest.

Five offline safety checks, syntax and whitespace checks pass. Prepared 118 checks:
23 PostgreSQL/offline, 27 focused workflow, 68 P1. Freeze covers 1,720 inputs; 1,673
original files remain unchanged, with 26 explicitly converted original inputs across
Stage A. Reuse the completed 198-check ingestion evidence for unchanged persistence
inputs. The full baseline comparison remains the A4 gate; no new skips or weakened
assertions. Legacy opt-in fixture conversion/mapping remains required before A4.
Evidence: /tmp/chessism-canonical-a2-selection-20261110-01. Integration results pending.

A2.3 selection gate launched; supervisor PID 2372150. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-selection-20261110-01/monitor.py`.
Returning control without waiting or polling. After the user reports completion,
review saved results, source integrity and owned cleanup before committing. Then
continue only the authorized A2.3 evaluation/publication and recovery work.

## A2.3 first gate stopped — concurrency observer correction

The 23-check first suite ran 22 passes and one error; no assertion failures/skips.
The workflow and P1 suites did not run after that gate failed. Exact error:
`CanonicalSelectionTests.test_recheck_sees_reservation_committed_while_initial_lock_waited`
timed out after two seconds waiting for pg_stat_activity to report a lock.
Supervisor 8.24s. All 1,720 frozen inputs matched; all three owned containers
were removed, independently confirmed by run label. The failed evidence remains
unchanged at /tmp/chessism-canonical-a2-selection-20261110-01.

The test's lock observer used the same open transaction that held the FEN lock;
its activity snapshot could remain stale. Other lock tests observed outside that
transaction and passed. Corrected only the new test fixture: use a separate
connection in autocommit, surface early waiter errors, require the specific blocker,
and verify the initial selection actually returned the FEN before the fresh claim
recheck excludes it. The two-second timeout is unchanged; no checks were dropped,
skipped or weakened. Application code remains byte-identical to the failed run.

Prepared a complete 118-check retry in
/tmp/chessism-canonical-a2-selection-20261110-02 with fresh resources and frozen
hashes. This correction still requires verification; no passing claim or commit.

A2.3 selection retry launched; supervisor PID 2397865. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-selection-20261110-02/monitor.py`.
Returning control without waiting or polling. Review the retry evidence only after
the user reports completion. No additional implementation or commit before that gate.

## A2.3 retry reviewed — stale ARQ fixture target

118 checks ran: 116 passed, two failed, zero errors/skips. All 23 PostgreSQL/offline
and all 27 workflow checks passed; P1 passed 66/68. The concurrency observer
correction now passes, including the intended blocker and fresh-claim assertions.
The two failures are AnalysisFailureArqTests.test_database_failure_is_stored_as_unsuccessful_with_failed_progress
and AnalysisFailureArqTests.test_genuinely_empty_selection_remains_successful.
Both reported their fake session rollback was never called. The actual ARQ logs
show UnboundExecutionError: that fixture still patched ask_db.AsyncDBSession after
the selector moved to analysis_selection. This was a missed test-adapter reference,
not evidence that the new selector suppresses a real database failure.

All 1,720 frozen inputs matched before correction, and all five containers were
removed; independently confirmed by run label. Failed evidence is retained in
/tmp/chessism-canonical-a2-selection-20261110-02 (15.45s supervisor).

Corrected only the ARQ fixture import/patch target and the equivalent stale target
in the standalone cloud queue checker. ARQ test contents match HEAD byte-for-byte
after reversing the module rename; no assertions, timeouts or application code
changed. A search of related selector fixtures found no other stale targets.

These are the already-planned test-target updates inside the approved selection
conversion. Applying the user's consolidated Stage A instruction to correct
demonstrated defects before proceeding; no scope/public-contract change or new
approval boundary is required for these mechanical corrections. The failed gate
still blocks any implementation commit or advance to the next subgate.

Prepared the full 118-check retry in /tmp/chessism-canonical-a2-selection-20261110-03.
Frozen inputs remain 1,720; 1,672 original files remain unchanged, and 27 changed
original inputs are explicitly recorded. Runtime sources are identical to attempt 2.

A2.3 third selection gate launched; supervisor PID 2403804. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-selection-20261110-03/monitor.py`.
Returning control without waiting or polling. Review completed evidence only after
the user reports its status. No commit or further subgate work before verification.

## A2.3 selection gate passed and reviewed

All 118 checks pass: 23 PostgreSQL/offline, 27 workflow and 68 accepted P1 checks;
zero failures/errors/skips. Supervisor 14.47s. Reviewed raw logs and structured
results. All 1,720 frozen inputs match. All five disposable containers were
removed, independently confirmed using the run label. Both earlier failed runs
and their corrections remain recorded. No source changes after the passing gate.

Canonical selection and queue work is ready for its local commit. A2.3 evaluation
publication, receipts and durable projection recovery remain; Stage A is still
in progress, with no acceptance, push, merge, deployment or production changes.

## A2.3 evaluation publication gate started

Selection/reservation is committed locally as `a0a2af9`. Repository/branch and
clean working tree were rechecked after the editor closed; no pending process.
The saved 118-check evidence remains the verified entry gate.

Inspection found live publication still delegates to DBInterface/profile choices.
The next bounded gate replaces that writer with canonical PostgreSQL functions,
retaining application-owned transactions and atomic cloud receipts. Backup helper
semantics remain unchanged; tablebase and durable summary recovery are the next
explicit gate. This subdivides the approved A2.3 work without changing its scope.
No runtime acceptance claim until the remaining consumers/startup are converted.

Canonical Stockfish publication is implemented; verification is pending. Removed
the unused DBInterface writer and instance. Local commit/cleanup/progress code is
unchanged apart from the persistence call. Cloud result/claim/count/receipt units
now use the accepted guarded transaction helper. Backup helper function body and
all backup/restore modules are unchanged. Tablebase, outbox and repair setup
conversion remain explicit subsequent work, not claimed complete.

Prepared 136 checks: 25 offline/PostgreSQL (20 new), 43 related unit contracts,
and all 68 P1 regressions. Five offline checks pass in 0.008s; syntax and diff
whitespace checks pass. Frozen inputs: 1,722; 1,659 original baseline files are
unchanged and 40 modified original inputs are listed explicitly. No new skips.
Scheduling/formatting tests remove only unused inner DBInterface mocks; existing
assertions remain. The invalid-input test now also asserts no connection borrow.
Evidence directory: /tmp/chessism-canonical-a2-evaluations-20261110-01.

A2.3 evaluation/receipt gate launched; supervisor PID 2448661. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-evaluations-20261110-01/monitor.py`.
Returning control immediately without polling. Review completed evidence and
owned-resource cleanup after the user reports completion; no commit before review.

## A2.3 publication gate stopped — synthetic MultiPV fixture corrected

First suite: 25 checks, 15 passes, 10 setup errors, zero failures/skips. All ten
canonical writer tests and five offline checks passed. Every cloud receipt case
failed before exercising import_batch: the synthetic fixture used terminal
`{score:0,pv:[]}` output for nonterminal positions; the unchanged worker validator
correctly raised `ValueError: Incomplete MultiPV analysis`. Workflow/P1 suites
were not run after this stop. Exact identities/traces remain in foundation-results
and foundation.log at /tmp/chessism-canonical-a2-evaluations-20261110-01.

Supervisor 7.07s. All 1,722 frozen inputs matched; all three owned containers were
removed, independently confirmed by the exact run label. No runtime correction
is indicated by this failure. Failed evidence is preserved.

Changed only the new fixture: deterministic legal first moves, complete MultiPV
ranks, nodes and WDL in shared setup and the task fixture. No validator, runtime,
assertion or timeout was weakened. A pure checkpoint preflight is added ahead of
the PostgreSQL tests in the pinned container. An attempted host preflight lacked
the chess dependency and stopped before validation; no packages were installed.

Full 136-check retry prepared at /tmp/chessism-canonical-a2-evaluations-20261110-02.
All application bytes match attempt 1, with the new test fixture the only changed
frozen input. No implementation commit or advance before the gate passes.

A2.3 publication retry launched; supervisor PID 2456371. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-evaluations-20261110-02/monitor.py`.
Returning control without polling. Review saved results and owned cleanup after
the user reports completion; no commit or next subgate before verification.

## A2.3 evaluation publication gate passed and reviewed

All 136 checks passed: 25 offline/PostgreSQL (20 new), 43 related unit contracts,
and all 68 P1 regressions; zero failures/errors/skips. The fixture preflight also
validated all four synthetic records. Supervisor 17.58s. Raw logs and structured
results were reviewed; injected rollback-error logging is expected test evidence.
All 1,722 frozen inputs match. All five disposable containers were removed,
independently confirmed using the run label. The failed fixture run is retained.

Publication and atomic receipts are ready for their local commit. Tablebase
persistence, durable recovery and later Stage A consumers remain outstanding.
No acceptance, push, merge, deployment or production-data operation.

## A2.3 tablebase persistence gate started

Verified publication/receipts committed locally as `853c720`; working tree was
clean. Inspection found profile-selected tablebase writes, mixed query/orchestration
responsibilities and unguarded lease cleanup, including the reporting boundary
after acquisition. Convert those persistence responsibilities in a bounded gate
using accepted P1 cleanup, preserving probe algorithms and application contracts.
Durable projection recovery remains a separate mandatory A2.3 gate before A3.

Canonical tablebase persistence is implemented; verification remains pending.
The feature adapter owns candidate SQL, read-only discovery/spooling, lease
acquisition and bounded COPY publication. The application owns the score/count
commit and guarded cleanup, now covering reporting failures immediately after
lease acquisition. Retired tablebase_batches only after its callers moved.
No probe-algorithm/process-budget, backup, schema-version or public-result change.

Prepared 131 checks: 21 offline/PostgreSQL (16 new), 42 related contracts (three
new cleanup regressions), and 68 accepted P1 checks. Syntax and diff checks pass.
The runner explicitly checks the retired module stays absent; its original hash
is retained separately. Frozen existing inputs: 1,723; original unchanged: 1,655;
changed/retired originals: 44, including one removed module. Failed attempts and
prior evidence remain intact. Resource limits and cleanup are unchanged.
Evidence: /tmp/chessism-canonical-a2-tablebase-20261110-01.

A2.3 tablebase verification launched; supervisor PID 2482924. Startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a2-tablebase-20261110-01/monitor.py`.
Returning control without polling. Review results/source integrity/owned cleanup
after the user reports completion; no commit before the gate passes.

## A2.3 tablebase persistence gate passed and reviewed

All 131 checks passed: 21 offline/PostgreSQL (16 new), 42 related contracts
(three new cleanup regressions), and all 68 accepted P1 checks; no failures,
errors or skips. Supervisor 15.65s. Reviewed structured results and raw logs.
All 1,723 frozen inputs match, and the retired tablebase_batches module remains
absent. All five owned containers were removed, independently confirmed by the
exact run label. Probe algorithms/process budgets remain unchanged.

Ready for the local tablebase persistence commit. Durable projection recovery
remains the next mandatory A2.3 gate; Stage A is not complete or accepted.

## A2.3 durable summary recovery gate started

Tablebase persistence is committed as `809e112`; entry working tree was clean.
The remaining queue still depends on runtime layout installation and writable
game_player rows. Canonical game_player is derived, so ownership triggers must
observe game instead. New canonical schema version 5 will establish the queue
and triggers atomically. Exact-revision acknowledgment/backoff and cloud claim
exclusion remain. Canonical CHECK constraints prohibit historical scored unknown
provenance, so the historical source-only relabel branch cannot occur; unscored
unavailable metadata does not change evaluation semantics. The queue therefore
needs no refresh_engine flag or direct CUDA dirty-table dependency. Recovery's
existing report fields remain, with source_only=0. Default summary readers and
legacy installer retirement remain explicitly A3/A4 work; not declared verified.

Canonical recovery schema/adapter/orchestration implemented; verification pending.
Version 5 installs the revisioned queue and statement triggers in the initializer.
Recovered work uses exact captured revisions, guarded application transactions and
strict callback completion before acknowledgment. Cloud exclusion and metadata
fast paths are retained. Game owners are observed on game; deletion work has no
game FK. Canonical provenance semantics remove the historical source-only branch
without changing report fields. Legacy installer/upgrade callers remain for A3/A4.

Prepared 237 checks: 151 offline/canonical PostgreSQL checks (20 new recovery
cases and all affected writer/schema suites), 18 related transaction/evaluation/
queue contracts, and 68 accepted P1 checks. Five offline checks pass in 0.011s;
Python syntax and diff checks pass. The synthetic cost case records an actual
trigger plan at 2,005 FENs/4,006 associations; it is not historical-scale evidence.
Full API/worker comparison remains the A4 gate. Default player/analytics callback
and GPU integration are explicitly unverified pending A3 conversion.
Evidence: /tmp/chessism-canonical-a2-recovery-20261110-01.

A2.3 durable recovery verification launched; supervisor PID 2513938. Startup
confirmed. Monitor: `python3 /tmp/chessism-canonical-a2-recovery-20261110-01/monitor.py`.
Returning control without polling. Review saved results, source integrity and
owned-resource cleanup after the user reports completion; no commit before that.

## A2.3 durable recovery gate passed and reviewed

All 237 checks passed: 151 offline/canonical PostgreSQL checks (20 new recovery
cases), 18 related contracts and all 68 accepted P1 regressions; no failures,
errors or skips. Supervisor 46.88s. Reviewed raw logs and structured results.
All 1,726 frozen inputs match; the retired module remains absent. All five owned
containers were removed, confirmed independently using the exact runner label.
The 2,005-FEN/4,006-association metadata plan measured 31.949ms execution and
0.989ms in the statement trigger. This is synthetic, not historical-scale evidence.

A2 persistence gates are complete and ready for the local recovery commit. A3
must still convert the default player/analytics callback before claiming complete
end-to-end recovery. Legacy installer retirement and startup validation remain A4.
Stage A is not yet complete or accepted. No push, merge, deployment or data access.

## A3.1 canonical summary callback gate started

A2.3 recovery is committed as `9432d16`; entry tree was clean. Inspection confirms
that the default recovery callback and CPU accuracy SQL still require old read
views and game_player surrogate identities. Their conversion is the first A3.1
gate, before page readers. Preserve numerical formulas and separate coverage/player
commits, manual CUDA deferral and all accepted P1 mechanisms. Define the needed
side-key projection/control schema first. Concrete steps and verification are in
the plan. Other feature consumers and retired opt-in fixtures remain later gates.

Canonical coverage/player summary conversion implemented; verification pending.
Version 6 defines side-key accuracy and manual control. Operations own the existing
two commits; SQL adapters require the caller's transaction. Default recovery uses
its supplied factory; errors always propagate and the redundant strict flag is
removed. Manual control is share-locked against concurrent activation. Shared
formula function ASTs and accepted cleanup mechanisms remain unchanged. Backup
refresh imports moved only; no backup body or public ingestion result change.

Prepared 285 checks: 168 offline/canonical PostgreSQL (17 new summary cases),
49 related contracts and 68 accepted P1 regressions. Five offline checks passed;
changed Python compiles and diff checks pass. Existing opt-in fixture conversion
and final full-suite comparison remain A4; no tests are newly skipped or weakened.
Evidence: /tmp/chessism-canonical-a3-summaries-20261110-01.

A3.1 summary verification launched; supervisor PID 2548302. Startup
confirmed. Monitor: `python3 /tmp/chessism-canonical-a3-summaries-20261110-01/monitor.py`.
Returning control without polling. Review saved evidence, source integrity and
owned cleanup after the user reports completion; no commit before that gate.

## A3.1 canonical summary callback gate passed and reviewed

All 285 checks passed: 168 offline/canonical PostgreSQL (17 new cases), 49 related
contracts and all 68 P1 regressions. No failures/errors/skips. Supervisor 54.80s.
The actual default recovery callback succeeds; query failure/backoff, cancellation,
manual deferral/activation and side ownership are exercised. The unchanged scalar
accuracy oracle agrees across 13 games/842 positions, maximum error 1.4211e-14.
The scoped SELECT measured 42.795ms on that fixture, not historical-scale evidence.
All 1,730 frozen inputs match. All five containers were removed, independently
confirmed by the exact run label. Raw and structured evidence reviewed.

Ready for local commit. Remaining game/player readers, CUDA publication and
other feature consumers remain A3; final retirement/full baseline comparison
remain A4. Stage A is not complete or accepted.

## A3.1 analysis pages and previews gate started

Summary callback gate committed as `eac036c`; entry working tree was clean.
Inspection found game coverage pages, player scope and ordered previews still
using historical ORM/views and opening their own unguarded sessions in ask_db.
Move these cohesive reads into a canonical SQL adapter and explicit application
transactions. Preserve normalized ordering, fair sampling and prepared-plan
contracts. Scope details and required checks were added to the authorized A3.1
plan before implementation. No schema change or GPU/engine work in this gate.

Canonical analysis pages/previews implemented; verification pending. Removed the
legacy/ORM page and scope branches from ask_db. A concrete SQL adapter owns shared
eligibility and visible-page aggregation; application operations own read-only or
prepared write transactions. Multi-query reads use repeatable-read snapshots and
UTC; prepared metadata/expiry cleanup/chunks are one atomic unit. Sampler ASTs,
prepared builder, schema version 6 and accepted cleanup mechanisms are unchanged.

Prepared 135 checks: 41 offline/canonical PostgreSQL (18 new cases), 26 related
transaction/route/sampling contracts and 68 P1 regressions. Five offline checks
passed; changed Python compiles and diff checks pass. Frozen inputs: 1,733.
Reuse the prior 285-check gate for untouched schema/summary writers. Full suite
comparison and older opt-in fixture mapping remain A4; no new skips or weakened
assertions. Evidence: /tmp/chessism-canonical-a3-views-20261110-01.

A3.1 analysis views verification launched; supervisor PID 2576811.
Startup confirmed. Monitor: `python3 /tmp/chessism-canonical-a3-views-20261110-01/monitor.py`.
Returning control without polling. Review results, source integrity and owned
cleanup after user completion; no commit or next gate before evidence review.

## A3.1 analysis pages and previews gate passed and reviewed

All 135 checks passed: 41 offline/canonical PostgreSQL (18 new cases), 26 related
contracts and 68 accepted P1 regressions. No failures/errors/skips. Supervisor
21.44s. Reviewed raw and structured evidence. All 1,733 frozen inputs match, and
the retired module remains absent. All five containers were removed, independently
confirmed with the exact run label. The 1,002-game/5,006-association page plan ran
its aggregate twice for two visible rows, 0.458ms execution; synthetic scale only.

Ready for local commit. Remaining player/global readers and feature consumers
remain A3; full-suite comparison and retirement remain A4. No Stage A acceptance,
push, merge, deployment or existing-database operation.

## A3.1 player navigation and basic game statistics gate started

Analysis views committed as `3d2136f`; entry tree clean. The remaining player
coverage/basic-game reads mix physical legacy fields, profile query rewriting and
session ownership. Convert this cohesive feature to canonical side relationships,
explicit standard/nonempty classification, guarded read snapshots and stable ties.
Preserve player-perspective self-play counts and all response/null/type contracts.
The concrete scope and tests are recorded in the plan before implementation;
hours/snapshot and other analytics are explicitly subsequent gates.

Canonical player navigation/basic views implemented; verification pending.
Twelve related operations moved from ask_db to a guarded feature read boundary.
The SQL adapter has one fixed canonical side projection, explicit eligibility and
no runtime profile/field rewrite. Self-play perspectives, public response/null/
numeric contracts and navigation policies are retained. Tie ordering is explicit;
the independent PostgreSQL oracle exercises actual dates, identities and sides.
Previous coverage SQL-shape assertions map to real-data tests and schema nullability;
all payload assertions remain, with added resource closure checks.

Prepared 123 checks: 40 offline/canonical PostgreSQL (17 new cases plus existing
analysis views), 15 transaction/coverage/read-comparison contracts and 68 P1 checks.
Five offline checks passed; changed Python compiles and diff checks pass. Frozen
inputs: 1,736. Reuse untouched schema/writer/recovery and selector evidence from
the 285/135-check gates. Full baseline comparison and old fixture mapping remain A4.
Evidence: /tmp/chessism-canonical-a3-player-views-20261110-01.

A3.1 player views verification launched; supervisor PID 2608471.
Startup confirmed. Monitor: `python3 /tmp/chessism-canonical-a3-player-views-20261110-01/monitor.py`.
Returning control without polling. Review saved results, source integrity and
owned cleanup after user completion; no commit before evidence review.

## A3.1 player navigation and basic statistics passed and reviewed

All 123 checks passed: 40 offline/canonical PostgreSQL (17 new cases), 15 related
contracts and 68 accepted P1 regressions. No failures/errors/skips. Supervisor
20.00s. Reviewed raw and structured evidence. All 1,736 frozen inputs match and
the retired module remains absent. All five containers were removed, independently
confirmed with the exact run label. No production access or service changes.

Ready for local commit. Remaining analytics and feature consumers remain A3;
full-suite baseline comparison and retirement remain A4. Stage A is not accepted.

## A3.1 FEN browsing and aggregate reads gate started

Player navigation/basic views committed as `59a305c`; entry source tree clean.
FEN browsing still uses the catch-all SQL helper/ORM, and explorer games/examples
reference historical participant and side fields. Convert these cohesive reads to
canonical SQL with application-owned snapshots. Scope and verification are now
recorded above A3.2 in the plan. No schema, cache policy, scoring algorithm,
production-resource or Stage B change.

Canonical FEN browsing implemented; verification pending. Six list/page/aggregate
reads moved from ask_db, with canonical explorer queries in the same adapter.
Read snapshots share the existing guarded transaction cleanup; previous cleanup
function ASTs are unchanged. Summary arithmetic ASTs are unchanged after extraction.
Canonical summary absence raises, rather than rebuilding during HTTP reads.
The top text route now renders NULL scores; numeric page arithmetic is widened
for the supported int32 evaluation range and bigint occurrence counts.

Prepared 136 checks: 57 offline/canonical PostgreSQL (17 new cases plus player and
analysis views), 11 transaction/explorer/coverage contracts and 68 P1 checks.
Five offline checks passed; changed Python compiles and diff checks pass. Frozen
inputs: 1,740. Reuse unchanged schema/writer/recovery evidence. Full baseline
comparison and historical opt-in fixture mapping remain A4, without new skips.
Evidence: /tmp/chessism-canonical-a3-fen-views-20261110-01.

A3.1 FEN views verification launched; supervisor PID 2642785.
Startup confirmed. Monitor: `python3 /tmp/chessism-canonical-a3-fen-views-20261110-01/monitor.py`.
Returning control without polling. Review results, source integrity and owned
cleanup after the user reports completion; no commit before evidence review.

## A3.1 FEN browsing gate passed and reviewed

All 136 checks passed: 57 offline/canonical PostgreSQL (17 new FEN cases and
35 existing schema/player/analysis checks), 11 related contracts and 68 accepted
P1 regressions. No failures/errors/skips. Supervisor 24.08s. Raw and structured
evidence agree. All 1,740 frozen inputs match; the retired module remains absent.
All five owned containers were removed, independently confirmed by exact label.

Ready for local commit. Remaining analytics/feature consumers and full-suite
baseline comparison/retirement remain A3/A4. Stage A is not complete or accepted.

## A3.1 dashboard counts and mode rankings gate started

FEN browsing committed as `c74864f`; source tree clean. Dashboard counts and ranked
players still use legacy game/player fields and read-triggered rebuilds. Two old
mode writers duplicate the canonical ingestion calculation. Consolidate those
writers and move the related views to explicit canonical boundaries. The plan
records exact scope, preserved semantics and the required concurrency checks.

Canonical dashboard/ranking changes implemented; verification pending. Six cohesive
read operations now use canonical SQL and explicit read transactions. Mode summary
publication is shared with ingestion, uses numeric identities and the existing A2
calculation, and serializes full/scoped rebuilds with the ingestion commit lock.
Standalone writes explicitly select READ COMMITTED and use accepted guarded cleanup.
Removed two duplicate writers and lazy ensure paths; the obsolete main startup
ensure call is gone, with other startup/storage-profile retirement still A4.

Preflight compilation caught two indentation changes during import cleanup; both
were corrected before test launch. All remaining ask_db function ASTs match HEAD.
Prepared 153 checks: 66 offline/canonical PostgreSQL (17 new cases plus canonical
ingestion and player/FEN views), 19 transaction/explicit-factory/automatic pipeline
contracts and 68 P1 checks. Five offline checks passed; 18 changed/new Python files
compile and diff checks pass. Frozen inputs: 1,745. No schema change. Historical
reference tools have moved imports/factory bindings; their fixture conversion and
complete baseline comparison remain A4. No new skips.
Evidence: /tmp/chessism-canonical-a3-dashboard-20261110-01.

A3.1 dashboard verification launched; supervisor PID 2682953.
Startup confirmed. Monitor: `python3 /tmp/chessism-canonical-a3-dashboard-20261110-01/monitor.py`.
Returning control without polling. Review saved results, frozen input integrity
and owned cleanup after user completion. No commit before the gate passes.

## User-requested safe stop — 2026-11-10 (America/Mexico_City)

The user superseded continuous A2–A4 execution and requested a safe checkpoint and
handoff, with no implementation/verification until explicit renewed authorization.
Read only the already completed dashboard evidence: all 153 checks passed
(66 foundation/PG, 19 contracts, 68 P1), no failures/errors/skips, 26.12s. All
1,745 frozen inputs match, and retired paths remain absent. No new tests were run.

Host process inspection confirms supervisor 2682953 exited. All five owned
containers were removed; exact-label and canonical-container queries return none.
No owned verification writer remains. User monitor 2698835 is a read-only viewer
and may be closed with Ctrl+C; it was not killed. No active transaction needs
cancellation or cleanup. No resource was forcefully terminated or deleted here.

Preserved 350 top-level text evidence files from 16 Stage A run directories,
including failed attempts, in docs/architecture/evidence/stage-a-stop-2026-11-10.tar.gz.
Archive bytes were read back against per-file SHA256 hashes. Original /tmp evidence
remains untouched; no database/socket/volume contents were copied. Existing DB,
verified recovery checkpoint, Neo4j, Redis queues and production/GCP services were
not accessed or modified. No push, merge or deployment.

Tracking is paused, not completed or accepted. The local WIP checkpoint contains
all task-related unfinished dashboard/ranking work and the resume handoff at
`docs/architecture/stage-a-resume.md`. Stop after checkpointing; tomorrow requires
explicit authorization. The most recent source changes have focused evidence;
full integration, remaining A3 consumers and A4 remain unfinished.

Safe-stop WIP checkpoint created locally as `05f0bf3bc059028f22c8bd5a693079cbef76522e`.
The follow-up commit only records its identity and finalizes the handoff. Final
hash inspection found the expected stop documentation update in the frozen
canonical-persistence.md; the other 1,744 inputs match. No application or test
code changed after the passing gate. This documentation exception is explicit in
the manifest and handoff; original test inventories/evidence are not rewritten.
No additional tests, services or implementation were started.

## Fast-track Stage A resumed — 2026-11-18

Explicit attached authorization supersedes the pause. Repository/branch/HEAD match
b00885a and working tree was clean. Archived last-gate inventory confirms 1,744
unchanged application/test inputs and exactly the recorded documentation exception.
No earlier verification container remains. Original /tmp evidence/dependencies were
lost after restart; durable evidence was read from the committed archive, not rerun.

Bounded remaining-consumer inventory is recorded at the top of refactor-plan.md.
Begin a coherent canonical time-control analytics conversion: shared queries,
operation-owned snapshots, no old aliases or new schema compatibility layers.
Preserve required feature calculations and public responses. No feature removal
proposed. Focused checks follow; A3/A4 remain incomplete and unaccepted.

Separate, previously authorized FEN backup completed: 6,974,790 new result versions,
500,023,737 new bytes; all 1,510 chain parts (75,408,203 versions) passed checksum,
decompression and record checks. No restore was run. One-off container removed.
The full PostgreSQL recovery checkpoint was not accessed or changed.

A3.1 time-control conversion prepared: eleven operations moved from ask_db into
one application module and one concrete canonical read adapter. Moves, normalized
opening counts/pages, length distributions and activity share SQL; global/player
counting and public response differences remain explicit. UTC read-only snapshots
use unchanged P1 cleanup. Weighted Jenks moved unchanged for its two consumers;
three independent offline tests pass. Remaining ask_db function ASTs are unchanged
apart from that moved helper reference. Only games/players router imports changed.
Opening counts now exclude empty sequences just as visible rows do; no public
feature removed. Stable self-play rating-chart ties use black-before-white.

Seventeen new PostgreSQL checks cover independent fixture oracles, side/game counts,
variants/custom/zero games, rating bounds and int32 extremes, ties/pagination,
normalized openings/results, percentile/bucket boundaries, UTC dense/sparse activity,
chart formulas/ranges, empty/invalid input, concurrent snapshot changes, failures,
read-only enforcement, repeated cancellation and actual routes. Three pure checks
passed; isolated gate has not yet run. Planned 116 checks: 42 foundation/player/time
control, six existing transaction and 68 P1. Reuse unchanged schema/writer evidence.
Old /tmp dependencies were gone; a brief network-isolated import probe succeeded on
cached API image Python 3.10.22. Tests mount current source read-only, shadow .env,
use private PG/Redis sockets, no network/ports and bounded CPU/memory. No services,
production data or recovery checkpoint are modified.
Evidence: /tmp/chessism-canonical-a3-time-controls-20261118-01.

Time-control gate launched; supervisor PID 314240. Startup confirmed. Returning control
without polling. Review exact results, source integrity and cleanup after completion;
no local commit before passing evidence review.

### Time-control gate attempt 1 — evidence reviewed, test oracles corrected

The first isolated run completed 42 foundation checks: 40 passed, two failures,
zero errors/skips. Transaction and P1 suites were not started. All three owned
containers were removed, independently confirmed by exact run label. All 1,750
frozen inputs still matched before correction. Original failure evidence retained
at /tmp/chessism-canonical-a3-time-controls-20261118-01; no baseline debt relabeling.

Failing identities:
- CanonicalTimeControlTests.test_global_moves_use_selected_side_rating_and_player_moves_use_owned_color:
  expected Nf3 instead of c4 on an equal-frequency tie. Python Unicode ordering
  disagreed with the disposable database's PostgreSQL collation. Original and
  converted SQL both order by count descending then move ascending. Tests now read
  only collation ranks for literal fixture tokens; owner/frequency calculations
  remain independent Python oracles and exact row equality is preserved.
- CanonicalTimeControlTests.test_lengths_match_independent_percentiles_and_distinct_histogram_contracts:
  expected 74.97 instead of 74.98 for mean 74.975. Existing and converted SQL both
  use ROUND(AVG(time_elapsed)::numeric,2). Replace binary Python rounding with
  Decimal ROUND_HALF_UP, and add explicit 1.005/1.015/2.675 regression cases for
  global and player endpoints. Inspection of the later assertions also found a
  fixed three-bucket expectation wrong for Bob's two-game subset. Assertions now
  compare exact independent move/elapsed histogram labels AND counts per subset.

No application code or public behavior changed to satisfy tests. No assertions
were relaxed or tests skipped. Next gate: 43 foundation + 6 transaction + 68 P1 =
117 checks, same isolation and limits, fresh disposable resources. PostgreSQL
collation/rounding evidence will be recorded explicitly. Syntax and diff checks
pass; the independent decimal examples were checked offline.
Evidence: /tmp/chessism-canonical-a3-time-controls-20261118-02.

Time-control gate attempt 2 launched; supervisor PID 416978; startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a3-time-controls-20261118-02/monitor.py`. Returning control without polling.
Review results and cleanup before any commit or subsequent implementation.

### Time-control launcher attempt 2 — infrastructure failure before tests

Exited after 1.68 seconds; zero tests ran and no test container was created.
The added psql diagnostic exited 2 immediately after pg_isready reported accepting
connections. Its exact stderr was not saved by the old subprocess wrapper, so
this record does not assert an unobserved server error message. Readiness was
insufficient: the pinned image starts a temporary socket server, creates the DB,
stops that server, and only then execs the final postgres process. Startup-script
inspection confirmed that sequence. Both owned containers were removed and an
independent exact-label check returned none. All 1,750 frozen source inputs match.

Repair only the disposable verification harness: require PID 1 to be postgres,
then a successful SELECT current_database() using explicit private socket/55432
before diagnostics/tests. Save subprocess stderr/stdout and owned server logs on
failure. Reuse unchanged application/tests and exact inventory. This was not a
second failed application test run; the first test-oracle correction has not yet
been executed. No tests, assertions, skip policy or expected counts changed.
Fresh launch evidence: /tmp/chessism-canonical-a3-time-controls-20261118-03.
Planned gate remains 117 focused checks. No local commit or next implementation
until this gate passes and evidence is reviewed.

Time-control gate launch 3: supervisor PID 426627; startup confirmed.
Monitor: `python3 /tmp/chessism-canonical-a3-time-controls-20261118-03/monitor.py`. Returning control without polling.

### Canonical time-control analytics — verification passed

Reviewed launch 3 raw and structured results: 43 foundation (including 18 new
PostgreSQL cases and three pure calculation tests), six transaction and 68 P1 =
117 passing checks, zero failures/errors/skips, 16.93 seconds, Python 3.10.22.
All 1,750 frozen source hashes match. All five owned disposable containers removed;
independent exact-label inspection returned none. The recorded en_US.utf8 collation
orders c4 before Nf3 and SQL numeric rounding gives 74.98, confirming oracle repairs.
No application changes followed the passing run. No full-suite rerun was necessary.

Eleven reads moved to cohesive operations/adapters with shared canonical SQL;
about 340 application lines removed net. Public fields and calculations preserved,
including side/game counts, UTC, filters, histograms and failure/cancellation behavior.
Empty opening entries are consistently excluded from both count and page.
Full evidence from all three launches archived (text only, read back/hash checked)
in docs/architecture/evidence/stage-a-time-controls-2026-11-18.tar.gz.
Stage A remains incomplete/unaccepted. Remaining summary publishers, player/auxiliary
consumers and A4 are still due. Original DB/checkpoint and deployed services unchanged.
