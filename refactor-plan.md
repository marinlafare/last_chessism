# P1 #1: Analysis selection failures

Current work: the independent review requested two blocking corrections. The
original implementation record below remains historical, not formal acceptance.
See the correction chunks at the end of this plan. No push, merge or deployment.

Source: the approved design in this conversation and the implementation instructions at
`/home/jon/.codex/attachments/2eca9586-fb0c-4f10-9c69-f9a72f07e4d5/Pasted text.txt`.

Created: 2026-11-09. Baseline: `4f77d15df240a06efc7904496af7ec5e7a7b89cb`.

Target: a database selection failure fails the current local analysis attempt,
releases its resources, and cannot be mistaken for successful exhaustion.

## Authorization and execution

The user explicitly authorized implementation of P1 #1 and the attached design,
including removing unnecessary error-suppression compatibility. The ordered
chunks below implement that authorization. Do not address P1 #2 or other findings.

For every long-running verification process, start it detached, record its identity,
give the user one command for the temporary status monitor, and end the turn. It
must refresh in place every 120 seconds, show start time and elapsed time, and remain
open until Ctrl+C, including after the process finishes (user's latest preference).
Give an evidence-based duration estimate where available; never invent an ETA.
Do not ask the user to watch application/system logs. Resume only after the user
reports completion. Do not poll or wait for these jobs. Short inspections and
focused deterministic checks may finish within the tool's initial response.

Use branch `refactor/p1-analysis-failure-propagation`. Preserve unrelated changes.
Commit verified chunks locally. No deployment, schema migration, production data
access, or changes to engine retry limits/timeouts are authorized by this plan.

## Decisions and invariants

- The caller audit found no legitimate consumer of suppressed selection errors.
  Remove `raise_errors` from all three selectors and their callers.
- Keep the existing `fetch_batch(limit)` callback and session/FEN tuple contract.
  `(None, None)` is reserved for successful empty selections, including an empty
  frozen-game input. Empty means no currently eligible unlocked work, not that
  every database position is analyzed.
- Keep query text, ordering, deduplication, limits, `FOR UPDATE SKIP LOCKED`, cloud
  claim exclusion, and the fresh-snapshot eligibility recheck unchanged.
- The selector owns its session until a successful handoff. Failure/cancellation
  must attempt rollback and always attempt close, including rollback failure.
  Cleanup errors must not replace an already active selection error/cancellation.
- The worker owns handed-off sessions. Existing committed batches stay committed;
  uncommitted work is released. Preserve progress count meanings and offsets.
- Acquisition errors escape processing retries. The coordinator cancels and
  awaits siblings, publishes failure best-effort, then re-raises the original
  exception. External cancellation remains cancellation.
- A failed query does not mean a known number of FENs failed. Do not fabricate
  failed-position counts or reset valid progress when reporting failure.
- Final `failed`/`complete` progress survives late Stockfish telemetry. An explicit
  reset permits another attempt; `loop_complete` is not a final terminal phase.
- Preserve API/queue names and payloads, successful return shapes, Redis keys/TTLs,
  cloud pause/recovery, tablebase behavior, and processing-error retry policies.
- ARQ may expose `status=complete` for a retained failed result. Assert
  `result.success == false` and, when available, `progress.phase == failed`.

## Preflight

- [x] Inspect working tree: clean at baseline commit.
- [x] Audit all selector definitions, direct callers, and injected local callbacks.
- [x] Confirm no configured Python lint/type-check commands or configuration.
- [x] Inspect the existing runtime image: includes API test dependencies absent
  from `.venv-cloud` (PyArrow, Neo4j, and pglast).
- [x] Create the working branch and commit these tracking documents.
- [x] Run the complete main Python offline unittest discovery in the corrected
  image, with repository source read-only, no network, and dummy database settings.
- [x] Run batch-worker offline unittest discovery in the same isolated Python 3.11
  environment. Record results separately because the worker requires Python 3.11+.
- [x] Establish available isolated Redis/PostgreSQL verification infrastructure.
  Never borrow application service connections or persistent application volumes.

## Verification commands

### Baseline finding and approved gate exception

The first baseline run finished with 1,364 tests, 3 failures, 18 errors and 276
skips in 52.923 test seconds (56.600 container seconds). No application or test
changes preceded it. This run does not establish a passing baseline.

Eight error reports are attributable to the test environment: two missing dummy
auth settings, one missing worker-test import path, one missing Google dependency,
and four uses of `TestCase.enterContext` unsupported by Python 3.10.

Three assertion failures and ten error reports expose existing test/code
mismatches. Details and test identities are recorded in the manifest and log.
They must not be silently fixed as part of P1 #1.

The user approved this exception with "ok proceed" after reviewing the baseline:
1. Correct the isolated test environment (Python 3.11+, all declared dependencies,
   dummy auth settings, and worker-test fixture path) and rerun the unchanged
   code to establish the exact remaining failures.
2. Keep unrelated source and tests unchanged. Record exact baseline failures;
   do not skip them or describe the full suite as passing.
3. Require every new P1 #1 regression and relevant previously passing test to
   pass. Require the full suites to introduce no new failures/errors and no new
   skips; investigate changes in baseline failures individually.
4. Retain isolated Redis/ARQ and PostgreSQL checks, scope review and all other
   chunk gates. Any unexpected P1-related baseline defect is investigated before
   proceeding; this is not a blanket waiver for analysis failures.

The skill's entry gate says "If not: STOP. Fix or escalate to user." That escalation
has been resolved by the user's explicit approval above; do not request it again.
The approved comparison against the corrected baseline replaces the requirement
for a globally green suite at the entry and chunk gates. Scope remains P1 #1.

### Corrected isolated environment and temporary monitoring

Use the existing `chessism-cloud-controller:latest` image (Python 3.11.17), with
Neo4j 6.3.0 and pglast 5.9 installed into a disposable dependency directory.
The package index no longer serves pglast 5.9, although the existing API image
contains it. Build exactly that version for Python 3.11 from upstream commit
`b3fc527daeb113fb1ee6490ce1000d8e49c29cc4` (release `v5.9`) and its pinned parser
submodule. The temporary dependency build container installs its own compiler
into its disposable filesystem and preserves the resulting wheel for reuse.
Only dependency installation has network access; it mounts no checkout or
credentials. Both test containers have `--network none`, read-only source and
root filesystem, dummy auth/database settings, and writable temporary paths.
Run test containers under the host UID/GID; the dependency builder returns wheel
and dependency ownership to that user so artifacts can be deleted afterward.
No Docker socket or production volumes enter the containers.

The temporary program is `/tmp/chessism-p1-verification/monitor.py`. It provides
a detached `--run` mode, instrumented unittest discovery via `--suite main` or
`--suite worker`, and a default console monitor. It records atomic status updates,
current test, elapsed time and structured results. Monitoring defaults to every
120 seconds, clears the previous terminal display and stays open after completion;
Ctrl+C stops only the monitor. ETA is shown
for the API stage based on its previous 53-second run, with runtime-change caveat;
other stages/overall ETA remain unavailable without evidence.

Monitoring command: `python3 /tmp/chessism-p1-verification/monitor.py`.

The wrapper discovers and executes the same tests as these commands (no filters
or skips added), with working directory `/workspace` for the main suite:

```sh
python -B -m unittest discover -s tests -p 'test*.py' -v
```

Mount this checkout read-only at `/workspace`; set
`PYTHONPATH=/test-deps:/workspace:/workspace/stockfish-batch-worker:/workspace/stockfish-service:/workspace/tests:/workspace/stockfish-batch-worker/tests`,
`DATABASE_URL=postgresql+asyncpg://test:test@127.0.0.1:1/test`, and
`PYTHONDONTWRITEBYTECODE=1`. Hide the checkout's `.env` using a read-only `/dev/null`
mount. Run with `--network none --read-only`, bounded resources and writable tmpfs
at `/tmp`. Do not enable database, Neo4j, CUDA, or engine integration opt-ins in
this offline suite. Any unavailable/opt-in tests must be reported as unverified,
not counted as passing integration checks.

Worker offline suite, from `/workspace/stockfish-batch-worker/`, in the same image:

```sh
python -B -m unittest discover -s tests -p 'test*.py' -v
```

Focused tests during implementation:

```sh
python -B -m unittest discover -s tests -p 'test_analysis_failure*.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_terminal_progress*.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_scheduling.py' -v
python -B -m unittest discover -s tests -p 'test_analysis_batch_limits.py' -v
python -B -m unittest discover -s tests -p 'test_pipeline_helpers.py' -v
```

Use an isolated Redis for Lua and ARQ tests, with uniquely named keys/queues and
automatic cleanup. Explicitly supply that test connection; no fallback to the
application Redis. Use isolated PostgreSQL for existing cloud-claim/selection
checks when available. Record exact launch commands and results in the manifest.

Run the complete relevant offline suites at each chunk gate; run targeted Redis
and PostgreSQL regressions covering changed contracts. Run `git diff --check`
and compile changed Python sources in memory. No Python type checker/linter is
configured; record those checks as not applicable, not as executed passes.

## Chunk 1: Keep terminal progress authoritative

Depends on: preflight meeting the approved baseline-comparison gate.

Files:
- `stockfish-service/routers/analysis.py`
- New `tests/test_analysis_terminal_progress.py`
- New `tests/analysis_failure_test_support.py`, only for isolated test connections
  and loading the Stockfish module without colliding with application routers.
- The three tracking documents.

Steps:
1. Add tests executing the actual Stockfish Lua script against isolated Redis.
   Show that late updates overwrite `failed` and final `complete` before the fix.
2. Move the terminal check ahead of all progress-set and payload mutations. Return
   the existing processed count without altering a terminal payload or its sets.
3. Cover ordinary progress, monotonic counts, intermediate `loop_complete`, and
   an actual `_reset_job_progress` followed by successful new progress.
4. Run focused and complete verification gates, record red/green evidence, and
   review that the engine execution/retry code is untouched.

Exit: terminal updates are atomic no-ops, resets work, existing suites pass.

Commit: `fix: preserve terminal analysis progress against late engine updates`

## Chunk 2: Propagate selection failures and finalize failed jobs

Depends on: verified and committed chunk 1.

Implementation files:
- `chessism_api/database/postgresql/ask_db.py`
- `chessism_api/operations/analysis.py`

Necessary caller updates (remove the obsolete keyword only):
- `chessism_api/operations/cloud_analysis/controller.py`
- `chessism_api/operations/cloud_analysis/input_preparation.py`
- `chessism_api/operations/cloud_analysis/work_units_controller.py`
- `stockfish-batch-worker/benchmark_vm16/large_database.py`
- `tests/test_cloud_analysis_database.py`
- `tests/test_cloud_sequences_database.py`
- `tests/test_cloud_work_units_database.py`
- `tests/check_cloud_game_queue_postgres.py`

Regression files:
- New `tests/test_analysis_failure_selection.py`
- New `tests/test_analysis_failure_jobs.py`
- New `tests/test_analysis_failure_arq.py`
- `tests/analysis_failure_test_support.py`
- `tests/test_analysis_batch_limits.py` only if its callback assertion needs to
  express behavior; keep it unchanged if the callback identity remains valid.
- The three tracking documents.

Steps:
1. Add deterministic regression tests and record failures on the unmodified
   selector/coordinator code. Bound failing-path tests so the known retry defect
   cannot hang the test run.
2. Remove the `raise_errors` parameter and suppression from the three selectors.
   Ensure failure/cancellation cleanup always attempts close even if rollback
   fails; preserve the original active exception. Use explicit try/finally or one
   small shared private cleanup function in `ask_db.py` if required to avoid
   repeating the same cleanup policy. Do not alter SQL or introduce a new layer.
3. Remove the obsolete keyword at every audited caller, including benchmark and
   integration-test callers. Search the repository again for remaining uses.
4. Move acquisition outside the broad processing retry handler in `_worker`.
   Keep successful batch execution, reservation limits and empty behavior intact.
5. Centralize fatal progress publication after sibling cancellation/cleanup in
   `_run_analysis_job`. Relocate the existing engine-request fatal publication
   to that coordinator while keeping its propagation and retry policy unchanged.
   Preserve the original exception if cleanup/progress reporting encounters
   another error. Do not translate external cancellation into database failure.
6. Test real callbacks from global, player, all/player loop, and frozen-game entry
   points. A failed pass must prevent later passes, cooldowns and success finalizers.
7. Test first-batch failure, failure after a committed batch, offsets, selectors
   cancelled before handoff, workers cancelled after handoff, rollback failure,
   progress-write failure, and ordinary empty completion. Use asyncio events for
   concurrency rather than real sleeps. Preserve processing-error behavior.
8. Use real isolated ARQ/Redis execution with mocked database acquisition to assert
   unsuccessful stored results and failed progress. Re-run terminal/reset tests.
9. Run full verification gates and existing isolated PostgreSQL locking/claim
   tests when available. Record any unavailable integration coverage accurately.

Exit: all required failure paths propagate without successful completion, all
owned resources are released, required regressions pass, and invariant checks
show no changes to queries, queue/API contracts, or P1 #2 policies.

Commit: `fix: fail local analysis when database batch selection fails`

## Postflight and handoff

- Run final verification (reuse an unchanged final chunk gate unless new changes
  or unresolved concerns require repeating it).
- Review diffs and compare the result against every required scenario/invariant.
- Update manifest/log, recording commit SHAs, exact results and any limitations.
- Stop after P1 #1. Report root cause, decisions, simplifications, files changed,
  red/green evidence, validation gaps, risks and scope compliance.
- Before ending any turn for a long-running check, record current progress and
  the monitoring command. Do not claim completion while verification is pending.

## Completion verification

- [x] Both implementation chunks meet the approved verification gate.
- [x] Thirty new regressions pass, including real Redis/Lua and ARQ execution.
- [x] Six existing isolated PostgreSQL locking/claim/recheck tests pass with no skips.
- [x] Full API suite: 1,425 tests, exactly the existing three failures and ten
  errors, 276 unchanged skips. No new failure/error/skip identities.
- [x] Worker suite: 287 tests, zero failures/errors, seven unchanged skips.
- [x] Query construction, ordering, locks, public entry points and engine request
  policies verified unchanged; required caller edits only remove `raise_errors`.
- [x] Changed Python sources compile; whitespace checks pass; no new TODO/FIXME.
- [x] Test services stopped. No deployment or production data/schema changes.
- [x] Final full-suite results reused because subsequent edits affect only the
  temporary PostgreSQL harness and tracking documents, not application/tests.

Remaining validation limits: production/GCP and real Stockfish execution were
not exercised. Opt-in tests outside the six selected PostgreSQL checks remain
skipped. The unrelated baseline failures remain as explicitly approved. There is
no configured Python linter/type checker. Redis progress publication remains
best-effort; ARQ's stored unsuccessful result is authoritative if Redis reporting
is unavailable. Engine retry limits/timeouts remain the separate P1 #2 concern.

## Independent-review corrections

Source: `/home/jon/.codex/attachments/c229c4e7-89a8-479c-892b-137a8cb77a95/Pasted text.txt`.
The user explicitly authorized these two focused corrections, in this order,
with separate local commits. This is the implementation plan for that request;
no additional scope or approval gate is being introduced. Keep the existing
baseline exception and long-process monitoring/handoff instructions above.

Entry evidence: unchanged application source since the verified original gate;
the independent review checked matching archived baseline failures and reran
the 20 selector/orchestration tests successfully. Rerun those focused tests at
correction entry. Full API/worker and isolated Redis/ARQ gates follow each chunk.
The six PostgreSQL checks remain part of final correction verification.

### Chunk 3: Give the coordinator sole ownership of cancellation

Depends on: completed chunks 1 and 2 and the approved baseline-comparison gate.

Files: `chessism_api/operations/analysis.py`,
`tests/test_analysis_failure_jobs.py`, and the three tracking documents.

Steps:
1. Add deterministic two-worker cancellation regressions, with staggered closes,
   successful/failed rollback and repeated external cancellation while draining.
   Use events and observed task completion, not timing-dependent sleeps. Record
   failing assertions against the unmodified coordinator.
2. Shield the initial worker gather so external cancellation reaches the
   coordinator, which cancels unfinished workers once. Retain the worker group
   and drain all workers before propagating the original error/cancellation.
   Shield the drain from later cancellation requests and continue awaiting that
   same drain; do not re-cancel workers. Keep this local to `_run_analysis_job`.
3. Keep failure publication exclusive to ordinary fatal exceptions, after
   cleanup. Preserve `CancelledError` and committed counters/results. Do not
   change selection, processing retries or timeouts. Use Python 3.10-compatible
   asyncio APIs, matching the application's Dockerfile.
4. Run focused tests, source compilation and whitespace checks, then the complete
   API/Redis/ARQ and worker suites against the exact approved baseline. Start the
   long gate detached, provide the existing 120-second monitor and end the turn.
5. Review saved results after the user's completion report and commit only this
   correction and its regression/record changes.

Exit: cleanup completes before cancellation escapes, even with failed rollback
and additional cancellation while draining; original P1 regressions stay green;
no new full-suite failure/error/skip identities.

Commit: `fix: drain analysis workers without repeated cancellation`

### Chunk 4: Scope progress updates to an execution attempt

Depends on: verified and committed chunk 3.

Files: `chessism_api/operations/analysis.py`,
`stockfish-service/routers/analysis.py`, `tests/test_analysis_terminal_progress.py`,
`tests/test_analysis_failure_jobs.py`, `tests/test_analysis_failure_arq.py`,
`tests/analysis_failure_test_support.py`; update `tests/test_pipeline_helpers.py`
and `tests/test_analysis_batch_limits.py` only where existing progress request
or mock expectations must express the new attempt contract. Three tracking files.

Steps:
1. Add isolated Redis regressions delivering old-attempt updates immediately
   after reset, after new progress, and after completion. Require zero mutation
   of the active payload, counters, FEN sets and expiry on rejected updates.
   Record failures against the current job-ID-only implementation.
2. Generate a fresh opaque attempt ID at each top-level reset. Atomically install
   that active ID and clear prior progress/deduplication state. Store it under a
   job-scoped Redis attempt key with the existing retention policy; include the
   attempt ID in new progress payloads. No delete-then-establish race.
3. Pass the same attempt explicitly through `_run_analysis_job`, all loop/frozen
   passes, coordinator progress writes, `_call_engine_service`, and Stockfish's
   `AnalysisRequest`/`_write_progress`. Live analysis without progress remains
   supported; progress-enabled requests require both job and attempt identity.
4. Both Lua update scripts must reject missing/mismatched active attempts before
   any mutation; Stockfish must also retain the terminal-phase guard. Never infer
   an incoming request's attempt from the current Redis value. Reset replaces the
   active identity; successful updates preserve its TTL alongside progress.
5. Test valid current-attempt updates, explicit reset uniqueness, current terminal
   protection, loop reuse, stale coordinator writes, missing identity and expired
   attempt state. Failure to publish/reset Redis remains best-effort and cannot
   change the database/ARQ result; later writes must not resurrect old attempts.
6. Rerun focused failure/ARQ/terminal and affected request-contract tests, then
   full API/worker suites and six existing isolated PostgreSQL invariants. Use
   the established monitoring/handoff workflow for long verification.
7. Review diffs for scope and unchanged selection/transaction/retry policies;
   record results, commit locally, and stop for the user's independent review.

Exit: only the current attempt can mutate progress, resets establish identity
atomically, terminal guards and all original P1 guarantees remain verified.

Commit: `fix: isolate analysis progress by execution attempt`

These corrections do not constitute acceptance of P1 #1. Formal acceptance is
reserved for the subsequent focused independent review requested by the user.
