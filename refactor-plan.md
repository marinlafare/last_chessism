# Chessism V1 architectural refactoring

## Active authorization — Stage A, Batch 1

The user approved the A → B → C → D roadmap and explicitly authorized only
**A1: Canonical schema and persistence contracts**. P1 #1 is accepted with the
documented limitations; the older pending-review statements below are historical.
Entry checkpoint: `0fde829d9477b3f73714b52e820a67bad49dad77`, clean branch
`refactor/p1-analysis-failure-propagation`. All 1,699 saved verification input
hashes match. Reuse that baseline and its exact unresolved debt; do not rerun
unaffected full suites. User authorization supersedes the skill's blanket
full-suite-per-chunk rule and additional plan-signoff requirement.

### A1 implementation boundary

Implementation and isolated verification are complete: 31 foundation checks
(5 offline, 26 PostgreSQL) and all 68 focused P1 checks passed without skips.
Evidence reviewed after user handoff; owned resources removed. Local commit and
its handoff record conclude A1. Independent review is still required before A2.

1. Add `docs/architecture/canonical-persistence.md`: schema scope, relationships,
   transaction contracts, conversion map, and outstanding integration boundaries.
2. Add `chessism_api/database/postgresql/canonical/`: independent SQL foundation
   and explicit initializer/version validator. Keep useful normalized v3 keys,
   integer scores, account/session semantics, ingestion accounting, and typed
   cloud recovery structures. No historical copy, compatibility views, release
   marker, or existing runtime import is a prerequisite.
3. Add `scripts/init_canonical_db.py`: explicit isolated Unix-socket target only;
   no environment fallback, destructive reset, live connection, or deployment.
4. Add `tests/canonical_schema_fixture.py`, `tests/check_canonical_schema_postgres.py`,
   and `tests/test_canonical_schema.py`: deterministic synthetic fixtures and
   real PostgreSQL tests for initialization/versioning, identities/constraints,
   accounts, variants, evaluations, atomicity, cloud state, and selection locks.
5. Verify with private PostgreSQL 15 using no network or application volumes.
   Check source preservation, new unit/integration tests, syntax/whitespace, and
   relevant existing P1 checks. No configured Python lint/typecheck exists.
6. Review saved evidence after the user reports completion. If passing, commit
   only A1 files locally as `feat: establish canonical PostgreSQL foundation`.
   Stop for Batch 1 review. Batches A2–A4 and all later stages remain unauthorized.

Exit: initializer is atomic and repeatable, validation performs no schema repair,
all new checks pass without skips, existing application/test files remain intact,
and limitations distinguish schema foundation from application integration.
Record schema artifact/source hashes and exact test results. Future extensions
must update the foundation version; this batch does not claim full runtime readiness.

### Remaining roadmap (approved direction, not implementation authorization)

- A2: canonical writes and analysis selection; preserve accepted P1 behavior.
- A3: canonical reads/analytics and derived projections.
- A4: remove retired profiles/interfaces after complete caller conversion.
- B: bounded engine execution and cloud dependency direction.
- C: shared frontend job outcomes, Positions state, explicit ingestion outcomes.
- D: full isolated integration and independent acceptance.

### Handoff and resource boundary

Any long verification runs detached with a temporary `monitor.py`: one display,
120-second refresh, start/elapsed/stage/status and evidence-based ETA only. Return
immediately after confirming startup; inspect results after the user reports back.
Owned test containers must be removed even on failure. Keep technical failures
in saved logs rather than printing raw command tracebacks in the monitor.
Do not touch existing databases, backups, services, unrelated files, or repositories.
No push, merge, deployment, runtime conversion, public-result change, or backup
behavior change is authorized in A1.

## Historical P1 #1 plan: Analysis selection failures

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

### Correction postflight

- [x] Chunk 3 verified and committed separately as `5542c9a`.
- [x] Chunk 4 verified and committed separately as `7f304be`.
- [x] Chunk 4 focused verification: 60 checks passed, including all 47 P1
  regressions (17 added by these corrections); 25 API checks pass on Python 3.10.
- [x] Final API suite: 1,442 tests, exactly three existing failures, ten existing
  errors and 276 unchanged skips. Worker: 287 tests, no failures/errors, seven skips.
- [x] Six isolated PostgreSQL checks pass without skips; test services stopped.
- [x] Final source hashes match the tested implementation. Compilation, whitespace
  and scope checks pass; no new TODO/FIXME. No application edits after the gate.
- [x] Correction implementation complete; no push, merge, deploy or P1 #2 work.
- [ ] Formal acceptance: reserved for the user's focused independent review.


## Final-review corrections A/B (authorized 2026-11-10)

Source: /home/jon/.codex/attachments/b905d848-5fe4-4e7e-85a9-d7f10c531a13/Pasted text.txt.
The user explicitly authorizes these two bounded implementation chunks; this is
not a request for another general plan sign-off. The only repository is
/home/jon/Desktop/workshop/last_chessism, branch refactor/p1-analysis-failure-propagation.
Verified starting HEAD 24e830e and clean worktree. No other repository/worktree,
production data, schemas, engine policies, P1 #2 or P1 #3 may be changed.

### Entry and verification policy

Preserve previous chunks and their evidence as historical implementation records.
The final independent review returned REQUEST CORRECTIONS: cancellation can
interrupt already-running closure, and Positions interprets telemetry/ARQ result
availability as successful execution. Existing four correction commits remain.

Before application changes, recapture the unchanged full API/worker baseline,
existing 60 focused and 25 Python 3.10 checks, six disposable PostgreSQL checks,
and frontend Node tests/architecture/source-size/build baseline. Compare API and
worker failure/error/skip identities against correction2 evidence, including
tracebacks with line numbers normalized. The explicitly retained API baseline is
1,442 entries, 3 failures, 10 errors, 276 skips; worker 287, 0 failures/errors,
7 skips. New tests must all pass; no silent weakening/skipping. Stop if new
baseline problems prevent meaningful verification. No configured Python or plain
JS type checker/linter; use Python syntax compilation, git diff --check and the
existing frontend architecture/source-size checks and Vite build. Build output
and isolated dependency installs stay in the temporary verification directory.

### Chunk 5: Protect analysis-session cleanup already in progress

Depends on: completed chunk 4 and reviewed entry baseline.
Files: chessism_api/database/postgresql/ask_db.py;
chessism_api/operations/analysis.py;
chessism_api/database/postgresql/analysis_cleanup.py (new, only if a small shared
protected-await helper is needed by both existing cleanup owners);
tests/test_analysis_failure_jobs.py; tests/test_analysis_failure_selection.py;
the three tracking documents.

1. Add deterministic event-controlled regressions for worker closure and
   selector closure after rollback failure, each interrupted by external
   cancellation or sibling acquisition failure. Include repeated cancellation,
   cleanup failure preserving the primary exception, and committed results.
   Assert the coordinator remains pending until closure actually finishes.
   Run and record the failures before application changes; no timed sleeps.
2. Protect cleanup before its first awaited rollback/close can be interrupted.
   A small shared helper may retain a cleanup task, shield every await, drain
   after first/repeated cancellation and then propagate the original outcome.
   Bare shield without draining the retained task is insufficient. Do not hide
   genuine cleanup failures when no primary error exists. Always attempt close
   after rollback failure. Keep selector versus worker ownership unchanged.
3. Retain the coordinator's cancellation and drain behavior, acquisition outside
   retries, prior commits, counters, SQL/claim rules and engine request policies.
   Use Python 3.10 APIs; introduce no service layer or generic framework.
4. Run focused regressions on Python 3.10 and the established runtime; run full
   API/Redis/ARQ and worker comparisons plus PostgreSQL invariants. Verify source
   hashes and no unrelated changes. Commit only after the gate is reviewed.

Exit: all four reproduced orderings and repeated cancellation complete cleanup;
original failure/cancellation survives; all prior P1 guarantees remain verified.
Commit: fix: protect analysis session cleanup already in progress

### Chunk 6: Interpret Positions outcomes from authoritative ARQ results

Depends on: verified and committed chunk 5.
Files: chessism_web/src/pages/positions/positionPageSupport.js;
chessism_web/src/pages/positions/positionStatusFreshness.js;
chessism_web/src/pages/positions/usePositionStatusPolling.js;
chessism_web/src/pages/positions/usePositionsPage.js;
chessism_web/tests/positionsProgress.test.mjs;
chessism_web/tests/positionStatusFreshness.test.mjs;
chessism_web/tests/positionJobOutcomes.test.mjs (new);
chessism_web/tests/positionJobPolling.test.mjs (new if needed);
the three tracking documents. A file in this list changes only if needed for the
status interpretation, outcome presentation or polling lifecycle. Shared polling
transport and unrelated pages remain unchanged; do not start P1 #3.

1. Confirm actual jobs API shapes: status=complete denotes result availability;
   result.success is the execution outcome. Retained-history endpoint already
   derives its terminal status from that outcome. No new wire fields or text
   inference. Inspect queued/deferred/running, cancellation and missing-result
   states before defining the minimal Positions-specific interpretation.
2. Add failing cases for failed reset retaining old completed progress, stale
   completion while queued/running, unsuccessful results with completed/missing
   progress, success, temporarily missing progress and unresolved result.
   Exercise polling continuation/termination through the actual shared polling
   primitive and Positions decisions using a deterministic fake clock. Cover
   freshness checks so stale telemetry cannot hide a confirmed result/retry.
3. Give confirmed execution outcomes precedence; never infer success from ARQ
   status=complete alone. Keep jobs with unresolved outcomes active and polling;
   prevent stale terminal progress from finalizing an active queue execution.
   Preserve valid intermediate phases and explicit unsuccessful/cancelled
   outcomes. Render a failure detail from the actual response contract, without
   claiming success or scheduling successful completion dismissal.
4. Adjust existing fixtures only when they falsely equate a complete queue
   status with success; retain their intended assertions and document why.
   No broad polling transport redesign or account/project work.
5. Run all frontend Node tests, relevant polling/progress tests, existing
   architecture/source-size checks and production build into temporary output.
   Run the final backend/Python 3.10/Redis/PostgreSQL preservation gate; compare
   all broader results to entry baseline, record source hashes, then commit.

Exit: actual outcome determines completion/failure, unresolved jobs keep polling,
all required frontend cases pass, and backend P1 guarantees remain unchanged.
Commit: fix: make Positions use confirmed ARQ execution outcomes

### Final handoff

All long jobs use /tmp/chessism-p1-final-corrections/monitor.py with an in-place
120-second display that remains open after completion. Show one watch command,
launch detached, record PID, and immediately end the turn; do not poll or wait.
Preserve /tmp/chessism-p1-verification evidence. Tests use fresh uniquely labelled
containers, no external network, read-only source and explicit synthetic settings;
only isolated frontend dependency preparation may access the package registry.
Stop after two bounded verification-fix attempts for a chunk. Commit verified
chunks separately and record completion with a tracking-only follow-up if needed.
Report findings, exact red/green evidence, compatibility, baseline debt and
remaining production/real-engine limits. Do not declare P1 #1 accepted, push,
merge, deploy or begin another task; independent acceptance review follows.

### Approved frontend baseline exception (2026-11-10)

The user explicitly authorized continuing chunks 5/6 with the four reproduced
frontend assertion failures. The complete baseline is 320 checks, 316 passes,
four failures, zero errors/skips/cancellations. Exact names and failure details
are frozen in /tmp/chessism-p1-final-corrections/frontend-baseline-complete/
failure-details.json and recorded in the manifest/log. These remain unresolved
testing debt, outside P1 #1. Require every new regression to pass and no additional
failures, errors or skips. Do not weaken/delete/skip tests or repair appearance/
navigation behavior. Stop if existing debt prevents meaningful verification.
The backend exception and all scope/safety restrictions remain unchanged.

### Final-review correction postflight (2026-11-10)

- [x] Correction A verified and committed separately as `49edfc5`.
- [x] Correction B verified and committed separately as `499a781`.
- [x] Eight backend regressions reproduced before correction; all 33 cleanup
  checks pass on Python 3.10 and 3.11, with completed-cleanup assertions.
- [x] Twenty-six frontend regressions pass; 21 reproduce old defects and five
  preserve correct behavior. All 34 focused frontend checks pass.
- [x] Complete API/worker results match approved failure/error/skip details;
  six isolated PostgreSQL checks and 68 focused checks pass.
- [x] Full frontend: 346 checks, 342 passes, exactly four approved preexisting
  failures, zero skips; exact failure details retained in the manifest.
- [x] Architecture, source-size and production build pass. Final source hashes
  match; backend source/image checks validate reuse of its complete passed gate.
- [x] Only approved production/test files and tracking documents changed;
  disposable test services removed and original ignored directory preserved.
- [x] No production changes, push, merge, deployment, P1 #2 or P1 #3 work.
- [ ] Formal P1 #1 acceptance: reserved for independent review.
