# Stage B — focused cancellation correction

Implemented and verified on 2026-11-19. **Stage B acceptance remains pending a
short independent re-review of this correction.** Stage A stays accepted; the
favorably reviewed cloud extraction is unchanged. No Stage C/D work began.

Repository: `/home/jon/Desktop/workshop/last_chessism`, branch
`refactor/p1-analysis-failure-propagation`. Entry was clean reviewed `62563a6`.
Authorization: `81f85ee1-b698-4f51-926d-6afe2536930a/Pasted text.txt`.
Implementation/test commit: `7783e54d573fd34135c39fc6f2aea90b51ebf63f`.

## Defect and correction

On the supported Python 3.10/3.11 runtimes, `asyncio.wait_for()` can return a
completed child's result when external cancellation arrives simultaneously.
Cleanup guarded only after catching cancellation cannot repair a cancellation
that was lost at that boundary. Secondary cancellation during cleanup could also
replace the original cancellation, or be swallowed and expose a retryable timeout.

The pre-correction PostgreSQL regression demonstrated the consequence: make the
HTTP response ready, cancel the analysis job without yielding, and the cancelled
job nevertheless commits one FEN score. Service regressions likewise reproduced
searching after cancelled slot acquisition and advancing to another search after
cancellation at search completion.

One 29-line function, `stockfish_core/deadlines.py::wait_with_timeout`, now owns
the child operation, waits with `asyncio.wait`, and returns results only through
normal completion. On deadline expiration or cancellation it cancels and drains
the child under shielding. Repeated cancellation cannot abandon cleanup. The
first external cancellation takes precedence over timeout/cleanup failure; genuine
deadline expiry without external cancellation still raises `asyncio.TimeoutError`.

The helper replaces all five Stage B `wait_for` boundaries: HTTP request budget,
engine startup, slot acquisition, position search and engine quit. Existing limits,
status classification, three HTTP attempts and 1s/2s backoff are unchanged.

Resource ownership remains explicit in the pool: it keeps the getter task and
returns a successfully dequeued slot exactly once if cancellation wins the
handoff. That request never starts a search. An untouched healthy slot is reusable;
an engine whose startup/search was interrupted is closed/discarded before release
and replaced on its next lease. Concurrent-waiter tests verify that no slot is
lost or duplicated.

`finish_engine_cleanup` accepts the original cancellation from initialization,
lease handling and endpoint finalization. A later cancellation cannot replace it.
Startup/lease cleanup no longer catches and suppresses a new external cancellation
as though it were an ordinary cleanup error. No scheduler, task registry, exception
hierarchy or cloud abstraction was introduced.

Production changes are limited to:

- `stockfish-batch-worker/stockfish_core/deadlines.py` — shared deadline operation.
- `chessism_api/operations/engine_client.py` — call that operation at the existing budget boundary.
- `stockfish-service/operations/engine.py` — startup/queue/quit waits, slot handoff and cancellation preservation.
- `stockfish-service/routers/analysis.py` — search wait and endpoint cleanup cancellation.

Tests: twelve new methods in `tests/test_engine_cancellation_races.py`, plus one
real PostgreSQL regression in `tests/check_analysis_execution_postgres.py`.

## Verification

Both reported races and the related startup/search/quit/cleanup cases were first
tested before production changes. All 777 tracked Python/SQL inputs matched the
reviewed commit then. Initial red results: 12 unit cases with 10 failures/2 errors
on Python 3.10, and 9 failures/2 errors/1 pass on Python 3.11. The PostgreSQL case
also failed before correction: `1 != 0` persisted scores after cancellation.

The first corrected run exposed a test-observation issue on Python 3.10: even a
plain cancelled `Event.wait()` task can lose `cancel(msg)` at the outer Task await.
A saved probe demonstrates the original reason inside the consumer and an empty
reason outside that task. Tests therefore still assert cancellation at the outer
await, and check the original reason inside the consuming coroutine. Assertions
were not removed or made conditional on Python version. The final tests were
rechecked against immutable read-only copies of all three changed production
modules from `62563a6`; their hashes and mount configuration are archived.

| Final evidence | Python 3.10.22 | Python 3.11.17 |
| --- | --- | --- |
| Same final 13 new regressions against reviewed code | 10 failures, 2 errors, 1 pass | 10 failures, 2 errors, 1 pass |
| Corrected focused gate: 13 new + 23 existing client/service checks | **36 passed** | **36 passed** |
| Existing P1 checks + Stage B PostgreSQL/Redis/ARQ execution | **61 passed** | Reused applicable unchanged evidence; new PG cancellation case included above |

All green gates have zero failures, errors or skips. Counts overlap. Existing
successful response, timeout, retry exhaustion, disconnect, repeated cancellation,
prior-commit preservation, selection failure, ARQ outcome and stale progress tests
remain passing. No existing assertion was weakened or test skipped. The 61-check
gate comprises 55 P1 checks and six real PostgreSQL execution checks.

The existing real Stockfish 16.1 smoke also passed with the corrected code: real
MultiPV search, mate/invalid serialization, cancellation, exited process, released
lease and subsequent process replacement. Six changed/new Python files compile;
whitespace checks pass. Both final runtimes tested identical inputs; all 779 final
Python/SQL hashes still match. Service score and progress code matches its prior
AST; canonical database code, analysis orchestration, database cleanup, worker
registration, retry policy, cloud transitions and frontend code remain unchanged.

All verification used network-isolated disposable PostgreSQL/Redis containers and
cached runtimes, read-only source, masked `.env`, synthetic settings and private
Unix sockets. An initial PG attempt was rejected by the existing isolation guard
because the temporary directory lacked its required `chessism-canonical-` prefix.
Only the temporary workspace name was corrected; the guard and fixture were not
weakened. That failed environment attempt and all earlier results remain archived.

Archive: [stage-b-cancellation-correction-2026-11-19.tar.gz](evidence/stage-b-cancellation-correction-2026-11-19.tar.gz).
SHA256: `32f4d72f5e00815edb24cb0f02d4f74bc8edf8fcb4d59002332334d4a1c42599`.
Contains runners, exact test selections/identities/results, logs, environment and
cleanup records, immutable baseline overlays, the runtime probe and source hashes.

## Scope and next action

Stage A, P1 #1 persistence/progress guarantees and the Stage B retry/cloud contracts
are preserved. The original Stage B broader comparison's unrelated failure/error
remain documented debt; no unrelated suite or cloud behavior was repaired or
rerun. Production/GCP and load performance remain unverified. The real-engine smoke
is small; deterministic tests provide the cancellation-race coverage.

All owned verification containers were removed and no verifier remains active.
Existing PostgreSQL, Redis, Neo4j, recovery backups and GCP were untouched. No push,
merge or deployment occurred. Every operation finished within five minutes.

Next: one short independent re-review of the cancellation correction and its
evidence. Do not repeat the full Stage B assessment or begin Stage C without
authorization. This checkpoint does not declare Stage B accepted.
