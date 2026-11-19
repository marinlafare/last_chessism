# Stage B — bounded analysis execution and cloud dependencies

Implemented and verified on 2026-11-19; **independent Stage B acceptance pending**.
Stage A remains formally accepted. No Stage C/D work is authorized or started.

Repository: `/home/jon/Desktop/workshop/last_chessism`, branch
`refactor/p1-analysis-failure-propagation`. Entry was clean accepted
`7d9fe8e97372603cccd2ec8d95de8a378203544c`.
Authorization: `da40c3b8-f712-44b7-bcd7-f885c7b44b66/Pasted text.txt`.

Local implementation commits:

- B1: `b8c59733c0c0c48ddb64ac9ff808a85df492251e` — engine execution and regressions.
- B2: `f07391418ba6b671573a55fa0a417f08ddb0f5d5` — shared cloud transitions and test seams.
- B3: this documentation/evidence checkpoint (see Git history).

## Execution policy and ownership

Previously, HTTP waits were indefinite, processing failures released their locks
and selected another batch indefinitely, and the service separately retried each
failed search. Queued and immediate analysis now call the same ordinary function
in `chessism_api/operations/engine_client.py`.

| Concern | Implemented contract |
| --- | --- |
| HTTP attempts | One initial attempt plus at most two retries; backoff 1s, then 2s. All attempts and backoff share one deadline. |
| Retryable HTTP | 408, 429, 502, 503, 504. Other statuses, including unexpected server 500, fail immediately. |
| Retryable transport | Connection/read/write errors, remote protocol errors and HTTPX timeouts. Cancellation and other request errors are not retried. |
| Invalid response | Malformed JSON, incomplete/mismatched FENs, missing or invalid scores fail immediately; no partial scored batch is committed. |
| Permanent position rejection | Immediate API preserves its `is_valid: false` envelope; queued persistence fails the job and rolls back its batch. |
| Connection/write/pool wait | HTTPX 5s limits; response read has a finite batch-derived bound. |
| Service limits | Queue 300s; engine startup 20s; each search 300s; engine quit 5s. A separate batch deadline includes queue and execution. |
| Request deadline | `300 + execution_seconds + 15`; execution is `300 + ceil(count * min(300, 2.1 * nodes/100000))`, capped so the request is at most 86,100s. A 1,000-position, 100,000-node batch has a 2,715s request deadline. |
| ARQ | Each of the four analysis functions explicitly registers a 24h deadline and `max_tries=1`; no queue-level multiplication of HTTP attempts. |

The 300s stall allowance, 2.1s/FEN estimate, 24h worker limit, and 20s/5s
startup/shutdown bounds come from existing application or cloud-engine behavior.
`stockfish_core/http_policy.py` shares these few constants/calculations between
the API and service; it adds no scheduler or configuration framework.

An important discovered discrepancy: installed ARQ has no `_job_timeout` enqueue
parameter. The old API supplied it as an ordinary function keyword instead of
setting the worker deadline. Those four analysis enqueue arguments and their
unused estimate helper are removed. The saved baseline records ARQ's signature;
the final tests inspect the actual production function registrations.

`operations/analysis.py` keeps the acquired session and row locks through all
HTTP attempts. A fatal acquisition/processing error stops admission before
rollback releases the locks, preventing sibling workers from reselecting the
failed position with a fresh retry budget. Existing cancellation-safe rollback,
close, sibling drainage and terminal progress handling remain in place. Earlier
committed batches stay committed; the unsuccessful job reaches ARQ as an error.
There is no permanent position quarantine or generic processing-retry loop.

The service performs one search per position per HTTP attempt. Timeout, engine
termination or cancellation discards the affected process before releasing its
lease. The next lease replaces it. Request disconnects cancel and drain the work;
repeated cancellation cannot cut short the guarded cleanup. Redis progress writes
have finite network timeouts. Existing attempt IDs and terminal-state guards are
unchanged, including rejection of late updates.

Raw White-relative scores, mate/tablebase conventions and serialization remain
unchanged. HTTP validation admits the full PostgreSQL INTEGER range and never
clamps scores. The immediate route has explicit min/max integer regressions.

## Cloud dependencies and architectural scope

`operations/cloud_analysis/transitions.py` now owns `save_run` and `save_job`.
The coordinator, backend controllers, sequence and cancellation helpers depend on
that module. Neither function's AST body changed: row locks, transaction ownership,
claims/receipts protections, verification/cleanup timestamps, completion checksums,
history, cancellation guards and terminal queue removal retain their behavior.
Tests patch the actual new owner; no forwarding aliases or interface layer remain.
An import-graph regression includes deferred imports and rejects paths from these
backends/helpers to the coordinator.

The cloud worker uses its existing direct engine execution, startup/shutdown,
configured run/stall bounds and durable recovery accounting. It does not call the
local HTTP policy. No worker algorithm, cloud retry accounting or deployment
configuration changed. Canonical database source files are byte-for-byte unchanged.

This keeps transport policy at the HTTP boundary, acquisition/commit ownership in
the analysis workflow, process ownership in the service pool, and shared durable
transitions outside the coordinator. Removed complexity consists of indefinite
HTTP waits, generic re-selection retries, nested service retries, invalid enqueue
timeout arguments and reverse coordinator imports. Ordinary functions suffice.

## Verification and baseline comparison

All checks used private disposable resources: PostgreSQL 15.18 and Redis 7 over
Unix sockets, no container network, no published ports, read-only source and a
masked `.env`. API tests used Python 3.10.22. The real-engine smoke used the cached
Python 3.11 service image and pinned Stockfish 16.1. No dependency installation or
live service was needed. Each operation took under five minutes.

| Evidence | Result |
| --- | --- |
| Unchanged accepted baseline | 174 checks; 1 failure, 1 error, 0 skips; all 769 recorded source/test hashes match `7d9fe8e`. |
| Integrated execution gate | 255 distinct checks passed, 0 failures/errors/skips, about 180s. Includes 145 real PostgreSQL checks: 5 new HTTP/ARQ persistence checks, 18 selection, 10 evaluation and 112 existing cloud persistence/recovery checks. Includes 55 existing P1 selection/job/ARQ/progress checks. |
| Final focused gate | 29 passed, 0 failures/errors/skips, about 3s: 13 client/job tests, 10 service tests, 1 dependency test and 5 PostgreSQL/Redis/ARQ tests. |
| Final relevant broader comparison | 209 checks; exactly the baseline's 1 failure and 1 error, 0 skips; no missing existing identities after two explicit contract-test renames. |
| Real Stockfish | 1 passed: real 100,000-node MultiPV search, mate/invalid envelopes, active-search cancellation, process exit, lease release and subsequent process replacement. |
| Static/integrity | 40 changed Python files compile; whitespace check passes; 777 final Python/SQL hashes have zero drift; moved transition bodies match accepted baseline ASTs. |

There are 30 new test methods overall, including the explicit real-engine test.
Counts above overlap; they must not be added as unique coverage. The final two
transport tests and the range-before-finiteness guard were added after the 255
gate; the 29 and 209 gates cover that final source. Cloud/persistence inputs did
not change afterward, so their longer gate was not repeated.

New integration tests check real lock ownership on every retry, preserved prior
commits and counters, ARQ success/failure records, real selection exceptions,
concurrent workers and cancellation during request/backoff. Service tests combine
the actual HTTP handler with the actual service route to assert three searches,
not nested retries, and cover disconnects, deadlines, partial initialization and
repeated cancellation during cleanup.

Unresolved baseline testing debt (full tracebacks preserved):

- `tests.test_cloud_auto_sequences.AutoSequenceTests.test_split_total_instead_of_multiplying_or_balancing`:
  `AssertionError: ValueError not raised` for `CloudJobRequest(backend='cloud_run', total_fens=200001)`.
- `tests.test_cloud_analysis.ControllerGateTests.test_real_cleanup_writes_tmp_plan_and_can_recreate_it_after_restart`:
  `AttributeError: 'types.SimpleNamespace' object has no attribute 'job_id'` in
  `bind(StageTimings(), run.job_id)`.

Failure identities and final exception messages match exactly. The broader suite
therefore exits 1; it is not reported as wholly green. No tests were skipped or
weakened. The old generic-retry test now explicitly requires failure/rollback/close
without reselection; the old disabled-timeout assertion now requires finite bounds.
Cloud assertion behavior is unchanged; patch targets follow the extracted owner.

Earlier attempts are retained: initial baseline missed a test-support path; the
first B1 run exposed the intentionally obsolete retry assertion; B12 exposed one
missed cloud mock owner; an integration launcher preparation error ran no tests.
Each was corrected and its affected behavior subsequently verified. No evidence
was silently discarded.

Archive: [stage-b-execution-2026-11-19.tar.gz](evidence/stage-b-execution-2026-11-19.tar.gz)
(runners, selections, logs, structured identities/results, image IDs, hashes,
baseline comparison and resource cleanup).
SHA256: `a1362793c103c1fdb70daf549b127c10d1faba05f447187e25c8d2fc7a5e0c9e`.

## Limits and next action

Production/GCP execution and large loaded Stockfish throughput were not exercised.
The real-engine test is a small isolated smoke, while failure timing is covered by
deterministic tests. Existing timeout estimates may need measured tuning under
real load; finite failure and cleanup semantics are intentional. Unchanged frontend
authoritative outcomes and cloud-worker algorithms reuse accepted Stage A evidence
rather than repeating broad suites. The prior documented Redis-reset visibility
limitation and unrelated testing debt remain separate from this stage.

All Stage B containers were removed; no verification process remains active.
Original PostgreSQL/Neo4j data, Redis queues, recovery backups and GCP were untouched.
No push, merge or deployment occurred.

Next action: one independent acceptance review of B1–B3, starting with the two
implementation commits, this handoff and the evidence archive. Do not begin Stage C.
