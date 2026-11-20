# Stage C — frontend state architecture

Implementation complete and verified; **independent acceptance pending**.
The initial independent review requested two corrections. Both are now implemented
and verified; see [the correction handoff](stage-c-corrections.md) for the current
state and focused re-review scope. Original implementation evidence below remains
as the baseline record.
Stage A and Stage B remain formally accepted. No Stage D work started.

- Repository: `/home/jon/Desktop/workshop/last_chessism`
- Branch: `refactor/p1-analysis-failure-propagation`
- Accepted baseline: `5c037d8ede820da423f9004941a90b874dfb7510`; initial worktree clean.
- Implementation: `d4e80765e1a1072491813916ac81408a43d80e4f`.
- Authorization: `270262d3-33bd-4c1e-9857-de1eed4f0d1b/Pasted text.txt`.

## Changes and responsibility boundaries

**C1 / P1 #3:** Existing `services/statusPolling.js` remains the polling transport:
one active request, abort/timeout, suppression of obsolete responses and bounded
backoff. Player Workspace's update/deletion loops and Download New Games' recovery
loop now reuse it through `watchGameJob` or `watchStatus`. A connection failure
changes the connection message, preserving the job ID and persistent tracking.
Only the API's matching 404 `job_id`/`not_found` payload counts as a missing job.
Automatic ingestion's unrelated-404 deletion was corrected too.

`services/jobOutcome.js` owns ARQ outcome interpretation shared by game jobs and
Positions. Boolean `result.success` determines execution outcome. ARQ `complete`
without that result remains unresolved; terminal Redis telemetry cannot complete
or fail a job. Both cancellation spellings in telemetry remain nonauthoritative.
Cloud workflow interpretation is unchanged and continues using durable outcomes.

`useTrackedGameJob` gives Main Characters' create/update jobs persistent identity,
request cleanup and terminal callbacks. It reuses the existing storage keys used
by Player Workspace and Download New Games, so navigation/remount restores work.
An enqueue acknowledgment arriving after unmount still saves its job identifier.
The update callback refreshes the matching player's data; deletion has its own
navigation action and cannot redirect a replacement player selection.

**C2 / P2 #6:** `usePlayerGameSelection` owns form/inspection/preview revisions,
abort controllers, and confirmation of the current frozen plan. Selection changes
invalidate earlier requests synchronously, including Alice → Bob → Alice. A
preview limit/order/date/scope change invalidates the plan. Batch size and cool-off
are execution settings, not frozen game-selection inputs; confirmation takes their
current values together with the valid plan. Confirmation consumes that plan once
before enqueue, and a late enqueue response cannot clear a newer preview. If
enqueue fails, the selection remains available to generate another preview.

`usePlayerPositionInspection` applies the same ownership rule to player FEN counts.
`useJobCompletionPresentation` owns completion sounds, visibility/focus handling
and dismissal timers. Its timers check job identity and clean up on unmount.
These are concrete lifecycle boundaries, not a new state-management framework.
The main Positions controller still coordinates page forms, summaries and jobs.

Positions discovery records which identities/outcomes were visible when a read
began. A delayed discovery cannot replace a newly queued/completed job or revive
a failure confirmed during that read. A subsequent explicit requeue after failure
still permits recovery, preserving the accepted P1 behavior. Individual reads
continue using the existing obsolete-response and progress-attempt checks.

**C3 / P2 #8:** Main Characters treats HTTP 202 as queue acknowledgment, tracks the
returned ID, and refreshes game lists/counts only after confirmed success. It no
longer classifies message text or announces success after enqueue. Counts decide
whether new games were saved; structured FEN status produces follow-up text.
List/count refreshes are abortable, so an older time-control request cannot replace
the selected mode. Add-player follows the same asynchronous completion rules.

Removed: two independent Player Workspace polling loops, the separate download
recovery loop, Main Characters message classifier and duplicate busy bookkeeping,
and the unused generic string formatter. Existing transport and cloud polling
were reused, not replaced. The Positions controller lost more than 300 lines;
most new code beyond focused lifecycle owners is regression coverage.

## Ingestion result contract

The public enqueue endpoints, HTTP 202 status, job IDs, queues, persistence writes
and transaction ownership are unchanged. Normal create/update worker returns now
contain:

```json
{
  "status": "complete",
  "player_name": "alice",
  "committed_games": 7,
  "zero_move_games": 2,
  "ingestion_run_id": "run-42",
  "fen_extraction": { "status": "queued", "job_id": "fen-42", "pending_games": 9 },
  "message": "Human-readable details"
}
```

This is nested in the existing `/jobs/{id}` envelope's `result.result`;
`result.success` remains ARQ's execution authority. `committed_games` counts newly
committed identities, including zero-move games; `zero_move_games` is a subset.
Duplicates and no-archive/no-new-game cases return zero. Counts are returned only
after transaction exit succeeds, never inferred from downloaded/parsed input.
FEN states are the existing `queued`, `already_active`, and `up_to_date` values;
`already_active` may have no job ID while the coordinator is being reserved.

Exceptions and cancellation still propagate. A failed ARQ job does not have a
successful ingestion-result object; absent counts are **unknown**, not zero. In
particular, a follow-up enqueue failure can occur after games committed. This does
not undo those commits, claim successful FEN extraction, or fabricate counts from
prose. Durable ingestion history/progress retain the existing failure/run details.
No new retry, scoring or persistence policy was introduced.

## Verification

All operations took less than five minutes and ran directly. No long-running
monitor or user handoff was needed. No tests were weakened, deleted or skipped.

| Gate | Baseline | Final |
|---|---:|---:|
| Frontend Node 22 tests | 346: 342 pass, 4 fail | 351: 347 pass, same 4 fail |
| Mounted React/Firefox checks | New controlled-request scenarios | 22 pass; zero runtime errors |
| Focused ingestion/backend checks | 52 pass | 60 pass |
| Build, page boundaries, source size | Existing scripts | Pass |
| Python syntax / whitespace | — | 4 compile checks / pass |

Zero new unexplained failures, errors, cancellations or skips. All existing test
identities remain. The entire four baseline failure blocks match, excluding only
runtime duration. Existing assertions were adapted only where the explicitly
changed ingestion return shape/progress contract or missing-job response required
it; new regressions cover those replacements.

Three new outcome/backoff regressions were also run against verified unchanged
baseline source overlays: all three failed, without skips. Green evidence covers
these same cases. The broader gate caught an omitted cancellation spelling and
failed-job requeue regression during implementation; both were corrected, with
existing tests preserved and an additional mounted stale-discovery/recovery check.

Mounted checks exercise actual Positions, Player Workspace, Main Characters and
Download New Games integration, not equivalent arithmetic or isolated helpers:
controlled promises, fake polling timers, replacement/unmount, restoration,
repeated errors, explicit missing jobs, contradictory outcomes, preview ABA races,
parameter changes, single frozen-plan confirmation, late enqueue acknowledgments,
deferred FEN extraction, cancellation and identity-safe dismissal/navigation.

Backend tests exercise actual importer/job/response-serializer functions with
isolated persistence/Redis doubles, including commit failure and counts after
transaction exit. No PostgreSQL or Stockfish suites were rerun: their code was not
changed. The import transaction's AST is identical to the accepted baseline.
Canonical database, analysis orchestration, engine client/service/policy, cloud
transitions, ARQ job-status router, enqueue router and worker registration are
unchanged. Accepted Stage A/B verification remains applicable.

Baseline source/test/script integrity: 555 hashes match `git show 5c037d8`.
Final frontend source/test/script integrity: 564 hashes match the reviewed files.
Backend hashes, exact test identities, failure details, commands and comparison
checks are included in the evidence archive.

### Unresolved baseline testing debt

1. `appearancePages.test.mjs`: registered target annotations stay in their owning page scopes.
2. `appearancePages.test.mjs`: individual home controls seed saved shared styles once, without relinking drafts.
3. `appearancePages.test.mjs`: game review owns its targets and preserves old styles without remaining linked.
4. `navigationAppearance.test.mjs`: Hegel page, document, navigation, asset alias and Markdown-only dependencies are removed.

These were reproduced on unchanged sources and not repaired in Stage C.

## Evidence, environment and limits

Archive: `evidence/stage-c-frontend-2026-11-19.tar.gz`.
SHA256: `47f9d46f9fb525143ed26105135347aef03a62b2fd534604b7795ed6f5737d15`.
Working evidence: `/tmp/chessism-stage-c-20261119/evidence`.

The host's initial offline npm setup could not use the existing incomplete lockfile
and cache. Verification used an isolated source copy, cached Chessism tooling and
public npm dependency installation there; repository package files stayed intact.
Final dependencies include React 18.3.1, Vite 5.4.21 and plugin-react 4.7.0. Native
Node 26/missing-asset attempts are retained as setup evidence; the comparable unit
gates use Node 22.23.2. Firefox uses a private temporary profile, and synthetic
fetch handlers prevent application API access. Test containers use no network and
remove themselves. Browser/Vite processes and profiles were cleaned up.

The build succeeds with Vite's chunk-size advisory. This stage does not tune bundle
sizes. Browser cases demonstrate component lifecycles with synthetic HTTP; they
are not production, GCP or full live-stack tests. Result expiry can yield an
explicit unknown/missing outcome; it does not justify claiming success. Existing
Stage A/B limitations and unrelated test debt remain separate from this stage.

No existing database, Redis queue, Neo4j data, recovery checkpoint, deployment or
GCP resource was modified. No push or merge. Only local implementation/evidence
commits were created. **Next: one independent Stage C acceptance review.**
