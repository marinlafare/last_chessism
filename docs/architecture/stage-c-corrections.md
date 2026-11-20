# Stage C — final correctness corrections

Both authorized corrections are implemented and verified. **Stage C is not yet
accepted.** Next action: a focused independent re-review of these two findings.
No Stage D work was started.

Repository: `/home/jon/Desktop/workshop/last_chessism`.
Branch: `refactor/p1-analysis-failure-propagation`.
Reviewed baseline: `0f3e1057570fbefba622e7371233bfc269454b6f` (initial worktree clean).
Authorization: attachment `761b6345-124c-4345-b398-e1592c72b217/Pasted text.txt`.

Local implementation commits:

- `a6fa760` — Reject incomplete archive downloads before ingestion.
- `2675b00` — Observe tracked job acknowledgments across navigation.

## A: incomplete archive coverage

Root cause: monthly HTTP errors, invalid JSON and missing payloads became `None`,
then disappeared from the downloader's result. The coordinator could interpret an
empty/subset result as successful exhaustion. Zero committed games subsequently
became Main Characters' “all games ready.”

The monthly HTTP adapter now raises on unsuccessful status, transport failure,
empty body after the existing retry, and malformed archive shape. `games: []`
is a valid successful empty archive; absence/null/non-list `games` is not. The
serial downloader retains unsuccessful month details while checking the remaining
requested months. Incomplete coverage raises `RuntimeError` with unsuccessful
months and downloaded/requested count, before returning anything to the importer.
Progress now says “Checked archive” instead of claiming each request downloaded.

**Chosen policy:** this attempt imports only after all requested archives download
successfully. A mixed response does not import its successful subset. Existing
commits from earlier attempts remain intact. This uses the existing transaction
boundary: downloads already preceded the import transaction. No per-archive
transactions, partial-success status, retry policy or persistence redesign.

The existing coordinator propagates the exception to ARQ. The existing API
serializer returns `result.success: false` and an exception `type`/`message`;
coverage details survive in that message. The frontend uses the boolean outcome,
not message parsing. It displays failure and leaves retry available, without a
successful refresh or “all games ready.” Failed results do not fabricate
`committed_games: 0`; absent counts retain the existing unknown/not-applicable
meaning. Successful imports retain exact post-commit counts, run identity and
separate queued/already-active/up-to-date FEN status. Follow-up enqueue failure
still fails ARQ without rolling back previously committed ingestion.

Production changes: `ingestion_pipeline/chesscom.py`, and the single progress
wording change in `ingestion_pipeline/jobs.py`. The importer, transactions,
serializer and outcome interpreter are unchanged.

## B: observable tracked-job ownership

Root cause: components read localStorage only on initialization. A producer that
unmounted could save a late acknowledgment after its replacement initialized,
without notifying that replacement. Response arrival order could also overwrite
newer tracking or revive completed work.

`gameJobService` now owns small per-key subscriber sets and enqueue ownership
identities. All enqueue producers obtain an identity **before** awaiting HTTP.
Publication requires that identity still own the request; a newer request or
accepted storage change invalidates older acknowledgments. Terminal clearing
checks the expected job ID, so an old callback cannot remove replacement work.
Writes/removals finish before synchronous same-document notifications. Browser
storage events also notify mounted subscribers of cross-document changes.

`useStoredGameJob` uses React's existing `useSyncExternalStore` with stable
serialized snapshots. Its subscription/snapshot recheck covers changes between
rendering and subscription; unmount releases listeners. `useTrackedGameJob`,
Player Workspace and Download New Games consume this same boundary. Main
Characters, Player Workspace and Download New Games all publish with request
ownership. A still-owned late acknowledgment persists even after its originating
component unmounts; the new mounted consumer adopts and polls it.

Polling callbacks check current persistent identity before applying status. Shared
transport/backoff, explicit missing-job handling and ARQ authority are unchanged.
Deletion navigation and matching-player refresh retain their separate actions.
Download busy state follows the tracked identity, preventing a preceding job's
terminal presentation from making newly adopted work appear idle. No global
state framework, generic event bus or new polling loop was introduced.

Frontend production files changed:

- `services/gameJobService.js`
- `services/useStoredGameJob.js` (new)
- `services/useTrackedGameJob.js`
- `pages/MainCharacters.jsx`
- `pages/players/PlayerWorkspace.jsx`
- `pages/DownloadNewGames.jsx`

Tests: new `tests/test_archive_download_outcomes.py`, new
`chessism_web/tests/gameJobStorage.test.mjs`, and expanded
`chessism_web/tests/fixtures/stageCFixture.jsx`. Tracking and this evidence handoff
are the only additional changes.

## Verification and baseline comparison

The reviewed Stage C evidence was reused after all 564 frontend and four backend
source hashes matched. Repository identity and accepted A/B ancestry were checked.

| Gate | Result |
|---|---|
| Backend red, unchanged application code | 10 methods; six reproduce defects, four preservation methods pass |
| Backend green | 70/70 passed, zero failures/errors/skips |
| Mounted React red, unchanged tracking code | 30 checks: six fail, 24 pass, zero runtime errors |
| Mounted React final | 33/33 passed, zero runtime errors |
| Frontend baseline | 351 tests: 347 pass, four known failures |
| Frontend final | 357 tests: 353 pass, identical four failures; zero cancellations/skips |
| Build, page boundaries, source size | Passed |
| Python syntax, whitespace, integrity | Passed; 713 frontend source/test/script/public hashes match |

Backend red has nine assertion-failure entries and one malformed-payload error
because one of the six failing methods contains subtests. The ten new methods
exercise actual downloader, JSON handling, parsing, importer, coordinator and
response serialization, with HTTP MockTransport and a transactional in-memory
persistence double. Cases include HTTP 503/404, transport errors, mixed coverage,
malformed responses, valid empty archives, new games, duplicates, earlier commits
surviving a later failure, and cancellation. Existing result-contract and ingestion
checks remain green. This is not a new real-PostgreSQL verification claim.

Mounted regressions use actual React components/hooks and shared localStorage.
They cover acknowledgment before remount and after replacement mount across all
three pages, callbacks from all enqueue producers, older acknowledgments after
newer active/completed work, render/subscription timing, transient polling errors,
confirmed completion, listener/timer cleanup, and incomplete-ingestion UI. The
six new storage unit checks also cover persist-before-notify, ownership, guarded
clearing, unsubscribing and observed cross-document updates. Counts overlap with
the frontend suite; they must not be added as independent gates.

All baseline frontend identities remain. The four complete failure blocks match
exactly except duration:

1. registered target annotations stay in their owning page scopes
2. individual home controls seed saved shared styles once, without relinking drafts
3. game review owns its targets and preserves old styles without remaining linked
4. Hegel page, document, navigation, asset alias and Markdown-only dependencies are removed

These remain unresolved appearance/navigation testing debt. No tests were removed,
weakened or skipped. An initial isolated frontend setup lacked public engine assets
and the container could not resolve a cached dependency symlink. That run is
retained separately; copying unchanged public assets and mounting cached modules
read-only restored the exact baseline comparison without changing tests.

## Preservation, evidence and limitations

Canonical PostgreSQL sources, importer transaction code, ARQ serializer, Redis
attempt isolation, bounded engine execution, cancellation cleanup, cloud durable
transitions, scoring, Positions selection revisions and completion presentation
remain unchanged. Accepted Stage A/B evidence remains applicable. No unrelated
PostgreSQL, Stockfish or cloud suites were rerun.

Evidence: `evidence/stage-c-corrections-2026-11-19.tar.gz`.
SHA256: `ed8ee6ff68d97039216e11b46cd9c49d5f50e25e54e827953ca39ba40eb5e1da`.
Workspace: `/tmp/chessism-stage-c-corrections-20261119`.
Archive includes red/green results, exact failure comparison, commands/runners,
source hashes, implementation diff and cleanup record. Backend used the cached
immutable Python 3.10 API image, offline network, read-only repository and masked
`.env`; all database/Redis dependencies were synthetic. Comparable frontend tests
used cached Node 22.23.2; mounted checks used isolated Firefox and controlled HTTP.

No production/GCP or complete live-stack integration was exercised. Build retains
the existing chunk-size advisory. Cross-document events refresh mounted readers;
this correction does not add cross-tab enqueue arbitration or server scheduling.
Incomplete attempts may need to download successful months again on retry; this
is the deliberate small, complete-coverage policy, without new retry machinery.

All owned test containers exited with `--rm`; browser/Vite children and private
profiles were removed. No existing database, Redis, Neo4j, backup, recovery
checkpoint, GCP or deployment resource was accessed or changed. No push/merge.
Every gate took under five minutes and ran directly. Stop for the authorized
focused independent re-review; do not declare Stage C accepted.
