# Frontend source map

`App.jsx` owns authentication and route selection. Route entry files live in `pages/`.
Small pages stay self-contained; pages with substantial state or markup use a feature folder:

- `pages/games/`: Games controller, view, charts, API adapters, and transformations.
- `pages/positions/`: analysis-job controller, view, progress components, and job helpers.
- `components/layout/`: application shell shared by routes.
- `components/home/`: Home-page sections.
- `services/`: shared HTTP, job, and status access.
- `utils/`: presentation-independent helpers.
- `styles/`: ordered CSS sections; `index.css` documents their cascade order.

For complex pages, keep network/state coordination in `use*Page.js`, markup in `*View.jsx`,
and pure helpers or small page-specific components beside them. `npm run check:source-size`
enforces the 1,000-line limit for JavaScript, TypeScript, JSX, TSX, and CSS source files.

## Shared progress lifecycle

`services/statusPolling.js` owns serial requests, backoff, cancellation and
optional timeouts/visibility suspension. `watchStatus` starts a watcher and returns
its cleanup; `createStatusPolling` exposes explicit refresh/invalidate/stop for UI
actions. Callbacks can return `false` only when their workflow confirms terminal
status. A timeout is a transport failure, never a failed server job. Cancellation
also settles reads whose transport ignores abort; late responses stay obsolete.

`hooks/usePoll.js`, ingestion's `gameJobPolling.js`, cloud's `cloudStatusPolling.js`
and local analysis's `usePositionStatusPolling.js` share that lifecycle. Their
polling intervals and completion contracts remain separate. Automatic ingestion,
local analysis and cloud status opt into 10-second deadlines and hidden-tab
suspension. Returning immediately reads current progress; explicit action
refreshes supersede older reads even while hidden. Cached data never advances
the last-success timestamp. Other `usePoll` consumers keep their existing defaults.

Local analysis keeps status requests independent of database-summary refreshes.
Discovery cannot overwrite newer job checkpoints or revive completed work. Only
an explicit matching `not_found` response removes a missing tracked job; an
unrelated proxy 404 retains progress and retries.

`components/progress/ProgressMeter.jsx` shares bounded geometry and accessibility,
not workflow state or styles. Unknown values never fabricate a percentage; 100%
saved does not declare cloud cleanup complete. Existing DOM classes, native
progress elements and page-specific Edit styles targets remain independent.

Completed ingestion results remain under `useIngestionReview` until explicitly
dismissed; cloud retention stays under `useCloudJobDisplay`. They share only the
`ProgressReviewButton` presentation, not storage schemas or terminal rules. Local
analysis's existing acknowledgement/fade policy is unchanged. Navigation never
submits, stops or resumes a backend job, and existing storage keys are preserved.

`npm run test:progress` checks contracts using fake clocks and responses.
`npm run test:progress:browser` covers real React hooks, counters, unknown totals,
timeouts, stale replies, completion, stop confirmation, independent Edit styles
and mobile layouts in isolated Firefox profiles. Every API request is mocked;
these checks do not contact live data or cloud jobs. The ingestion browser script
also accepts `--local-only` for the local-analysis regression.
