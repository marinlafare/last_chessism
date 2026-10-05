"""Bounded research calculations; no implicit worker imports.

Architecture
------------
routers/research_algorithms.py owns the superuser HTTP contract. This package
owns execution, never a general-purpose code editor or arbitrary SQL runner.

config.py: allowlisted operation, input columns and frozen matrix recipe.
inputs.py: read-only REPEATABLE READ source extraction via matrix_sql().
numerics.py: pure float64 streaming summaries, correlations and sampling.
jobs.py: extract -> calculate -> remove temporary inputs -> publish results.
repository.py: durable progress and cooperative cancellation.
storage.py: disk reserve, temporary workspaces and single-worker ownership.
worker.py: dedicated algorithms_queue; one job, separate from Stockfish.
backups.py: compact definition/completed-result integrity digests.

The browser page and its own API, state, charts and CSS live under
chessism_web/src/pages/research/algorithms/, composed by Algorithms.jsx.

Storage and reproducibility
---------------------------
AlgorithmDefinition.config freezes the matrix instructions and preparation.
AlgorithmRun.config freezes the complete instructions again for that run.
Completed results contain summaries, Pearson correlations, at most 1000
uniformly reservoir-sampled scatter points with row identifiers, row/missing
counts, source timestamp and checksum, seed and implementation version.
Definitions can be deleted without deleting historical run instructions.

Inputs are temporary float64 disk-backed arrays plus ordered row identifiers.
Normal success, error and cancellation remove them. On startup the sole worker
cleans only its matching run-* directories, fails interrupted running jobs and
requeues durable queued requests previously submitted by the user. It never
starts a new calculation merely because a page opened or a backup ran.

A run sees a consistent database snapshot, but rerunning later reads the live
database again. Fixed instructions and a seed do NOT promise identical inputs
after ingestion, salience or Stockfish updates. Pinning inputs is not implemented.
Rows are the recipe's ordered prefix, not a random population sample. Only the
display scatter is randomly sampled; summary/correlation statistics use every
included source row within the configured cap.

Limits
------
Version 1 supports Feature relationships on CPU NumPy, up to 1 million rows,
2-12 base numeric columns (plus optional rating_difference), chunks of 2048,
5 queued/running requests, a one-hour job timeout, 60-second source statements,
one worker CPU, 1 GiB worker RAM and a 20 GB local free-space reserve. Database
query work occurs on PostgreSQL and is not covered by the worker CPU quota.
Missing/non-finite values are excluded or explicitly mean-imputed. Categorical
codes are rejected. Constant-column correlations are undefined, not zero.
Summary statistics use original units; standardization affects the scatter.

Deployment and tests
--------------------
Build chessism-api and worker-backup, initialize additive tables through normal
API startup, then start worker-algorithms. Update worker-backup only when idle.
Do not restart analysis/ingestion/salience workers for this feature.
Tests: test_research_algorithms; opt-in test_research_algorithms_database uses
connection-local TEMP tables only. UI: npm run test:algorithms and
npm run test:algorithms:browser (isolated mock APIs in headless Firefox).
"""
