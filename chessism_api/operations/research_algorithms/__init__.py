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
builder_schema.py: v2 typed acyclic step graph, output and resource contracts.
expressions.py: bounded AST math interpreter (no eval/exec/attribute access).
builder_inputs.py: consistent read-only extraction of numeric/text columns.
builder_frames.py: vectorized filters, formulas, transforms, grouping and sorting.
builder_outputs.py: independently selected table/chart/scalar output adapters.
builder_runner.py: graph scheduling, last-consumer frame release, step previews.

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
V2 definitions also freeze ordered steps, independent outputs, revision number
and parent definition ID. Saving an edit inserts a NEW immutable definition;
it never overwrites a historical definition. No schema migration is needed:
both definition and run contracts were already JSON, and backup version 5
fingerprints their complete content, including new steps/results/revisions.

Inputs are temporary float64 disk-backed arrays plus ordered row identifiers.
V2 uses bounded temporary typed arrays in worker RAM instead of disk-backed
arrays. Text values are retained as interned grouping keys, never category
codes used as numeric measurements. Both execution paths discard inputs.
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

Version 2 supports a single input matrix, 1-12 typed source columns, 100,000
source rows, 20 acyclic steps, 64 columns per frame, 10,000 groups and 6 outputs.
It rejects text values longer than 256 characters or more than 10,000 categories
per input column. Retained intermediate arrays have a 256 MiB allowance under
the same 1 GiB worker limit. References to prior steps allow branching; frames
are released after the last consuming step, with outputs captured immediately.
Numerical reductions operate on ALL selected rows, not the display sample.
Table displays cap at 100 rows, scatter at 1000 seeded random points, line/bar
at 200 prefix points, and intermediate previews at 8 rows. Truncation is explicit.
Sample runs use the same worker/config with a cap of 500 source rows; they save
run history, not a definition, and are marked sample_run. They are not population
estimates. Date helpers use UTC epoch seconds. Weighted means require nonnegative
weights; zero weight totals and empty/constant correlations produce N/A.
No arbitrary Python, joins between matrices, CUDA or TensorFlow execution yet.

Deployment and tests
--------------------
For an initial v1 installation, build chessism-api and worker-backup, initialize
additive tables through normal API startup, then start worker-algorithms.
For the v2 builder update, rebuild chessism-api and recreate only the API and
the idle worker-algorithms. No schema migration or backup-worker restart is
needed when version 5 backup support is already installed.
Do not restart analysis/ingestion/salience workers for this feature.
Tests: test_research_algorithms and test_algorithm_builder cover the old
calculation and new interpreter/graph. Opt-in test_research_algorithms_database
uses connection-local TEMP tables only. UI: npm run test:algorithms and
npm run test:algorithms:browser (isolated mock APIs in headless Firefox).
"""
