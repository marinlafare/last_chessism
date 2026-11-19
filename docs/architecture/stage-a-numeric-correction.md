# Stage A numeric correction — independent re-review pending

Reviewed baseline: `e02d47a623dbda63bcb76c11b12473891f37eb2b`.
Branch: `refactor/p1-analysis-failure-propagation`. Date: 2026-11-19.
Scope: the two PostgreSQL arithmetic overflow paths identified by the Stage A
review. No new stage, schema change, public contract change or scoring formula.

## Verified score semantics

- `stockfish-service/operations/engine.py:convert_to_serializable` calls
  `PovScore.white().score(mate_score=10000)`. Stored ordinary scores are signed,
  White-relative integer centipawns. A mate uses signed `10000 - distance`;
  examples: mate in two `9998`, being mated in two `-9998`, terminal mate ±10000.
- `chessism_api/operations/tablebase_probe.py:_tablebase_score_payload` converts
  decisive White wins/losses to +1000/-1000, draws (including cursed wins and
  blessed losses under the fifty-move rule) to zero. This is a tablebase outcome
  encoding, not an ordinary CP estimate. Probability WDL uses White's perspective;
  `tablebase_wdl` and `tablebase_dtz` retain the raw side-to-move probe values.
  Source, timestamp and those fields distinguish the result from CP or mate.
- Canonical `fen.score` and `evaluations.validate_result` accept the full signed
  INTEGER range. Its extrema are permissible raw values, including unattributed
  restored values; they are not defined dedicated mate/sentinel encodings.
  `mate_distance` decodes only attributed Stockfish values with magnitude
  9000–10000. Verified evaluation typing calls unattributed values `unknown`.
- `accuracy.py`, SQL engine summaries and CUDA accuracy already bound the
  numbers used for win probability to ±1000. Raw storage, page aggregates and CP
  deltas are not bounded. Engine summaries apply the player's sign at read time.

Existing semantic difference retained explicitly: legacy accuracy classifies
tablebase by source first, then uses magnitude >=9000 as its mate heuristic even
for unattributed values. It also saturates encoded mate/tablebase numbers for
win probability. Their classifications remain separate; they do not become CP
gain/loss or final CP. Verified evaluation typing and mate-distance decoding are
stricter. This correction preserves those established formulas and does not
declare extreme unknown values genuine mates. Changing that policy would be a
separate scoring decision, not an overflow remedy.

## Exact correction and boundaries

`game_analysis_reads.page_sql`: change both `ABS(f.score)` operands to
`ABS(f.score::bigint)`; retain AVG/MAX and their `float8` output casts.

`engine_summary.VALUES_SQL`: cast both branches of player orientation to bigint
before negating Black's score, and change classification to
`ABS(fen.score::bigint)`. This matches the existing matrix readers' widening.
Both INTEGER extrema and their negations fit exactly in bigint.

Only four application lines change. No import, interface, transaction, locking,
eligibility, publication, schema, normalization or formula changes. Arithmetic
representation stays in the PostgreSQL adapters (Clean Architecture); direct
casts solve the defect without another scoring abstraction (Kent Beck).

## Regression evidence

`tests/check_canonical_score_bounds_postgres.py` adds five tests, with parameterized
cases, against actual page queries and `refresh_engine_summaries`:

- INTEGER min/max, zero, ±300, ±1000, ±1500 and mate encodings; real two-page
  pagination; exact raw AVG/ABS/MAX values and float result types.
- Both White and Black summaries, including self-play identities; scalar accuracy
  expectations and integer count/float metric contracts.
- All seven requested CP normalization pairs. SQL probability-derived metrics
  match the existing scalar formula using bounded inputs, while stored +1500
  stays +1500 and raw CP deltas/final CP retain their magnitude.
- Signed mate distances, terminal mates and separate tablebase metadata; no CP
  deltas or final CP for these kinds. Exact raw rows remain unchanged after reads
  and refresh, and session pools have no checked-out connections afterward.

**Red:** unchanged reviewed application sources, five tests, two errors:
`test_paginated_raw_aggregates_accept_full_integer_range` and
`test_summary_extrema_preserve_both_perspectives_and_legacy_formula`, each at
`score=-2147483648`, with `asyncpg.exceptions.NumericValueOutOfRangeError:
integer out of range`. No assertion failures or skips. Other subcases passed.

**Green:** 64 tests, zero failures/errors/skips; 20.15 seconds in unittest,
23.26 seconds including isolated container startup and cleanup:

| Coverage | Tests |
|---|---:|
| New numeric PostgreSQL regressions | 5 |
| Existing canonical analysis views | 18 |
| Existing canonical recovery/transaction/cancellation checks | 20 |
| Existing matrix-specific PostgreSQL checks, including extrema | 6 |
| CUDA accuracy formula with NumPy test backend | 3 |
| Evaluation contracts and writer validation | 6 |
| Stockfish serialization, tablebase representation and accuracy helpers | 6 |

PostgreSQL 15.18 and Python 3.10 use cached image identities recorded in the
evidence. Each test creates and drops its own synthetic database. The server has
no TCP listener, published ports or network, with its data on disposable tmpfs.
The application source is mounted read-only and `.env` is masked; authentication
values are synthetic. Owned containers were gracefully stopped and removed.

One initial green attempt stopped before test execution because importing the
existing scoring tests required missing dummy authentication settings. Its log
and cleanup evidence are retained separately. Supplying those synthetic settings
resolved the runner issue. No application code or test assertion was weakened.
The final helper selection is restricted to scoring/serialization tests.

Lightweight checks passed: Python compilation, `git diff --check`, reviewed-source
hash match for red, no source/test hash drift after green, and identical Python
AST structure outside string literals in the two changed application modules.
No new imports, boundary dependencies or abstractions were introduced.

## Evidence and limits

Archive: `evidence/stage-a-numeric-correction-2026-11-19.tar.gz`. It contains test
identities, full red/green output, environment-attempt output, source/test hashes,
runtime/container identities, cleanup records, lightweight checks and the exact
isolated runner. SHA256 is recorded in `refactor-manifest.json` and `refactor-log.md`.

No full backend/frontend rerun; prior unrelated debt remains as documented in
the Stage A handoff. This gate does not claim real Stockfish engine, Syzygy file,
CUDA hardware, GCP, production performance or deployment verification. Existing
databases, recovery checkpoint, Redis and Neo4j were neither read nor changed.

Stop here. The correction is verified locally; Stage A is **not yet accepted**.
Next action: independent re-review of this numeric finding and its evidence.

Local correction/evidence commit: `c13f07d3edaa228dd864f721d6622f88b71280aa`.
