# Player salience pipeline

## Ownership

- `operations/ingestion_pipeline/game_repository.py` marks affected tracked
  players stale and records pending game keys in the ingestion transaction.
- `operations/player_salience.py` owns the queue, state, and chart/report APIs.
- `operations/player_salience_calculation.py` selects a full or incremental
  calculation and atomically publishes its results.
- `operations/salience_staging.py` shares the ordered-occurrence aggregation
  used for both full corpora and newly ingested games.
- `operations/player_move_salience.py` derives move-level details when requested.
- `database/models.py` defines state, pending work, repeated frequencies and
  game scores. `database/engine.py` handles legacy projection migrations;
  already-current schemas skip backfill scans and ALTER locks.

## Formula and storage

For a player's color-specific corpus, let `F` be the number of games containing
a FEN and `k` its chronological occurrence number inside this game:

```text
position_salience = 1 / (F * k)
depth_weight = 0.25 + 0.75 * min(ply / 16, 1)
game_salience = sum(depth_weight * position_salience) / sum(depth_weight)
```

Every appearance participates, including repetitions within a game. Both
players' moves contribute to the salience of a game from a tracked player's
perspective. A FEN is already canonical at ingestion; no reparsing is needed.

`player_position_frequency` stores only `F > 1`; absence means frequency one
within a ready corpus. `game_player_salience` stores scores and the numerator
and denominator needed for exact future updates. Move rows are derived, not
duplicated permanently. `player_salience_summary` is readiness/corpus state;
`player_salience_pending_game` is durable ingestion work.

## Work reduction

Full rebuilds group the occurrence window directly into game/FEN rows, avoiding
an additional occurrence-sized temporary write/read/delete. Their frequency
stage also stores only repeats. Indexed per-game lateral scans keep small
corpora from accidentally scanning the global position-appearance table.

An incremental update is used for positive pending counts up to the smaller of
5,000 games and 5% of the baseline (minimum one). It adjusts old numerators by
`weighted_mass(game,FEN) * (1/new_frequency - 1/old_frequency)` and inserts new
games. Repeated frequencies are upserted, avoiding delete/reinsert churn.

Incremental is not constant-time: finding affected old games still reads the
player's existing appearances. Common opening positions can legitimately
change many old scores. Avoid replacing this player-bounded scan with a global
join on opening FENs: that can visit millions of unrelated rows. No additional
per-appearance permanent cache or GPU dependency is introduced.

Publishing scores, frequencies and pending-work removal is transactional.
Multi-query readers use repeatable-read snapshots to avoid mixing old game
scores with new frequencies. Worker cancellations mark state failed/stale so
a timed-out calculation does not remain shown as running.

## Testing

```sh
python -m unittest tests.test_salience_migration
CHESSISM_DB_INTEGRATION=1 python -m unittest tests.test_salience_database
```

The integration test shadows all source and output tables with PostgreSQL
session-local fixtures. It compares repeated incremental updates with a full
rebuild and an independent Python formula, for both colors, singleton-to-repeat
transitions, intra-game repeats, and games beyond the depth-weight cap. It runs
no migrations and changes no production player scores.
