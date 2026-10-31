# Player hero analytics API

These authenticated endpoints exclusively support the analytics workspace on
the player hero page. Their backend implementation lives in the
`player_hero_analytics.py` and `player_hero_efficiency.py` operation modules;
the page-owned API client lives in
`chessism_web/src/pages/players/playerAnalysisApi.js`.

Unless stated otherwise, aggregate endpoints accept `mode=all|bullet|blitz|rapid` and optional
`date_from` and `date_to`. Behavioral endpoints automatically resolve the
player's local time. Engine-measure endpoints may additionally accept an
explicit IANA `timezone`. Without an explicit timezone, the backend resolves
time in this order:

1. a stored player timezone;
2. a confidently matched timezone from the player's location;
3. the geographical center of the country's IANA timezones;
4. UTC when no timezone can be inferred.

Country zones are ordered west-to-east. An odd count uses the middle zone; an
even count uses the arithmetic mean of the two middle zones. Every response
contains `time_context`, which states the selected zones, source, and whether
the result is estimated or UTC-based.

## Playing Patterns

The hero mounts Results and Rating distribution first, before the Rating,
Hours, Days, Opponents and Opening repertoire disclosures. Loading behavioral
data keeps that layout stable; the rating distribution loads independently.

## Unexpected endings

`GET /players/{player}/analysis/measures/unexpected-endings`
uses the existing authenticated player API. It returns all three modes in
`by_mode` (`bullet`, `blitz`, `rapid`) so mode toggles need no additional request.
The boundary is fixed at 200 cp; 100 cp is one pawn. The optional legacy
`threshold_cp` parameter accepts only 200. There is no date filter.
Version 2 returns these four ending keys in each mode:

- `lost_on_time`: recorded loss on time.
- `won_on_time`: recorded win on time.
- `lost_by_resignation`: recorded loss by resignation.
- `won_by_resignation`: recorded win by resignation.

Each entry has `eligible_games` (games with this result and ending),
`assessable_games` (those with a final centipawn evaluation),
`ahead_games`, `balanced_games`, `behind_games`,
`missing_final_position`, `missing_final_score`, `mate_games` and `tablebase_games`.
`score_bands` describes the disjoint ranges: **Ahead > +200**, **Balanced
−200 through +200 inclusive**, **Behind < −200**. Zero and both boundary values
belong to Balanced. Band counts sum to `assessable_games`.

The UI shows two 100% horizontal stacked-bar charts: Time endings and
Resignations, each with a Lost and a Won bar. Each segment's percentage is
`100 * band_games / assessable_games`. The assessed count is shown to the right;
the recorded total and exclusions are available by hovering that count, without
coverage paragraphs below the charts.
When the denominator is zero, show an empty bar, not a 100% balanced bar.
For combined modes sum counts before dividing. Missing, mate and tablebase
counts are mutually exclusive exclusions, not balanced scores or ordinary
centipawn values. Both charts share the mode controls and one cached request.

The losing side's recorded reason determines the ending. Abandonment, unknown
results and timeout draws do not qualify. Winning/losing describes the engine
evaluation, not a guaranteed outcome or a claim that a player knew the score.

The query checks the **last recorded ply**, requiring the last move number to
match `game.n_moves` and an unambiguous association for that move and mover.
It then reads that FEN's stored evaluation, converting White's score into the
selected player's perspective. It never substitutes an earlier scored move,
and does not require the rest of the game to be scored. Games without a
verifiable final position are reported as missing coverage.

The UI loads this only when Unexpected endings is opened. Responses have a
60-second Redis cache, `generated_at`, `max_age_seconds` and `cache_hit`.
Database reads are read-only with a 15-second statement timeout, 2-second lock
timeout, and at most two concurrent reads per API process. No new tables,
engine calculations, ingestion changes or cloud tasks are involved.

Click-through reuses `POST /players/{player}/analysis/measures/explore-games`:

```json
{
  "scope": "unexpected_endings",
  "ending": "lost_by_resignation",
  "score_band": "balanced",
  "modes": ["bullet", "blitz", "rapid"],
  "analyzed_only": false,
  "limit": 30,
  "cursor": null
}
```

The same classification applies to both counts and game lists. `analyzed_only`
must be false because full-game coverage is not required. Other chart filters
cannot be combined with this scope. Games are ordered by start date descending,
then game ID and player side, with unknown dates last. `final_player_cp` is
included on each game; cursor identity includes the version, threshold, modes,
player, ending and score band. Version-1 cursors are rejected, and clients must
reload the list. The four original selection names remain accepted as aliases
for their corresponding ending and Ahead/Behind band at the new fixed boundary.
Live scores may change between cached counts and subsequent pages.
Invalid filters return 422, missing players 404, and busy/timed-out reads 503
with `Retry-After: 3`. A timeout never returns partial counts as complete.

## Playing-pattern endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /players/{player}/analysis/behavioural/activity` | Dense weekday, hour, and weekday×hour game proportions plus absolute wins, draws, and losses. The response also includes `by_mode` breakdowns for independent Bullet, Blitz, and Rapid chart filters without additional requests. |
| `GET /players/{player}/analysis/behavioural/ratings` | Dense daily last-rating series, kept separate for Bullet, Blitz, and Rapid. Days without games contain `last_rating: null`. |
| `GET /players/{player}/analysis/behavioural/days/{date}` | All rating observations and absolute W/D/L totals in 24 resolved-local-time hourly buckets for one local date. |

### Opponents and opening repertoire

`GET /players/{player}/analysis/behavioural/catalog` accepts
`kind=opponents|openings`, `mode=all|bullet|blitz|rapid`,
`color=all|white|black`, `page` (default 1), `limit` (default 10, maximum 50),
and optional `search` (at most 128 characters). This endpoint does not accept
date filters. Rows contain `rank`, `key`, `games`, `wins`, `draws`, `losses`,
and `unknown`; the response also contains totals and pagination metadata.

Opponent rows additionally contain `latest_ratings`, keyed by the selected
mode, or all three modes when `mode=all`. Each value is the opponent's rating
in their most recent dated game recorded in Chessism for that mode, across
all opponents—not their average rating or only games against the viewed
player. Ties use the descending game ID and then the Black perspective for
self-play. A missing or nonpositive rating is returned as `null`; the UI
shows a dash, not zero. These are recorded game ratings, not a live rating
fetched from Chess.com.

Ratings are enriched after pagination in one read-only batch for the visible
opponents, using the existing per-side player/mode/date indexes on normalized
storage. The result shares the catalog's 60-second cache, read semaphore,
and statement/lock timeouts. Opening rows do not perform this lookup.
The frontend also accepts older responses without `latest_ratings`, hiding
the ratings until the matching backend release is deployed.

## Engine Measures

### Score-distribution position browsing

`POST /players/{player}/analysis/measures/score-distribution/positions` accepts
`bin_id` (0–12), `modes`, the histogram `snapshot`, and an optional opaque
`cursor`. It returns up to ten distinct FENs in `rows`, `next_cursor`, and the
list's `snapshot`. Subsequent requests must use that returned snapshot.
Rows are sorted by appearances descending, then numeric FEN ID ascending,
within the selected player's games, modes and player-perspective score bin.
The ±50 cp central interval is inclusive. Unscored, mate and tablebase
evaluations are excluded. The UI appends pages near the bottom of the list;
existing rows remain available while the next page loads or fails.

The query counts narrow FEN/side identities first, reads candidate scores in
bounded batches, and loads full FEN strings only for the returned ten rows.
It does not join every occurrence to its score, count a separate total of
matching FENs, or calculate another histogram. Exact frequency ordering still
requires an occurrence aggregation on an uncached page. Page caching and a
separate single-reader slot prevent repeat clicks from duplicating work or
waiting behind a histogram refresh. Reads remain repeatable-read/read-only,
with statement, lock and overall deadlines. A changed source on an uncached
continuation returns HTTP 409 rather than mixing rankings; retained pages can
still be read. Old cursor versions also return 409 with a restart instruction.

### Score semantics

Stockfish scores are stored from White's perspective. These endpoints invert
the sign when the selected player was Black. For consecutive centipawn scores:

- `cp_change = current_player_score - previous_player_score`;
- `cp_gain = max(cp_change, 0)`;
- `cp_loss = max(-cp_change, 0)`.

Thus a player-perspective change from `+20` to `+80` is `cp_change: +60`,
`cp_gain: 60`, and `cp_loss: 0`. Mate and tablebase sentinel values are kept
out of centipawn arithmetic and reported separately.

Blunders copy Lichess's current classifier. Centipawns are converted to
Lichess winning chances after capping CP at `[-1000, 1000]`:

`2 / (1 + exp(-0.00368208 * cp)) - 1`.

A CP-to-CP player move is a blunder when it loses at least `0.30` on that
`[-1, 1]` scale. Forced-mate creation/loss uses Lichess's separate mate
boundaries. Every comparison is made after orienting the score toward the
player, so increasingly negative White scores are improvements for Black.
The implementation follows Lichess's
[`Advice.scala`](https://github.com/lichess-org/lila/blob/master/modules/tree/src/main/Advice.scala)
and the winning-chance formula in
[`eval.scala`](https://github.com/lichess-org/scalachess/blob/master/core/src/main/scala/eval.scala).
Game efficiency follows Lichess's
[`AccuracyPercent.scala`](https://github.com/lichess-org/lila/blob/master/modules/analyse/src/main/AccuracyPercent.scala).

| Endpoint | Purpose |
| --- | --- |
| `GET /players/{player}/analysis/measures/daily-efficiency` | Lightweight local-date series containing exactly `[date_game_init, game_efficiency]`. Each value is the mean 0–100 game efficiency for the selected comma-separated modes, so days remain comparable regardless of game volume. |
| `GET /players/{player}/analysis/measures/daily-game-cp` | Lightweight local-date series containing exactly `[date_game_init, game_cp]` per active day. `game_cp` is the sum of `own_move_cp_gain - own_move_cp_loss` across all fully analyzed games initialized that day. |
| `POST /players/{player}/analysis/measures/range-games-score` | One compact score per fully analyzed game selected by either `game_ids` or `date_from`/`date_to`. Results are paginated and include 0–100 efficiency, mean/median WinPercent loss, player CP gain/loss/net, blunders, final CP, result, and ending reason. |
| `GET /players/{player}/analysis/measures/quality-calendar` | Compatibility aggregate of the selected player's own CP gain/loss grouped by weekday and hour. |
| `GET /players/{player}/analysis/measures/games/{game_id}` | Complete ordered scored-position sequence for one game, including raw White score and player-oriented score/change/gain/loss. |
| `GET /players/{player}/analysis/measures/days/{date}/hours/{hour}` | Cursor-paginated game sequences initialized in one resolved local hour. `hour` is 0–23. |

`game_player_engine_summary` contains one compact row per fully analyzed game
and player color. Its `game_efficiency` follows Lichess's current game-accuracy
method: per-move WinPercent accuracy with the uncertainty bonus, a volatility
weight taken from a 2–8-ply rolling window, and the mean of the weighted and
harmonic accuracies. The row also stores mean/median WinPercent loss, analyzed
player moves, own-move CP gain/loss, Lichess blunder count, mate-position
counts, final player CP, result, and normalized ending reason. Stockfish and
tablebase writes refresh newly complete games automatically. Existing rows are
a rebuildable cache and receive efficiency lazily when their player is first
requested after this schema addition.

The range request must use exactly one selector:

```json
{"game_ids": [123, 456], "mode": "all", "page": 1, "page_size": 100}
```

or:

```json
{"date_from": "2026-01-01", "date_to": "2026-03-31", "mode": "blitz"}
```
