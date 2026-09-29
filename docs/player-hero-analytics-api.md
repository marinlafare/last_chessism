# Player hero analytics API

These authenticated endpoints exclusively support the analytics workspace on
the player hero page. Their backend implementation lives in
`chessism_api/operations/player_hero_analytics.py`; the page-owned API client
lives in `chessism_web/src/pages/players/playerAnalysisApi.js`.

All aggregate endpoints accept `mode=all|bullet|blitz|rapid` and optional
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

| Endpoint | Purpose |
| --- | --- |
| `GET /players/{player}/analysis/behavioural/activity` | Dense weekday, hour, and weekday×hour game proportions plus absolute wins, draws, and losses. The response also includes `by_mode` breakdowns for independent Bullet, Blitz, and Rapid chart filters without additional requests. |
| `GET /players/{player}/analysis/behavioural/ratings` | Dense daily last-rating series, kept separate for Bullet, Blitz, and Rapid. Days without games contain `last_rating: null`. |
| `GET /players/{player}/analysis/behavioural/days/{date}` | All rating observations and absolute W/D/L totals in 24 resolved-local-time hourly buckets for one local date. |

## Engine Measures

Stockfish scores are stored from White's perspective. These endpoints invert
the sign when the selected player was Black. For consecutive centipawn scores:

- `cp_change = current_player_score - previous_player_score`;
- `cp_gain = max(cp_change, 0)`;
- `cp_loss = max(-cp_change, 0)`.

Thus a player-perspective change from `+20` to `+80` is `cp_change: +60`,
`cp_gain: 60`, and `cp_loss: 0`. Mate and tablebase sentinel values are kept
out of centipawn arithmetic and reported separately.

Blunders copy Lichess's current classifier. Centipawns are converted to
Lichess winning chances:

`2 / (1 + exp(-0.00368208 * cp)) - 1`.

A CP-to-CP player move is a blunder when it loses at least `0.30` on that
`[-1, 1]` scale. Forced-mate creation/loss uses Lichess's separate mate
boundaries. Every comparison is made after orienting the score toward the
player, so increasingly negative White scores are improvements for Black.
The implementation follows Lichess's
[`Advice.scala`](https://github.com/lichess-org/lila/blob/master/modules/tree/src/main/Advice.scala)
and the winning-chance formula in
[`eval.scala`](https://github.com/lichess-org/scalachess/blob/master/core/src/main/scala/eval.scala).

| Endpoint | Purpose |
| --- | --- |
| `POST /players/{player}/analysis/measures/range-games-score` | One compact score per fully analyzed game selected by either `game_ids` or `date_from`/`date_to`. Results are paginated and include player CP gain/loss/net, blunders, final CP, result, and ending reason. |
| `GET /players/{player}/analysis/measures/quality-calendar` | Compatibility aggregate of the selected player's own CP gain/loss grouped by weekday and hour. |
| `GET /players/{player}/analysis/measures/games/{game_id}` | Complete ordered scored-position sequence for one game, including raw White score and player-oriented score/change/gain/loss. |
| `GET /players/{player}/analysis/measures/days/{date}/hours/{hour}` | Cursor-paginated game sequences initialized in one resolved local hour. `hour` is 0–23. |

`game_player_engine_summary` contains one compact row per fully analyzed game
and player color. It stores player identity, analyzed player moves, own-move CP
gain/loss, Lichess blunder count, mate-position counts, final player CP,
result, and normalized ending reason. Stockfish and tablebase writes refresh
newly complete games automatically.

The range request must use exactly one selector:

```json
{"game_ids": [123, 456], "mode": "all", "page": 1, "page_size": 100}
```

or:

```json
{"date_from": "2026-01-01", "date_to": "2026-03-31", "mode": "blitz"}
```
