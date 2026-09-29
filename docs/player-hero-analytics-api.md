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
| `GET /players/{player}/analysis/behavioural/activity` | Dense weekday, hour, and weekday×hour game proportions plus absolute wins, draws, and losses. |
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

| Endpoint | Purpose |
| --- | --- |
| `GET /players/{player}/analysis/measures/quality-calendar` | Weekday, hour, and weekday×hour CP sums, signed changes, gain/loss totals, mover split, mates, and tablebase outcomes. Empty CP buckets contain `null` CP measures. |
| `GET /players/{player}/analysis/measures/games/{game_id}` | Complete ordered scored-position sequence for one game, including raw White score and player-oriented score/change/gain/loss. |
| `GET /players/{player}/analysis/measures/days/{date}/hours/{hour}` | Cursor-paginated game sequences initialized in one resolved local hour. `hour` is 0–23. |

The quality-calendar endpoint reads `game_player_engine_summary`, one compact
row per fully analyzed game and player color. Stockfish and tablebase writes
refresh newly complete games automatically, so opening the hero page does not
rescan every position appearance.
