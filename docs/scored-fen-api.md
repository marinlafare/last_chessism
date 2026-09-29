# Scored-FEN API inventory

All routes below require the existing superuser session. A stored score is
defined by `fen.score IS NOT NULL`; score `0` is analyzed and must not be
treated as pending.

The table uses backend paths. From the production site, prefix them with
`https://chessism.blackcrowlabs.com/api`.

## Direct score analysis

| Method and path | Inputs | Information returned |
| --- | --- | --- |
| `GET /fens/scored/overview` | None | Precomputed global totals, evaluation buckets, occurrence-weighted counts, side balance, average score/absolute score, and average WDL. |
| `GET /fens/scored` | `sort=frequency|impact|evaluation`, `min_abs_score=0..10000`, `page`, `page_size<=50` | Raw scored FEN rows: FEN, score, WDL, repetition count, side to move, move metadata, and impact (`n_games * abs(score)`). |
| `GET /fens/scored/advantage_by_rating` | None | Fully analyzed games split into three weighted-Jenks `avg_elo` groups, with equal/small/clear/decisive/mate occurrence counts and per-game rating points. |
| `GET /fens/scored/games/overview` | None | Global game-completion coverage: games with positions, fully analyzed games, incomplete games, and analyzed/unscored position occurrences. |
| `GET /fens/scored/games` | `status=fully|incomplete|all`, `page`, `page_size<=50` | Per-game coverage plus average score, average absolute score, maximum absolute score, players, mode, date, and result. |
| `GET /players/{player}/analysis/engine` | `mode=all|bullet|blitz|rapid`, optional `date_from`, `date_to` | Score-derived player statistics: phase ACPL and error rates, expected-score loss, advantage conversion, resilience, time-pressure quality, and tablebase endgame precision. Uses at most 2,000 evenly distributed fully analyzed games and is cached for 5 minutes. |

## Coverage and workload selection

| Method and path | Inputs | Information returned |
| --- | --- | --- |
| `GET /fens/analysis_counts` | None | Distinct global FEN totals: total, analyzed (including score `0`), unscored, and non-zero scored. |
| `GET /fens/players/{player}/analysis_counts` | Player path | Player game and position-occurrence coverage, fully analyzed games, pending positions, and latest recorded rating. |
| `GET /players/{player}/fen_counts` | Player path | Compatibility endpoint returning the same payload as the preceding player analysis-count route. |
| `GET /fens/pending/repeated` | `page`; fixed page size 5 | Structured pending FENs ordered by repetition count. This is the useful endpoint for choosing high-reuse analysis work. |
| `POST /analysis/player_games/scope` | Player, selection mode, optional date bounds | Complete/incomplete game counts and available date extent for a player selection. |
| `POST /analysis/player_games/preview` | Scope plus ordering/sample size | Exact selected-game workload: occurrence counts, unique analyzed/pending FENs, tablebase candidates, Stockfish FENs, and estimated duration. Also creates the short-lived confirmation plan. |

## Supporting and legacy routes

| Method and path | Information returned / caveat |
| --- | --- |
| `GET /games/database/generalities` | Dashboard totals including `n_positions` and `scored_fens`. |
| `GET /games/generalities` | Backward-compatible alias of `/games/database/generalities`. |
| `GET /games/_database_generalities` | Collision-safe alias of `/games/database/generalities`. |
| `GET /fens/top?limit=N` | Legacy text response listing the most repeated FENs and their score (possibly null). Prefer `/fens/scored` for analysis code. |
| `GET /fens/top_unscored?limit=N` | Legacy text response of repeated pending FENs. Prefer `/fens/pending/repeated`. |
| `GET /analysis/backups` | Backup files and their analyzed-record counts, timestamps, size, and checksum; no score distribution. |
| `POST /analysis/fen` | Returns a fresh Stockfish score/WDL/PV for supplied FENs but does **not** persist the result in the FEN database. It is for interactive analysis, not dataset queries. |

The queue endpoints under `/analysis/run_*`, `/analysis/tablebase`, and
`/analysis/player_games/run_job` start work but do not return scored-position
data. `/analysis_times/summary` reports engine runtime, not chess evaluations.
