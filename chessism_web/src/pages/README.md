# Frontend page ownership

Each route has one obvious maintenance entry point. Route components own their
stylesheet import, and HTTP paths live in a nearby `*Api.js` module. UI modules
should call named API functions instead of importing `services/apiClient.js`.

| Route | UI entry | API boundary | Stylesheet |
| --- | --- | --- | --- |
| `/` | `Home.jsx` | `home/homeApi.js` | `home/home.css` |
| `/games` | `Games.jsx` | `games/gamesApi.js` | `games/games.css` |
| `/players` | `Players.jsx` | `players/playerApi.js`, `players/playerAnalysisApi.js` | `players/players.css`, `players/player-analysis.css` |
| `/download_new_games` | `DownloadNewGames.jsx` | `download-new-games/downloadNewGamesApi.js` | `download-new-games/downloadNewGames.css` |
| `/main_characters` | `MainCharacters.jsx` | `main-characters/mainCharactersApi.js` | `main-characters/mainCharacters.css` |
| `/secondary_character` | `SecondaryCharacter.jsx` | No backend data | `secondary-character/secondaryCharacter.css` |
| `/analize_positions` | `Positions.jsx` | `positions/positionsApi.js` | `positions/positions.css` |
| `/live_analysis` | `LiveAnalysis.jsx` | `live-analysis/liveAnalysisApi.js` | `live-analysis/liveAnalysis.css` |
| `/scored_positions` | `ScoredPositions.jsx` | `scored-positions/scoredPositionsApi.js` | `scored-positions/scoredPositions.css` |
| `/analyze_times` | `AnalyzeTimes.jsx` | `analyze-times/analyzeTimesApi.js` | `analyze-times/analyzeTimes.css` |

`styles/foundation.css`, `styles/dashboard-shared.css`, and
`styles/responsive.css` contain application-wide primitives only. Authentication
transport belongs to `services/authApi.js`; browser job persistence belongs to
`services/gameJobService.js`.

Run `npm run check:architecture` after adding or moving a route. The check keeps
raw HTTP calls out of view components and verifies that every route imports its
owned stylesheet.
