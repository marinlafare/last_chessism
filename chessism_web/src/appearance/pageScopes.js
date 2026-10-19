// Canonical route aliases; verified against the API capability registry in tests.
export const PAGE_SCOPES = {
  "/": "home",
  "/players": "home",
  "/games": "games",
  "/system_status": "system_status",
  "/database": "database",
  "/add_games": "download_games",
  "/download_new_games": "download_games",
  "/main_characters": "main_characters",
  "/secondary_character": "secondary_character",
  "/analize_positions": "positions",
  "/positions": "positions",
  "/live_analysis": "live_analysis",
  "/live_analyzis": "live_analysis",
  "/scored_positions": "scored_positions",
  "/analyze_times": "analyze_times",
  "/research": "research",
  "/research/chessism-coefficient": "accuracy_coefficient",
  "/research/matrices": "matrices",
  "/research/matrices/collection": "algorithms",
  "/research/algorithms": "algorithms",
  "/research/salience": "salience",
  "/backups": "backups"
}

export function scopeForPath(path = '/') {
  const normalized = path.split('?')[0].replace(/\/+$/, '') || '/'
  return PAGE_SCOPES[normalized] || (normalized.startsWith('/games/') ? 'games' : 'home')
}
