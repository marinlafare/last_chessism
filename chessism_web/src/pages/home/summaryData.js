export function positionSlices(summary) {
  if (summary?.refreshed_at == null) return []
  return [
    { key: 'titled', label: 'Titled-player positions', value: summary.titled_fens, target: 'summary.titled' },
    { key: 'other', label: 'Other positions', value: summary.total_fens - summary.titled_fens, target: 'summary.other' },
  ]
}

export const GAME_PLAYERS_PER_PAGE = 4

export function playerAvatarUrl(value) {
  if (typeof value !== 'string') return ''
  const url = value.trim()
  // Public profile images or same-origin assets; never executable/data URLs.
  return /^https?:\/\//i.test(url) || /^\/(?![\/\\])/.test(url) ? url : ''
}

export function rankedGamePlayers(players = []) {
  // Bars are independent counts of games played, not fractions of a pie.
  // Do not mutate the API snapshot (which is ordered by old pie credits).
  return players.filter(player => Number.isFinite(player.games) && player.games > 0)
    .slice().sort((a, b) => b.games - a.games || a.player_name.localeCompare(b.player_name))
}

export function gamePlayerPage(players, requestedPage = 0) {
  const ranked = rankedGamePlayers(players)
  const pages = Math.max(1, Math.ceil(ranked.length / GAME_PLAYERS_PER_PAGE))
  const page = Math.max(0, Math.min(pages - 1, Math.trunc(requestedPage) || 0))
  const visible = ranked.slice(page * GAME_PLAYERS_PER_PAGE, (page + 1) * GAME_PLAYERS_PER_PAGE)
  return { page, pages, total: ranked.length, maxGames: visible[0]?.games || 0,
    start: page * GAME_PLAYERS_PER_PAGE,
    players: visible }
}

export function gameAxisMax(maxGames) {
  if (maxGames <= 4) return 4
  const magnitude = 10 ** Math.floor(Math.log10(maxGames / 4))
  // Fit each page fairly closely while retaining a zero baseline and readable,
  // whole-game ticks, including pages with only a handful of games.
  const step = Math.ceil([1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]
    .find(value => value * magnitude >= maxGames / 4) * magnitude)
  return 4 * step
}

export function sliceDescription(slice) {
  const number = value => Number(value).toLocaleString('en-US', { maximumFractionDigits: 1 })
  return `${slice.label}: ${number(slice.value)} FENs · ${(slice.fraction * 100).toFixed(1)}%`
}
