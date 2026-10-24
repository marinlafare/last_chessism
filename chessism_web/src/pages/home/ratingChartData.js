import { normalizePlayer } from '../../services/navigation.js'

export const RATING_PLAYERS_PER_PAGE = 10
// Left to right within each player's tick, independent of the sorting mode.
export const RATING_BAR_MODES = ['blitz', 'rapid', 'bullet']
export const PLAYER_FETCH_LIMIT = 5000

export function overlappingRatingBars(binX, slot) {
  // Keep the Hours-style widths and offsets, sharing one tick and baseline.
  // SVG paint order is back to front: Rapid, Blitz, then Bullet.
  return [
    { mode: 'rapid', x: binX + slot * .125, width: slot * .75 },
    { mode: 'blitz', x: binX + slot * .1, width: slot * .55 },
    { mode: 'bullet', x: binX + slot * .35, width: slot * .55 },
  ]
}

export function playerRating(player) {
  const raw = player?.rating ?? player?.last_rating ?? player?.avg_game_rating
  if (raw == null || raw === '') return null
  const value = Number(raw)
  return Number.isFinite(value) && value > 0 ? value : null
}

export const playerDisplayName = player => String(player?.full_name || '').trim()
  || String(player?.player_name || '').trim() || 'Unknown player'

export function groupedRatingPlayers(players, modes = {}) {
  const lookup = Object.fromEntries(['rapid', 'bullet'].map(mode => [mode,
    new Map((modes[mode] || []).map(player => [normalizePlayer(player.player_name), playerRating(player)])),
  ]))
  // The blitz roster is authoritative: other modes must never reorder it or
  // add players that are absent from the list. Missing ratings are not zeroes.
  return players.map(player => ({ ...player, ratings: {
    blitz: playerRating(player),
    rapid: lookup.rapid.get(normalizePlayer(player.player_name)) ?? null,
    bullet: lookup.bullet.get(normalizePlayer(player.player_name)) ?? null,
  } }))
}

export function ratingPlayerPage(players = [], requestedPage = 0) {
  const pages = Math.max(1, Math.ceil(players.length / RATING_PLAYERS_PER_PAGE))
  const page = Math.min(pages - 1, Math.max(0, Math.floor(Number(requestedPage) || 0)))
  const start = page * RATING_PLAYERS_PER_PAGE
  // A shared zero-based scale stays fixed when browsing between players.
  const maximum = players.reduce((max, player) => Math.max(max, ...RATING_BAR_MODES.map(mode => player.ratings[mode] ?? 0)), 0)
  return { players: players.slice(start, start + RATING_PLAYERS_PER_PAGE), page, pages, start,
    total: players.length, axisMax: Math.max(500, Math.ceil(maximum / 500) * 500) }
}
