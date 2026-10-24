import { getJson } from '../../services/apiClient'

const withRatingRange = (params, minRating, maxRating) => {
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  return params
}

export const fetchDatabaseGeneralities = () => getJson('/games/database/generalities')

export const fetchPlayerGameCount = (playerName) => (
  getJson(`/games/${encodeURIComponent(playerName)}/count`)
)

export const fetchRecentGamesPage = (playerName, page, pageSize = 10) => (
  getJson(`/games/${encodeURIComponent(playerName)}/recent?page=${page}&page_size=${pageSize}`)
)

export const fetchGameSummary = (playerName) => (
  getJson(`/games/${encodeURIComponent(playerName)}/summary`)
)

export const fetchTimeControlCounts = () => getJson('/games/time_controls')

export function fetchTimeControlTopMoves(
  mode,
  moveColor,
  minRating,
  maxRating,
  page,
  pageSize,
  maxMove,
) {
  const params = withRatingRange(new URLSearchParams({
    player_color: moveColor,
    page: String(page),
    page_size: String(pageSize),
    max_move: String(maxMove),
  }), minRating, maxRating)
  return getJson(`/games/time_controls/${encodeURIComponent(mode)}/top_moves?${params}`)
}

export function fetchTimeControlTopOpenings(
  mode,
  minRating,
  maxRating,
  nMoves,
  page,
  pageSize,
) {
  const params = withRatingRange(new URLSearchParams({
    page: String(page),
    page_size: String(pageSize),
    n_moves: String(nMoves),
  }), minRating, maxRating)
  return getJson(`/games/time_controls/${encodeURIComponent(mode)}/top_openings?${params}`)
}

export const fetchTimeControlRatingChart = (mode) => (
  getJson(`/games/rating_time_control_chart?time_control=${encodeURIComponent(mode)}`)
)

const fetchModeAnalytics = (mode, suffix, minRating, maxRating) => {
  const params = withRatingRange(new URLSearchParams(), minRating, maxRating)
  return getJson(`/games/time_controls/${encodeURIComponent(mode)}/${suffix}?${params}`)
}

export const fetchTimeControlResultColorMatrix = (mode, minRating, maxRating) => (
  fetchModeAnalytics(mode, 'result_color_matrix', minRating, maxRating)
)

export const fetchTimeControlGameLengthAnalytics = (mode, minRating, maxRating) => (
  fetchModeAnalytics(mode, 'game_length_analytics', minRating, maxRating)
)

export const fetchTimeControlActivityTrend = (mode, minRating, maxRating) => (
  fetchModeAnalytics(mode, 'activity_trend', minRating, maxRating)
)
