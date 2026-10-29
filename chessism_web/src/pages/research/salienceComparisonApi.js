import { getJson, postJson } from '../../services/apiClient'

const base = '/research/salience-comparisons'
export const fetchComparisons = signal => getJson(base, { signal })
export const fetchComparisonGames = (id, filters, signal) =>
  getJson(`${base}/${encodeURIComponent(id)}/games?${new URLSearchParams(filters)}`, { signal })
export const fetchComparisonGame = (id, gameId, signal) =>
  getJson(`${base}/${encodeURIComponent(id)}/games/${gameId}`, { signal })
export const saveComparisonReview = (id, gameId, judgement, note) =>
  postJson(`${base}/${encodeURIComponent(id)}/games/${gameId}/review`, { judgement, note })
