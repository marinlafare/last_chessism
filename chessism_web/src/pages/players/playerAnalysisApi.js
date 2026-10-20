import { postJson, requestJson } from '../../services/apiClient'
import { pollScoreDistribution } from './scoreDistributionRequest.js'

const responseCache = new Map()
const RESPONSE_CACHE_LIMIT = 32

const playerPath = (playerName) => encodeURIComponent(playerName)

export function fetchPlayerGameCatalog({ playerName, kind, mode = 'all', color = 'all', page = 1, search = '', signal }) {
  const params = new URLSearchParams({ kind, mode, color, page: String(page), limit: '10', search })
  return requestJson(`/players/${playerPath(playerName)}/analysis/behavioural/catalog?${params}`, { signal })
}

export function fetchPlayerRatingDistribution({ playerName, mode = 'all', signal }) {
  // Server-side counts expire after 60s; do not put them in the timeless
  // per-tab analytics cache, which would hide newly ingested games.
  return requestJson(`/players/${playerPath(playerName)}/ppd?${new URLSearchParams({ mode })}`, { signal })
}

export function fetchPlayerScoreDistribution({ playerName, signal, refresh = false, onProgress }) {
  return pollScoreDistribution(
    () => requestJson(`/players/${playerPath(playerName)}/analysis/measures/score-distribution${refresh ? '?refresh=true' : ''}`, { signal }),
    { signal, onProgress })
}

export function fetchPlayerScorePositions({ playerName, selection, signal }) {
  return postJson(`/players/${playerPath(playerName)}/analysis/measures/score-distribution/positions`, selection, { signal })
}

export function fetchPlayerBlunderDistribution({ playerName, signal }) {
  return requestJson(`/players/${playerPath(playerName)}/analysis/measures/blunder-distribution`, { signal })
}

function analyticsSearch({ mode = 'all', dateFrom, dateTo, timezone, cursor, limitGames } = {}) {
  const search = new URLSearchParams({ mode })
  if (dateFrom) search.set('date_from', dateFrom)
  if (dateTo) search.set('date_to', dateTo)
  if (timezone) search.set('timezone', timezone)
  if (cursor) search.set('cursor', cursor)
  if (limitGames) search.set('limit_games', String(limitGames))
  return search.toString()
}

async function cachedRequest(path, options = {}) {
  const { signal, bypassCache = false } = options
  if (!bypassCache && responseCache.has(path)) return responseCache.get(path)
  const payload = await requestJson(path, { signal })
  responseCache.set(path, payload)
  if (responseCache.size > RESPONSE_CACHE_LIMIT) {
    responseCache.delete(responseCache.keys().next().value)
  }
  return payload
}

export function fetchBehaviouralActivity({ playerName, signal, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/behavioural/activity?${search}`,
    { signal }
  )
}

export function fetchBehaviouralRatings({ playerName, signal, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/behavioural/ratings?${search}`,
    { signal }
  )
}

export function fetchQualityCalendar({ playerName, signal, bypassCache = false, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/quality-calendar?${search}`,
    { signal, bypassCache }
  )
}

export function fetchDailyGameCp({ playerName, signal, bypassCache = false, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/daily-game-cp?${search}`,
    { signal, bypassCache }
  )
}

export function fetchDailyEfficiency({ playerName, signal, bypassCache = false, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/daily-efficiency?${search}`,
    { signal, bypassCache }
  )
}

export function fetchDailySalienceAccuracy({ playerName, signal, bypassCache = false, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/daily-salience-accuracy?${search}`,
    { signal, bypassCache }
  )
}

export function fetchGameMeasures({ playerName, gameId, timezone, signal }) {
  const search = analyticsSearch({ timezone })
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/games/${encodeURIComponent(gameId)}?${search}`,
    { signal }
  )
}

export function fetchRangeGameScores({ playerName, selection, signal }) {
  return postJson(
    `/players/${playerPath(playerName)}/analysis/measures/range-games-score`,
    selection,
    { signal }
  )
}

export function fetchHourMeasures({ playerName, date, hour, signal, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/days/${encodeURIComponent(date)}/hours/${encodeURIComponent(hour)}?${search}`,
    { signal, bypassCache: Boolean(filters.cursor) }
  )
}

export function explorePlayerGames({ playerName, selection, signal }) {
  return postJson(
    `/players/${playerPath(playerName)}/analysis/measures/explore-games`,
    selection,
    { signal }
  )
}

export function fetchGameScore({ gameId, signal }) {
  return requestJson(`/games/${encodeURIComponent(gameId)}/score`, { signal })
}
