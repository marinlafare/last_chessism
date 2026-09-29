import { requestJson } from '../../services/apiClient'

const responseCache = new Map()
const RESPONSE_CACHE_LIMIT = 32

const playerPath = (playerName) => encodeURIComponent(playerName)

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

export function fetchQualityCalendar({ playerName, signal, ...filters }) {
  const search = analyticsSearch(filters)
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/quality-calendar?${search}`,
    { signal }
  )
}

export function fetchGameMeasures({ playerName, gameId, timezone, signal }) {
  const search = analyticsSearch({ timezone })
  return cachedRequest(
    `/players/${playerPath(playerName)}/analysis/measures/games/${encodeURIComponent(gameId)}?${search}`,
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
