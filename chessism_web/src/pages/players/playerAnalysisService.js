import { requestJson } from '../../services/apiClient'

const responseCache = new Map()
const RESPONSE_CACHE_LIMIT = 24

const cacheKey = ({ playerName, tab, mode, dateFrom, dateTo }) => (
  [playerName, tab, mode, dateFrom || '', dateTo || ''].join('|')
)

const readCachedResponse = (key) => {
  const payload = responseCache.get(key)
  if (!payload) return null
  responseCache.delete(key)
  responseCache.set(key, payload)
  return payload
}

const cacheResponse = (key, payload) => {
  responseCache.set(key, payload)
  if (responseCache.size > RESPONSE_CACHE_LIMIT) {
    responseCache.delete(responseCache.keys().next().value)
  }
}

export async function fetchPlayerAnalysis({
  playerName,
  tab,
  mode,
  dateFrom,
  dateTo,
  signal
}) {
  const request = { playerName, tab, mode, dateFrom, dateTo }
  const key = cacheKey(request)
  const cached = readCachedResponse(key)
  if (cached) return cached

  const search = new URLSearchParams({ mode })
  if (dateFrom) search.set('date_from', dateFrom)
  if (dateTo) search.set('date_to', dateTo)

  const payload = await requestJson(
    `/players/${encodeURIComponent(playerName)}/analysis/${tab}?${search}`,
    { signal }
  )
  cacheResponse(key, payload)
  return payload
}
