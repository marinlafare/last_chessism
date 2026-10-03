import { getJson } from '../../services/apiClient'

const na = null

export const fetchDashboardSummary = ({ signal } = {}) => Promise.all([
  getJson('/games/generalities', { signal }),
  getJson('/games/time_controls', { signal }),
])

export const fetchTopMainCharacters = (timeControl, limit, { signal } = {}) => (
  getJson(
    `/players/main_characters/top?time_control=${encodeURIComponent(timeControl)}&limit=${limit}`,
    { signal },
  )
)

const normalizeStatus = (payload) => ({
  api: {
    ok: payload?.api?.ok ?? true,
    latency_ms: payload?.api?.latency_ms ?? na,
  },
  workers: {
    total: payload?.workers?.total ?? na,
    busy: payload?.workers?.busy ?? payload?.workers?.active ?? na,
    idle: payload?.workers?.idle ?? na,
  },
  jobs: {
    queued: payload?.jobs?.queued ?? na,
    running: payload?.jobs?.running ?? na,
    failed: payload?.jobs?.failed ?? na,
    completed: payload?.jobs?.completed ?? na,
  },
  version: {
    backend: payload?.version?.backend ?? payload?.backend_version ?? na,
    stockfish: payload?.version?.stockfish ?? payload?.stockfish_version ?? na,
  },
  source: '/status',
  timestamp: payload?.timestamp || new Date().toISOString(),
})

export async function fetchStatus({ signal } = {}) {
  const startedAt = performance.now()
  const normalized = normalizeStatus(await getJson('/status', { signal }))
  if (normalized.api.latency_ms === null || normalized.api.latency_ms === undefined) {
    normalized.api.latency_ms = Math.round(performance.now() - startedAt)
  }
  return normalized
}
