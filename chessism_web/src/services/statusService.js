import { API_BASE_URL } from '../config'

const na = null

const requestJson = async (path, signal) => {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    signal,
    credentials: 'include',
    headers: { Accept: 'application/json' }
  })
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} on ${path}`)
  }
  return response.json()
}

const normalizeFromStatus = (payload) => ({
  api: {
    ok: payload?.api?.ok ?? true,
    latency_ms: payload?.api?.latency_ms ?? na
  },
  workers: {
    total: payload?.workers?.total ?? na,
    busy: payload?.workers?.busy ?? payload?.workers?.active ?? na,
    idle: payload?.workers?.idle ?? na
  },
  jobs: {
    queued: payload?.jobs?.queued ?? na,
    running: payload?.jobs?.running ?? na,
    failed: payload?.jobs?.failed ?? na,
    completed: payload?.jobs?.completed ?? na
  },
  version: {
    backend: payload?.version?.backend ?? payload?.backend_version ?? na,
    stockfish: payload?.version?.stockfish ?? payload?.stockfish_version ?? na
  },
  source: '/status',
  timestamp: payload?.timestamp || new Date().toISOString()
})

export async function fetchStatus({ signal }) {
  const startedAt = performance.now()
  const payload = await requestJson('/status', signal)
  const normalized = normalizeFromStatus(payload)
  if (normalized.api.latency_ms === null || normalized.api.latency_ms === undefined) {
    normalized.api.latency_ms = Math.round(performance.now() - startedAt)
  }
  return normalized
}
