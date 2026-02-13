import { API_BASE_URL } from '../config'

const na = null

const requestJson = async (path, signal) => {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    signal,
    headers: { Accept: 'application/json' }
  })
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} on ${path}`)
  }
  return response.json()
}

const requestRoot = async (signal) => {
  const startedAt = performance.now()
  const response = await fetch(`${API_BASE_URL}/`, {
    signal,
    headers: { Accept: 'text/plain,application/json' }
  })
  const latencyMs = Math.round(performance.now() - startedAt)
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} on /`)
  }

  const bodyText = (await response.text()).trim()
  return { latencyMs, bodyText }
}

const readVersion = (value) => {
  if (!value) return na
  if (typeof value === 'string') return value
  if (typeof value === 'object') {
    return value.backend || value.api || value.version || na
  }
  return na
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

const normalizeFromFallback = ({ root, health, metrics, jobs }) => {
  const workers = metrics?.workers || metrics?.analysis_workers || metrics?.worker || {}
  const queue = jobs || metrics?.jobs || metrics?.queue || {}

  const apiOk =
    typeof health?.ok === 'boolean'
      ? health.ok
      : typeof health?.status === 'string'
        ? health.status.toLowerCase() === 'ok'
        : true

  return {
    api: {
      ok: apiOk,
      latency_ms: root?.latencyMs ?? na
    },
    workers: {
      total: workers.total ?? workers.count ?? na,
      busy: workers.busy ?? workers.active ?? na,
      idle: workers.idle ?? na
    },
    jobs: {
      queued: queue.queued ?? queue.pending ?? na,
      running: queue.running ?? queue.in_progress ?? na,
      failed: queue.failed ?? queue.errors ?? na,
      completed: queue.completed ?? queue.done ?? na
    },
    version: {
      backend: readVersion(metrics?.version || health?.version),
      stockfish: metrics?.stockfish_version ?? metrics?.stockfish ?? na
    },
    source: 'fallback',
    timestamp: new Date().toISOString()
  }
}

export async function fetchStatus({ signal }) {
  try {
    const payload = await requestJson('/status', signal)
    return normalizeFromStatus(payload)
  } catch {
    const rootPromise = requestRoot(signal).catch(() => null)
    const healthPromise = requestJson('/health', signal).catch(() => null)
    const metricsPromise = requestJson('/metrics', signal)
      .catch(() => requestJson('/system', signal))
      .catch(() => null)
    const jobsPromise = requestJson('/jobs/summary', signal).catch(() => null)

    const [root, health, metrics, jobs] = await Promise.all([
      rootPromise,
      healthPromise,
      metricsPromise,
      jobsPromise
    ])

    if (!root && !health && !metrics && !jobs) {
      throw new Error('Backend unreachable. Check API base URL.')
    }

    return normalizeFromFallback({ root, health, metrics, jobs })
  }
}
