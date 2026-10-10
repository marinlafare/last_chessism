import { getJson } from '../../services/apiClient'

// Read the controller's saved status; never launch or mutate cloud resources.
export const fetchCloudStatus = ({ signal } = {}) => getJson('/analysis/cloud', { signal })
export const fetchCloudBilling = ({ signal } = {}) => getJson('/analysis/cloud/billing', { signal })
export const fetchCloudJobUsage = (id, { signal } = {}) => getJson(`/analysis/cloud/${encodeURIComponent(id)}/usage`, { signal })

export async function fetchStatus({ signal } = {}) {
  const startedAt = performance.now()
  const payload = await getJson('/status', { signal })
  return {
    api: { ok: payload?.api?.ok ?? true, latency_ms: payload?.api?.latency_ms ?? Math.round(performance.now() - startedAt) },
    workers: {
      total: payload?.workers?.total ?? null,
      busy: payload?.workers?.busy ?? payload?.workers?.active ?? null,
      idle: payload?.workers?.idle ?? null,
    },
    jobs: {
      queued: payload?.jobs?.queued ?? null, running: payload?.jobs?.running ?? null,
      failed: payload?.jobs?.failed ?? null, completed: payload?.jobs?.completed ?? null,
    },
    version: {
      backend: payload?.version?.backend ?? payload?.backend_version ?? null,
      stockfish: payload?.version?.stockfish ?? payload?.stockfish_version ?? null,
    },
    source: '/status', timestamp: payload?.timestamp || new Date().toISOString(),
  }
}
