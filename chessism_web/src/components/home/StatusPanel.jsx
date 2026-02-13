import { useMemo } from 'react'
import { FRONTEND_VERSION } from '../../config'

const STATUS_STALE_MS = 30000

const fmt = (value) => {
  if (value === null || value === undefined) {
    return <span title="Not available">-</span>
  }
  return value
}

function cardTone(ok, stale) {
  if (stale) return 'warn'
  return ok ? 'ok' : 'down'
}

function StatusPanel({ loading, error, data, lastSuccessAt }) {
  const stale = useMemo(() => {
    if (!lastSuccessAt) return true
    return Date.now() - lastSuccessAt > STATUS_STALE_MS
  }, [lastSuccessAt])

  const hasKnownData = Boolean(data)
  const apiOk = data?.api?.ok ?? false
  const apiLatency = data?.api?.latency_ms
  const label = apiOk ? 'Up' : hasKnownData ? 'Down' : '-'

  const lastUpdated = lastSuccessAt
    ? new Date(lastSuccessAt).toLocaleTimeString()
    : '-'

  return (
    <section className="status" id="status" aria-live="polite">
      <div className="section-head">
        <h2>System Status</h2>
        <p>Operational view across API, workers, queue, and versions.</p>
      </div>

      {error ? (
        <div className="status-banner warn">
          {hasKnownData ? 'Degraded: showing last known values.' : 'Backend unreachable.'}{' '}
          {!hasKnownData ? 'Check API base URL.' : ''}
        </div>
      ) : null}

      {!error && stale && hasKnownData ? (
        <div className="status-banner warn">Status is stale (no successful update in the last 30s).</div>
      ) : null}

      {loading && !hasKnownData ? (
        <div className="status-skeleton" aria-label="Loading status">
          <div />
          <div />
          <div />
          <div />
          <div />
        </div>
      ) : (
        <div className="status-grid">
          <article className={`status-card ${cardTone(apiOk, stale)}`}>
            <h3>API Health</h3>
            <p className="value">{label}</p>
            <p className="meta">latency {fmt(apiLatency)} ms</p>
          </article>

          <article className="status-card neutral">
            <h3>Stockfish Workers</h3>
            <p className="value">{fmt(data?.workers?.busy)}/{fmt(data?.workers?.idle)}/{fmt(data?.workers?.total)}</p>
            <p className="meta">active / idle / total</p>
          </article>

          <article className="status-card neutral">
            <h3>Job Queue</h3>
            <p className="value">{fmt(data?.jobs?.queued)}/{fmt(data?.jobs?.running)}/{fmt(data?.jobs?.failed)}/{fmt(data?.jobs?.completed)}</p>
            <p className="meta">queued / running / failed / completed</p>
          </article>

          <article className="status-card neutral">
            <h3>Version</h3>
            <p className="value">FE {FRONTEND_VERSION}</p>
            <p className="meta">BE {fmt(data?.version?.backend)} | SF {fmt(data?.version?.stockfish)}</p>
          </article>

          <article className="status-card neutral">
            <h3>Last Updated</h3>
            <p className="value">{lastUpdated}</p>
            <p className="meta">source {data?.source || '-'}</p>
          </article>
        </div>
      )}
    </section>
  )
}

export default StatusPanel
