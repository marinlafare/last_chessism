import { useMemo } from 'react'
import { FRONTEND_VERSION } from '../../config'

const STATUS_STALE_MS = 30000

const fmt = (value) => {
  if (value === null || value === undefined) {
    return <span title="Not available">-</span>
  }
  return value
}

const hasValue = (value) => value !== null && value !== undefined

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
  const queuedJobs = Number(data?.jobs?.queued ?? 0)
  const runningJobs = Number(data?.jobs?.running ?? 0)
  const activeJobs = queuedJobs + runningJobs
  const activityLabel = stale ? 'Stale' : activeJobs > 0 ? 'Active' : 'Idle'
  const activityClass = stale ? 'queue-stale' : activeJobs > 0 ? 'queue-active' : 'queue-idle'
  const activityMeta = stale
    ? `last update ${lastSuccessAt ? new Date(lastSuccessAt).toLocaleTimeString() : '-'}`
    : `${fmt(queuedJobs)} queued / ${fmt(runningJobs)} running`
  const workerValues = [data?.workers?.busy, data?.workers?.idle, data?.workers?.total]
  const workersHaveData = workerValues.every(hasValue)
  const workerText = workersHaveData ? workerValues.map(fmt).join('/') : '-/-/-'
  const jobValues = [data?.jobs?.queued, data?.jobs?.running, data?.jobs?.failed, data?.jobs?.completed]
  const jobsHaveData = jobValues.every(hasValue)
  const jobsHaveAnyCount = jobsHaveData && jobValues.some((value) => Number(value || 0) > 0)
  const jobsText = jobsHaveData ? jobValues.map(fmt).join('/') : '-/-/-/-'

  const lastUpdated = lastSuccessAt
    ? new Date(lastSuccessAt).toLocaleTimeString()
    : '-'

  return (
    <section className="status" id="status" aria-live="polite">
      <div className="section-head">
        <h2>System Status</h2>
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
          <article className={`status-card ${cardTone(hasKnownData, stale)} ${activityClass}`}>
            <h3>Queue Activity</h3>
            <p className="value">{activityLabel}</p>
            <p className="meta">{activityMeta}</p>
          </article>

          <article className="status-card neutral">
            <h3>Stockfish</h3>
            <p className={`value ${workersHaveData ? 'status-value-good' : 'status-value-bad'}`}>{workerText}</p>
            <p className="meta">active / idle / total</p>
          </article>

          <article className="status-card neutral">
            <h3>Jobs</h3>
            <p className={`value ${jobsHaveAnyCount ? 'status-value-good' : 'status-value-bad'}`}>{jobsText}</p>
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
