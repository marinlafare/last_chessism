import { useCallback, useEffect, useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import {
  fetchBackupJob,
  fetchDatabaseBackups,
  queueDatabaseBackup,
} from './backups/backupsApi'
import './backups/backups.css'

const TERMINAL_PHASES = new Set(['complete', 'failed', 'unavailable'])

const formatBytes = (value) => {
  const bytes = Number(value)
  if (!Number.isFinite(bytes) || bytes < 0) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let amount = bytes
  let unit = 0
  while (amount >= 1000 && unit < units.length - 1) {
    amount /= 1000
    unit += 1
  }
  return `${amount.toFixed(unit < 2 ? 0 : 2)} ${units[unit]}`
}

const formatDate = (value) => (
  value ? new Date(value).toLocaleString() : '-'
)

export default function DatabaseBackups() {
  const [overview, setOverview] = useState(null)
  const [job, setJob] = useState(null)
  const [queueing, setQueueing] = useState(false)
  const [error, setError] = useState('')

  const loadOverview = useCallback(async ({ signal } = {}) => {
    const payload = await fetchDatabaseBackups({ signal })
    setOverview(payload)
    return payload
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    loadOverview({ signal: controller.signal }).catch((loadError) => {
      if (loadError.name !== 'AbortError') setError(loadError.message || 'Unable to read database backups.')
    })
    return () => controller.abort()
  }, [loadOverview])

  useEffect(() => {
    if (!job?.jobId || TERMINAL_PHASES.has(job.phase)) return undefined
    let cancelled = false
    const poll = async () => {
      try {
        const payload = await fetchBackupJob(job.jobId)
        if (cancelled) return
        const progress = payload.progress || {}
        const result = payload.result?.result || progress.result || null
        const phase = progress.phase || payload.status || 'queued'
        setJob((current) => ({
          ...current,
          phase,
          detail: progress.detail || current.detail,
          result,
        }))
        if (TERMINAL_PHASES.has(phase)) await loadOverview()
      } catch (pollError) {
        if (!cancelled) setError(pollError.message || 'Unable to refresh the backup job.')
      }
    }
    poll()
    const timer = window.setInterval(poll, 3000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [job?.jobId, job?.phase, loadOverview])

  useEffect(() => {
    if (job?.jobId || overview?.status?.status !== 'running') return undefined
    let cancelled = false
    const pollOverview = async () => {
      try {
        await loadOverview()
      } catch (pollError) {
        if (!cancelled) setError(pollError.message || 'Unable to refresh the running backup.')
      }
    }
    const timer = window.setInterval(pollOverview, 3000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [job?.jobId, loadOverview, overview?.status?.status])

  const startBackup = async () => {
    setQueueing(true)
    setError('')
    try {
      const payload = await queueDatabaseBackup()
      setJob({
        jobId: payload.job_id,
        phase: 'queued',
        detail: payload.message || 'Database backup queued.',
        result: null,
      })
      await loadOverview()
    } catch (startError) {
      setError(startError.message || 'Unable to queue the database backup.')
    } finally {
      setQueueing(false)
    }
  }

  const storage = overview?.storage || {}
  const status = overview?.status || {}
  const backups = Array.isArray(overview?.backups) ? overview.backups : []
  const latestBackup = backups[0] || null
  const active = Boolean(queueing || (job?.jobId && !TERMINAL_PHASES.has(job.phase)) || status.status === 'running')
  const usagePercent = useMemo(() => {
    const application = Number(storage.application_bytes || 0)
    const quota = Number(storage.quota_bytes || 0)
    return quota > 0 ? Math.min(100, (application / quota) * 100) : 0
  }, [storage.application_bytes, storage.quota_bytes])

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="database-backups-main">
          <section className="database-backups-hero">
            <div className="section-head">
              <div>
                <p className="eyebrow">RECOVERY</p>
                <h1>Database Backups</h1>
                <p className="database-backups-copy">Complete PostgreSQL recovery chains, stored separately from the portable FEN-analysis export.</p>
              </div>
              <button
                className="btn btn-primary btn-inline"
                type="button"
                disabled={active || !storage.available || storage.error}
                onClick={startBackup}
              >
                {active ? 'Backup running' : 'Backup database'}
              </button>
            </div>

            {error ? <div className="status-banner warn">{error}</div> : null}
            {!storage.available ? <div className="status-banner warn">{storage.error || 'Dedicated backup volume unavailable.'}</div> : null}
            {storage.available && storage.error ? <div className="status-banner warn">{storage.error}</div> : null}
          </section>

          <section className="database-backups-panel">
            <div className="section-head"><div><p className="eyebrow">STORAGE</p><h2>Protected Allocation</h2></div></div>
            <div className="database-backups-stats">
              <article><span>Available</span><strong>{formatBytes(storage.free_bytes)}</strong></article>
              <article><span>Database protected</span><strong>{formatBytes(latestBackup?.database_bytes)}</strong></article>
              <article><span>Backup storage used</span><strong>{formatBytes(storage.application_bytes)}</strong></article>
              <article><span>Chessism allocation</span><strong>{formatBytes(storage.quota_bytes)}</strong></article>
              <article><span>Protected reserve</span><strong>{formatBytes(storage.free_floor_bytes)}</strong></article>
            </div>
            <div className="database-backups-meter" aria-label={`${usagePercent.toFixed(1)} percent of the Chessism backup allocation used`}>
              <div style={{ width: `${usagePercent}%` }} />
            </div>
            {latestBackup ? (
              <p className="database-backups-storage-note">
                The protected database size is measured before compression. Its pgBackRest repository currently uses {formatBytes(latestBackup.repository_bytes)}; backup storage used also includes FEN exports, research files, and metadata.
              </p>
            ) : null}
            <p className="database-backups-location">{storage.storage_location || '/main-monitor-db-backups/chessism'}</p>
          </section>

          <section className="database-backups-panel">
            <div className="section-head">
              <div>
                <p className="eyebrow">STATUS</p>
                <div className="database-backups-status-title">
                  <h2>{String(job?.phase || status.status || 'idle').toUpperCase()}</h2>
                  {active ? (
                    <span
                      className="database-backups-live-light"
                      role="status"
                      aria-label="Database backup is running"
                      title="Database backup is running"
                    />
                  ) : null}
                </div>
              </div>
            </div>
            <div className={`database-backups-progress ${['failed', 'unavailable'].includes(job?.phase || status.status) ? 'failed' : ''}`}>
              <strong>{job?.detail || status.detail || 'No database backup has run yet.'}</strong>
              <span>Latest verified: {formatDate(status.last_verified_at)}</span>
              <span>Backup type: {status.backup_type || job?.result?.type || '-'}</span>
              <span>Backup ID: {status.backup_id || job?.result?.backup_id || '-'}</span>
            </div>
          </section>

          <section className="database-backups-panel">
            <div className="section-head"><div><p className="eyebrow">RECOVERY CHAINS</p><h2>Verified Backups</h2></div></div>
            <div className="database-backups-table-wrap">
              <table className="database-backups-table">
                <thead><tr><th>Backup</th><th>Type</th><th>Completed</th><th>Changed data</th><th>Repository data</th><th>Restore tested</th></tr></thead>
                <tbody>
                  {backups.map((backup) => (
                    <tr key={backup.backup_id}>
                      <td>{backup.backup_id}</td>
                      <td>{backup.type}</td>
                      <td>{formatDate(backup.completed_at)}</td>
                      <td>{formatBytes(backup.database_delta_bytes)}</td>
                      <td>{formatBytes(backup.repository_delta_bytes)}</td>
                      <td>{backup.restore_tested ? 'yes' : 'not yet'}</td>
                    </tr>
                  ))}
                  {!backups.length ? <tr><td colSpan="6">No verified database backups yet.</td></tr> : null}
                </tbody>
              </table>
            </div>
            <p className="database-backups-copy">The first run is full. Later runs are incremental until the active chain reaches 30 incrementals, becomes 30 days old, or WAL continuity is interrupted.</p>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
