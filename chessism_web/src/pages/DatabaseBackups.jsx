import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import {
  fetchBackupJob,
  fetchDatabaseBackups,
  queueDatabaseBackup,
  queueDatabaseRestoreTest,
} from './backups/backupsApi'
import { playCompletionSound, unlockCompletionAudio } from '../utils/completionAudio'
import './backups/backups.css'

const TERMINAL_PHASES = new Set(['complete', 'failed', 'unavailable'])
const TRANSIENT_RESULT_PHASES = new Set(['complete', 'success', 'failed'])
const RESULT_VISIBILITY_MS = 2 * 60 * 1000

const completedAtMs = (value) => {
  const timestamp = value ? new Date(value).getTime() : Number.NaN
  return Number.isFinite(timestamp) ? timestamp : null
}

const showRecentResult = (phase, completedAt, now) => {
  if (!TRANSIENT_RESULT_PHASES.has(String(phase || '').toLowerCase())) return true
  const timestamp = completedAtMs(completedAt)
  return timestamp !== null && now - timestamp < RESULT_VISIBILITY_MS
}

const formatBytes = (value) => {
  if (value === null || value === undefined || value === '') return '-'
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
  const [queueingAction, setQueueingAction] = useState('')
  const [error, setError] = useState('')
  const [statusClock, setStatusClock] = useState(() => Date.now())
  const audioContextRef = useRef(null)
  const backupSoundArmedRef = useRef(false)
  const backupSoundPlayedRef = useRef(false)

  const playBackupCompletion = () => {
    if (!backupSoundArmedRef.current || backupSoundPlayedRef.current) return
    backupSoundPlayedRef.current = true
    backupSoundArmedRef.current = false
    playCompletionSound(audioContextRef)
  }

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
        const resultInfo = payload.result || null
        const result = resultInfo?.result || progress.result || null
        const jobFailed = resultInfo?.success === false
        const phase = jobFailed ? 'failed' : progress.phase || payload.status || 'queued'
        const failedDetail = jobFailed
          ? result?.message || result?.type || 'The background job failed before completion.'
          : ''
        setJob((current) => ({
          ...current,
          phase,
          detail: failedDetail || progress.detail || current.detail,
          result,
          terminalAt: TERMINAL_PHASES.has(phase)
            ? current.terminalAt || new Date().toISOString()
            : null,
        }))
        if (phase === 'complete') playBackupCompletion()
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
    const phase = String(overview?.status?.status || '').toLowerCase()
    if (phase === 'running') {
      backupSoundArmedRef.current = true
      backupSoundPlayedRef.current = false
    } else if (phase === 'success') {
      playBackupCompletion()
    } else if (phase === 'failed' || phase === 'unavailable') {
      backupSoundArmedRef.current = false
    }
  }, [overview?.status?.status])

  useEffect(() => {
    const backupRunning = overview?.status?.status === 'running'
    const restoreRunning = ['queued', 'running'].includes(overview?.restore_test?.status)
    if (job?.jobId || (!backupRunning && !restoreRunning)) return undefined
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
  }, [job?.jobId, loadOverview, overview?.restore_test?.status, overview?.status?.status])

  const startBackup = async () => {
    unlockCompletionAudio(audioContextRef)
    backupSoundArmedRef.current = true
    backupSoundPlayedRef.current = false
    setQueueingAction('backup')
    setError('')
    try {
      const payload = await queueDatabaseBackup()
      setJob({
        jobId: payload.job_id,
        kind: 'backup',
        phase: 'queued',
        detail: payload.message || 'Database backup queued.',
        result: null,
        terminalAt: null,
      })
      await loadOverview()
    } catch (startError) {
      setError(startError.message || 'Unable to queue the database backup.')
    } finally {
      setQueueingAction('')
    }
  }

  const startRestoreTest = async () => {
    unlockCompletionAudio(audioContextRef)
    backupSoundArmedRef.current = true
    backupSoundPlayedRef.current = false
    setQueueingAction('restore_test')
    setError('')
    try {
      const payload = await queueDatabaseRestoreTest()
      setJob({
        jobId: payload.job_id,
        kind: 'restore_test',
        phase: 'queued',
        detail: payload.message || 'Restore test queued.',
        result: null,
        terminalAt: null,
      })
      await loadOverview()
    } catch (startError) {
      setError(startError.message || 'Unable to queue the restore test.')
    } finally {
      setQueueingAction('')
    }
  }

  const storage = overview?.storage || {}
  const status = overview?.status || {}
  const restoreTest = overview?.restore_test || {}
  const backups = Array.isArray(overview?.backups) ? overview.backups : []
  const policy = overview?.policy || {}
  const latestBackup = backups[0] || null
  const trackedJobActive = Boolean(job?.jobId && !TERMINAL_PHASES.has(job.phase))
  const backupActive = Boolean(
    queueingAction === 'backup'
      || (trackedJobActive && job?.kind === 'backup')
      || status.status === 'running'
  )
  const restoreActive = Boolean(
    queueingAction === 'restore_test'
      || (trackedJobActive && job?.kind === 'restore_test')
      || ['queued', 'running'].includes(restoreTest.status)
  )
  const active = backupActive || restoreActive
  const rawBackupPhase = job?.kind === 'backup' ? job.phase : status.status
  const rawBackupDetail = job?.kind === 'backup' ? job.detail : status.detail
  const backupCompletedAt = job?.kind === 'backup' ? job.terminalAt : status.completed_at
  const rawRestorePhase = job?.kind === 'restore_test' ? job.phase : restoreTest.status
  const rawRestoreDetail = job?.kind === 'restore_test' ? job.detail : restoreTest.detail
  const restoreCompletedAt = job?.kind === 'restore_test' ? job.terminalAt : restoreTest.completed_at
  const backupResultVisible = showRecentResult(rawBackupPhase, backupCompletedAt, statusClock)
  const restoreResultVisible = showRecentResult(rawRestorePhase, restoreCompletedAt, statusClock)
  const backupDisplayPhase = backupResultVisible ? rawBackupPhase : 'ready'
  const backupDisplayDetail = backupResultVisible
    ? rawBackupDetail
    : 'Waiting for a new database backup.'
  const restoreDisplayPhase = restoreResultVisible ? rawRestorePhase : 'ready'
  const restoreDisplayDetail = restoreResultVisible
    ? rawRestoreDetail
    : 'Waiting for a new restore rehearsal.'
  const usagePercent = useMemo(() => {
    const application = Number(storage.application_bytes || 0)
    const quota = Number(storage.quota_bytes || 0)
    return quota > 0 ? Math.min(100, (application / quota) * 100) : 0
  }, [storage.application_bytes, storage.quota_bytes])

  useEffect(() => {
    const now = Date.now()
    const expirations = [backupCompletedAt, restoreCompletedAt]
      .map(completedAtMs)
      .filter((timestamp) => timestamp !== null)
      .map((timestamp) => timestamp + RESULT_VISIBILITY_MS)
      .filter((timestamp) => timestamp > now)
    if (!expirations.length) return undefined
    const timer = window.setTimeout(
      () => setStatusClock(Date.now()),
      Math.min(...expirations) - now + 50,
    )
    return () => window.clearTimeout(timer)
  }, [backupCompletedAt, restoreCompletedAt, statusClock])

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
              <div className="database-backups-actions">
                <button
                  className="btn btn-primary btn-inline"
                  type="button"
                  disabled={active || !storage.available || storage.error}
                  onClick={startBackup}
                >
                  {backupActive ? 'Backup running' : policy.migration_required ? 'Create new full base' : 'Backup database'}
                </button>
                <button
                  className="btn btn-inline"
                  type="button"
                  disabled={active || !storage.available || !latestBackup}
                  onClick={startRestoreTest}
                >
                  {restoreActive ? 'Test running' : 'Test backup'}
                </button>
              </div>
            </div>

            {error ? <div className="status-banner warn">{error}</div> : null}
            {!storage.available ? <div className="status-banner warn">{storage.error || 'Dedicated backup volume unavailable.'}</div> : null}
            {storage.available && storage.error ? <div className="status-banner warn">{storage.error}</div> : null}
            {policy.migration_required ? (
              <div className="status-banner">
                Block-incremental storage is ready. The existing chain stays intact until the new full verifies, then its superseded files are removed.
              </div>
            ) : null}
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
                  <h2>{String(backupDisplayPhase || 'idle').toUpperCase()}</h2>
                  {backupActive ? (
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
            <div className={`database-backups-progress ${['failed', 'unavailable'].includes(backupDisplayPhase) ? 'failed' : ''}`}>
              <strong>{backupDisplayDetail || 'No database backup has run yet.'}</strong>
              <span>Latest verified: {formatDate(status.last_verified_at)}</span>
              <span>Backup type: {status.backup_type || (job?.kind === 'backup' ? job?.result?.type : null) || '-'}</span>
              <span>Backup ID: {status.backup_id || (job?.kind === 'backup' ? job?.result?.backup_id : null) || '-'}</span>
              <span>Storage mode: {policy.active_storage_mode === 'block_incremental_v1' ? 'block incremental' : policy.active_storage_mode ? 'legacy file incremental' : 'not initialized'}</span>
            </div>
          </section>

          <section className="database-backups-panel">
            <div className="section-head">
              <div>
                <p className="eyebrow">RESTORE REHEARSAL</p>
                <div className="database-backups-status-title">
                  <h2>{String(restoreDisplayPhase || 'not tested').replaceAll('_', ' ').toUpperCase()}</h2>
                  {restoreActive ? (
                    <span
                      className="database-backups-live-light"
                      role="status"
                      aria-label="Database restore test is running"
                      title="Database restore test is running"
                    />
                  ) : null}
                </div>
              </div>
            </div>
            <div className={`database-backups-progress ${restoreDisplayPhase === 'failed' ? 'failed' : ''}`}>
              <strong>{restoreDisplayDetail || 'No recovery point has been restored and tested yet.'}</strong>
              <span>Backup ID: {restoreTest.backup_id || (job?.kind === 'restore_test' ? job?.result?.backup_id : null) || '-'}</span>
              <span>Last tested: {formatDate(restoreTest.completed_at)}</span>
              <span>Duration: {restoreTest.elapsed_seconds !== null && restoreTest.elapsed_seconds !== undefined ? `${Number(restoreTest.elapsed_seconds).toFixed(1)} seconds` : '-'}</span>
              <span>Temporary local space required: {formatBytes(restoreTest.required_local_bytes)}</span>
            </div>
            <p className="database-backups-copy">Runs only when a superuser clicks Test backup. The exact latest recovery point is restored on a private PostgreSQL socket, validated, and deleted afterward.</p>
          </section>

          <section className="database-backups-panel">
            <div className="section-head"><div><p className="eyebrow">RECOVERY CHAINS</p><h2>Verified Backups</h2></div></div>
            <div className="database-backups-table-wrap">
              <table className="database-backups-table">
                <thead><tr><th>Backup</th><th>Type</th><th>Storage</th><th>Completed</th><th>Changed data</th><th>Repository data</th><th>Restore tested</th></tr></thead>
                <tbody>
                  {backups.map((backup) => (
                    <tr key={backup.backup_id}>
                      <td>{backup.backup_id}</td>
                      <td>{backup.type}</td>
                      <td>{backup.storage_mode === 'block_incremental_v1' ? 'block' : 'legacy'}</td>
                      <td>{formatDate(backup.completed_at)}</td>
                      <td>{formatBytes(backup.database_delta_bytes)}</td>
                      <td>{formatBytes(backup.repository_delta_bytes)}</td>
                      <td>
                        {backup.restore_tested
                          ? `passed · ${formatDate(backup.restore_tested_at)}`
                          : backup.restore_test_status === 'failed'
                            ? 'failed'
                            : 'not yet'}
                      </td>
                    </tr>
                  ))}
                  {!backups.length ? <tr><td colSpan="7">No verified database backups yet.</td></tr> : null}
                </tbody>
              </table>
            </div>
            <p className="database-backups-copy">A block-enabled full stores the maps used by later block incrementals. A new full starts after 30 incrementals, 30 days, or interrupted WAL continuity.</p>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
