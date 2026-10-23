import { pageStyleTarget } from '../appearance/styleCore.js'
import { useCallback, useEffect, useRef, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import BackupVolume from './backups/BackupVolume.jsx'
import FenBackupPanel from './backups/FenBackupPanel.jsx'
import { backupCompletionTimes } from './backups/backupCompletionTimes.js'
import {
  fetchBackupJob,
  fetchDatabaseBackups,
  queueDatabaseBackup,
  queueDatabaseRestoreTest,
} from './backups/backupsApi'
import { playCompletionSound, unlockCompletionAudio } from '../utils/completionAudio'
import './backups/backups.css'

const TERMINAL_PHASES = new Set(['complete', 'failed', 'unavailable'])

const completedAtMs = (value) => {
  const timestamp = value ? new Date(value).getTime() : Number.NaN
  return Number.isFinite(timestamp) ? timestamp : null
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

const formatElapsedTime = (elapsedMs) => {
  const totalSeconds = Math.max(0, Math.floor(Number(elapsedMs || 0) / 1000))
  const days = Math.floor(totalSeconds / 86400)
  const hours = Math.floor((totalSeconds % 86400) / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  const clock = [hours, minutes, seconds]
    .map((part) => String(part).padStart(2, '0'))
    .join(':')
  return days > 0 ? `${days}d ${clock}` : clock
}

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
          total: Number(progress.total ?? current.total ?? 0),
          processed: Number(progress.processed ?? current.processed ?? 0),
          etaSeconds: progress.eta_seconds ?? null,
          rateBytesPerSecond: progress.rate_bytes_per_second ?? null,
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
    if ((job?.jobId && !TERMINAL_PHASES.has(job.phase)) || (!backupRunning && !restoreRunning)) return undefined
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
  }, [job?.jobId, job?.phase, loadOverview, overview?.restore_test?.status, overview?.status?.status])

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
        startedAt: new Date().toISOString(),
        total: 0,
        processed: 0,
        etaSeconds: null,
        rateBytesPerSecond: null,
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
  const latestBackupTested = Boolean(latestBackup?.restore_tested)
  const snapshotNeedsTest = latestBackup?.storage_mode === 'compressed_snapshot_v1' && !latestBackup?.restore_tested
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
  const completed = backupCompletionTimes(overview)
  const rawBackupPhase = job?.kind === 'backup' ? job.phase : status.phase || status.status
  const rawBackupDetail = job?.kind === 'backup' ? job.detail : status.detail
  const rawRestorePhase = job?.kind === 'restore_test' ? job.phase : restoreTest.status
  const rawRestoreDetail = job?.kind === 'restore_test' ? job.detail : restoreTest.detail
  const backupFailed = ['failed', 'unavailable'].includes(rawBackupPhase)
  const restoreFailed = ['failed', 'unavailable'].includes(rawRestorePhase)
  const backupStartedAt = status.status === 'running'
    ? status.started_at
    : job?.kind === 'backup'
      ? job.startedAt
      : null
  const backupStartedAtMs = completedAtMs(backupStartedAt)
  const backupElapsedMs = backupActive && backupStartedAtMs !== null
    ? statusClock - backupStartedAtMs
    : null
  const trackedProgressAvailable = job?.kind === 'backup' && Number(job.total || 0) > 0
  const persistedProgressAvailable = status.status === 'running'
    && Number(status.progress_total_bytes || 0) > 0
  const backupProgressTotal = trackedProgressAvailable
    ? Number(job.total)
    : persistedProgressAvailable
      ? Number(status.progress_total_bytes)
      : 0
  const backupProgressCompleted = trackedProgressAvailable
    ? Number(job.processed || 0)
    : persistedProgressAvailable
      ? Number(status.progress_completed_bytes || 0)
      : 0
  const backupEtaSeconds = trackedProgressAvailable
    ? Number(job.etaSeconds)
    : persistedProgressAvailable
      ? Number(status.progress_eta_seconds)
      : Number.NaN
  const backupRateBytesPerSecond = trackedProgressAvailable
    ? Number(job.rateBytesPerSecond)
    : persistedProgressAvailable
      ? Number(status.progress_rate_bytes_per_second)
      : Number.NaN
  const backupProgressPercent = backupProgressTotal > 0
    ? Math.min(100, Math.max(0, (backupProgressCompleted / backupProgressTotal) * 100))
    : 0
  const normalizedBackupPhase = String(rawBackupPhase || '').toLowerCase()
  const backupEtaLabel = normalizedBackupPhase === 'saving_matrices'
    ? 'Saving completed matrix snapshots · unchanged copies are reused'
    : normalizedBackupPhase === 'verifying'
    ? 'Transfer complete · verifying the recovery point'
    : Number.isFinite(backupEtaSeconds) && backupEtaSeconds > 0
      ? `Estimated transfer remaining: ${formatElapsedTime(backupEtaSeconds * 1000)}`
      : backupProgressTotal > 0 && backupProgressCompleted >= backupProgressTotal
        ? 'Database transfer complete'
        : backupProgressTotal > 0
          ? 'Calculating transfer ETA…'
          : 'Measuring database bytes…'

  useEffect(() => {
    if (!backupActive) return undefined
    setStatusClock(Date.now())
    const timer = window.setInterval(() => setStatusClock(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [backupActive])

  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="database-backups-main">
          <section {...pageStyleTarget('backup.actions')} className="database-backups-hero database-backups-actions" aria-label="Database recovery actions">
                <button {...pageStyleTarget('backup.run')}
                  className="database-backup-action"
                  data-running={backupActive ? 'true' : undefined}
                  aria-busy={backupActive}
                  type="button"
                  disabled={active || !storage.available || storage.error || snapshotNeedsTest}
                  onClick={startBackup}
                >
                  Backup
                </button>
                <button {...pageStyleTarget('backup.test')}
                  className="database-backup-action"
                  data-running={restoreActive ? 'true' : undefined}
                  aria-busy={restoreActive}
                  type="button"
                  disabled={active || !storage.available || Boolean(storage.error) || !latestBackup || latestBackupTested}
                  title={latestBackupTested ? 'The latest backup has already passed its restore test.' : 'Test the latest backup.'}
                  onClick={startRestoreTest}
                >
                  Test backup
                </button>
          </section>
          {(error || !storage.available || storage.error || policy.migration_required || snapshotNeedsTest) && <div role="status">
            {error ? <div className="status-banner warn">{error}</div> : null}
            {!storage.available ? <div className="status-banner warn">{storage.error || 'Dedicated backup volume unavailable.'}</div> : null}
            {storage.available && storage.error ? <div className="status-banner warn">{storage.error}</div> : null}
            {policy.migration_required ? (
              <div className="status-banner">
                Compressed snapshots are ready. Existing recovery data stays intact until a new snapshot passes Test backup. Archived WAL is retired only after database archiving is disabled.
              </div>
            ) : null}
            {snapshotNeedsTest ? <div className="status-banner">Test the new compressed snapshot before replacing the previous backup.</div> : null}
          </div>}

          <section {...pageStyleTarget('storage.panel')} className="database-backups-panel">
            <BackupVolume />
          </section>

          <section {...pageStyleTarget('summary.panel')} className="database-backups-panel database-backups-summary" aria-label="Latest backups">
            <div className="database-backups-results">
              <div className="database-backups-timestamps" aria-live="polite">
                {[
                  ['backup.last', 'Last backup', completed.backup],
                  ['restore.last', 'Last test', completed.test],
                ].map(([target, label, date]) => (
                  <div key={target} {...pageStyleTarget(target)} className="database-backups-timestamp">
                    <span>{label}</span>
                    {date ? <time dateTime={date}>{formatDate(date)}</time> : <span>{overview ? 'Not yet' : 'Loading…'}</span>}
                  </div>
                ))}
              </div>
              {(backupActive || backupFailed) && <div className={`database-backups-progress ${backupFailed ? 'failed' : ''}`} role="status" aria-label="Database backup status">
                <div className="database-backups-status-title">
                  <strong>Backup · {String(rawBackupPhase || 'queued').replaceAll('_', ' ')}</strong>
                  {backupActive ? (
                    <span
                      className="database-backups-live-light"
                      role="status"
                      aria-label="Database backup is running"
                      title="Database backup is running"
                    />
                  ) : null}
                </div>
                <strong {...pageStyleTarget('value')}>{rawBackupDetail || status.error_message || 'Database backup queued.'}</strong>
                {backupElapsedMs !== null ? (
                  <span className="database-backups-elapsed">Elapsed: {formatElapsedTime(backupElapsedMs)}</span>
                ) : null}
                {backupActive ? (
                  <div className="database-backups-transfer-progress">
                    <div
                      className={`database-backups-transfer-meter ${backupProgressTotal > 0 ? '' : 'indeterminate'}`}
                      role="progressbar"
                      aria-label="Database backup byte progress"
                      aria-valuemin="0"
                      aria-valuemax={backupProgressTotal || undefined}
                      aria-valuenow={backupProgressTotal ? backupProgressCompleted : undefined}
                    >
                      <div style={backupProgressTotal > 0 ? { width: `${backupProgressPercent}%` } : undefined} />
                    </div>
                    <div className="database-backups-transfer-meta">
                      <span>
                        {backupProgressTotal > 0
                          ? `${formatBytes(backupProgressCompleted)} / ${formatBytes(backupProgressTotal)} · ${backupProgressPercent.toFixed(1)}%`
                          : status.storage_mode === 'compressed_snapshot_v1'
                            ? `${formatBytes(status.compressed_bytes_written || 0)} compressed · total size known when complete`
                            : 'Waiting for backup progress'}
                      </span>
                      <span>{backupEtaLabel}</span>
                      {Number.isFinite(backupRateBytesPerSecond) && backupRateBytesPerSecond > 0 ? (
                        <span>{formatBytes(backupRateBytesPerSecond)}/s</span>
                      ) : null}
                    </div>
                  </div>
                ) : null}
              </div>}
              {(restoreActive || restoreFailed) && <div className={`database-backups-progress ${restoreFailed ? 'failed' : ''}`} role="status" aria-label="Database restore test status">
                <div className="database-backups-status-title">
                  <strong>Test backup · {String(rawRestorePhase || 'queued').replaceAll('_', ' ')}</strong>
                  {restoreActive ? (
                    <span
                      className="database-backups-live-light"
                      role="status"
                      aria-label="Database restore test is running"
                      title="Database restore test is running"
                    />
                  ) : null}
                </div>
                <strong {...pageStyleTarget('value')}>{rawRestoreDetail || restoreTest.error_message || 'Restore test queued.'}</strong>
              </div>}
            </div>
            <FenBackupPanel />
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
