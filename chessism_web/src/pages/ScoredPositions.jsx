import { useEffect, useState } from 'react'
import './scored-positions/scoredPositions.css'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { formatNumber } from '../utils/formatters'
import AppearanceChartPanel from './scored-positions/AppearanceChartPanel'
import RatingChartDisclosure from './scored-positions/RatingChartDisclosure'
import AutomaticIngestionPanel from './scored-positions/AutomaticIngestionPanel'
import { styleTarget } from '../appearance/styleCore'
import {
  fetchBackupJobStatus,
  fetchFenAnalysisBackups,
  fetchScoredGamesOverview,
  fetchScoredOverview,
  queueFenAnalysisBackup,
  queueFenAnalysisRestore,
} from './scored-positions/scoredPositionsApi'

const BACKUP_JOB_STORAGE_KEY = 'chessism-fen-analysis-backup-job'
const TERMINAL_JOB_PHASES = new Set(['complete', 'failed'])

const formatBytes = (value) => {
  const bytes = Number(value || 0)
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const unitIndex = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const amount = bytes / (1024 ** unitIndex)
  return `${amount.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function ScoredPositions() {
  const [overview, setOverview] = useState(null)
  const [gameOverview, setGameOverview] = useState(null)
  const [error, setError] = useState('')
  const [gameOverviewError, setGameOverviewError] = useState('')
  const [reloadToken, setReloadToken] = useState(0)
  const [backups, setBackups] = useState([])
  const [backupLocation, setBackupLocation] = useState('/home/jon/Desktop/workshop/db_backups/chessism')
  const [backupError, setBackupError] = useState('')
  const [backupJob, setBackupJob] = useState(null)

  const latestBackup = backups[0] || null
  const backupJobActive = Boolean(
    backupJob?.jobId && !TERMINAL_JOB_PHASES.has(backupJob.phase)
  )

  useEffect(() => {
    const controller = new AbortController()

    const load = async (fetcher, setData, setLoadError) => {
      try {
        const payload = await fetcher({ signal: controller.signal })
        if (controller.signal.aborted) return
        setData(payload)
        setLoadError('')
      } catch (loadError) {
        if (!controller.signal.aborted) {
          setLoadError(loadError.message || 'Summary unavailable.')
        }
      }
    }

    load(fetchScoredOverview, setOverview, setError)
    load(fetchScoredGamesOverview, setGameOverview, setGameOverviewError)
    return () => controller.abort()
  }, [reloadToken])

  useEffect(() => {
    const controller = new AbortController()

    fetchFenAnalysisBackups({ signal: controller.signal })
      .then((payload) => {
        setBackups(Array.isArray(payload.backups) ? payload.backups : [])
        if (payload.storage_location) setBackupLocation(payload.storage_location)
        setBackupError('')
      })
      .catch((loadError) => {
        if (loadError?.name !== 'AbortError') {
          setBackupError(loadError.message || 'FEN-analysis backups unavailable.')
        }
      })

    return () => controller.abort()
  }, [reloadToken])

  useEffect(() => {
    try {
      const storedJob = JSON.parse(window.localStorage.getItem(BACKUP_JOB_STORAGE_KEY) || 'null')
      if (storedJob?.jobId) setBackupJob(storedJob)
    } catch {
      window.localStorage.removeItem(BACKUP_JOB_STORAGE_KEY)
    }
  }, [])

  useEffect(() => {
    const jobId = backupJob?.jobId
    if (!jobId || TERMINAL_JOB_PHASES.has(backupJob.phase)) return undefined

    const controller = new AbortController()
    let timerId

    const poll = async () => {
      try {
        const payload = await fetchBackupJobStatus(jobId, { signal: controller.signal })
        const progress = payload.progress || {}
        const resultEnvelope = payload.result || null
        const result = progress.result || resultEnvelope?.result || null
        let phase = progress.phase || payload.status || 'queued'

        if (resultEnvelope?.success === false) phase = 'failed'
        if (payload.status === 'complete') {
          phase = resultEnvelope?.success === false ? 'failed' : 'complete'
        }

        const detail = progress.detail
          || (phase === 'failed' && result?.message)
          || (phase === 'complete' ? 'FEN-analysis operation completed.' : `Job is ${phase}.`)
        setBackupError('')
        setBackupJob((current) => (
          current?.jobId === jobId
            ? { ...current, phase, detail, result, processed: progress.processed,
                bytes: progress.bytes, elapsed: progress.elapsed_seconds }
            : current
        ))

        if (TERMINAL_JOB_PHASES.has(phase)) {
          window.localStorage.removeItem(BACKUP_JOB_STORAGE_KEY)
          setReloadToken((current) => current + 1)
          return
        }
        timerId = window.setTimeout(poll, 2000)
      } catch (pollError) {
        if (pollError?.name === 'AbortError') return
        if (pollError?.status === 404) {
          window.localStorage.removeItem(BACKUP_JOB_STORAGE_KEY)
          setBackupJob(null)
          setBackupError('The previous backup job status expired. The saved-backup list was refreshed.')
          setReloadToken((current) => current + 1)
          return
        }
        setBackupError(pollError.message || 'Unable to read backup job status.')
        timerId = window.setTimeout(poll, 3000)
      }
    }

    poll()
    return () => {
      controller.abort()
      window.clearTimeout(timerId)
    }
  }, [backupJob?.jobId, backupJob?.phase])

  const rememberBackupJob = (job) => {
    setBackupJob(job)
    window.localStorage.setItem(BACKUP_JOB_STORAGE_KEY, JSON.stringify(job))
  }

  const saveFenAnalysis = async () => {
    setBackupError('')
    try {
      const payload = await queueFenAnalysisBackup()
      rememberBackupJob({
        jobId: payload.job_id,
        kind: 'backup',
        phase: 'queued',
        detail: payload.message || 'FEN-analysis backup update queued.'
      })
    } catch (saveError) {
      setBackupError(saveError.message || 'Unable to queue the FEN-analysis backup.')
    }
  }

  const restoreLatestBackup = async () => {
    if (!latestBackup) return
    const confirmed = window.confirm(
      `Restore analyzed values from ${latestBackup.filename}? Existing matching FEN results will be overwritten.`
    )
    if (!confirmed) return

    setBackupError('')
    try {
      const payload = await queueFenAnalysisRestore(latestBackup.filename)
      rememberBackupJob({
        jobId: payload.job_id,
        kind: 'restore',
        filename: latestBackup.filename,
        phase: 'queued',
        detail: payload.message || 'FEN-analysis restore queued.'
      })
    } catch (restoreError) {
      setBackupError(restoreError.message || 'Unable to queue the FEN-analysis restore.')
    }
  }

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      if (document.visibilityState === 'visible') {
        setReloadToken((current) => current + 1)
      }
    }, 15000)

    return () => window.clearInterval(intervalId)
  }, [])

  return (
    <div {...styleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <main className="scored-positions-main">
          <section className="scored-top-grid">
            <section {...styleTarget('metrics.panel')} className="scored-positions-panel scored-engine-panel">
              {error ? <div className="status-banner warn">{error}</div> : null}
              {gameOverviewError ? <div className="status-banner warn">{gameOverviewError}</div> : null}

              <div className="scored-coverage-grid">
                <article {...styleTarget('metric.scored_positions')} className="metric-card">
                  <span {...styleTarget('metric.title')}>Positions</span>
                  <strong {...styleTarget('metric.value')} title={overview ? formatNumber(overview.scored_positions) : undefined}>{overview ? `${formatNumber(overview.scored_positions / 1000000, 1)} M` : '-'}</strong>
                </article>
                <article {...styleTarget('metric.unscored_positions')} className="metric-card">
                  <span {...styleTarget('metric.title')}>Unscored</span>
                  <strong {...styleTarget('metric.value')} title={overview ? formatNumber(overview.unscored_fens) : undefined}>{overview ? `${formatNumber(overview.unscored_fens / 1000000, 1)} M` : '-'}</strong>
                </article>
                <article {...styleTarget('metric.games')} className="metric-card">
                  <span {...styleTarget('metric.title')}>Games</span>
                  <strong {...styleTarget('metric.value')}>{gameOverview ? formatNumber(gameOverview.fully_analyzed_games) : '-'}</strong>
                </article>
                <article {...styleTarget('metric.unscored_games')} className="metric-card">
                  <span {...styleTarget('metric.title')}>Unscored</span>
                  <strong {...styleTarget('metric.value')}>{gameOverview ? formatNumber(gameOverview.incomplete_games) : '-'}</strong>
                </article>
              </div>
            </section>

            <AppearanceChartPanel reloadToken={reloadToken} />
          </section>

          <section {...styleTarget('cuda.panel')} className="scored-positions-panel scored-rating-advantage-panel cuda-reservation" aria-label="Advantage by rating">
            <h2 {...styleTarget('panel.title')} className="panel-title">Advantage by rating</h2>
            <div className="cuda-reservation-content" role="note">
              <svg className="cuda-reservation-icon" viewBox="0 0 48 48" aria-hidden="true" focusable="false">
                <circle cx="24" cy="24" r="19" />
                <path d="m17 17 14 14 M31 17 17 31" />
              </svg>
              <span>reserved for cuda</span>
            </div>
          </section>

          <RatingChartDisclosure />

          <section {...styleTarget('backup.panel')} className="scored-positions-panel analysis-backup-block" aria-label="Fen Backup">
            <div className="analysis-backup-head">
              <h2 {...styleTarget('panel.title')} className="panel-title">Fen Backup</h2>
              <div className="analysis-backup-actions">
                <button
                  className="btn btn-primary btn-inline"
                  type="button"
                  disabled={backupJobActive}
                  onClick={saveFenAnalysis}
                  title="Save new or updated FEN analysis to the backup"
                >
                  backup
                </button>
                <button
                  className="btn btn-secondary btn-inline"
                  type="button"
                  disabled={backupJobActive || !latestBackup}
                  onClick={restoreLatestBackup}
                  title="Restore the latest saved FEN analysis; confirmation required"
                >
                  reroll
                </button>
              </div>
            </div>
            <div className="analysis-backup-location">
              <span>Location</span>
              <code>{backupLocation}</code>
            </div>
            <div className="analysis-backup-latest" aria-label="Latest FEN backup">
              <h3 className="analysis-backup-latest-title" title={latestBackup?.filename}>latest</h3>
              <dl className="analysis-backup-details">
                <div>
                  <dt>fens</dt>
                  <dd data-backup-fens title={latestBackup?.format_version === 3
                    ? 'Saved FEN result records; an updated FEN can have multiple versions in incremental backups.' : undefined}>
                    {latestBackup ? formatNumber(latestBackup.records) : '—'}
                    {latestBackup?.format_version === 3 && <small> records</small>}
                  </dd>
                </div>
                <div><dt>size</dt><dd data-backup-size>{latestBackup ? formatBytes(latestBackup.bytes) : '—'}</dd></div>
                <div><dt>date</dt><dd data-backup-date>{latestBackup?.updated_at || latestBackup?.created_at
                  ? <time dateTime={latestBackup.updated_at || latestBackup.created_at}>
                    {new Date(latestBackup.updated_at || latestBackup.created_at).toLocaleString()}
                  </time> : '—'}</dd></div>
              </dl>
              {!latestBackup && <p className="analysis-backup-empty">No saved FEN analysis yet.</p>}
            </div>
            {backupJob ? (
              <div
                className={`analysis-backup-status ${backupJob.phase === 'failed' ? 'failed' : ''}`}
                aria-live="polite"
              >
                <strong>{backupJob.kind === 'restore' ? 'Restore' : 'Backup'}:</strong>
                {' '}{backupJob.detail}
                {Number.isFinite(backupJob.elapsed) && <div className="analysis-backup-metrics" aria-label="Backup progress">
                  <span>Phase: {backupJob.phase.replaceAll('_', ' ')}</span>
                  <span>{formatNumber(backupJob.processed || 0)} result versions processed</span>
                  {backupJob.kind === 'backup' && <span>{formatBytes(backupJob.bytes)} written this update</span>}
                  <span>Elapsed: {Math.floor(backupJob.elapsed / 60)}m {Math.floor(backupJob.elapsed % 60)}s</span>
                </div>}
                {backupJob.result?.missing ? ` ${formatNumber(backupJob.result.missing)} FENs were not present.` : ''}
              </div>
            ) : null}
            {backupError ? <div className="status-banner warn">{backupError}</div> : null}
          </section>
          <AutomaticIngestionPanel />
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default ScoredPositions
