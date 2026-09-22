import { useEffect, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import {
  DOWNLOAD_JOB_STORAGE_KEY,
  UPDATE_JOB_STORAGE_KEY,
  fetchJobStatus,
  formatStatusMessage,
  isTerminalJobStatus,
  loadStoredJob,
  sendPlayerAction,
  storeJob
} from '../services/gameJobService'

function renderUpdateProgress(status) {
  const progress = status?.progress
  if (!progress) return null

  const total = Math.max(1, Number(progress.total || 1))
  const processed = Math.min(total, Number(progress.processed || 0))
  const percent = Math.min(100, Math.round((processed / total) * 100))
  const phase = progress.phase || status.status || 'working'

  return (
    <div className="job-progress" aria-label="Update progress">
      <div className="job-progress-head">
        <strong>{phase}</strong>
        <span>{percent}%</span>
      </div>
      <div className="job-progress-track">
        <div className="job-progress-fill" style={{ width: `${percent}%` }} />
      </div>
      <p className="result-line">{progress.detail || `${processed} of ${total}`}</p>
    </div>
  )
}

function DownloadNewGames() {
  const [downloadPlayer, setDownloadPlayer] = useState('')
  const [updatePlayer, setUpdatePlayer] = useState('')
  const [downloadResult, setDownloadResult] = useState('')
  const [updateResult, setUpdateResult] = useState('')
  const [downloadLoading, setDownloadLoading] = useState(false)
  const [updateLoading, setUpdateLoading] = useState(false)
  const [downloadJob, setDownloadJob] = useState(() => loadStoredJob(DOWNLOAD_JOB_STORAGE_KEY))
  const [updateJob, setUpdateJob] = useState(() => loadStoredJob(UPDATE_JOB_STORAGE_KEY))
  const [downloadJobStatus, setDownloadJobStatus] = useState(null)
  const [updateJobStatus, setUpdateJobStatus] = useState(null)
  const downloadJobActive = downloadJob?.jobId && !isTerminalJobStatus(downloadJobStatus)
  const updateJobActive = updateJob?.jobId && !isTerminalJobStatus(updateJobStatus)
  const actionBusy = downloadLoading || updateLoading || downloadJobActive || updateJobActive

  useEffect(() => {
    if (!downloadJob?.jobId) return undefined

    let cancelled = false

    const poll = async () => {
      try {
        const status = await fetchJobStatus(downloadJob.jobId)
        if (cancelled) return
        setDownloadJobStatus(status)

        if (isTerminalJobStatus(status)) {
          const resultMessage = formatStatusMessage(status.progress?.result || status.progress?.detail || status.result?.result)
          setDownloadResult(resultMessage || (status.progress?.phase === 'failed' ? 'Games download failed.' : 'Games download finished.'))
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, null)
          setDownloadJob(null)
        }
      } catch (error) {
        if (cancelled) return
        setDownloadJobStatus({ status: 'not_found', progress: { phase: 'failed', detail: error instanceof Error ? error.message : 'Unable to read job status.' } })
        setDownloadResult(error instanceof Error ? error.message : 'Unable to read job status.')
        storeJob(DOWNLOAD_JOB_STORAGE_KEY, null)
        setDownloadJob(null)
      }
    }

    poll()
    const interval = window.setInterval(poll, 1500)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [downloadJob?.jobId])

  useEffect(() => {
    if (!updateJob?.jobId) return undefined

    let cancelled = false

    const poll = async () => {
      try {
        const status = await fetchJobStatus(updateJob.jobId)
        if (cancelled) return
        setUpdateJobStatus(status)

        if (isTerminalJobStatus(status)) {
          const resultMessage = formatStatusMessage(status.progress?.result || status.progress?.detail || status.result?.result)
          setUpdateResult(resultMessage || (status.progress?.phase === 'failed' ? 'Games update failed.' : 'Games update finished.'))
          storeJob(UPDATE_JOB_STORAGE_KEY, null)
          setUpdateJob(null)
        }
      } catch (error) {
        if (cancelled) return
        setUpdateJobStatus({ status: 'not_found', progress: { phase: 'failed', detail: error instanceof Error ? error.message : 'Unable to read job status.' } })
        setUpdateResult(error instanceof Error ? error.message : 'Unable to read job status.')
        storeJob(UPDATE_JOB_STORAGE_KEY, null)
        setUpdateJob(null)
      }
    }

    poll()
    const interval = window.setInterval(poll, 1500)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [updateJob?.jobId])

  const handleDownload = async () => {
    if (actionBusy) return
    const player = downloadPlayer.trim()
    if (!player) {
      setDownloadResult('Enter a player name first.')
      return
    }

    setDownloadLoading(true)
    setDownloadResult('')
    try {
      const payload = await sendPlayerAction('/games', player)
      if (payload.job_id) {
        const job = { jobId: payload.job_id, playerName: payload.player_name || player }
        storeJob(DOWNLOAD_JOB_STORAGE_KEY, job)
        setDownloadJob(job)
        setDownloadResult(payload.message || 'Games download started.')
      } else {
        setDownloadResult(payload.message || 'Games download started.')
      }
    } catch (error) {
      setDownloadResult(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setDownloadLoading(false)
    }
  }

  const handleUpdate = async () => {
    if (actionBusy) return
    const player = updatePlayer.trim()
    if (!player) {
      setUpdateResult('Enter a player name first.')
      return
    }

    setUpdateLoading(true)
    setUpdateResult('')
    setUpdateJobStatus(null)
    try {
      const payload = await sendPlayerAction('/games/update', player)
      if (payload.job_id) {
        const job = { jobId: payload.job_id, playerName: payload.player_name || player }
        storeJob(UPDATE_JOB_STORAGE_KEY, job)
        setUpdateJob(job)
        setUpdateResult(payload.message || 'Games update started.')
      } else {
        setUpdateResult(payload.message || 'Games update started.')
      }
    } catch (error) {
      setUpdateResult(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setUpdateLoading(false)
    }
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section className="games-hero">
            <h1>Add Games</h1>
          </section>

          <section className="games-actions" aria-label="Games actions">
            <article className="game-card">
              <h2>Download Games of a Player</h2>
              <label htmlFor="download-player">Player name</label>
              <input
                id="download-player"
                className="text-input"
                type="text"
                value={downloadPlayer}
                onChange={(event) => setDownloadPlayer(event.target.value)}
                onKeyDown={(event) => event.key === 'Enter' && handleDownload()}
                placeholder="e.g. hikaru"
              />
              <button className="btn btn-primary" type="button" onClick={handleDownload} disabled={actionBusy}>
                {downloadLoading ? 'Queueing...' : downloadJobActive ? 'Downloading...' : actionBusy ? 'Busy...' : 'Download Games'}
              </button>
              {renderUpdateProgress(downloadJobStatus)}
              <p className="result-line">{downloadResult || '-'}</p>
            </article>

            <article className="game-card">
              <h2>Update Games of a Player</h2>
              <label htmlFor="update-player">Player name</label>
              <input
                id="update-player"
                className="text-input"
                type="text"
                value={updatePlayer}
                onChange={(event) => setUpdatePlayer(event.target.value)}
                onKeyDown={(event) => event.key === 'Enter' && handleUpdate()}
                placeholder="e.g. magnuscarlsen"
              />
              <button className="btn btn-primary" type="button" onClick={handleUpdate} disabled={actionBusy}>
                {updateLoading ? 'Queueing...' : updateJobActive ? 'Updating...' : actionBusy ? 'Busy...' : 'Update Games'}
              </button>
              {renderUpdateProgress(updateJobStatus)}
              <p className="result-line">{updateResult || '-'}</p>
            </article>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default DownloadNewGames
