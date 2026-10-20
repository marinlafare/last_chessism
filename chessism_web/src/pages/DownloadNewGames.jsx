import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import './download-new-games/downloadNewGames.css'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import {
  DOWNLOAD_JOB_STORAGE_KEY,
  UPDATE_JOB_STORAGE_KEY,
  formatStatusMessage,
  isTerminalJobStatus,
  loadStoredJob,
  storeJob
} from '../services/gameJobService'
import { recoverableGameJobs, watchGameJob } from '../services/gameJobPolling'
import {
  downloadPlayerGames,
  fetchActiveGameJobs,
  fetchGameJobStatus,
  updatePlayerGames,
} from './download-new-games/downloadNewGamesApi'

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
        <strong {...pageStyleTarget('value')}>{phase}</strong>
        <span>{percent}%</span>
      </div>
      <div className="job-progress-track">
        <div className="job-progress-fill" style={{ width: `${percent}%` }} />
      </div>
      <p {...pageStyleTarget('text')} className="result-line">{progress.detail || `${processed} of ${total}`}</p>
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
  const [recoveringJobs, setRecoveringJobs] = useState(true)
  const [recoveryMessage, setRecoveryMessage] = useState('')
  const downloadJobActive = downloadJob?.jobId && !isTerminalJobStatus(downloadJobStatus)
  const updateJobActive = updateJob?.jobId && !isTerminalJobStatus(updateJobStatus)
  const actionBusy = recoveringJobs || downloadLoading || updateLoading || downloadJobActive || updateJobActive

  useEffect(() => {
    let cancelled = false
    let timer
    let controller
    const recover = async () => {
      controller = new AbortController()
      const timeout = window.setTimeout(() => controller.abort(), 10000)
      try {
        const payload = await fetchActiveGameJobs({ signal: controller.signal })
        if (cancelled) return
        const recovered = recoverableGameJobs(payload.jobs)
        if (recovered.download) {
          const { job, status } = recovered.download
          setDownloadJob(job)
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, job)
          setDownloadPlayer(current => current || job.playerName)
          setDownloadJobStatus(status)
          setDownloadResult(`Reconnected to the download for ${job.playerName}.`)
        }
        if (recovered.update) {
          const { job, status } = recovered.update
          setUpdateJob(job)
          storeJob(UPDATE_JOB_STORAGE_KEY, job)
          setUpdatePlayer(current => current || job.playerName)
          setUpdateJobStatus(status)
          setUpdateResult(`Reconnected to the update for ${job.playerName}.`)
        }
        setRecoveringJobs(false)
        setRecoveryMessage('')
      } catch {
        if (cancelled) return
        setRecoveryMessage('Unable to check active downloads right now. Retrying automatically…')
        timer = window.setTimeout(recover, 5000)
      } finally {
        window.clearTimeout(timeout)
      }
    }
    recover()
    return () => {
      cancelled = true
      window.clearTimeout(timer)
      controller?.abort()
    }
  }, [])

  useEffect(() => {
    if (!downloadJob?.jobId) return undefined
    return watchGameJob({
      jobId: downloadJob.jobId, fetchStatus: fetchGameJobStatus,
      onStatus: status => {
        setDownloadJobStatus(status)
        if (status.status === 'not_found') {
          setDownloadResult('The server no longer has this job’s status. Check the player’s games before starting another download.')
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, null)
          setDownloadJob(null)
        } else if (isTerminalJobStatus(status)) {
          const failed = status.progress?.phase === 'failed' || status.result?.success === false
          const resultMessage = formatStatusMessage(status.progress?.result || status.progress?.detail || status.result?.result)
          setDownloadResult(resultMessage || (failed ? 'Games download failed.' : 'Games download finished.'))
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, null)
          setDownloadJob(null)
        } else {
          setDownloadResult(status.progress?.detail || `Download for ${downloadJob.playerName} is ${status.status}.`)
        }
      },
      onUnavailable: () => setDownloadResult('Status connection interrupted. The download may still be running; reconnecting automatically…'),
    })
  }, [downloadJob?.jobId])

  useEffect(() => {
    if (!updateJob?.jobId) return undefined
    return watchGameJob({
      jobId: updateJob.jobId, fetchStatus: fetchGameJobStatus,
      onStatus: status => {
        setUpdateJobStatus(status)
        if (status.status === 'not_found') {
          setUpdateResult('The server no longer has this job’s status. Check the player’s games before starting another update.')
          storeJob(UPDATE_JOB_STORAGE_KEY, null)
          setUpdateJob(null)
        } else if (isTerminalJobStatus(status)) {
          const failed = status.progress?.phase === 'failed' || status.result?.success === false
          const resultMessage = formatStatusMessage(status.progress?.result || status.progress?.detail || status.result?.result)
          setUpdateResult(resultMessage || (failed ? 'Games update failed.' : 'Games update finished.'))
          storeJob(UPDATE_JOB_STORAGE_KEY, null)
          setUpdateJob(null)
        } else {
          setUpdateResult(status.progress?.detail || `Update for ${updateJob.playerName} is ${status.status}.`)
        }
      },
      onUnavailable: () => setUpdateResult('Status connection interrupted. The update may still be running; reconnecting automatically…'),
    })
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
    setDownloadJobStatus(null)
    try {
      const payload = await downloadPlayerGames(player)
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
      const payload = await updatePlayerGames(player)
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
    <div {...pageStyleTarget('page')} className="page-frame download-games-page">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section {...pageStyleTarget('panel')} className="games-hero">
            <h1 {...pageStyleTarget('heading')}>Add Games</h1>
          </section>

          {recoveringJobs ? <p {...pageStyleTarget('text')} className="result-line" role="status">
            {recoveryMessage || 'Checking for active downloads…'}
          </p> : null}
          <section {...pageStyleTarget('panel')} className="games-actions" aria-label="Games actions">
            <article {...pageStyleTarget('card')} className="game-card">
              <h2 {...pageStyleTarget('title')}>Download Games of a Player</h2>
              <label {...pageStyleTarget('label')} htmlFor="download-player">Player name</label>
              <input {...pageStyleTarget('field')}
                id="download-player"
                className="text-input"
                type="text"
                value={downloadPlayer}
                onChange={(event) => setDownloadPlayer(event.target.value)}
                onKeyDown={(event) => event.key === 'Enter' && handleDownload()}
                placeholder="e.g. hikaru"
              />
              <button {...pageStyleTarget('button')} className="btn btn-primary" type="button" onClick={handleDownload} disabled={actionBusy}>
                {downloadLoading ? 'Queueing...' : downloadJobActive ? 'Downloading...' : actionBusy ? 'Busy...' : 'Download Games'}
              </button>
              {renderUpdateProgress(downloadJobStatus)}
              <p {...pageStyleTarget('text')} className="result-line">{downloadResult || '-'}</p>
            </article>

            <article {...pageStyleTarget('card')} className="game-card">
              <h2 {...pageStyleTarget('title')}>Update Games of a Player</h2>
              <label {...pageStyleTarget('label')} htmlFor="update-player">Player name</label>
              <input {...pageStyleTarget('field')}
                id="update-player"
                className="text-input"
                type="text"
                value={updatePlayer}
                onChange={(event) => setUpdatePlayer(event.target.value)}
                onKeyDown={(event) => event.key === 'Enter' && handleUpdate()}
                placeholder="e.g. magnuscarlsen"
              />
              <button {...pageStyleTarget('button')} className="btn btn-primary" type="button" onClick={handleUpdate} disabled={actionBusy}>
                {updateLoading ? 'Queueing...' : updateJobActive ? 'Updating...' : actionBusy ? 'Busy...' : 'Update Games'}
              </button>
              {renderUpdateProgress(updateJobStatus)}
              <p {...pageStyleTarget('text')} className="result-line">{updateResult || '-'}</p>
            </article>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default DownloadNewGames
