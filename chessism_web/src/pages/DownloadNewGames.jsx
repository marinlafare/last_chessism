import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useRef, useState } from 'react'
import './download-new-games/downloadNewGames.css'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import AutomaticIngestionPanel from './scored-positions/AutomaticIngestionPanel'
import {
  DOWNLOAD_JOB_STORAGE_KEY,
  UPDATE_JOB_STORAGE_KEY,
  isTerminalJobStatus,
  loadStoredJob,
  beginJobRequest,
  storeJob
} from '../services/gameJobService'
import { watchStatus } from '../services/statusPolling.js'
import { useStoredGameJob } from '../services/useStoredGameJob.js'
import { jobStatusMessage } from '../services/jobOutcome.js'
import { recoverableGameJobs, watchGameJob } from '../services/gameJobPolling'
import {
  downloadPlayerGames,
  fetchActiveGameJobs,
  fetchGameJobStatus,
} from './download-new-games/downloadNewGamesApi'

function DownloadNewGames() {
  const [downloadPlayer, setDownloadPlayer] = useState('')
  const [downloadResult, setDownloadResult] = useState('')
  const [downloadLoading, setDownloadLoading] = useState(false)
  const downloadJob = useStoredGameJob(DOWNLOAD_JOB_STORAGE_KEY)
  const updateJob = useStoredGameJob(UPDATE_JOB_STORAGE_KEY)
  const mounted = useRef(true)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  const [recoveringJobs, setRecoveringJobs] = useState(true)
  const [recoveryMessage, setRecoveryMessage] = useState('')
  // Terminal callbacks release the shared identity. Old presentation status must
  // never make a newly adopted identity look idle.
  const downloadJobActive = Boolean(downloadJob?.jobId)
  const updateJobActive = Boolean(updateJob?.jobId)
  const actionBusy = recoveringJobs || downloadLoading || downloadJobActive || updateJobActive

  useEffect(() => {
    return watchStatus({
      fetcher: fetchActiveGameJobs, requestTimeoutMs: 10000, intervalMs: 2500, maxBackoffMs: 15000,
      onData(payload) {
        const recovered = recoverableGameJobs(payload.jobs)
        if (recovered.download) {
          const { job, status } = recovered.download
          if (!loadStoredJob(DOWNLOAD_JOB_STORAGE_KEY)) {
            storeJob(DOWNLOAD_JOB_STORAGE_KEY, job)
          }
          setDownloadPlayer(current => current || job.playerName)
        }
        if (recovered.update) {
          const { job, status } = recovered.update
          if (!loadStoredJob(UPDATE_JOB_STORAGE_KEY)) {
            storeJob(UPDATE_JOB_STORAGE_KEY, job)
          }
        }
        setRecoveringJobs(false)
        setRecoveryMessage('')
        return false
      },
      onError: () => setRecoveryMessage('Unable to check active downloads right now. Retrying automatically…'),
    })
  }, [])

  useEffect(() => {
    if (!downloadJob?.jobId) return undefined
    return watchGameJob({
      jobId: downloadJob.jobId, fetchStatus: fetchGameJobStatus,
      onStatus: status => {
        if (loadStoredJob(DOWNLOAD_JOB_STORAGE_KEY)?.jobId !== downloadJob.jobId) return
        if (status.status === 'not_found') {
          setDownloadResult('The server no longer has this job’s status. Check the player’s games before starting another download.')
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, null, { expectedJobId: downloadJob.jobId })
        } else if (isTerminalJobStatus(status)) {
          setDownloadResult(jobStatusMessage(status, 'Games download'))
          storeJob(DOWNLOAD_JOB_STORAGE_KEY, null, { expectedJobId: downloadJob.jobId })
        } else {
          setDownloadResult(jobStatusMessage(status, 'Games download'))
        }
      },
      onUnavailable: () => {
        if (loadStoredJob(DOWNLOAD_JOB_STORAGE_KEY)?.jobId === downloadJob.jobId) setDownloadResult('Status connection interrupted. The download may still be running; reconnecting automatically…')
      },
    })
  }, [downloadJob?.jobId])

  useEffect(() => {
    if (!updateJob?.jobId) return undefined
    // Updates are started in the player hero, but still prevent overlapping downloads.
    return watchGameJob({
      jobId: updateJob.jobId, fetchStatus: fetchGameJobStatus,
      onStatus: status => {
        if (loadStoredJob(UPDATE_JOB_STORAGE_KEY)?.jobId !== updateJob.jobId) return
        if (status.status === 'not_found' || isTerminalJobStatus(status)) {
          storeJob(UPDATE_JOB_STORAGE_KEY, null, { expectedJobId: updateJob.jobId })
        }
      },
      // A disconnected status request must not unblock another ingestion.
      onUnavailable: () => {},
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
    const request = beginJobRequest(DOWNLOAD_JOB_STORAGE_KEY)
    try {
      const payload = await downloadPlayerGames(player)
      if (payload.job_id) {
        const job = { jobId: payload.job_id, playerName: payload.player_name || player }
        if (!storeJob(DOWNLOAD_JOB_STORAGE_KEY, job, { request }) || !mounted.current) return
        setDownloadResult(payload.message || 'Games download started.')
      } else if (mounted.current) {
        setDownloadResult(payload.message || 'Games download started.')
      }
    } catch (error) {
      if (mounted.current) setDownloadResult(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      if (mounted.current) setDownloadLoading(false)
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
              <p {...pageStyleTarget('text')} className="result-line">{downloadResult || '-'}</p>
            </article>
          </section>
          <AutomaticIngestionPanel />
        </main>
      </div>
    </div>
  )
}

export default DownloadNewGames
