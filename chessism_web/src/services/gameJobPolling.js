import { isTerminalJobStatus } from './gameJobService.js'
import { watchStatus } from './statusPolling.js'

export const isMissingJobResponse = (error, jobId) => error?.status === 404
  && error.payload?.job_id === jobId && error.payload.status === 'not_found'

// A transport failure says nothing about the worker's state. Only an actual
// job response (including the API's explicit not_found payload) can end tracking.
export function watchGameJob({
  jobId, fetchStatus, onStatus, onUnavailable,
  setTimer = setTimeout, clearTimer = clearTimeout,
}) {
  return watchStatus({
    fetcher: options => fetchStatus(jobId, options),
    intervalMs: 1500, maxBackoffMs: 15000, requestTimeoutMs: 10000,
    setTimer, clearTimer,
    onData(status) {
      onStatus(status)
      return !isTerminalJobStatus(status)
    },
    onError(error) {
      if (isMissingJobResponse(error, jobId)) {
        onStatus(error.payload)
        return false
      }
      onUnavailable(error)
    },
  })
}

export function recoverableGameJobs(jobs = []) {
  const found = {}
  for (const status of jobs) {
    if (!status?.job_id || isTerminalJobStatus(status)) continue
    const type = ({
      run_create_player_games_job: 'download',
      run_update_player_games_job: 'update',
    })[status.info?.function]
    if (!type || found[type]) continue
    found[type] = {
      job: {
        jobId: status.job_id,
        playerName: status.progress?.player_name || status.info?.args?.[0]?.player_name || '',
      },
      status,
    }
  }
  return found
}
