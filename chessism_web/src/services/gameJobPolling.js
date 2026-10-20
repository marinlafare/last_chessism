import { isTerminalJobStatus } from './gameJobService.js'

// A transport failure says nothing about the worker's state. Only an actual
// job response (including the API's explicit not_found payload) can end tracking.
export function watchGameJob({
  jobId, fetchStatus, onStatus, onUnavailable,
  setTimer = setTimeout, clearTimer = clearTimeout,
}) {
  let stopped = false
  let timer
  let controller
  let failures = 0
  const poll = async () => {
    if (stopped) return
    controller = new AbortController()
    const timeout = setTimer(() => controller.abort(), 10000)
    try {
      const status = await fetchStatus(jobId, { signal: controller.signal })
      if (stopped) return
      onStatus(status)
      if (isTerminalJobStatus(status)) return
      failures = 0
    } catch (error) {
      if (stopped) return
      if (error.status === 404 && error.payload?.job_id === jobId && error.payload.status === 'not_found') {
        onStatus(error.payload)
        return
      }
      failures += 1
      onUnavailable(error)
    } finally {
      clearTimer(timeout)
    }
    if (!stopped) timer = setTimer(poll, failures ? Math.min(15000, 1500 * 2 ** Math.min(failures, 4)) : 1500)
  }
  poll()
  return () => {
    stopped = true
    clearTimer(timer)
    controller?.abort()
  }
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
