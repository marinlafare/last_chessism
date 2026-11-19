const TERMINAL_PROGRESS = new Set(['complete', 'failed', 'unavailable', 'not_found', 'cancelled', 'canceled'])
const TERMINAL_JOBS = new Set(['complete', 'failed', 'unavailable', 'not_found', 'cancelled'])

// ARQ completion means a result exists. Redis progress never proves execution.
// Durable cloud workflows have their own outcome contract and do not use this.
export function jobPhase(status = {}) {
  if (status?.result?.success === false) return 'failed'
  if (status?.result?.success === true) return 'complete'
  const queue = status?.status
  if (TERMINAL_JOBS.has(queue) && queue !== 'complete') return queue
  if (queue === 'queued' || queue === 'deferred') return queue
  const progress = status?.progress?.phase
  if (progress && !TERMINAL_PROGRESS.has(progress)) return progress
  return queue === 'complete' ? 'in_progress' : queue || 'queued'
}

export const isTerminalJobStatus = status => TERMINAL_JOBS.has(jobPhase(status))

export function jobFailureDetail(status) {
  const failure = status?.result?.success === false ? status.result.result : null
  if (failure?.type === 'CancelledError' || status?.status === 'cancelled') return 'Job was cancelled.'
  if (jobPhase(status) === 'not_found') return 'The server no longer has this job’s status. Its outcome is unknown.'
  if (status?.result?.success === false) return failure?.message || (typeof failure === 'string' ? failure : '')
    || (status.progress?.phase === 'failed' ? status.progress.detail : '') || 'Job failed.'
  return failure?.message || (typeof failure === 'string' ? failure : '') || status?.progress?.detail || 'Job failed.'
}

export function jobStatusMessage(status, label = 'Job') {
  const phase = jobPhase(status)
  if (isTerminalJobStatus(status) && phase !== 'complete') return jobFailureDetail(status)
  if (phase === 'complete') {
    const result = status.result.result
    if (Number.isInteger(result?.committed_games) && result.committed_games >= 0) {
      const saved = result.committed_games === 0 ? 'No new games saved.' : `${result.committed_games} new games saved.`
      const followup = {
        queued: 'FEN extraction queued.',
        already_active: 'FEN extraction is already running.',
        up_to_date: 'FEN extraction is up to date.',
      }[result.fen_extraction?.status] || 'FEN extraction status is unavailable.'
      return `${saved} ${followup}`
    }
    return result?.message || (typeof result === 'string' ? result : '') || `${label} finished.`
  }
  if (TERMINAL_PROGRESS.has(status?.progress?.phase) || status?.status === 'complete') return 'Waiting for confirmed job outcome.'
  return status?.progress?.detail || `${label} is ${phase}.`
}
