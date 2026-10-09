// Compute success and 100% imported are not workflow completion: cleanup and
// the final database-summary refresh must also finish.
export function cloudJobDone(job) {
  return ['complete', 'limit_reached'].includes(job.status)
    && Array.isArray(job.runs) && job.runs.every((run) => run.status === 'complete')
}

export const CLOUD_JOB_STEPS = [
  'Queue and reserve FENs',
  'Check workspace and quota',
  'Publish worker image',
  'Upload selected FENs',
  'Start cloud workers',
  'Analyze FENs and import results',
  'Verify imported results',
  'Clean up cloud resources and eligible logs',
  'Refresh database totals',
  'Done',
]

const phaseSteps = {
  preparing: 1, publishing: 2, uploading: 3,
  submitting: 4, run_creating: 4, run_start: 4, run_discover: 4,
  running: 5, failed: 5, finalizing: 6,
  cleanup_planning: 7, cleaning: 7, log_planning: 7, log_cleaning: 7,
  refreshing: 8,
}

export function cloudJobProgress(job) {
  const done = cloudJobDone(job)
  const runs = job.runs || []
  const active = runs.find((run) => run.status !== 'complete')
  const step = done ? 9 : active ? (phaseSteps[active.status] ?? 0) : runs.length ? 8 : 0
  const interrupted = ['paused', 'failed', 'waiting', 'cancelled'].includes(job.status)
  const attention = { paused: 'Paused', failed: 'Failed', waiting: 'Waiting', cancelled: 'Cancelled' }[job.status]
  const label = done ? job.status === 'limit_reached' ? 'Done — available FENs only' : 'Done'
    : attention ? `${attention} — ${CLOUD_JOB_STEPS[step]}` : CLOUD_JOB_STEPS[step]
  return {
    done, label,
    steps: CLOUD_JOB_STEPS.map((title, index) => ({
      title,
      state: done || index < step ? 'complete'
        : index === step ? interrupted ? 'attention' : 'current' : 'pending',
    })),
  }
}

export function cloudJobIndicator(job) {
  const { done, label } = cloudJobProgress(job)
  if (done) return {
    state: 'success',
    label: job.status === 'limit_reached'
      ? 'Completed — available FENs analyzed, imported and cleaned'
      : 'Succeeded — analysis, import and cleanup complete',
  }
  if (job.status === 'failed' || job.runs?.some((run) => run.status === 'failed')) {
    return { state: 'error', label: 'Failed — recovery required' }
  }
  if (['paused', 'waiting'].includes(job.status)) return { state: 'attention', label }
  if (['queued', 'running'].includes(job.status)) return { state: 'running', label: `In progress — ${label}` }
  return { state: 'idle', label: job.status === 'cancelled' ? 'Cancelled' : 'Completion not confirmed' }
}

export const CLOUD_DONE_VISIBLE_MS = 10000
export const CLOUD_DONE_FADE_MS = 400

const displayTerminal = (job) => cloudJobDone(job) || job.status === 'cancelled'

// Initial history is hidden immediately. A job seen active on this page gets a
// ten-second completion message. Polling must not restart that countdown.
export function reconcileCloudJobDisplay(previous, jobs, now) {
  return Object.fromEntries(jobs.map((job) => {
    const old = previous[job.id]
    const terminal = displayTerminal(job)
    return [job.id, {
      terminal,
      endedAt: terminal ? old?.terminal ? old.endedAt : old ? now : -Infinity : null,
    }]
  }))
}

export function cloudJobDisplayState(job, entry, now) {
  if (!displayTerminal(job)) return 'visible'
  if (!entry) return 'hidden'
  // The hook will record this newly observed completion after this render.
  if (!entry.terminal) return 'visible'
  const elapsed = now - entry.endedAt
  if (elapsed >= CLOUD_DONE_VISIBLE_MS + CLOUD_DONE_FADE_MS) return 'hidden'
  return elapsed >= CLOUD_DONE_VISIBLE_MS ? 'fading' : 'visible'
}
