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
  cloud_queued: 5, replanning: 3, selecting: 0,
  cleanup_planning: 7, cleaning: 7, log_planning: 7, log_cleaning: 7,
  refreshing: 8, complete: 8,
}

const liveCloudStates = new Set(['RUNNING', 'QUEUED', 'SCHEDULED', 'RETRYING', 'RECOVERING'])
const terminalCloudStates = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'STOPPED'])
const sharedFinalization = new Set(['cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete'])

// Automatic sequences compute ahead of local imports. A completed cloud task
// can stay "running" during catch-up or "finalizing" until shared cleanup;
// neither should hide the later task that Google is actually executing.
export function cloudActiveRun(job) {
  const runs = job.runs || []
  if (!job.sequence?.automatic) return runs.find(run => run.status !== 'complete')
  if (sharedFinalization.has(job.sequence.phase) || cloudJobDone(job)) return undefined
  const pending = runs.filter(run => run.positions > 0 && !['complete', 'finalizing'].includes(run.status))
  const mayRun = !terminalCloudStates.has(job.sequence.cloud_status)
  if (mayRun) {
    const executing = pending.find(run => liveCloudStates.has(run.cloud_state))
    if (executing) return executing
  }
  const failed = pending.find(run => ['FAILED', 'CANCELLED'].includes(run.cloud_state) || run.status === 'failed')
  if (failed) return failed
  if (mayRun) {
    const queued = pending.find(run => run.status === 'cloud_queued' || run.cloud_state === 'PENDING')
    if (queued) return queued
  }
  return pending.find(run => run.status !== 'cloud_queued' && run.cloud_state !== 'PENDING')
}

export function cloudTaskLabel(job, run) {
  const task = `task ${job.runs.indexOf(run) + 1} of ${job.sequence.times}`
  const state = run.cloud_state
  if (job.sequence.phase === 'running') {
    if (!terminalCloudStates.has(job.sequence.cloud_status)) {
      if (liveCloudStates.has(state)) return `Cloud processing: ${task} — ${state}`
      if (state === 'PENDING' || run.status === 'cloud_queued') return `Next cloud task: ${task} — waiting to start`
    }
    if (state === 'SUCCEEDED') return `${run.imported < run.positions ? 'Local import' : 'Local verification'}: ${task} — cloud analysis finished`
    if (['FAILED', 'CANCELLED'].includes(state)) return `Cloud ${task} — ${state}`
  }
  return `Local processing: ${task} — ${cloudJobProgress(job).label}`
}

export function cloudJobProgress(job) {
  if (job.stop_cleanup) {
    const phases = ['stopping', 'importing', 'planning', 'cleaning', 'logs', 'refreshing', 'finishing', 'complete']
    const names = ['Stop cloud workers and recovery', 'Save completed results', 'Prepare cleanup',
      'Clean up cloud resources', 'Clean up eligible logs', 'Refresh database totals', 'Release unfinished FENs', 'Cancelled — cleaned']
    const current = Math.max(0, phases.indexOf(job.stop_cleanup.phase))
    const finished = job.status === 'cancelled' && job.stop_cleanup.phase === 'complete'
    return { done: false, label: `${job.status === 'paused' ? 'Paused — ' : ''}${names[current]}`,
      steps: names.map((title, index) => ({ title, state: finished || index < current ? 'complete'
        : index === current ? job.status === 'paused' ? 'attention' : 'current' : 'pending' })) }
  }
  const done = cloudJobDone(job)
  const runs = job.runs || []
  const active = runs.find((run) => run.status !== 'complete')
  let phase = job.sequence?.automatic ? job.sequence.phase : active?.status
  // The sequence remains "running" while its final run is being verified.
  // Use explicit run states, never infer completion from a 100% import bar.
  const selectedRuns = runs.filter(run => run.positions > 0)
  if (job.sequence?.automatic && phase === 'running' && selectedRuns.length
    && selectedRuns.every(run => ['finalizing', 'complete'].includes(run.status))) phase = 'finalizing'
  const step = done ? 9 : phase ? (phaseSteps[phase] ?? 0) : runs.length ? 8 : 0
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
  if (job.status === 'cancelled') return { state: 'idle', label: job.stop_cleanup?.phase === 'complete' ? 'Cancelled — cleaned; saved results retained' : 'Cancelled' }
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
