// Compute success and 100% imported are not workflow completion: cleanup and
// the final database-summary refresh must also finish.
import { uploadStepProgress } from './cloudUploadProgress.js'
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

// Stable identities keep a step's appearance independent of its current state
// and of the separate cancellation workflow.
export const CLOUD_JOB_STEP_IDS = [
  'reserve', 'preflight', 'publish', 'upload', 'start',
  'analyze', 'verify', 'cleanup', 'summaries', 'done',
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
    const phases = ['stopping', 'settling', 'importing', 'planning', 'cleaning', 'logs', 'refreshing', 'finishing', 'complete']
    const names = ['Stop cloud workers and recovery', 'Wait for late uploads', 'Save completed results', 'Prepare cleanup',
      'Clean up cloud resources', 'Clean up eligible logs', 'Refresh database totals', 'Release unfinished FENs', 'Cancelled — cleaned']
    const current = Math.max(0, phases.indexOf(job.stop_cleanup.phase))
    const finished = job.status === 'cancelled' && job.stop_cleanup.phase === 'complete'
    return { done: false, label: `${job.status === 'paused' ? 'Paused — ' : ''}${names[current]}`,
      steps: names.map((title, index) => ({ id: `stop-${phases[index]}`, title, state: finished || index < current ? 'complete'
        : index === current ? job.status === 'paused' ? 'attention' : 'current' : 'pending' })) }
  }
  const done = cloudJobDone(job)
  const runs = job.runs || []
  const active = runs.find((run) => run.status !== 'complete')
  let phase = job.sequence?.automatic ? job.sequence.phase : active?.status
  // The sequence remains "running" while its final run is being verified.
  // Use explicit run states, never infer completion from a 100% import bar.
  const selectedRuns = runs.filter(run => run.positions > 0)
  const awaitingVerification = run => run?.status === 'running' && run.cloud_state === 'SUCCEEDED'
    && Number.isSafeInteger(run.positions) && run.positions > 0
    && Number.isSafeInteger(run.imported) && run.imported === run.positions
  // The controller keeps status="running" until the verification proof is
  // committed. Confirmed cloud success + all imports means verification is
  // CURRENT, not complete; cleanup and Done must remain pending.
  if (phase === 'running' && !job.sequence?.automatic && awaitingVerification(active)) phase = 'finalizing'
  if (job.sequence?.automatic && phase === 'running' && job.sequence.cloud_status === 'SUCCEEDED'
    && selectedRuns.length && selectedRuns.every(run =>
      ['finalizing', 'complete'].includes(run.status) || awaitingVerification(run))) phase = 'finalizing'
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
      id: CLOUD_JOB_STEP_IDS[index],
      title,
      state: done || index < step ? 'complete'
        : index === step ? interrupted ? 'attention' : 'current' : 'pending',
    })),
  }
}

export function cloudStepProgress(job, step) {
  const upload = uploadStepProgress(job, step)
  if (upload) return upload
  if (step.state === 'complete') return { value: 100, detail: 'Complete' }
  if (step.state === 'pending') return { value: 0, detail: 'Pending' }
  // Batch's eligible-log phases follow object/image cleanup. Do not leave its
  // previous image label visible while the separate log controller is working.
  const phase = job.sequence?.phase || cloudActiveRun(job)?.status
  if (step.id === 'cleanup' && ['log_planning', 'log_cleaning'].includes(phase)) {
    return { value: null, detail: phase === 'log_planning'
      ? 'Checking eligible logs' : 'Cleaning up eligible logs', showDetail: true }
  }
  if (['cleanup', 'stop-cleaning'].includes(step.id) && job.cleanup_progress) {
    const { phase, total, removed } = job.cleanup_progress
    if (phase === 'objects' && Number.isSafeInteger(total) && total > 0
      && Number.isSafeInteger(removed) && removed >= 0 && removed <= total) {
      const value = removed / total * 100
      return { value, detail: `${removed.toLocaleString()} / ${total.toLocaleString()} files removed · ${Math.floor(value)}%` }
    }
    const labels = { compute: 'Waiting for cloud resources to stop', objects: 'Checking remaining files',
      image: 'Removing worker image', logs: 'Cleaning up eligible logs' }
    if (labels[phase]) return { value: null, detail: labels[phase], showDetail: true }
  }
  let completed, total, unit
  if (step.id === 'reserve' && job.sequence) {
    completed = job.sequence.reserved
    total = job.sequence.requested
    unit = 'FENs reserved'
  } else if (step.id === 'analyze') {
    // Legacy cycles use their own counts; automatic sequences import several
    // cycles concurrently, so use their overall saved-result count.
    const run = !job.sequence?.automatic && cloudActiveRun(job)
    completed = run?.positions > 0 ? run.imported : job.imported
    total = run?.positions > 0 ? run.positions : job.target
    unit = 'FENs saved locally'
  }
  if (Number.isFinite(completed) && completed >= 0 && Number.isFinite(total) && total > 0) {
    const value = Math.min(100, completed / total * 100)
    // Do not round 99.9% to 100%, nor infer workflow completion from counts.
    return { value, detail: `${completed.toLocaleString()} / ${total.toLocaleString()} ${unit} · ${Math.floor(value)}%` }
  }
  // Stages without observed work counters retain an indeterminate meter. An
  // animated activity bar is honest; elapsed time is not a completion estimate.
  return { value: null, detail: step.state === 'attention' ? 'Needs attention' : 'In progress — percentage unavailable' }
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
