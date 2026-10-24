import { cloudEtaPhase, estimateCloudEta, formatCloudEta } from './cloudEta.js'
import { cloudJobDone } from '../positions/cloudJobProgress.js'

const tailPhases = ['finalizing', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing']
const backend = job => job.selection?.backend || 'batch_spot'
const timestamp = value => value ? Date.parse(value) : NaN
const first = values => values.length ? Math.min(...values) : NaN
const last = values => values.length ? Math.max(...values) : NaN

export function comparableCloudJobs(job, jobs) {
  return jobs.filter(other => other.id !== job.id && cloudJobDone(other)
    && backend(other) === backend(job)
    && Boolean(other.sequence?.automatic) === Boolean(job.sequence?.automatic)
    && other.target > 0 && other.target >= job.target / 4 && other.target <= job.target * 4)
    .sort((a, b) => Math.abs(Math.log(a.target / job.target)) - Math.abs(Math.log(b.target / job.target))
      || timestamp(b.created_at) - timestamp(a.created_at)).slice(0, 3)
}

function rootPhases(job, usage) {
  const roots = new Set(job.runs.map(run => run.id))
  return (usage?.phases || []).filter(row => !row.run_id || roots.has(row.run_id))
}

// Use wall-clock phase boundaries, not sums of overlapping per-VM or per-task
// durations. Earlier tasks can wait in finalizing while later tasks compute.
export function completionProfile(job, usage) {
  const phases = rootPhases(job, usage)
  if (phases.some(row => tailPhases.includes(row.phase) && row.outcome === 'failed')) return null
  const starts = phase => phases.filter(row => row.phase === phase && row.outcome === 'complete')
    .map(row => timestamp(row.started_at)).filter(Number.isFinite)
  const boundaries = [last(starts('finalizing')), ...tailPhases.slice(1).map(phase => first(starts(phase)))]
  const finished = last(phases.filter(row => row.phase === 'global_summaries' && row.outcome === 'complete')
    .map(row => timestamp(row.finished_at)).filter(Number.isFinite))
  boundaries.push(finished)
  if (!boundaries.every(Number.isFinite) || boundaries.some((at, i) => i > 0 && at < boundaries[i - 1])) return null
  return Object.fromEntries(tailPhases.map((phase, i) => [phase, (boundaries[i + 1] - boundaries[i]) / 1000]))
}

function phaseStarted(job, usage, phase) {
  const phases = rootPhases(job, usage)
  const matches = phases.filter(row => row.phase === phase).map(row => timestamp(row.started_at)).filter(Number.isFinite)
  return phase === 'finalizing' ? last(matches) : first(matches)
}

export function estimateCloudCompletion(history, job, options, context) {
  const unknown = label => ({ state: 'pending', label, seconds: null })
  const { now, available, lastSuccessAt } = options
  if (!available || !lastSuccessAt || now - lastSuccessAt > 30000) return unknown('Unavailable')
  if (cloudJobDone(job)) return { state: 'complete', label: 'Done', seconds: 0 }
  if (['paused', 'failed', 'waiting', 'cancelled'].includes(job.status)) return unknown('Paused')
  if (!context || context.jobId !== job.id) return unknown('Loading history…')
  // A timing request failure must not silently become an analysis-only ETA.
  if (context.error || now - context.observedAt > 90000) return unknown('Unavailable')
  const sample = context.completed.map(({ job: prior, usage }) => ({ prior, profile: completionProfile(prior, usage) }))
    .find(entry => entry.profile)
  if (!sample) return unknown('Not enough history')
  const ratio = job.target / sample.prior.target
  const budgets = tailPhases.map(phase => sample.profile[phase]
    * (['finalizing', 'cleaning', 'refreshing'].includes(phase) ? ratio : 1))
  const phase = cloudEtaPhase(job)
  const tailIndex = tailPhases.indexOf(phase === 'cleanup_planning' ? 'finalizing' : phase)
  let seconds
  if (tailIndex >= 0) {
    const started = phaseStarted(job, context.current, tailPhases[tailIndex])
    if (!Number.isFinite(started)) return unknown('Updating estimate…')
    const elapsed = Math.max(0, now - started) / 1000
    if (elapsed > budgets[tailIndex] * 1.25 + 30) return unknown('Recalculating…')
    seconds = Math.max(0, budgets[tailIndex] - elapsed) + budgets.slice(tailIndex + 1).reduce((a, b) => a + b, 0)
  } else if (phase === 'running') {
    let remaining = 0
    if (job.imported < job.target) {
      const recent = estimateCloudEta(history, job, options)
      if (recent.label === 'Recalculating…') return unknown('Recalculating…')
      if (recent.seconds != null) remaining = recent.seconds
      else {
        // Durable baseline works immediately after a page reload. It includes
        // the actual wall time for computing and importing the current job.
        const started = phaseStarted(job, context.current, 'running')
        const elapsed = (now - started) / 1000
        if (!Number.isFinite(elapsed) || elapsed < 60 || job.imported <= 0) return unknown('Calculating…')
        remaining = (job.target - job.imported) * elapsed / job.imported
      }
    }
    seconds = remaining + budgets.reduce((a, b) => a + b, 0)
  } else {
    return unknown('Waiting to start…')
  }
  // Completion is confirmed by the job state, never by the estimated clock.
  seconds = Math.max(1, seconds)
  return { state: 'estimating', label: formatCloudEta(seconds), seconds, referenceJob: sample.prior.id }
}
