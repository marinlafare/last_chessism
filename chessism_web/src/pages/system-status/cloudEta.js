// ETA is based on observed imports, not an assumed per-CPU Stockfish speed.
export const ETA_WINDOW_MS = 5 * 60 * 1000
export const ETA_MIN_SAMPLE_MS = 60 * 1000
export const ETA_NO_PROGRESS_MS = 2 * 60 * 1000
const MAX_SAMPLES = 80
const finalPhases = new Set(['finalizing', 'cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete'])

export function cloudEtaPhase(job) {
  return job.sequence?.automatic ? job.sequence.phase : job.runs?.find(run => run.status !== 'complete')?.status
}

export function recordCloudEta(previous, job, at) {
  if (!job || job.status !== 'running' || cloudEtaPhase(job) !== 'running' || !Number.isFinite(at)) return null
  const count = Math.max(0, Number(job.imported) || 0)
  const key = [job.id, job.target, job.selection?.n_cpus, job.selection?.n_vms].join(':')
  const last = previous?.samples.at(-1)
  // A new job, changed workload/concurrency, regressed counter or long gap
  // needs a new baseline. Never infer a rate from progress before this visit.
  if (!previous || previous.key !== key || count < last.count || at < last.at
      || at - last.at >= ETA_NO_PROGRESS_MS
      || (count > last.count && at - previous.changedAt >= ETA_NO_PROGRESS_MS)) {
    return { key, startedAt: at, changedAt: at, samples: [{ at, count }] }
  }
  if (at === last.at) return previous
  return { ...previous, changedAt: count > last.count ? at : previous.changedAt,
    samples: [...previous.samples.filter(sample => at - sample.at <= ETA_WINDOW_MS), { at, count }].slice(-MAX_SAMPLES) }
}

export function estimateCloudEta(history, job, { now, lastSuccessAt, available }) {
  const pending = (label, state = 'pending') => ({ label, state, seconds: null, remainingPercent: null })
  if (!available || !lastSuccessAt || now - lastSuccessAt > 30000) return pending('Unavailable', 'unavailable')
  if (['paused', 'waiting', 'failed', 'cancelled'].includes(job.status)) return pending('Paused', 'paused')
  if (finalPhases.has(cloudEtaPhase(job)) || Number(job.imported) >= Number(job.target)) {
    return pending('Finalizing…', 'finalizing')
  }
  if (cloudEtaPhase(job) !== 'running') return pending('Waiting to start…')
  if (!history) return pending('Calculating…')
  const samples = history.samples
  const first = samples[0], last = samples.at(-1)
  if (now - history.changedAt >= ETA_NO_PROGRESS_MS) return pending('Recalculating…')
  const elapsed = last.at - first.at
  const advances = samples.reduce((sum, sample, index) => sum + (index > 0 && sample.count > samples[index - 1].count ? 1 : 0), 0)
  if (elapsed < ETA_MIN_SAMPLE_MS || advances < 2) return pending('Calculating…')
  const rate = (last.count - first.count) / (elapsed / 1000)
  if (!Number.isFinite(rate) || rate <= 0) return pending('Calculating…')
  const remaining = Math.max(0, Number(job.target) - last.count) / rate
  // Never count down to zero while results are still missing. A delayed batch
  // or changed rate will revise this estimate on the next successful poll.
  const seconds = Math.max(1, remaining - Math.max(0, now - last.at) / 1000)
  const observedSeconds = Math.max(0, now - history.startedAt) / 1000
  return { label: formatCloudEta(seconds), state: 'estimating', seconds,
    remainingPercent: 100 * seconds / (observedSeconds + seconds) }
}

export function formatCloudEta(seconds) {
  if (seconds < 60) return '~<1 min'
  const minutes = Math.ceil(seconds / 60)
  const hours = Math.floor(minutes / 60)
  return hours ? `~${hours}h ${minutes % 60}m` : `~${minutes} min`
}
