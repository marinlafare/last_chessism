export const timestamp = value => {
  if (value == null) return null
  const ms = typeof value === 'number' ? value * 1000 : Date.parse(value)
  return Number.isFinite(ms) ? ms : null
}

export function elapsedLabel(started, finished, now = Date.now()) {
  const start = timestamp(started), end = timestamp(finished) ?? now
  if (start == null) return null
  const seconds = Math.max(0, Math.floor((end - start) / 1000))
  const hours = Math.floor(seconds / 3600), minutes = Math.floor(seconds / 60) % 60
  return `${hours ? `${hours}h ` : ''}${minutes}m ${seconds % 60}s`
}

const phases = {
  reserve: ['selecting', 'prepare_inventory', 'prepare_claims', 'prepare_persist'],
  preflight: ['preparing', 'prepare_requirements', 'prepare_image_check'],
  publish: ['publishing'], upload: ['uploading', 'replanning'],
  start: ['submitting', 'run_creating', 'run_start', 'run_discover'],
  analyze: ['running', 'cloud_queued'], verify: ['finalizing', 'verify_task_artifacts', 'verify_unit_artifacts'],
  cleanup: ['cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning'],
  summaries: ['refreshing', 'global_summaries', 'summary_compaction'],
}

export function cloudStepTiming(job, step, runId) {
  if (step.state === 'pending') return {}
  const names = phases[step.id] || [step.id.replace('stop-', 'cancel_')]
  const rows = (job.phase_timings || []).filter(row => names.includes(row.phase)
    && (!runId || !row.run_id || row.run_id === runId))
  const starts = rows.map(row => timestamp(row.started_at)).filter(value => value != null)
  const ends = rows.map(row => timestamp(row.finished_at)).filter(value => value != null)
  const started = starts.length ? new Date(Math.min(...starts)).toISOString()
    : step.id === 'reserve' ? job.created_at : null
  // Completed steps never keep counting. Missing history is not fabricated.
  const finished = step.state === 'complete' && ends.length ? new Date(Math.max(...ends)).toISOString() : null
  return { started, finished }
}
