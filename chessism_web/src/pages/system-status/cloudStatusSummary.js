import { blockingCloudJob, cloudIsBusy, fenProgress } from '../positions/cloudJobState.js'

const phaseLabels = {
  selecting: 'Preparing', preparing: 'Preparing', publishing: 'Publishing', uploading: 'Uploading',
  submitting: 'Starting', run_creating: 'Starting', run_start: 'Starting', run_discover: 'Starting',
  running: 'Active', cloud_queued: 'Queued', replanning: 'Preparing', finalizing: 'Verifying',
  cleanup_planning: 'Cleaning', cleaning: 'Cleaning', log_planning: 'Cleaning', log_cleaning: 'Cleaning',
  refreshing: 'Finalizing',
}

export function cloudStatusSummary(data, { error = null, loading = false } = {}) {
  const job = data ? blockingCloudJob(data) : null
  const busy = data && cloudIsBusy(data)
  const isRun = job?.selection.backend === 'cloud_run'
  // Requested concurrency, not a live VM inventory or measured CPU utilization.
  // These match the fixed Batch Spot and Cloud Run worker profiles.
  const missing = !data || (busy && !job)
  const vms = missing ? null : job && !isRun ? Number(job.selection.n_vms || 1) : 0
  const cpus = missing ? null : job ? isRun ? Number(job.selection.n_cpus || 1) : vms * 16 : 0
  const phase = job?.sequence?.automatic ? job.sequence.phase : job?.runs?.find(run => run.status !== 'complete')?.status
  let activity = job ? phaseLabels[phase] || (job.status === 'queued' ? 'Queued' : 'Active') : busy ? 'Active' : 'Idle'
  let tone = busy ? 'queue-active' : 'queue-idle'
  let notice = null
  if (job && ['failed', 'paused', 'waiting'].includes(job.status)) {
    activity = { failed: 'Failed', paused: 'Paused', waiting: 'Waiting' }[job.status]
    tone = job.status === 'failed' ? 'queue-stale' : 'queue-active'
  }
  if (data && !data.controller_online) {
    activity = 'Controller offline'; tone = 'queue-stale'
    notice = 'Google work may continue; progress may be outdated.'
  }
  if (error) {
    activity = data ? 'Stale' : 'Unavailable'; tone = 'queue-stale'
    notice = data ? 'Refresh failed · last known progress' : 'Cloud status could not be loaded.'
  } else if (!data) {
    activity = loading ? 'Checking…' : 'Unknown'; tone = 'cloud-status-unknown'
  }
  return { job, isRun, vms, cpus, activity, tone, notice, progress: fenProgress(job || {}),
    controller: error ? 'Unknown' : !data ? 'Checking…' : data.controller_online ? 'Online' : 'Offline' }
}
