const openStates = new Set(['queued', 'running', 'paused', 'waiting', 'failed'])

export function blockingCloudJob(data) {
  return (data.blocking_job_id && data.jobs.find((job) => job.id === data.blocking_job_id))
    || data.jobs.find((job) => openStates.has(job.status))
    || null
}

export function cloudIsBusy(data) {
  return Boolean(data.cloud_busy || blockingCloudJob(data))
}

export function fenProgress(job) {
  const target = Number.isFinite(job.target) ? Math.max(0, job.target) : 0
  const imported = Number.isFinite(job.imported) ? Math.max(0, Math.min(target, job.imported)) : 0
  return { target, imported, percent: target ? Math.floor(100 * imported / target) : 0 }
}
