// Upload telemetry is optional: a reader failure must not hide job/stop status.
export async function attachUploadProgress(data, fetchProgress, options) {
  return { ...data, jobs: await Promise.all(data.jobs.map(async job => {
    if (!['running', 'paused'].includes(job.status) || job.stop_cleanup
        || (job.sequence ? job.sequence.phase !== 'uploading'
          : !job.runs?.some(run => run.status === 'uploading'))) return job
    try {
      return { ...job, upload_progress: await fetchProgress(job.id, options) }
    } catch {
      return { ...job, upload_progress: { state: 'unavailable' } }
    }
  })) }
}

export function uploadStepProgress(job, step) {
  if (step.id !== 'upload' || step.state === 'pending') return null
  let total, completed
  if (step.state === 'complete') {
    total = job.runs?.reduce((sum, run) => sum + (Number.isSafeInteger(run.positions) ? run.positions : 0), 0)
    completed = total
  } else if (job.upload_progress?.state === 'ready') {
    total = job.upload_progress.total_fens
    completed = job.upload_progress.uploaded_fens
  }
  if (!Number.isSafeInteger(total) || total <= 0 || !Number.isSafeInteger(completed)
      || completed < 0 || completed > total) return null
  const value = completed / total * 100
  return { value, detail: `${completed.toLocaleString()} / ${total.toLocaleString()} FENs uploaded · ${Math.floor(value)}%` }
}
