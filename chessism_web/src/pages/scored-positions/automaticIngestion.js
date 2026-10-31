import { getPositionJobKey, isTrackedJobActive, parseTimestampSeconds } from '../positions/positionPageSupport.js'

const PIPELINE_KEYS = new Set(['gameParsing', 'fen', 'tablebase'])
const FUNCTION_KEYS = {
  run_create_player_games_job: 'gameParsing',
  run_update_player_games_job: 'gameParsing',
  run_fen_generation_job: 'fen',
  run_fen_pipeline: 'fen',
  run_tablebase_analysis_job: 'tablebase',
}

function trackedJob(job) {
  const failed = job.result?.success === false || job.result?.result?.success === false
  const status = failed ? { ...job, status: 'failed', progress: { ...job.progress, phase: 'failed' } } : job
  const phase = status.progress?.phase || status.status
  return {
    jobId: job.job_id, status,
    error: ['failed', 'unavailable'].includes(phase)
      ? status.progress?.detail || 'Automatic pipeline stage failed.' : '',
  }
}

// Keep only the latest job for each automatic stage, never Stockfish/cloud jobs.
// A job disappearing from /active is not evidence of success: read its result.
export async function readAutomaticIngestion(previous, { fetchActive, fetchJob, signal }) {
  const payload = await fetchActive({ signal })
  if (!Array.isArray(payload.jobs)) throw new Error('Automatic pipeline status unavailable.')
  const next = { ...previous }
  const currentIds = new Set()
  const jobs = [...payload.jobs].sort((a, b) => (
    (parseTimestampSeconds(a.progress?.updated_at) || 0) - (parseTimestampSeconds(b.progress?.updated_at) || 0)
  ))
  for (const job of jobs) {
    const key = getPositionJobKey(job) || FUNCTION_KEYS[job.info?.function]
    if (!job.job_id || !PIPELINE_KEYS.has(key)) continue
    currentIds.add(job.job_id)
    for (const [oldKey, state] of Object.entries(next)) {
      if (oldKey !== key && state.jobId === job.job_id) delete next[oldKey]
    }
    next[key] = trackedJob(job)
  }
  await Promise.all(Object.entries(next).map(async ([key, state]) => {
    if (!isTrackedJobActive(state) || currentIds.has(state.jobId)) return
    try {
      const job = await fetchJob(state.jobId, { signal })
      next[key] = trackedJob({ ...job, job_id: state.jobId })
      if (job.status === 'not_found') delete next[key]
    } catch (error) {
      if (error.status === 404) delete next[key]
      else throw error
    }
  }))
  return next
}
