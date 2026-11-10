import { getTrackedJobPhase, isTrackedJobActive, parseTimestampSeconds } from './positionPageSupport.js'

// Progress timestamps order telemetry, not ARQ outcomes or separate attempts.
export function obsoletePositionStatus(state, incoming) {
  if (!state?.status || state.jobId !== incoming?.job_id) return false
  if (typeof incoming.result?.success === 'boolean') return false
  if (getTrackedJobPhase(state) === 'complete'
    && isTrackedJobActive({ jobId: state.jobId, status: incoming })) return true
  if (incoming.status !== state.status.status) return false
  if (incoming.progress?.attempt_id !== state.status.progress?.attempt_id) return false
  const previousAt = parseTimestampSeconds(state.status.progress?.updated_at)
  const incomingAt = parseTimestampSeconds(incoming.progress?.updated_at)
  return previousAt !== null && incomingAt !== null && incomingAt < previousAt
}
