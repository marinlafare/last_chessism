import { getTrackedJobPhase, isTrackedJobActive, parseTimestampSeconds } from './positionPageSupport.js'

// Discovery and per-job reads can arrive in either order. Neither may revive a
// completed job or replace a newer checkpoint with an older snapshot. A failed
// attempt can legitimately return to the queue during recovery.
export function obsoletePositionStatus(state, incoming) {
  if (!state?.status || state.jobId !== incoming?.job_id) return false
  if (getTrackedJobPhase(state) === 'complete'
    && isTrackedJobActive({ jobId: state.jobId, status: incoming })) return true
  const previousAt = parseTimestampSeconds(state.status.progress?.updated_at)
  const incomingAt = parseTimestampSeconds(incoming.progress?.updated_at)
  return previousAt !== null && incomingAt !== null && incomingAt < previousAt
}
