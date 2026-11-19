import { getTrackedJobPhase, isTrackedJobActive, parseTimestampSeconds } from './positionPageSupport.js'

// Progress timestamps order telemetry, not ARQ outcomes or separate attempts.
export function obsoletePositionStatus(state, incoming) {
  if (!state?.status || state.jobId !== incoming?.job_id) return false
  // An explicit requeue starts recovery after failure. Callers must also
  // reject discovery reads begun before the current outcome was established.
  if (getTrackedJobPhase(state) === 'failed' && ['queued', 'deferred'].includes(incoming.status)
    && typeof incoming.result?.success !== 'boolean') return false
  if (!isTrackedJobActive(state)) return getTrackedJobPhase(state) !== getTrackedJobPhase({ status: incoming })
  if (typeof incoming.result?.success === 'boolean') return false
  if (incoming.status !== state.status.status) return false
  if (incoming.progress?.attempt_id !== state.status.progress?.attempt_id) return false
  const previousAt = parseTimestampSeconds(state.status.progress?.updated_at)
  const incomingAt = parseTimestampSeconds(incoming.progress?.updated_at)
  return previousAt !== null && incomingAt !== null && incomingAt < previousAt
}
