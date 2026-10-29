import { createStatusPolling } from '../../services/statusPolling.js'

// Cloud actions use refresh/invalidate so a pre-action response cannot undo an
// acknowledgement. A cloud job at 100% still polls through verification/cleanup.
export function cloudStatusPolling({ fetchStatus, onData, onError, isHidden,
  visibilityDocument, intervalMs = 5000, schedule = setTimeout, unschedule = clearTimeout }) {
  return createStatusPolling({
    fetcher: fetchStatus, onData, onError, isHidden, visibilityDocument,
    intervalMs, requestTimeoutMs: 10000, pauseWhenHidden: true,
    setTimer: schedule, clearTimer: unschedule,
  })
}
