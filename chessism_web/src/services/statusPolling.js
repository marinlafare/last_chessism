// Read-only polling lifecycle. Cancelling a request never cancels server work.
export function createStatusPolling({
  fetcher, onData, onError, intervalMs = 8000, maxBackoffMs = 30000,
  requestTimeoutMs = 0, pauseWhenHidden = false,
  visibilityDocument = globalThis.document,
  isHidden = () => visibilityDocument?.visibilityState === 'hidden',
  setTimer = setTimeout, clearTimer = clearTimeout,
}) {
  let stopped = false
  let timer = null
  let active = null
  let delayMs = intervalMs
  const hidden = () => pauseWhenHidden && isHidden()
  const clearScheduled = () => {
    if (timer !== null) clearTimer(timer)
    timer = null
  }
  const cancelActive = () => {
    const request = active
    active = null
    if (request) {
      request.cancel(Object.assign(new Error('Status read cancelled'), { name: 'AbortError' }))
      if (request.timeout !== null) clearTimer(request.timeout)
    }
  }

  const invalidate = () => {
    clearScheduled()
    cancelActive()
  }
  const stop = () => {
    stopped = true
    invalidate()
    if (pauseWhenHidden) visibilityDocument?.removeEventListener('visibilitychange', visibilityChanged)
  }

  // Explicit action refreshes supersede old reads, including in a hidden tab.
  // Automatic refreshes never overlap and resume on the visibility event.
  const refresh = async ({ automatic = false } = {}) => {
    if (stopped || (automatic && active)) return
    invalidate()
    if (automatic && hidden()) return
    const controller = new AbortController()
    let rejectCancellation
    const cancellation = new Promise((_, reject) => { rejectCancellation = reject })
    const request = {
      timeout: null,
      cancel(error) { rejectCancellation(error); controller.abort() },
    }
    active = request
    if (requestTimeoutMs > 0) {
      request.timeout = setTimer(() => request.cancel(Object.assign(
        new Error('Status request timed out; retrying automatically.'), { name: 'TimeoutError' },
      )), requestTimeoutMs)
    }
    try {
      // Race also releases the loop if a fetcher ignores its abort signal.
      const result = await Promise.race([
        // Start immediately, but turn synchronous throws into rejections so
        // cancellation always has a race handler (including terminal errors).
        new Promise(resolve => resolve(fetcher({ signal: controller.signal }))), cancellation,
      ])
      if (stopped || active !== request) return
      // Only the workflow adapter decides whether this job is terminal.
      if (onData(result) === false) { stop(); return }
      delayMs = intervalMs
    } catch (error) {
      if (stopped || active !== request || error?.name === 'AbortError') return
      if (onError(error) === false) { stop(); return }
      delayMs = Math.min(maxBackoffMs, Math.max(intervalMs, delayMs * 2))
    } finally {
      if (request.timeout !== null) clearTimer(request.timeout)
      if (active === request) {
        active = null
        if (!stopped && !hidden()) timer = setTimer(() => refresh({ automatic: true }), delayMs)
      }
    }
  }

  const visibilityChanged = () => {
    if (hidden()) {
      invalidate()
    } else {
      refresh({ automatic: true })
    }
  }
  if (pauseWhenHidden) visibilityDocument?.addEventListener('visibilitychange', visibilityChanged)
  return { refresh, invalidate, stop }
}

export function watchStatus(options) {
  const polling = createStatusPolling(options)
  polling.refresh({ automatic: true })
  return polling.stop
}
