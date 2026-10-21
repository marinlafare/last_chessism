// One status request/timer at a time. An action refresh supersedes an older
// poll even when the transport finishes after its AbortSignal was cancelled.
export function cloudStatusPolling({ fetchStatus, onData, onError, isHidden = () => false,
  intervalMs = 5000, schedule = setTimeout, unschedule = clearTimeout }) {
  let stopped = false
  let request = null
  let timer = null
  let hasData = false
  let delay = intervalMs
  const invalidate = () => {
    unschedule(timer)
    timer = null
    request?.abort()
    request = null
  }
  const refresh = async ({ automatic = false } = {}) => {
    if (stopped) return
    invalidate()
    if (automatic && hasData && isHidden()) {
      timer = schedule(() => refresh({ automatic: true }), intervalMs)
      return
    }
    const current = new AbortController()
    request = current
    try {
      const data = await fetchStatus({ signal: current.signal })
      if (stopped || request !== current) return
      hasData = true
      delay = intervalMs
      onData(data)
    } catch (error) {
      if (stopped || request !== current || error?.name === 'AbortError') return
      delay = Math.min(30000, delay * 2)
      onError(error)
    } finally {
      if (!stopped && request === current) {
        request = null
        timer = schedule(() => refresh({ automatic: true }), delay)
      }
    }
  }
  return {
    refresh,
    invalidate,
    stop: () => { stopped = true; invalidate() },
  }
}
