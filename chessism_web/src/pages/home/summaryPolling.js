// Keep an open homepage current after ingestion, including work in another tab.
// Never overlap requests or discard the last successful chart while refreshing.
export function summaryPolling({ fetchSummary, onData, onError, isHidden = () => false,
  intervalMs = 15000, schedule = setTimeout, unschedule = clearTimeout }) {
  let stopped = false
  let current = null
  let timer = null
  let deadline = null
  let delay = intervalMs

  const refresh = async () => {
    if (stopped || current) return
    unschedule(timer)
    timer = null
    if (isHidden()) return
    const request = new AbortController()
    current = request
    deadline = schedule(() => request.abort(), 20000)
    try {
      const data = await fetchSummary({ signal: request.signal })
      if (stopped || request.signal.aborted) return
      delay = intervalMs
      onData(data)
    } catch (error) {
      if (!stopped) {
        delay = Math.min(60000, delay * 2)
        onError(error)
      }
    } finally {
      unschedule(deadline)
      current = null
      if (!stopped) timer = schedule(refresh, delay)
    }
  }

  return {
    refresh,
    stop: () => {
      stopped = true
      unschedule(timer)
      unschedule(deadline)
      current?.abort()
    },
  }
}
