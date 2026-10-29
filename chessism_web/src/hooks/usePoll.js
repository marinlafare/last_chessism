import { useCallback, useEffect, useState } from 'react'
import { watchStatus } from '../services/statusPolling.js'

export function usePoll(fetcher, intervalMs = 8000, options = {}) {
  const { maxBackoffMs = 30000, requestTimeoutMs = 0, pauseWhenHidden = false } = options
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lastSuccessAt, setLastSuccessAt] = useState(null)
  const [revision, setRevision] = useState(0)
  const refresh = useCallback(() => setRevision(value => value + 1), [])

  useEffect(() => watchStatus({
    fetcher, intervalMs, maxBackoffMs, requestTimeoutMs, pauseWhenHidden,
    onData(result) {
      setData(result)
      setError(null)
      setLoading(false)
      setLastSuccessAt(Date.now())
    },
    onError(reason) {
      setError(reason)
      setLoading(false)
    },
  }), [fetcher, intervalMs, maxBackoffMs, requestTimeoutMs, pauseWhenHidden, revision])

  return { data, loading, error, lastSuccessAt, refresh }
}
