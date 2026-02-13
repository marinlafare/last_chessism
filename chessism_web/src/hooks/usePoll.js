import { useEffect, useRef, useState } from 'react'

export function usePoll(fetcher, intervalMs = 8000, options = {}) {
  const { maxBackoffMs = 30000 } = options
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lastSuccessAt, setLastSuccessAt] = useState(null)
  const abortRef = useRef(null)

  useEffect(() => {
    let stopped = false
    let timeoutId = null
    let delayMs = intervalMs

    const run = async () => {
      if (stopped) return

      abortRef.current?.abort()
      const controller = new AbortController()
      abortRef.current = controller

      try {
        const result = await fetcher({ signal: controller.signal })
        if (stopped) return

        setData(result)
        setError(null)
        setLoading(false)
        setLastSuccessAt(Date.now())
        delayMs = intervalMs
      } catch (err) {
        if (stopped || err?.name === 'AbortError') return

        setError(err)
        setLoading(false)
        delayMs = Math.min(maxBackoffMs, Math.max(intervalMs, delayMs * 2))
      } finally {
        if (!stopped) {
          timeoutId = setTimeout(run, delayMs)
        }
      }
    }

    run()

    return () => {
      stopped = true
      abortRef.current?.abort()
      if (timeoutId) clearTimeout(timeoutId)
    }
  }, [fetcher, intervalMs, maxBackoffMs])

  return { data, loading, error, lastSuccessAt }
}
