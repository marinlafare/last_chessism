import { useEffect, useState } from 'react'
import { graphRead } from './graphExplorerApi'

// Never paint an old player's rows under a new filter while effects catch up.
export function useGraphRead(kind, parameters, enabled = true) {
  const key = JSON.stringify([kind, parameters])
  const [stored, setStored] = useState(null)
  const [retry, setRetry] = useState(0)
  useEffect(() => {
    if (!enabled) return undefined
    const controller = new AbortController()
    setStored({ key, loading: true })
    graphRead(kind, parameters, controller.signal).then(data => {
      if (!controller.signal.aborted) setStored({ key, data, loading: false })
    }).catch(error => {
      if (!controller.signal.aborted) setStored({ key, error: error.message, loading: false })
    })
    return () => controller.abort()
  }, [key, enabled, retry]) // key includes every request parameter.
  return { ...(stored?.key === key && enabled ? stored : { loading: enabled }), retry: () => setRetry(n => n + 1) }
}
