import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchCloudAnalysis } from './positionsApi'
import { cloudStatusPolling } from './cloudStatusPolling.js'

export default function useCloudStatus(onProgress) {
  const [data, setData] = useState({ controller_online: false, jobs: [] })
  const [error, setError] = useState('')
  const poller = useRef(null)
  const progress = useRef(onProgress)
  progress.current = onProgress
  useEffect(() => {
    let previous = null
    const polling = cloudStatusPolling({
      fetchStatus: fetchCloudAnalysis,
      isHidden: () => document.visibilityState === 'hidden',
      onData: next => {
        setData(next)
        setError('')
        const stamp = next.jobs.map(job => `${job.id}:${job.imported}:${job.status}:${job.runs.map(run => run.status).join(',')}`).join('|')
        if (previous !== null && previous !== stamp) progress.current?.()
        previous = stamp
      },
      onError: err => {
        setError(err.message)
        setData(old => ({ ...old, controller_online: false }))
      },
    })
    poller.current = polling
    const visible = () => { if (document.visibilityState !== 'hidden') polling.refresh() }
    document.addEventListener('visibilitychange', visible)
    polling.refresh()
    return () => {
      polling.stop()
      poller.current = null
      document.removeEventListener('visibilitychange', visible)
    }
  }, [])
  const refresh = useCallback(() => poller.current?.refresh(), [])
  const updateData = useCallback(update => {
    // Stop acknowledgement must not be overwritten by a pre-stop status poll.
    poller.current?.invalidate()
    setData(update)
  }, [])
  return { data, error, setError, refresh, updateData }
}
