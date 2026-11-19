import { useEffect, useRef, useState } from 'react'
import { loadStoredJob, storeJob } from './gameJobService.js'
import { watchGameJob } from './gameJobPolling.js'
import { isTerminalJobStatus, jobStatusMessage } from './jobOutcome.js'

// Persistence owns the job identity; transport loss changes only the message.
export function useTrackedGameJob(storageKey, fetchStatus, onTerminal) {
  const [job, setJob] = useState(() => loadStoredJob(storageKey))
  const [status, setStatus] = useState(null)
  const [message, setMessage] = useState('')
  const latest = useRef({ job, onTerminal })
  latest.current = { job, onTerminal }
  const mounted = useRef(true)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  useEffect(() => {
    if (!job?.jobId) return
    return watchGameJob({
      jobId: job.jobId, fetchStatus,
      onStatus(value) {
        if (latest.current.job?.jobId !== job.jobId) return
        setStatus(value)
        setMessage(jobStatusMessage(value))
        if (!isTerminalJobStatus(value)) return
        if (loadStoredJob(storageKey)?.jobId === job.jobId) storeJob(storageKey, null)
        latest.current.job = null
        setJob(null)
        latest.current.onTerminal?.(value, job)
      },
      onUnavailable() {
        if (latest.current.job?.jobId === job.jobId) setMessage('Status connection interrupted; reconnecting automatically…')
      },
    })
  }, [job?.jobId, storageKey, fetchStatus])
  const track = next => {
    if (!next?.jobId) throw new Error('The server did not return a job identifier.')
    // Acknowledgments arriving after navigation must still be recoverable.
    storeJob(storageKey, next)
    latest.current.job = next
    if (!mounted.current) return
    setStatus(null)
    setMessage(`Queued work for ${next.playerName}.`)
    setJob(next)
  }
  return { job, status, message, track }
}
