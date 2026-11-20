import { useEffect, useRef, useState } from 'react'
import { loadStoredJob, storeJob } from './gameJobService.js'
import { watchGameJob } from './gameJobPolling.js'
import { isTerminalJobStatus, jobStatusMessage } from './jobOutcome.js'
import { useStoredGameJob } from './useStoredGameJob.js'

// Persistence owns the job identity; transport loss changes only the message.
export function useTrackedGameJob(storageKey, fetchStatus, onTerminal) {
  const job = useStoredGameJob(storageKey)
  const [status, setStatus] = useState(null)
  const [message, setMessage] = useState('')
  const latest = useRef(onTerminal)
  latest.current = onTerminal
  const mounted = useRef(true)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  useEffect(() => {
    if (!job?.jobId) return
    setStatus(null)
    setMessage(`Queued work for ${job.playerName}.`)
    return watchGameJob({
      jobId: job.jobId, fetchStatus,
      onStatus(value) {
        if (!mounted.current || loadStoredJob(storageKey)?.jobId !== job.jobId) return
        setStatus(value)
        setMessage(jobStatusMessage(value))
        if (!isTerminalJobStatus(value)) return
        if (!storeJob(storageKey, null, { expectedJobId: job.jobId })) return
        latest.current?.(value, job)
      },
      onUnavailable() {
        if (mounted.current && loadStoredJob(storageKey)?.jobId === job.jobId) setMessage('Status connection interrupted; reconnecting automatically…')
      },
    })
  }, [job?.jobId, storageKey, fetchStatus])
  const track = (next, request) => {
    if (!next?.jobId) throw new Error('The server did not return a job identifier.')
    // Acknowledgments arriving after navigation must still be recoverable.
    return storeJob(storageKey, next, { request })
  }
  return { job, status, message, track }
}
