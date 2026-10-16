import { useCallback, useRef } from 'react'
import { usePoll } from '../../hooks/usePoll'
import { fetchCloudJobUsage } from './systemStatusApi'
import { comparableCloudJobs } from './cloudCompletionEta'

export default function useCloudCompletionContext(job, jobs) {
  const candidates = comparableCloudJobs(job, jobs)
  const signature = candidates.map(prior => prior.id).join(',')
  const cache = useRef(new Map())
  const fetcher = useCallback(async ({ signal }) => {
    const ids = signature ? signature.split(',') : []
    for (const id of cache.current.keys()) if (!ids.includes(id)) cache.current.delete(id)
    const [current, ...completed] = await Promise.all([
      fetchCloudJobUsage(job.id, { signal }),
      ...ids.map(async id => {
        if (!cache.current.has(id)) {
          const usage = await fetchCloudJobUsage(id, { signal })
          if (!signal.aborted) cache.current.set(id, usage)
          return usage
        }
        return cache.current.get(id)
      }),
    ])
    return { jobId: job.id, current, completed: ids.map((id, i) => ({ id, usage: completed[i] })), observedAt: Date.now() }
  }, [job.id, signature])
  const status = usePoll(fetcher, 30000)
  if (!status.data || status.data.jobId !== job.id) return status.error ? { jobId: job.id, error: status.error } : null
  return { ...status.data, error: status.error,
    completed: status.data.completed.map(entry => ({ job: candidates.find(prior => prior.id === entry.id), usage: entry.usage }))
      .filter(entry => entry.job) }
}
