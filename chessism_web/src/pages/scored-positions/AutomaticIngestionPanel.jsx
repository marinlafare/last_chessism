import { useCallback, useRef } from 'react'
import { usePoll } from '../../hooks/usePoll'
import FenIngestionPanel from '../positions/FenIngestionPanel'
import useIngestionReview from '../positions/useIngestionReview'
import { readAutomaticIngestion } from './automaticIngestion'
import { fetchAutomaticIngestionJobs, fetchIngestionJobStatus, fetchLatestIngestionTiming } from './scoredPositionsApi'

export default function AutomaticIngestionPanel() {
  const jobsRef = useRef(null)
  const timingRef = useRef(null)
  const fetchJobs = useCallback(async ({ signal }) => {
    if (document.visibilityState === 'hidden' && jobsRef.current) return jobsRef.current
    const jobs = await readAutomaticIngestion(jobsRef.current || {}, {
      fetchActive: fetchAutomaticIngestionJobs, fetchJob: fetchIngestionJobStatus, signal,
    })
    if (!signal.aborted) jobsRef.current = jobs
    return jobs
  }, [])
  const fetchTiming = useCallback(async ({ signal }) => {
    if (document.visibilityState === 'hidden' && timingRef.current) return timingRef.current
    const timing = await fetchLatestIngestionTiming({ signal })
    if (!signal.aborted) timingRef.current = timing
    return timing
  }, [])
  const jobs = usePoll(fetchJobs, 3000)
  const timing = usePoll(fetchTiming, 5000)
  const { completedRuns, dismiss } = useIngestionReview(timing.data?.run)

  return (
    <FenIngestionPanel
      className="scored-positions-panel scored-ingestion-panel"
      jobState={jobs.data || {}}
      ingestionTiming={timing.data?.run}
      loading={jobs.loading}
      error={jobs.error?.message || ''}
      timingError={timing.error ? `Latest ingestion timing unavailable: ${timing.error.message}` : ''}
      completedRuns={completedRuns}
      onDismiss={dismiss}
    />
  )
}
