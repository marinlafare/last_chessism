import { useCallback, useRef } from 'react'
import { usePoll } from '../../hooks/usePoll'
import FenIngestionPanel from '../positions/FenIngestionPanel'
import useIngestionReview from '../positions/useIngestionReview'
import { readAutomaticIngestion } from './automaticIngestion'
import { fetchAutomaticIngestionJobs, fetchIngestionJobStatus, fetchLatestIngestionTiming } from './scoredPositionsApi'

const POLLING_OPTIONS = { pauseWhenHidden: true, requestTimeoutMs: 10000 }

export default function AutomaticIngestionPanel() {
  const jobsRef = useRef(null)
  const fetchJobs = useCallback(async ({ signal }) => {
    const jobs = await readAutomaticIngestion(jobsRef.current || {}, {
      fetchActive: fetchAutomaticIngestionJobs, fetchJob: fetchIngestionJobStatus, signal,
    })
    if (!signal.aborted) jobsRef.current = jobs
    return jobs
  }, [])
  const jobs = usePoll(fetchJobs, 3000, POLLING_OPTIONS)
  const timing = usePoll(fetchLatestIngestionTiming, 5000, POLLING_OPTIONS)
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
