import { useEffect, useState } from 'react'
import { recordCloudEta } from './cloudEta'
import { estimateCloudCompletion } from './cloudCompletionEta'
import useCloudCompletionContext from './useCloudCompletionContext'

export default function CloudEtaBar({ job, jobs, available, lastSuccessAt }) {
  const context = useCloudCompletionContext(job, jobs)
  const [history, setHistory] = useState(null)
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    setNow(Date.now())
    setHistory(previous => available ? recordCloudEta(previous, job, lastSuccessAt) : null)
  }, [job, available, lastSuccessAt])
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  const eta = estimateCloudCompletion(history, job, { now, available, lastSuccessAt }, context)
  return <div className="cloud-status-eta" data-eta-state={eta.state}
    title="Approximate time until the whole job finishes: remaining analysis/import plus verification, cloud cleanup, and database finalization. Based on this job's progress and comparable completed jobs; pauses or interruptions can extend it.">
    <div className="cloud-status-eta-label"><span>Estimated time left</span><strong>{eta.label}</strong></div>
  </div>
}
