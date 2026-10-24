import { useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { recoverCloudJob } from './systemStatusApi'

export default function CloudJobRecovery({ job, available, lastSuccessAt, refresh }) {
  const [pending, setPending] = useState(false)
  const [acceptedAt, setAcceptedAt] = useState(null)
  const [error, setError] = useState('')
  const inFlight = useRef(false)
  const latestSnapshot = useRef(lastSuccessAt)
  latestSnapshot.current = lastSuccessAt
  const recoverable = ['paused', 'waiting'].includes(job.status)
  const awaitingStatus = acceptedAt !== null && (lastSuccessAt || 0) <= acceptedAt
  const busy = pending || awaitingStatus

  const recover = async () => {
    if (!recoverable || !available || busy || inFlight.current) return
    inFlight.current = true
    setPending(true)
    setError('')
    try {
      await recoverCloudJob(job.id)
      // Keep the button locked until a fresh poll confirms the new status.
      // This also prevents a second POST while the old paused snapshot is shown.
      setAcceptedAt(latestSnapshot.current || 0)
      refresh()
    } catch (failure) {
      setError(failure.message || 'Recovery could not be requested. Try again.')
      refresh()
    } finally {
      inFlight.current = false
      setPending(false)
    }
  }

  if (!recoverable) return null
  return <div {...pageStyleTarget('cloud.recovery.panel')} className="cloud-job-recovery" aria-label="Cloud job recovery">
    <div {...pageStyleTarget('cloud.recovery.text')} className="cloud-job-recovery-message">
      <p role="status">{pending ? 'Requesting recovery…' : awaitingStatus ? 'Recovery requested · checking status…' : 'The local workflow needs recovery.'}</p>
      {job.error && <p className="cloud-job-recovery-detail">{job.error}</p>}
      {!available && <p>Recovery requires an online controller and up-to-date status.</p>}
      {error && <p role="alert">{error}</p>}
    </div>
    <button {...pageStyleTarget('cloud.recovery.button')} type="button" className="cloud-job-recovery-button"
      disabled={!available || busy} aria-busy={busy} onClick={recover}
      title="Resume this existing job from its saved checkpoint. This does not request a new analysis or a cloud-execution retry.">
      {busy && <span className="cloud-job-recovery-spinner" aria-hidden="true" />}
      {busy ? 'Recovering…' : 'Recover job'}
    </button>
  </div>
}
