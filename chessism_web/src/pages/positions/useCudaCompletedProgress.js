import { useEffect, useState } from 'react'

const COMPLETE_DISPLAY_MS = 20_000

// Polls must not reset the deadline or reopen a completed run. Failures stay
// visible for recovery; history is never dismissed by this presentation timer.
export default function useCudaCompletedProgress(run) {
  const [hidden, setHidden] = useState(null)
  const id = run?.id
  const status = run?.status
  const finishedAt = run?.finished_at
  useEffect(() => {
    if (status !== 'complete') {
      setHidden(null)
      return undefined
    }
    const finished = Date.parse(finishedAt)
    const delay = Number.isFinite(finished)
      ? Math.max(0, Math.min(COMPLETE_DISPLAY_MS, finished + COMPLETE_DISPLAY_MS - Date.now()))
      : COMPLETE_DISPLAY_MS
    if (!delay) {
      setHidden(id)
      return undefined
    }
    const timer = window.setTimeout(() => setHidden(id), delay)
    return () => window.clearTimeout(timer)
  }, [id, status, finishedAt])
  const finished = Date.parse(finishedAt)
  return Boolean(run) && (status !== 'complete' || (hidden !== id &&
    (!Number.isFinite(finished) || Date.now() < finished + COMPLETE_DISPLAY_MS)))
}
