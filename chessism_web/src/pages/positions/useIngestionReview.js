import { useEffect, useMemo, useState } from 'react'

const STORAGE_KEY = 'chessism.ingestion-progress.review.v1'
const finished = run => Boolean(run?.run_id && [
  'complete', 'failed', 'error', 'warning', 'cancelled', 'canceled', 'partial',
].includes(run.status))
// A retried run can reuse its ID; acknowledge only this completed attempt.
const reviewKey = run => `${run.run_id}:${run.finished_at || run.status}`

function loadReview() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY))
    if (Array.isArray(saved?.pending) && Array.isArray(saved?.dismissed)) {
      return { pending: saved.pending.filter(finished), dismissed: saved.dismissed.filter(id => typeof id === 'string') }
    }
  } catch { /* Retention still works in memory if browser storage is unavailable. */ }
  return { pending: [], dismissed: [] }
}

function remember(review, run) {
  if (!run?.run_id) return review
  const pending = review.pending.filter(item => item.run_id !== run.run_id)
  if (finished(run) && !review.dismissed.includes(reviewKey(run))) pending.push(run)
  if (JSON.stringify(pending) === JSON.stringify(review.pending)) return review
  return { ...review, pending }
}

export default function useIngestionReview(run) {
  const [review, setReview] = useState(loadReview)
  const current = useMemo(() => remember(review, run), [review, run])
  useEffect(() => {
    if (current !== review) setReview(current)
  }, [current, review])
  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(current)) } catch { /* Keep the visible result in memory. */ }
  }, [current])
  const dismiss = key => setReview(previous => {
    const latest = remember(previous, run)
    if (!latest.pending.some(item => reviewKey(item) === key)) return latest
    return {
      pending: latest.pending.filter(item => reviewKey(item) !== key),
      dismissed: [...new Set([...latest.dismissed, key])].slice(-200),
    }
  })
  return { completedRuns: current.pending.map(item => ({ run: item, key: reviewKey(item) })), dismiss }
}
