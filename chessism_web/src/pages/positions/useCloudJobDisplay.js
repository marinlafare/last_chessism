import { useEffect, useState } from 'react'
import { trackProgress, showProgress, dismissProgress } from './cloudProgressReview'

const STORAGE_KEY = 'chessism.cloud-progress.review.v1'
const initial = () => {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY))
    if (saved && Number.isFinite(saved.since) && Array.isArray(saved.tracked) && Array.isArray(saved.dismissed)) return saved
  } catch { /* Browser storage may be unavailable. */ }
  return { since: Date.now(), initialized: false, tracked: [], dismissed: [] }
}

export default function useCloudJobDisplay(jobs) {
  const [review, setReview] = useState(initial)
  useEffect(() => {
    setReview(old => trackProgress(old, jobs))
  }, [jobs])
  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(review)) } catch { /* In-memory acknowledgement still works. */ }
  }, [review])
  const dismiss = id => setReview(old => dismissProgress(old, jobs, id))
  return {
    displayedJobs: jobs.filter(job => showProgress(job, review))
      .map(job => ({ job })),
    dismiss,
  }
}
