import { cloudJobDone } from './cloudJobProgress.js'

export const terminalProgress = job => cloudJobDone(job) || job.status === 'cancelled'

export function trackProgress(review, jobs) {
  if (!jobs.length) return review
  const recent = [...jobs].sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))[0]
  const tracked = [...new Set([...review.tracked, ...jobs.filter(job => !terminalProgress(job)
    || Date.parse(job.created_at) >= review.since || (!review.initialized && job.id === recent.id)).map(job => job.id)])].slice(-200)
  if (review.initialized && JSON.stringify(tracked) === JSON.stringify(review.tracked)) return review
  return { ...review, initialized: true, tracked }
}

export const showProgress = (job, review) => !terminalProgress(job)
  || (review.tracked.includes(job.id) && !review.dismissed.includes(job.id))

export function dismissProgress(review, jobs, id) {
  if (!jobs.some(job => job.id === id && terminalProgress(job))) return review
  return { ...review, dismissed: [...new Set([...review.dismissed, id])].slice(-200) }
}
