import { useEffect, useState } from 'react'
import {
  reconcileCloudJobDisplay, cloudJobDisplayState, CLOUD_DONE_VISIBLE_MS, CLOUD_DONE_FADE_MS,
} from './cloudJobProgress'

export default function useCloudJobDisplay(jobs) {
  const [display, setDisplay] = useState(() => ({ entries: {}, now: Date.now() }))
  useEffect(() => {
    const now = Date.now()
    setDisplay((old) => ({ entries: reconcileCloudJobDisplay(old.entries, jobs, now), now }))
  }, [jobs])
  useEffect(() => {
    const deadlines = Object.values(display.entries).flatMap(({ terminal, endedAt }) => (
      terminal && Number.isFinite(endedAt)
        ? [endedAt + CLOUD_DONE_VISIBLE_MS, endedAt + CLOUD_DONE_VISIBLE_MS + CLOUD_DONE_FADE_MS] : []
    )).filter((time) => time > display.now)
    if (!deadlines.length) return undefined
    const timer = setTimeout(() => setDisplay((old) => ({ ...old, now: Date.now() })),
      Math.max(0, Math.min(...deadlines) - Date.now()))
    return () => clearTimeout(timer)
  }, [display])
  return jobs.map((job) => ({ job, state: cloudJobDisplayState(job, display.entries[job.id], display.now) }))
    .filter(({ state }) => state !== 'hidden')
}
