import { useEffect, useRef } from 'react'
import { COMPLETED_JOB_FADE_MS, COMPLETED_JOB_VISIBLE_MS, isTrackedJobComplete, pageHasAttention } from './positionPageSupport.js'
import { playCompletionSound } from '../../utils/completionAudio'
const AUTOMATIC_PIPELINE_JOB_KEYS = new Set(['gameParsing', 'fen', 'tablebase'])

// Presentation timers and sounds observe confirmed outcomes; they do not decide them.
export function useJobCompletionPresentation(jobState, ingestionTiming, updateJobState, clearJobState) {
  const audioContextRef = useRef(null)
  const completedSoundJobsRef = useRef(new Set())
  const ingestionSoundStateRef = useRef({ initialized: false, runId: null, status: null })
  const completionTimersRef = useRef(new Map())
  const fadeTimersRef = useRef(new Map())
  const jobStateRef = useRef(jobState)
  const jobCardNodesRef = useRef(new Map())
  const jobCardVisibilityRef = useRef(new Map())
  const jobCardObserverRef = useRef(null)
  jobStateRef.current = jobState
  const callbacks = useRef({ updateJobState, clearJobState })
  callbacks.current = { updateJobState, clearJobState }
  const cancelJobDismissTimers = (key) => {
    const completionTimerState = completionTimersRef.current.get(key)
    if (completionTimerState?.timer) {
      window.clearTimeout(completionTimerState.timer)
    }
    completionTimersRef.current.delete(key)

    const fadeTimer = fadeTimersRef.current.get(key)
    if (fadeTimer) {
      window.clearTimeout(fadeTimer)
      fadeTimersRef.current.delete(key)
    }
  }

  const clearAllJobDismissTimers = () => {
    completionTimersRef.current.forEach((timerState) => {
      if (timerState?.timer) window.clearTimeout(timerState.timer)
    })
    completionTimersRef.current.clear()
    fadeTimersRef.current.forEach((timer) => window.clearTimeout(timer))
    fadeTimersRef.current.clear()
  }

  const canRunJobDismissCountdown = (key) => {
    const state = jobStateRef.current[key]
    return (
      isTrackedJobComplete(state) &&
      !state?.fading &&
      pageHasAttention() &&
      jobCardVisibilityRef.current.get(key) === true
    )
  }

  const pauseJobDismissCountdown = (key) => {
    const timerState = completionTimersRef.current.get(key)
    if (!timerState?.timer) return

    window.clearTimeout(timerState.timer)
    const elapsedMs = Date.now() - Number(timerState.startedAt || Date.now())
    completionTimersRef.current.set(key, {
      ...timerState,
      timer: null,
      startedAt: null,
      remainingMs: Math.max(0, Number(timerState.remainingMs || 0) - elapsedMs)
    })
  }

  const startJobDismissCountdown = (key) => {
    if (!canRunJobDismissCountdown(key)) return

    const timerState = completionTimersRef.current.get(key) || {
      timer: null,
      startedAt: null,
      remainingMs: COMPLETED_JOB_VISIBLE_MS
    }
    if (timerState.timer) return

    const jobId = jobStateRef.current[key]?.jobId
    const remainingMs = Math.max(0, Number(timerState.remainingMs ?? COMPLETED_JOB_VISIBLE_MS))
    const timer = window.setTimeout(() => {
      completionTimersRef.current.delete(key)
      if (jobStateRef.current[key]?.jobId !== jobId) return
      callbacks.current.updateJobState(key, { fading: true }, jobId)

      const fadeTimer = window.setTimeout(() => {
        fadeTimersRef.current.delete(key)
        callbacks.current.clearJobState(key, jobId)
      }, COMPLETED_JOB_FADE_MS)

      fadeTimersRef.current.set(key, fadeTimer)
    }, remainingMs)

    completionTimersRef.current.set(key, {
      jobId,
      timer,
      startedAt: Date.now(),
      remainingMs
    })
  }

  const syncJobDismissCountdown = (key) => {
    if (canRunJobDismissCountdown(key)) {
      startJobDismissCountdown(key)
    } else {
      pauseJobDismissCountdown(key)
    }
  }

  const syncAllJobDismissCountdowns = () => {
    Object.keys(jobStateRef.current).forEach(syncJobDismissCountdown)
  }

  const setJobCardRef = (key) => (node) => {
    const previousNode = jobCardNodesRef.current.get(key)
    const observer = jobCardObserverRef.current

    if (previousNode && previousNode !== node && observer) {
      observer.unobserve(previousNode)
    }

    if (!node) {
      jobCardNodesRef.current.delete(key)
      jobCardVisibilityRef.current.delete(key)
      pauseJobDismissCountdown(key)
      return
    }

    jobCardNodesRef.current.set(key, node)
    node.dataset.jobKey = key

    if (observer) {
      observer.observe(node)
      return
    }

    jobCardVisibilityRef.current.set(key, true)
    syncJobDismissCountdown(key)
  }

  const handleCompletedJob = (key, state, status, patch) => {
    const jobId = state.jobId
    const succeeded = status.result?.success === true
    patch.completedAt = patch.completedAt || state.completedAt || Date.now()
    patch.fading = false

    const isAutomaticIngestionStage = AUTOMATIC_PIPELINE_JOB_KEYS.has(key)
    if (
      succeeded && !isAutomaticIngestionStage && jobId &&
      !completedSoundJobsRef.current.has(jobId)
    ) {
      completedSoundJobsRef.current.add(jobId)
      playCompletionSound(audioContextRef)
    }
  }

  useEffect(() => {
    const runId = ingestionTiming?.run_id || null
    const status = String(ingestionTiming?.status || '').toLowerCase() || null
    if (!runId) return

    const previous = ingestionSoundStateRef.current
    if (!previous.initialized) {
      ingestionSoundStateRef.current = { initialized: true, runId, status }
      return
    }

    const completedNow = status === 'complete' && (
      previous.runId !== runId || previous.status !== 'complete'
    )
    const completionKey = `ingestion:${runId}`
    if (completedNow && !completedSoundJobsRef.current.has(completionKey)) {
      completedSoundJobsRef.current.add(completionKey)
      playCompletionSound(audioContextRef)
    }

    ingestionSoundStateRef.current = { initialized: true, runId, status }
  }, [ingestionTiming?.run_id, ingestionTiming?.status])

  useEffect(() => {
    Array.from(completionTimersRef.current.keys()).forEach((key) => {
      const state = jobState[key]
      if (!state || !isTrackedJobComplete(state) || completionTimersRef.current.get(key)?.jobId !== state.jobId) {
        cancelJobDismissTimers(key)
      }
    })

    Object.entries(jobState).forEach(([key, state]) => {
      if (!isTrackedJobComplete(state) || state.fading) return

      if (!completionTimersRef.current.has(key)) {
        completionTimersRef.current.set(key, {
          timer: null,
          startedAt: null,
          jobId: state.jobId,
          remainingMs: COMPLETED_JOB_VISIBLE_MS
        })
      }
      syncJobDismissCountdown(key)
    })
  }, [jobState])

  useEffect(() => {
    if (typeof window !== 'undefined' && typeof IntersectionObserver !== 'undefined') {
      const observer = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          const key = entry.target.dataset.jobKey
          if (!key) return
          jobCardVisibilityRef.current.set(key, entry.isIntersecting && entry.intersectionRatio > 0)
          syncJobDismissCountdown(key)
        })
      }, { threshold: 0.25 })

      jobCardObserverRef.current = observer
      jobCardNodesRef.current.forEach((node) => observer.observe(node))
    } else {
      jobCardNodesRef.current.forEach((node, key) => {
        jobCardVisibilityRef.current.set(key, true)
        syncJobDismissCountdown(key)
      })
    }

    const handleAttentionChange = () => {
      syncAllJobDismissCountdowns()
    }

    document.addEventListener('visibilitychange', handleAttentionChange)
    window.addEventListener('focus', handleAttentionChange)
    window.addEventListener('blur', handleAttentionChange)

    return () => {
      document.removeEventListener('visibilitychange', handleAttentionChange)
      window.removeEventListener('focus', handleAttentionChange)
      window.removeEventListener('blur', handleAttentionChange)
      jobCardObserverRef.current?.disconnect()
      jobCardObserverRef.current = null
      clearAllJobDismissTimers()
    }
  }, [])

  return { audioContextRef, setJobCardRef, cancelJobDismissTimers, handleCompletedJob }
}
