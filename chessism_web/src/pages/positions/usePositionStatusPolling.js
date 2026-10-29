import { useCallback, useEffect, useRef } from 'react'
import { createStatusPolling, watchStatus } from '../../services/statusPolling.js'
import { isMissingJobResponse } from '../../services/gameJobPolling.js'
import { fetchPositionJobStatus } from './positionsApi'
import { isTrackedJobActive } from './positionPageSupport.js'
import { obsoletePositionStatus } from './positionStatusFreshness.js'

const options = { intervalMs: 3000, requestTimeoutMs: 10000, pauseWhenHidden: true }

// Stable callbacks prevent normal progress renders from restarting status reads.
export function usePositionStatusFeed(fetcher, onData, onError = () => {}) {
  const callbacks = useRef({ onData, onError })
  callbacks.current = { onData, onError }
  const polling = useRef(null)
  useEffect(() => {
    const current = createStatusPolling({
      ...options, fetcher,
      onData: value => callbacks.current.onData(value),
      onError: error => callbacks.current.onError(error),
    })
    polling.current = current
    current.refresh({ automatic: true })
    return () => { current.stop(); polling.current = null }
  }, [fetcher])
  return useCallback(() => polling.current?.refresh(), [])
}

export function usePositionJobPolling(jobState, onStatus, onUnavailable) {
  const latest = useRef({ jobState, onStatus, onUnavailable })
  latest.current = { jobState, onStatus, onUnavailable }
  const signature = JSON.stringify(Object.entries(jobState)
    .filter(([, state]) => isTrackedJobActive(state))
    .map(([key, state]) => [key, state.jobId]).sort())
  useEffect(() => {
    const stops = JSON.parse(signature).map(([key, jobId]) => {
      const currentState = () => {
        const state = latest.current.jobState[key]
        return state?.jobId === jobId && isTrackedJobActive(state) ? state : null
      }
      return watchStatus({
        ...options, fetcher: request => fetchPositionJobStatus(jobId, request),
        onData(status) {
          const state = currentState()
          if (!state) return false
          if (obsoletePositionStatus(state, status)) return
          latest.current.onStatus(key, state, status)
          return isTrackedJobActive({ ...state, status })
        },
        onError(error) {
          const state = currentState()
          if (!state) return false
          if (isMissingJobResponse(error, jobId)) {
            latest.current.onStatus(key, state, error.payload)
            return false
          }
          // HTTP/network errors do not prove that the server job has stopped.
          latest.current.onUnavailable(key, state, error)
        },
      })
    })
    return () => stops.forEach(stop => stop())
  }, [signature])
}
