export const UPDATE_JOB_STORAGE_KEY = 'chessism:download-new-games:update-job'
export const DOWNLOAD_JOB_STORAGE_KEY = 'chessism:download-new-games:download-job'
export const PLAYER_DELETE_JOB_STORAGE_KEY = 'chessism:players:delete-job'

const listeners = new Map()
const pendingRequests = new Map()

export function storedJobSnapshot(storageKey) {
  if (typeof window === 'undefined') return null
  return window.localStorage.getItem(storageKey)
}

export function parseStoredJob(snapshot) {
  try {
    const parsed = JSON.parse(snapshot || 'null')
    return parsed?.jobId ? parsed : null
  } catch {
    return null
  }
}

export const loadStoredJob = storageKey => parseStoredJob(storedJobSnapshot(storageKey))

// Ownership starts before enqueue, so response arrival order cannot choose the job.
export function beginJobRequest(storageKey) {
  const request = Symbol(storageKey)
  pendingRequests.set(storageKey, request)
  return request
}

export function subscribeStoredJob(storageKey, listener) {
  const target = window
  if (!listeners.has(storageKey)) listeners.set(storageKey, new Set())
  listeners.get(storageKey).add(listener)
  const onStorage = event => {
    if (event.storageArea !== target.localStorage || (event.key !== null && event.key !== storageKey)) return
    pendingRequests.delete(storageKey)
    listener()
  }
  target.addEventListener('storage', onStorage)
  return () => {
    listeners.get(storageKey)?.delete(listener)
    if (!listeners.get(storageKey)?.size) listeners.delete(storageKey)
    target.removeEventListener('storage', onStorage)
  }
}

export function storeJob(storageKey, job, { request, expectedJobId } = {}) {
  if (typeof window === 'undefined') return false
  if (request !== undefined && pendingRequests.get(storageKey) !== request) return false
  if (expectedJobId !== undefined && loadStoredJob(storageKey)?.jobId !== expectedJobId) return false
  if (!job?.jobId) {
    window.localStorage.removeItem(storageKey)
  } else {
    window.localStorage.setItem(storageKey, JSON.stringify(job))
  }
  pendingRequests.delete(storageKey)
  // Same-document localStorage writes do not fire a browser storage event.
  for (const listener of listeners.get(storageKey) || []) listener()
  return true
}

export { isTerminalJobStatus } from './jobOutcome.js'
