import { getJson, postJson } from './apiClient'

export const UPDATE_JOB_STORAGE_KEY = 'chessism:download-new-games:update-job'
export const DOWNLOAD_JOB_STORAGE_KEY = 'chessism:download-new-games:download-job'
export const PLAYER_DELETE_JOB_STORAGE_KEY = 'chessism:players:delete-job'

export async function sendPlayerAction(path, playerName) {
  return postJson(path, { player_name: playerName })
}

export async function fetchJobStatus(jobId) {
  return getJson(`/jobs/${encodeURIComponent(jobId)}`)
}

export function loadStoredJob(storageKey) {
  if (typeof window === 'undefined') return null
  try {
    const parsed = JSON.parse(window.localStorage.getItem(storageKey) || 'null')
    return parsed?.jobId ? parsed : null
  } catch {
    window.localStorage.removeItem(storageKey)
    return null
  }
}

export function storeJob(storageKey, job) {
  if (typeof window === 'undefined') return
  if (!job?.jobId) {
    window.localStorage.removeItem(storageKey)
    return
  }
  window.localStorage.setItem(storageKey, JSON.stringify(job))
}

export function isTerminalJobStatus(status) {
  const phase = status?.progress?.phase
  return status?.status === 'complete'
    || status?.status === 'not_found'
    || phase === 'complete'
    || phase === 'failed'
}

export function formatStatusMessage(value) {
  if (!value) return ''
  return typeof value === 'string' ? value : JSON.stringify(value)
}
