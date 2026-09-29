import { getJson, requestJson } from '../../services/apiClient'

export const fetchScoredOverview = ({ signal } = {}) => (
  getJson('/fens/scored/overview', { signal })
)

export const fetchScoredGamesOverview = ({ signal } = {}) => (
  getJson('/fens/scored/games/overview', { signal })
)

export const fetchAdvantageByRating = ({ signal } = {}) => (
  getJson('/fens/scored/advantage_by_rating', { signal })
)

export const fetchRepeatedPendingFens = (page, { signal } = {}) => (
  getJson(`/fens/pending/repeated?page=${page}`, { signal })
)

export const fetchFenAnalysisBackups = ({ signal } = {}) => (
  getJson('/analysis/backups', { signal })
)

export const fetchBackupJobStatus = (jobId, { signal } = {}) => (
  getJson(`/jobs/${encodeURIComponent(jobId)}`, { signal })
)

export const queueFenAnalysisBackup = () => requestJson('/analysis/backups', { method: 'POST' })

export const queueFenAnalysisRestore = (filename) => (
  requestJson(`/analysis/backups/${encodeURIComponent(filename)}/restore`, { method: 'POST' })
)
