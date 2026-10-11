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

export const fetchRatingGroups = ({ signal } = {}) => (
  getJson('/fens/scored/rating_groups', { signal })
)

export const fetchPendingRepetitions = ({ signal } = {}) => (
  getJson('/fens/pending/repetition_histogram', { signal })
)

export const fetchFenAppearances = ({ signal } = {}) => (
  getJson('/fens/appearance_histogram', { signal })
)

export const fetchCommonPositionPage = (cursor, { signal, retry = false } = {}) => (
  getJson(`/fens/common_positions/page?${new URLSearchParams({ cursor, ...(retry ? { retry: 'true' } : {}) })}`, { signal })
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
