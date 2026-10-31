import { getJson } from '../../services/apiClient'

export const fetchAutomaticIngestionJobs = ({ signal } = {}) => getJson('/jobs/active', { signal })
export const fetchIngestionJobStatus = (jobId, { signal } = {}) => getJson(`/jobs/${encodeURIComponent(jobId)}`, { signal })
export const fetchLatestIngestionTiming = ({ signal } = {}) => getJson('/fens/pipeline/timings/latest', { signal })

export const fetchScoredOverview = ({ signal } = {}) => (
  getJson('/fens/scored/overview', { signal })
)

export const fetchScoredGamesOverview = ({ signal } = {}) => (
  getJson('/fens/scored/games/overview', { signal })
)

// Use the same database totals as the Analyze page, including games without FENs.
export const fetchScoredCoverage = ({ signal } = {}) => (
  getJson('/games/generalities', { signal })
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
