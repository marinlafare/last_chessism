import { getJson, postJson } from '../../services/apiClient'

export const fetchGameJobStatus = (jobId, options) => getJson(`/jobs/${encodeURIComponent(jobId)}`, options)
export const fetchActiveGameJobs = (options) => getJson('/jobs/active', options)

export const downloadPlayerGames = (playerName) => (
  postJson('/games', { player_name: playerName })
)

export const updatePlayerGames = (playerName) => (
  postJson('/games/update', { player_name: playerName })
)
