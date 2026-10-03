import { getJson, postJson } from '../../services/apiClient'

export const fetchGameJobStatus = (jobId) => getJson(`/jobs/${encodeURIComponent(jobId)}`)

export const downloadPlayerGames = (playerName) => (
  postJson('/games', { player_name: playerName })
)

export const updatePlayerGames = (playerName) => (
  postJson('/games/update', { player_name: playerName })
)
