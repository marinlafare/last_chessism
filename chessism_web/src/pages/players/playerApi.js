import { deleteJson, getJson, postJson } from '../../services/apiClient'

const playerPath = (playerName) => encodeURIComponent(playerName)

export const fetchPlayerProfile = (playerName) => getJson(`/players/${playerPath(playerName)}`)
export const fetchPlayerHours = (playerName) => getJson(`/games/${playerPath(playerName)}/hours_played`)
export const fetchPlayerPositionStats = (playerName) => getJson(`/fens/players/${playerPath(playerName)}/analysis_counts`)
export const fetchPlayerNeighbors = (playerName) => getJson(`/players/${playerPath(playerName)}/neighbors`)
export const fetchPlayerNavigation = () => getJson('/players/navigation')
export const fetchPlayerDeletionPreview = (playerName) => getJson(`/players/${playerPath(playerName)}/deletion-preview`)
export const fetchPlayerJobStatus = (jobId) => getJson(`/jobs/${encodeURIComponent(jobId)}`)
export const downloadPlayerGames = (playerName) => postJson('/games', { player_name: playerName })
export const updatePlayerGames = (playerName) => postJson('/games/update', { player_name: playerName })
export const deletePlayer = (playerName, confirmation) => (
  deleteJson(`/players/${playerPath(playerName)}`, {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(confirmation),
  })
)
