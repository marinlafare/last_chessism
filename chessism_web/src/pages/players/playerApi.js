import { deleteJson, getJson, postJson } from '../../services/apiClient'

const playerPath = (playerName) => encodeURIComponent(playerName)

export const fetchPlayerProfile = (playerName, options) => getJson(`/players/${playerPath(playerName)}/hero`, options)
export const fetchPlayerHours = (playerName, options) => getJson(`/games/${playerPath(playerName)}/hours_played`, options)
export const fetchPlayerPositionStats = (playerName, options) => getJson(`/fens/players/${playerPath(playerName)}/analysis_counts`, options)
export const fetchPlayerLatestGame = (playerName, options) => getJson(`/games/${playerPath(playerName)}/latest`, options)
export const fetchPlayerNeighbors = (playerName, options) => getJson(`/players/${playerPath(playerName)}/neighbors`, options)
export const fetchPlayerNavigation = (sameGroupAs, options) => getJson(
  `/players/navigation${sameGroupAs ? `?same_group_as=${playerPath(sameGroupAs)}` : ''}`, options
)
export const fetchPlayerDeletionPreview = (playerName) => getJson(`/players/${playerPath(playerName)}/deletion-preview`)
export const fetchPlayerJobStatus = (jobId) => getJson(`/jobs/${encodeURIComponent(jobId)}`)
export const updatePlayerGames = (playerName) => postJson('/games/update', { player_name: playerName })
export const deletePlayer = (playerName, confirmation) => (
  deleteJson(`/players/${playerPath(playerName)}`, {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(confirmation),
  })
)
