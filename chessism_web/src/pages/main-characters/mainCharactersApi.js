import { getJson, postJson } from '../../services/apiClient'

export const fetchMainCharacterTimeControls = () => (
  getJson('/players/main_characters/time_controls')
)

export const fetchTopMainCharacters = (timeControl, limit) => (
  getJson(`/players/main_characters/top?time_control=${encodeURIComponent(timeControl)}&limit=${limit}`)
)

export const updatePlayerGames = (playerName) => (
  postJson('/games/update', { player_name: playerName })
)

export const createPlayerGames = (playerName) => (
  postJson('/games', { player_name: playerName })
)
