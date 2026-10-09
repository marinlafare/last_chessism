import { getJson, postJson } from '../../services/apiClient'

export const fetchSalienceOverview = (signal) => getJson('/research/salience?limit=1000', { signal })

export const startSalienceBackfill = () => (
  postJson('/research/salience/backfill?limit=100', {})
)

export const rebuildPlayerSalience = (playerName) => (
  postJson(`/research/salience/players/${encodeURIComponent(playerName)}`, {})
)
