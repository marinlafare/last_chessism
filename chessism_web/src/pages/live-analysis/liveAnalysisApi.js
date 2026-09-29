import { postJson } from '../../services/apiClient'

export async function analyzeLiveFen(fen, nodesLimit, multipv, { signal } = {}) {
  const payload = await postJson('/analysis/fen', {
    fens: [fen],
    nodes_limit: nodesLimit,
    multipv,
  }, { signal })
  return Array.isArray(payload) ? payload[0] : payload
}
