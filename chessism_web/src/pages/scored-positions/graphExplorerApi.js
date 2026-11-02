import { getJson } from '../../services/apiClient'

export const graphRead = (kind, parameters, signal) => getJson(
  `/graph-explorer/${kind}?${new URLSearchParams(parameters)}`, { signal })
