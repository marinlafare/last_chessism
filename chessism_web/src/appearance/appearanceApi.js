import { getJson, postJson } from '../services/apiClient'
export const fetchAppearance = options => getJson('/appearance', options)
export const saveAppearance = (action, payload) => postJson(`/appearance/${action}`, payload, {
  headers: { 'X-Chessism-Appearance': '1' },
})
