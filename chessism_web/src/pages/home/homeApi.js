import { getJson } from '../../services/apiClient'

export const fetchDashboardSummary = ({ signal } = {}) => getJson('/games/home_summary', { signal })

export const fetchTopMainCharacters = (timeControl, limit, { signal, titleStatus = 'titled' } = {}) => (
  getJson(
    `/players/main_characters/top?time_control=${encodeURIComponent(timeControl)}&limit=${limit}&title_status=${encodeURIComponent(titleStatus)}`,
    { signal },
  )
)
