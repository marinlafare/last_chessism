import { getJson } from '../../services/apiClient'

export const fetchAnalysisTimesSummary = () => getJson('/analysis_times/summary?limit=10')
