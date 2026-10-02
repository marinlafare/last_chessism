import { deleteJson, getJson, postJson } from '../../services/apiClient'

export const fetchCoverage = () => getJson('/games/generalities')
export const fetchGameAnalysisOverview = () => getJson('/fens/scored/games/overview')
export const fetchAnalysisCounts = () => getJson('/fens/analysis_counts')
export const fetchLatestIngestionTiming = () => getJson('/fens/pipeline/timings/latest')
export const fetchAnalysisProcesses = () => getJson('/jobs/analysis')
export const fetchActiveJobs = () => getJson('/jobs/active')
export const fetchPositionJobStatus = (jobId) => getJson(`/jobs/${encodeURIComponent(jobId)}`)
export const fetchPlayerAnalysisCounts = (playerName) => (
  getJson(`/fens/players/${encodeURIComponent(playerName)}/analysis_counts`)
)
export const inspectPlayerGameScope = (payload) => postJson('/analysis/player_games/scope', payload)
export const previewPlayerGameAnalysis = (payload) => postJson('/analysis/player_games/preview', payload)
export const queueGlobalAnalysis = (payload) => postJson('/analysis/run_job', payload)
export const queuePlayerAnalysis = (payload) => postJson('/analysis/run_player_job', payload)
export const queueAnalysisLoop = (payload) => postJson('/analysis/run_loop_job', payload)
export const queuePlayerGameAnalysis = (payload) => postJson('/analysis/player_games/run_job', payload)
export const deleteQueuedAnalysisJob = (jobId) => deleteJson(`/jobs/${encodeURIComponent(jobId)}/queued`)
