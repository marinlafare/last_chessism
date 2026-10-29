import { deleteJson, getJson, postJson } from '../../services/apiClient'
import { attachUploadProgress } from './cloudUploadProgress.js'

export const fetchCoverage = () => getJson('/games/generalities')
export const fetchCudaSummaries = options => getJson('/analysis/cuda-summaries', options)
export const runCudaSummaries = () => postJson('/analysis/cuda-summaries/run', {})
export const retryCudaSummaries = id => postJson('/analysis/cuda-summaries/' + encodeURIComponent(id) + '/retry', {})
export const fetchGameAnalysisOverview = () => getJson('/fens/scored/games/overview')
export const fetchAnalysisCounts = () => getJson('/fens/analysis_counts')
export const fetchLatestIngestionTiming = (options) => getJson('/fens/pipeline/timings/latest', options)
export const fetchAnalysisProcesses = (options) => getJson('/jobs/analysis', options)
export const fetchAnalysisHistory = (backend, offset = 0, options) => getJson(
  backend === 'local' ? `/jobs/analysis/history?limit=20&offset=${offset}`
    : `/analysis/cloud/history?backend=${encodeURIComponent(backend)}&limit=20&offset=${offset}`, options)
export const fetchCloudHistoryUsage = (id, options) => getJson(`/analysis/cloud/${encodeURIComponent(id)}/usage`, options)
export const fetchActiveJobs = (options) => getJson('/jobs/active', options)
export const fetchPositionJobStatus = (jobId, options) => getJson(`/jobs/${encodeURIComponent(jobId)}`, options)
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
export const fetchCloudUploadProgress = (id, options) => getJson(`/analysis/cloud/${encodeURIComponent(id)}/upload-progress`, options)
export const fetchCloudAnalysis = async (options) => attachUploadProgress(
  await getJson('/analysis/cloud', options), fetchCloudUploadProgress, options)
export const createCloudAnalysis = (payload) => postJson('/analysis/cloud', payload)
export const resumeCloudAnalysis = (id) => postJson(`/analysis/cloud/${encodeURIComponent(id)}/resume`, {})
export const retryCloudAnalysis = (id) => postJson(`/analysis/cloud/${encodeURIComponent(id)}/retry-cloud`, {})
export const stopAndCleanCloudAnalysis = (id) => postJson(`/analysis/cloud/${encodeURIComponent(id)}/stop-and-cleanup`, {})
export const reviseCloudVms = (id, n_vms) => postJson(`/analysis/cloud/${encodeURIComponent(id)}/vms`, { n_vms })
const draftPath = (id) => `/analysis/cloud/drafts/${encodeURIComponent(id)}`
export const fetchCloudDraft = (options) => getJson('/analysis/cloud/drafts/current', options)
export const createCloudDraft = () => postJson('/analysis/cloud/drafts', {})
export const saveCloudDraftEntry = (id, payload) => postJson(`${draftPath(id)}/entries`, payload)
export const removeCloudDraftEntry = (id, entry, revision) => postJson(`${draftPath(id)}/entries/${encodeURIComponent(entry)}/remove`, { revision })
export const previewCloudDraft = (id, revision) => postJson(`${draftPath(id)}/preview`, { revision })
export const submitCloudDraft = (id, payload) => postJson(`${draftPath(id)}/submit`, payload)
