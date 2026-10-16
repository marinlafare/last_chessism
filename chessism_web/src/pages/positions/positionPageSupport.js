import { formatNumber } from '../../utils/formatters.js'

const DEFAULT_ANALYSIS_NODES = 100_000
const MAX_ANALYSIS_BATCH_SIZE = 500
const MAX_GLOBAL_ANALYSIS_BATCH_SIZE = 1000
const MAX_LOOP_ANALYSIS_BATCH_SIZE = 1000
const POSITION_JOBS_STORAGE_KEY = 'chessism:positions:jobs'
const COMPLETED_JOB_VISIBLE_MS = 5000
const COMPLETED_JOB_FADE_MS = 700
const ETA_RATE_SMOOTHING = 0.35

const formatTruncatedMillions = (value) => {
  const millions = Math.floor(Math.max(0, Number(value) || 0) / 10_000) / 100
  return `${millions.toFixed(2)}M`
}

const formatDuration = (seconds) => {
  const totalMinutes = Math.max(0, Math.ceil(Number(seconds || 0) / 60))
  const hours = Math.floor(totalMinutes / 60)
  const minutes = totalMinutes % 60
  if (hours <= 0) return `${minutes}m`
  return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`
}

const formatCountdown = (seconds) => {
  const totalSeconds = Math.max(0, Math.ceil(Number(seconds || 0)))
  const days = Math.floor(totalSeconds / 86400)
  const hours = Math.floor((totalSeconds % 86400) / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const remainingSeconds = totalSeconds % 60
  const clock = [hours, minutes, remainingSeconds]
    .map((value) => String(value).padStart(2, '0'))
    .join(':')

  return days > 0 ? `${days}d ${clock}` : clock
}

const parseTimestampSeconds = (value) => {
  const numeric = Number(value)
  if (Number.isFinite(numeric) && numeric > 0) return numeric
  const parsed = Date.parse(String(value || ''))
  return Number.isFinite(parsed) ? parsed / 1000 : null
}

const getProgressSnapshot = (progress) => {
  if (!progress) return null
  const total = Number(progress.total ?? progress.target ?? 0)
  const processed = Number(
    progress.processed ?? progress.analyzed ?? progress.extracted ?? 0
  )
  if (!Number.isFinite(total) || total <= 0 || !Number.isFinite(processed)) return null

  return {
    total,
    processed: Math.min(total, Math.max(0, processed)),
    updatedAt: parseTimestampSeconds(progress.updated_at ?? progress.updatedAt) || Date.now() / 1000
  }
}

const clampAnalysisBatchInput = (value, maximum = MAX_ANALYSIS_BATCH_SIZE) => {
  if (value === '') return ''
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  return Math.min(maximum, Math.max(1, Math.trunc(numeric)))
}

const isTrackedJobActive = (state) => {
  if (!state?.jobId) return false
  const phase = getTrackedJobPhase(state)
  return phase !== 'complete' && phase !== 'failed' && phase !== 'unavailable' && phase !== 'not_found'
}

const isTrackedJobComplete = (state) => {
  return Boolean(state?.jobId && getTrackedJobPhase(state) === 'complete')
}

const pageHasAttention = () => {
  if (typeof document === 'undefined') return false
  return document.visibilityState === 'visible' && document.hasFocus()
}

const getTrackedJobPhase = (state) => {
  const queueStatus = state?.status?.status
  if (queueStatus === 'queued' || queueStatus === 'deferred') return queueStatus
  return state?.status?.progress?.phase || queueStatus || state?.progress?.phase || 'queued'
}

const getAnalysisButtonLabel = (state) => {
  if (state?.loading) return 'Queueing'
  if (!isTrackedJobActive(state)) return 'Analyze'
  const phase = getTrackedJobPhase(state)
  if (phase === 'queued' || phase === 'deferred') return 'Queued'
  return phase === 'cooling' ? 'Cooling off' : 'Analyzing'
}

const getAnalysisJobProgress = (state) => {
  // Only server progress for this job counts. Cached UI totals and database-wide
  // scored counts may include another job's work, including while this job waits.
  const status = state?.status || {}
  const progress = status.progress || {}
  const kwargs = status.info?.kwargs || {}
  const phase = getTrackedJobPhase(state)
  const isQueued = phase === 'queued' || phase === 'deferred'
  const target = Math.max(0, Number(
    progress.total ?? kwargs.total_fens_to_process ?? kwargs.planned_fens ??
    (kwargs.positions_per_run ? kwargs.positions_per_run * (kwargs.runs || 1) : null) ??
    state?.targetFens ?? state?.targetPlayerFens ?? state?.progress?.target ?? 0
  ) || 0)
  const analyzed = isQueued ? 0 : Math.min(target, Math.max(0, Number(progress.processed) || 0))
  return {
    analyzed,
    failed: isQueued ? 0 : Math.max(0, Number(progress.failed) || 0),
    target,
    percent: target > 0 ? Math.min(100, Math.round(analyzed / target * 100)) : 0,
    phase,
    detail: isQueued ? 'Waiting to start.' : progress.detail,
  }
}

const getPositionJobKey = (job) => {
  const kind = String(job?.progress?.kind || job?.progress?.job_kind || '').toLowerCase()
  if (kind === 'game_update') return 'gameParsing'
  if (kind === 'fen_extraction') return 'fen'
  if (kind === 'tablebase_analysis') return 'tablebase'
  if (kind === 'character_repeated' || kind === 'player') return 'player'
  if (kind === 'most_repeated' || kind === 'global') return 'global'

  const functionName = job?.info?.function
  if (functionName === 'run_fen_pipeline') return 'fen'
  if (functionName === 'run_player_games_analysis_job') return 'gameCompletion'
  if (functionName === 'run_analysis_loop_job') return 'loop'
  if (functionName === 'run_player_analysis_job') return 'player'
  if (functionName === 'run_analysis_job') return 'global'
  return null
}

const getLoopScopeLabel = (state) => {
  const kwargs = state?.status?.info?.kwargs || {}
  const scope = String(kwargs.scope || state?.loopScope || state?.payload?.scope || 'all')
  const playerName = String(
    kwargs.player_name || state?.loopPlayerName || state?.payload?.player_name || ''
  ).trim()

  return scope === 'player'
    ? `Player: ${playerName || 'unknown'}`
    : 'All positions: most repeated first'
}

const getPlayerGameSelectionLabel = (selectionOrder) => {
  const value = String(selectionOrder || '').toLowerCase()
  if (value === 'fair_range') return 'Fair across calendar months'
  if (value === 'oldest') return 'Oldest first'
  if (value === 'latest') return 'Latest first'
  return selectionOrder || '--'
}

const isLoopJobKey = (key) => key === 'loop' || key === 'loopNext'
const isAnalysisJobKey = (key) => (
  key === 'global' || key === 'player' || key === 'gameCompletion' || isLoopJobKey(key)
)

const getAnalysisProcessView = (job) => {
  const kwargs = job?.info?.kwargs || {}
  const progress = job?.progress || {}
  const functionName = String(job?.info?.function || '')
  const isGameCompletion = functionName === 'run_player_games_analysis_job'
  const scope = isGameCompletion
    ? 'player_games'
    : kwargs.scope || (functionName === 'run_player_analysis_job' ? 'player' : 'all')
  const playerName = String(kwargs.player_name || '').trim()
  const positionsPerRun = Number(
    kwargs.positions_per_run || kwargs.chunk_size || kwargs.total_fens_to_process || progress.total || 0
  )
  const runs = Number(kwargs.runs || (isGameCompletion
    ? Math.max(1, Math.ceil(Number(progress.total || kwargs.planned_fens || 0) / Math.max(1, positionsPerRun)))
    : 1))
  const total = Math.max(0, Number(
    progress.total || (isGameCompletion ? kwargs.planned_fens : positionsPerRun * runs)
  ))
  const queueStatus = String(job?.status || 'queued')
  const progressPhase = String(progress.phase || '')
  const isQueued = queueStatus === 'queued' || queueStatus === 'deferred'
  const processed = isQueued ? 0 : Math.min(total, Math.max(0, Number(progress.processed || 0)))
  const percent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0
  const phase = isQueued
    ? 'queued'
    : progressPhase === 'cooling'
      ? 'cooling'
      : 'analyzing'

  return {
    ...job,
    progress: isQueued ? { ...progress, processed: 0, phase: 'queued', detail: 'Waiting to start.' } : job.progress,
    batchSize: Number(kwargs.batches || kwargs.batch_size || 0),
    coolOff: Number(kwargs.cool_off || 0),
    isQueued,
    percent,
    phase,
    playerName,
    positionsPerRun,
    processed,
    runs,
    scope,
    scopeLabel: scope === 'player_games'
      ? `Complete games: ${playerName || 'unknown'}`
      : scope === 'player' ? `Player: ${playerName || 'unknown'}` : 'All positions',
    total,
  }
}

const loadStoredJobState = () => {
  if (typeof window === 'undefined') return {}
  try {
    const parsed = JSON.parse(window.localStorage.getItem(POSITION_JOBS_STORAGE_KEY) || '{}')
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

const storeJobState = (state) => {
  if (typeof window === 'undefined') return
  const trackedState = Object.fromEntries(
    Object.entries(state).filter(([, value]) => value?.jobId)
  )
  if (!Object.keys(trackedState).length) {
    window.localStorage.removeItem(POSITION_JOBS_STORAGE_KEY)
    return
  }
  window.localStorage.setItem(POSITION_JOBS_STORAGE_KEY, JSON.stringify(trackedState))
}

export {
  COMPLETED_JOB_FADE_MS,
  COMPLETED_JOB_VISIBLE_MS,
  DEFAULT_ANALYSIS_NODES,
  ETA_RATE_SMOOTHING,
  MAX_ANALYSIS_BATCH_SIZE,
  MAX_GLOBAL_ANALYSIS_BATCH_SIZE,
  MAX_LOOP_ANALYSIS_BATCH_SIZE,
  POSITION_JOBS_STORAGE_KEY,
  clampAnalysisBatchInput,
  formatCountdown,
  formatDuration,
  formatNumber,
  formatTruncatedMillions,
  getAnalysisButtonLabel,
  getAnalysisJobProgress,
  getAnalysisProcessView,
  getLoopScopeLabel,
  getPlayerGameSelectionLabel,
  getPositionJobKey,
  getProgressSnapshot,
  getTrackedJobPhase,
  isAnalysisJobKey,
  isLoopJobKey,
  isTrackedJobActive,
  isTrackedJobComplete,
  loadStoredJobState,
  pageHasAttention,
  parseTimestampSeconds,
  storeJobState,
}
