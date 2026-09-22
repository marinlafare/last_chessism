import { Chess } from 'chess.js'
import { deleteJson, getJson as fetchJson, postJson } from '../../services/apiClient'
import { formatNumber } from '../../utils/formatters'

const START_FEN = new Chess().fen()
const DEFAULT_ANALYSIS_NODES = 1_000_000
const MAX_ANALYSIS_BATCH_SIZE = 500
const MAX_LOOP_ANALYSIS_BATCH_SIZE = 1000
const POSITION_JOBS_STORAGE_KEY = 'chessism:positions:jobs'
const COMPLETED_JOB_VISIBLE_MS = 5000
const COMPLETED_JOB_FADE_MS = 700
const ETA_RATE_SMOOTHING = 0.35

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

const normalizeFen = (value) => String(value || '').trim().replace(/\s+/g, ' ')

const clampAnalysisBatchInput = (value, maximum = MAX_ANALYSIS_BATCH_SIZE) => {
  if (value === '') return ''
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  return Math.min(maximum, Math.max(1, Math.trunc(numeric)))
}

const validateFen = (value) => {
  const fen = normalizeFen(value)
  try {
    const game = new Chess(fen)
    return { isValid: true, fen: game.fen(), game }
  } catch (err) {
    return { isValid: false, fen, error: err.message || 'Invalid FEN' }
  }
}

const getScoreLabel = (score) => {
  const numeric = Number(score)
  if (!Number.isFinite(numeric)) return '--'

  if (Math.abs(numeric) >= 9000) {
    const mateDistance = Math.abs(Math.round(Math.abs(numeric) - 10000))
    return mateDistance > 0 ? `Mate ${mateDistance}` : 'Mate'
  }

  const pawns = numeric / 100
  return `${pawns > 0 ? '+' : ''}${pawns.toFixed(2)}`
}

const moveToSan = (fen, move) => {
  const text = String(move || '')
  if (text.length < 4) return text || '--'

  try {
    const game = new Chess(fen)
    const result = game.move({
      from: text.slice(0, 2),
      to: text.slice(2, 4),
      promotion: text.slice(4, 5) || undefined
    })
    return result?.san || text
  } catch {
    return text
  }
}

const getAnalysisLines = (result) => {
  const analysis = result?.analysis
  if (Array.isArray(analysis)) return analysis
  return analysis && typeof analysis === 'object' ? [analysis] : []
}

const isTrackedJobActive = (state) => {
  if (!state?.jobId) return false
  const phase = getTrackedJobPhase(state)
  return phase !== 'complete' && phase !== 'failed' && phase !== 'not_found'
}

const isTrackedJobComplete = (state) => {
  return Boolean(state?.jobId && getTrackedJobPhase(state) === 'complete')
}

const pageHasAttention = () => {
  if (typeof document === 'undefined') return false
  return document.visibilityState === 'visible' && document.hasFocus()
}

const getTrackedJobPhase = (state) => {
  return state?.status?.progress?.phase || state?.progress?.phase || state?.status?.status || 'queued'
}

const getPositionJobKey = (job) => {
  const kind = String(job?.progress?.kind || job?.progress?.job_kind || '').toLowerCase()
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
  const processed = Math.min(total, Math.max(0, Number(progress.processed || 0)))
  const percent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0
  const queueStatus = String(job?.status || 'queued')
  const progressPhase = String(progress.phase || '')
  const isQueued = queueStatus === 'queued' || queueStatus === 'deferred'
  const phase = isQueued
    ? 'queued'
    : progressPhase === 'cooling'
      ? 'cooling'
      : 'analyzing'

  return {
    ...job,
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

const getAudioContext = () => {
  const AudioContextClass = window.AudioContext || window.webkitAudioContext
  return AudioContextClass ? new AudioContextClass() : null
}

const unlockCompletionAudio = (audioContextRef) => {
  if (typeof window === 'undefined') return
  const context = audioContextRef.current || getAudioContext()
  if (!context) return
  audioContextRef.current = context
  context.resume?.()
}

const playCompletionSound = (audioContextRef) => {
  if (typeof window === 'undefined') return
  const context = audioContextRef.current || getAudioContext()
  if (!context) return
  audioContextRef.current = context

  const now = context.currentTime
  const notes = [
    { frequency: 246.94, start: 0, duration: 0.16 },
    { frequency: 277.18, start: 0.17, duration: 0.16 },
    { frequency: 293.66, start: 0.34, duration: 0.22 }
  ]

  notes.forEach((note) => {
    const oscillator = context.createOscillator()
    const gain = context.createGain()
    oscillator.type = 'triangle'
    oscillator.frequency.setValueAtTime(note.frequency, now + note.start)
    gain.gain.setValueAtTime(0.0001, now + note.start)
    gain.gain.exponentialRampToValueAtTime(0.12, now + note.start + 0.02)
    gain.gain.exponentialRampToValueAtTime(0.0001, now + note.start + note.duration)
    oscillator.connect(gain)
    gain.connect(context.destination)
    oscillator.start(now + note.start)
    oscillator.stop(now + note.start + note.duration + 0.03)
  })
}

async function analyzeFen({ fen, nodesLimit, multipv }) {
  const payload = await postJson('/analysis/fen', {
    fens: [fen],
    nodes_limit: Number(nodesLimit),
    multipv: Number(multipv)
  })
  return Array.isArray(payload) ? payload[0] : payload
}

export {
  COMPLETED_JOB_FADE_MS,
  COMPLETED_JOB_VISIBLE_MS,
  DEFAULT_ANALYSIS_NODES,
  ETA_RATE_SMOOTHING,
  MAX_ANALYSIS_BATCH_SIZE,
  MAX_LOOP_ANALYSIS_BATCH_SIZE,
  POSITION_JOBS_STORAGE_KEY,
  START_FEN,
  analyzeFen,
  clampAnalysisBatchInput,
  deleteJson,
  fetchJson,
  formatCountdown,
  formatDuration,
  formatNumber,
  getAnalysisLines,
  getAnalysisProcessView,
  getLoopScopeLabel,
  getPlayerGameSelectionLabel,
  getPositionJobKey,
  getProgressSnapshot,
  getScoreLabel,
  getTrackedJobPhase,
  isAnalysisJobKey,
  isLoopJobKey,
  isTrackedJobActive,
  isTrackedJobComplete,
  loadStoredJobState,
  moveToSan,
  pageHasAttention,
  parseTimestampSeconds,
  playCompletionSound,
  postJson,
  storeJobState,
  unlockCompletionAudio,
  validateFen,
}
