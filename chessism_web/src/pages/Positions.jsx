import { useEffect, useMemo, useRef, useState } from 'react'
import { Chess } from 'chess.js'
import { Chessboard } from 'react-chessboard'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const START_FEN = new Chess().fen()
const DEFAULT_ANALYSIS_NODES = 1_000_000
const MAX_ANALYSIS_BATCH_SIZE = 500
const MAX_LOOP_ANALYSIS_BATCH_SIZE = 1000
const POSITION_JOBS_STORAGE_KEY = 'chessism:positions:jobs'
const COMPLETED_JOB_VISIBLE_MS = 5000
const COMPLETED_JOB_FADE_MS = 700
const ETA_RATE_SMOOTHING = 0.35

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US')
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

async function fetchJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { Accept: 'application/json' }
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function postJson(path, body) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function deleteJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: 'DELETE',
    headers: { Accept: 'application/json' }
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function analyzeFen({ fen, nodesLimit, multipv }) {
  const payload = await postJson('/analysis/fen', {
    fens: [fen],
    nodes_limit: Number(nodesLimit),
    multipv: Number(multipv)
  })
  return Array.isArray(payload) ? payload[0] : payload
}

function Positions() {
  const [fenInput, setFenInput] = useState(START_FEN)
  const [nodesLimit, setNodesLimit] = useState(1_000_000)
  const [multipv, setMultipv] = useState(4)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [boardWidth, setBoardWidth] = useState(420)
  const [coverage, setCoverage] = useState(null)
  const [coverageError, setCoverageError] = useState('')
  const [analysisCounts, setAnalysisCounts] = useState(null)
  const [remainingFenGames, setRemainingFenGames] = useState(null)
  const [globalJob, setGlobalJob] = useState({ totalFens: 100, batchSize: MAX_ANALYSIS_BATCH_SIZE })
  const [playerJob, setPlayerJob] = useState({
    playerName: '',
    totalFens: 100,
    batchSize: MAX_ANALYSIS_BATCH_SIZE
  })
  const [loopJob, setLoopJob] = useState({
    scope: 'all',
    playerName: '',
    positionsPerRun: 20_000,
    runs: 4,
    batchSize: MAX_ANALYSIS_BATCH_SIZE,
    coolOff: 300
  })
  const [playerGameAnalysis, setPlayerGameAnalysis] = useState({
    playerName: '',
    selectionMode: 'latest',
    dateFrom: '',
    dateTo: '',
    gameLimit: 200,
    useAll: true,
    rangeOrder: 'latest',
    batchSize: 500,
    coolOff: 120,
    scope: null,
    preview: null,
    loadingScope: false,
    loadingPreview: false,
    error: ''
  })
  const [playerInspection, setPlayerInspection] = useState({ loading: false, error: '', data: null })
  const [jobState, setJobState] = useState(loadStoredJobState)
  const [analysisProcesses, setAnalysisProcesses] = useState([])
  const [analysisProcessesError, setAnalysisProcessesError] = useState('')
  const [deletingAnalysisJobIds, setDeletingAnalysisJobIds] = useState(() => new Set())
  const [etaClockMs, setEtaClockMs] = useState(Date.now())
  const [, setEtaRevision] = useState(0)
  const boardWrapRef = useRef(null)
  const audioContextRef = useRef(null)
  const completedSoundJobsRef = useRef(new Set())
  const completionTimersRef = useRef(new Map())
  const fadeTimersRef = useRef(new Map())
  const jobStateRef = useRef(jobState)
  const jobCardNodesRef = useRef(new Map())
  const jobCardVisibilityRef = useRef(new Map())
  const jobCardObserverRef = useRef(null)
  const activeJobPollSignatureRef = useRef('')
  const etaEstimatesRef = useRef(new Map())

  const validation = useMemo(() => validateFen(fenInput), [fenInput])
  const analysisLines = useMemo(() => getAnalysisLines(result), [result])
  const bestLine = analysisLines[0]
  const turnLabel = validation.game?.turn() === 'b' ? 'Black' : 'White'
  const scoredPositions = Number(analysisCounts?.analyzed_fens || 0)
  const pendingPositions = Number(
    analysisCounts?.unscored_fens ?? Math.max(0, Number(coverage?.n_positions || 0) - scoredPositions)
  )
  const coverageBarItems = useMemo(() => {
    const gamesValue = Number(coverage?.n_games_in_db || 0)
    const analyzedValue = scoredPositions
    const visualLimit = Math.max(1, gamesValue, analyzedValue) * 2
    const items = [
      { key: 'games', label: 'Games', value: gamesValue, ready: Boolean(coverage) },
      { key: 'positions', label: 'Positions', value: Number(coverage?.n_positions || 0), ready: Boolean(coverage) },
      { key: 'analyzed', label: 'Scored Positions', value: analyzedValue, ready: Boolean(analysisCounts) }
    ]

    return items.map((item) => {
      const capped = item.key === 'positions' && item.ready && item.value > visualLimit
      const visualValue = capped ? visualLimit : item.value

      return {
        ...item,
        capped,
        percent: item.ready && visualValue > 0 ? Math.max(2, (visualValue / visualLimit) * 100) : 0
      }
    })
  }, [analysisCounts, coverage, scoredPositions])
  const activeLoopJobCount = [jobState.loop, jobState.loopNext].filter(isTrackedJobActive).length
  const loopJobIsQueueing = Boolean(jobState.loop?.loading || jobState.loopNext?.loading)
  const analysisProcessViews = useMemo(
    () => analysisProcesses.map(getAnalysisProcessView),
    [analysisProcesses]
  )
  const hasRunningAnalysis = analysisProcessViews.some((process) => !process.isQueued) || (
    Object.values(jobState).some((state) => {
      if (!isTrackedJobActive(state)) return false
      const phase = getTrackedJobPhase(state)
      return phase !== 'queued' && phase !== 'deferred'
    })
  )
  const playerGamePreview = playerGameAnalysis.preview
  const playerGamePreviewPauses = playerGamePreview
    ? Math.max(0, Math.ceil(Number(playerGamePreview.stockfish_fens || 0) / 5000) - 1)
    : 0
  const playerGamePreviewTotalSeconds = playerGamePreview
    ? Number(playerGamePreview.estimated_seconds || 0) + (
        playerGamePreviewPauses * Math.max(0, Number(playerGameAnalysis.coolOff) || 0)
      )
    : 0

  useEffect(() => {
    const observations = [
      ...analysisProcesses.map((job) => ({
        jobId: job.job_id,
        progress: job.progress,
        status: job.status,
        enqueueTime: job.info?.enqueue_time
      })),
      ...Object.values(jobState).map((state) => ({
        jobId: state?.jobId,
        progress: state?.status?.progress || state?.progress,
        status: state?.status?.status || getTrackedJobPhase(state),
        enqueueTime: state?.status?.info?.enqueue_time
      }))
    ]
    let changed = false

    observations.forEach((observation) => {
      const jobId = String(observation.jobId || '')
      const snapshot = getProgressSnapshot(observation.progress)
      if (!jobId || !snapshot) return

      const phase = String(observation.progress?.phase || observation.status || '').toLowerCase()
      if (phase === 'complete' || phase === 'failed' || phase === 'not_found') {
        changed = etaEstimatesRef.current.delete(jobId) || changed
        return
      }

      const previous = etaEstimatesRef.current.get(jobId)
      if (previous && snapshot.processed < previous.processed) return
      if (
        previous &&
        snapshot.processed === previous.processed &&
        snapshot.total === previous.total
      ) return

      let rate = previous?.rate || null
      if (
        previous &&
        snapshot.processed > previous.processed &&
        snapshot.updatedAt > previous.sampleAt
      ) {
        const measuredRate = (
          (snapshot.processed - previous.processed) /
          (snapshot.updatedAt - previous.sampleAt)
        )
        if (Number.isFinite(measuredRate) && measuredRate > 0) {
          rate = rate
            ? (rate * (1 - ETA_RATE_SMOOTHING)) + (measuredRate * ETA_RATE_SMOOTHING)
            : measuredRate
        }
      } else if (!previous && snapshot.processed > 0) {
        const enqueuedAt = parseTimestampSeconds(observation.enqueueTime)
        const elapsed = enqueuedAt ? snapshot.updatedAt - enqueuedAt : 0
        if (elapsed >= 3) rate = snapshot.processed / elapsed
      }

      etaEstimatesRef.current.set(jobId, {
        processed: snapshot.processed,
        total: snapshot.total,
        sampleAt: snapshot.updatedAt,
        rate
      })
      changed = true
    })

    if (changed) setEtaRevision((revision) => revision + 1)
  }, [analysisProcesses, jobState])

  useEffect(() => {
    if (!hasRunningAnalysis) return undefined
    setEtaClockMs(Date.now())
    const timer = window.setInterval(() => setEtaClockMs(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [hasRunningAnalysis])

  useEffect(() => {
    const node = boardWrapRef.current
    if (!node) return undefined

    const updateWidth = () => {
      setBoardWidth(Math.max(260, Math.min(420, Math.floor(node.clientWidth))))
    }

    updateWidth()
    const observer = new ResizeObserver(updateWidth)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  const loadCoverage = async () => {
    try {
      const payload = await fetchJson('/games/generalities')
      setCoverage(payload)
      setCoverageError('')
      return payload
    } catch (err) {
      setCoverageError(err.message || 'Coverage unavailable')
      return null
    }
  }

  const loadFenRemaining = async () => {
    const payload = await fetchJson('/fens/remaining_games')
    const remaining = Number(payload.remaining_games || 0)
    setRemainingFenGames(remaining)
    return remaining
  }

  const loadAnalysisCounts = async () => {
    const payload = await fetchJson('/fens/analysis_counts')
    setAnalysisCounts(payload)
    return payload
  }

  const loadAnalysisProcesses = async () => {
    try {
      const payload = await fetchJson('/jobs/analysis')
      setAnalysisProcesses(Array.isArray(payload.jobs) ? payload.jobs : [])
      setAnalysisProcessesError('')
    } catch (err) {
      setAnalysisProcessesError(err instanceof Error ? err.message : 'Unable to load analysis processes.')
    }
  }

  const hydrateActiveJobs = async () => {
    const payload = await fetchJson('/jobs/active')
    const jobs = Array.isArray(payload.jobs) ? payload.jobs : []
    jobs.forEach((job) => {
      const progress = job.progress || {}
      const key = getPositionJobKey(job)
      if (!key) return

      setJobState((current) => {
        const next = { ...current }
        Object.entries(next).forEach(([trackedKey, trackedState]) => {
          if (trackedKey !== key && trackedState?.jobId === job.job_id) {
            delete next[trackedKey]
          }
        })

        if (isTrackedJobActive(next[key]) && next[key]?.jobId !== job.job_id) {
          storeJobState(next)
          return next
        }

        const target = Math.max(1, Number(progress.total || 1))
        const processed = Number(progress.processed || 0)
        const trackedProgress = key === 'fen'
          ? {
              extracted: Math.min(target, processed),
              target,
              percent: Math.min(100, Math.round((Math.min(target, processed) / target) * 100)),
              phase: progress.phase,
              detail: progress.detail
            }
          : {
              analyzed: Math.min(target, processed),
              failed: Number(progress.failed || 0),
              target,
              percent: Math.min(100, Math.round((Math.min(target, processed) / target) * 100)),
              phase: progress.phase,
              detail: progress.detail
            }
        next[key] = {
          ...(next[key] || {}),
          payload: { job_id: job.job_id },
          jobId: job.job_id,
          loading: false,
          error: '',
          status: job,
          targetFens: target,
          ...(key === 'loop' ? {
            loopScope: job?.info?.kwargs?.scope || 'all',
            loopPlayerName: job?.info?.kwargs?.player_name || ''
          } : {}),
          progress: trackedProgress
        }
        storeJobState(next)
        return next
      })
    })
  }

  useEffect(() => {
    loadCoverage()
    loadFenRemaining().catch(() => {})
    loadAnalysisCounts().catch(() => {})
    hydrateActiveJobs().catch(() => {})
    loadAnalysisProcesses().catch(() => {})
  }, [])

  useEffect(() => {
    const timer = window.setInterval(() => {
      loadAnalysisProcesses().catch(() => {})
      loadFenRemaining().catch(() => {})
      hydrateActiveJobs().catch(() => {})
    }, 3000)
    return () => window.clearInterval(timer)
  }, [])

  const updateJobState = (key, patch) => {
    setJobState((current) => {
      const next = {
        ...current,
        [key]: {
          ...(current[key] || {}),
          ...patch
        }
      }
      storeJobState(next)
      return next
    })
  }

  const cancelJobDismissTimers = (key) => {
    const completionTimerState = completionTimersRef.current.get(key)
    if (completionTimerState?.timer) {
      window.clearTimeout(completionTimerState.timer)
    }
    completionTimersRef.current.delete(key)

    const fadeTimer = fadeTimersRef.current.get(key)
    if (fadeTimer) {
      window.clearTimeout(fadeTimer)
      fadeTimersRef.current.delete(key)
    }
  }

  const clearAllJobDismissTimers = () => {
    completionTimersRef.current.forEach((timerState) => {
      if (timerState?.timer) window.clearTimeout(timerState.timer)
    })
    completionTimersRef.current.clear()
    fadeTimersRef.current.forEach((timer) => window.clearTimeout(timer))
    fadeTimersRef.current.clear()
  }

  const clearJobState = (key) => {
    setJobState((current) => {
      if (!current[key]) return current
      const next = { ...current }
      delete next[key]
      storeJobState(next)
      return next
    })
  }

  const canRunJobDismissCountdown = (key) => {
    const state = jobStateRef.current[key]
    return (
      isTrackedJobComplete(state) &&
      !state?.fading &&
      pageHasAttention() &&
      jobCardVisibilityRef.current.get(key) === true
    )
  }

  const pauseJobDismissCountdown = (key) => {
    const timerState = completionTimersRef.current.get(key)
    if (!timerState?.timer) return

    window.clearTimeout(timerState.timer)
    const elapsedMs = Date.now() - Number(timerState.startedAt || Date.now())
    completionTimersRef.current.set(key, {
      timer: null,
      startedAt: null,
      remainingMs: Math.max(0, Number(timerState.remainingMs || 0) - elapsedMs)
    })
  }

  const startJobDismissCountdown = (key) => {
    if (!canRunJobDismissCountdown(key)) return

    const timerState = completionTimersRef.current.get(key) || {
      timer: null,
      startedAt: null,
      remainingMs: COMPLETED_JOB_VISIBLE_MS
    }
    if (timerState.timer) return

    const remainingMs = Math.max(0, Number(timerState.remainingMs ?? COMPLETED_JOB_VISIBLE_MS))
    const timer = window.setTimeout(() => {
      completionTimersRef.current.delete(key)
      updateJobState(key, { fading: true })

      const fadeTimer = window.setTimeout(() => {
        fadeTimersRef.current.delete(key)
        clearJobState(key)
      }, COMPLETED_JOB_FADE_MS)

      fadeTimersRef.current.set(key, fadeTimer)
    }, remainingMs)

    completionTimersRef.current.set(key, {
      timer,
      startedAt: Date.now(),
      remainingMs
    })
  }

  const syncJobDismissCountdown = (key) => {
    if (canRunJobDismissCountdown(key)) {
      startJobDismissCountdown(key)
    } else {
      pauseJobDismissCountdown(key)
    }
  }

  const syncAllJobDismissCountdowns = () => {
    Object.keys(jobStateRef.current).forEach(syncJobDismissCountdown)
  }

  const setJobCardRef = (key) => (node) => {
    const previousNode = jobCardNodesRef.current.get(key)
    const observer = jobCardObserverRef.current

    if (previousNode && previousNode !== node && observer) {
      observer.unobserve(previousNode)
    }

    if (!node) {
      jobCardNodesRef.current.delete(key)
      jobCardVisibilityRef.current.delete(key)
      pauseJobDismissCountdown(key)
      return
    }

    jobCardNodesRef.current.set(key, node)
    node.dataset.jobKey = key

    if (observer) {
      observer.observe(node)
      return
    }

    jobCardVisibilityRef.current.set(key, true)
    syncJobDismissCountdown(key)
  }

  const handleCompletedJob = (key, state, status, patch) => {
    const jobId = state.jobId
    const succeeded = status.result?.success !== false
    patch.completedAt = patch.completedAt || state.completedAt || Date.now()
    patch.fading = false

    if (succeeded && jobId && !completedSoundJobsRef.current.has(jobId)) {
      completedSoundJobsRef.current.add(jobId)
      playCompletionSound(audioContextRef)
    }
  }

  useEffect(() => {
    jobStateRef.current = jobState

    Array.from(completionTimersRef.current.keys()).forEach((key) => {
      const state = jobState[key]
      if (!state || !isTrackedJobComplete(state)) {
        cancelJobDismissTimers(key)
      }
    })

    Object.entries(jobState).forEach(([key, state]) => {
      if (!isTrackedJobComplete(state) || state.fading) return

      if (!completionTimersRef.current.has(key)) {
        completionTimersRef.current.set(key, {
          timer: null,
          startedAt: null,
          remainingMs: COMPLETED_JOB_VISIBLE_MS
        })
      }
      syncJobDismissCountdown(key)
    })
  }, [jobState])

  useEffect(() => {
    if (typeof window !== 'undefined' && typeof IntersectionObserver !== 'undefined') {
      const observer = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          const key = entry.target.dataset.jobKey
          if (!key) return
          jobCardVisibilityRef.current.set(key, entry.isIntersecting && entry.intersectionRatio > 0)
          syncJobDismissCountdown(key)
        })
      }, { threshold: 0.25 })

      jobCardObserverRef.current = observer
      jobCardNodesRef.current.forEach((node) => observer.observe(node))
    } else {
      jobCardNodesRef.current.forEach((node, key) => {
        jobCardVisibilityRef.current.set(key, true)
        syncJobDismissCountdown(key)
      })
    }

    const handleAttentionChange = () => {
      syncAllJobDismissCountdowns()
    }

    document.addEventListener('visibilitychange', handleAttentionChange)
    window.addEventListener('focus', handleAttentionChange)
    window.addEventListener('blur', handleAttentionChange)

    return () => {
      document.removeEventListener('visibilitychange', handleAttentionChange)
      window.removeEventListener('focus', handleAttentionChange)
      window.removeEventListener('blur', handleAttentionChange)
      jobCardObserverRef.current?.disconnect()
      jobCardObserverRef.current = null
      clearAllJobDismissTimers()
    }
  }, [])

  useEffect(() => {
    const activeJobs = Object.entries(jobState).filter(([, state]) => {
      return isTrackedJobActive(state)
    })

    if (!activeJobs.length) return undefined
    const activeSignature = activeJobs
      .map(([key, state]) => `${key}:${state.jobId}`)
      .sort()
      .join('|')

    const pollJobs = async () => {
      await Promise.all(
        activeJobs.map(async ([key, state]) => {
          try {
            const status = await fetchJson(`/jobs/${encodeURIComponent(state.jobId)}`)
            const patch = { status, error: '' }

            if (key === 'fen') {
              const remaining = await loadFenRemaining()
              const serverProgress = status.progress
              if (serverProgress) {
                const targetGames = Math.max(1, Number(serverProgress.total || 1))
                const extracted = Math.min(targetGames, Math.max(0, Number(serverProgress.processed || 0)))
                patch.progress = {
                  remaining,
                  extracted,
                  target: targetGames,
                  percent: Math.min(100, Math.round((extracted / targetGames) * 100)),
                  phase: serverProgress.phase,
                  detail: serverProgress.detail
                }
              }
            }

            if (isAnalysisJobKey(key) && status.progress) {
              const processed = Number(status.progress.processed || 0)
              const failed = Number(status.progress.failed || 0)
              const targetFens = Math.max(1, Number(status.progress.total || state.targetFens || state.targetPlayerFens || 1))
              patch.progress = {
                analyzed: Math.min(targetFens, processed),
                failed,
                target: targetFens,
                percent: Math.min(100, Math.round((Math.min(targetFens, processed) / targetFens) * 100)),
                phase: status.progress.phase,
                detail: status.progress.detail
              }
            } else if (key === 'global') {
              const latestCounts = await loadAnalysisCounts()
              const startAnalyzed = Number(state.startAnalyzed ?? latestCounts?.analyzed_fens ?? 0)
              const currentAnalyzed = Number(latestCounts?.analyzed_fens ?? startAnalyzed)
              const targetFens = Math.max(1, Number(state.targetFens || 1))
              const analyzed = Math.max(0, currentAnalyzed - startAnalyzed)
              patch.progress = {
                analyzed: Math.min(targetFens, analyzed),
                target: targetFens,
                percent: Math.min(100, Math.round((Math.min(targetFens, analyzed) / targetFens) * 100))
              }
            }
            const phase = status.progress?.phase || status.status
            if (phase === 'complete') {
              await loadCoverage()
              await loadAnalysisCounts()
              handleCompletedJob(key, state, status, patch)
            }
            updateJobState(key, patch)
          } catch (err) {
            const message = err?.message || 'Job status unavailable'
            if (message.includes('HTTP 404')) {
              clearJobState(key)
              return
            }
            updateJobState(key, { loading: false, error: message })
          }
        })
      )
    }

    if (activeJobPollSignatureRef.current !== activeSignature) {
      activeJobPollSignatureRef.current = activeSignature
      pollJobs()
    }
    const timer = window.setInterval(pollJobs, 3000)
    return () => window.clearInterval(timer)
  }, [jobState])

  const enqueueJob = async ({ key, path, body, meta = {} }) => {
    cancelJobDismissTimers(key)
    updateJobState(key, { loading: true, error: '', payload: null, status: null, jobId: null, ...meta })
    try {
      const payload = await postJson(path, body)
      updateJobState(key, { loading: false, payload, jobId: payload.job_id || null })
      await loadCoverage()
      await loadAnalysisCounts()
      return payload
    } catch (err) {
      updateJobState(key, { loading: false, error: err.message || 'Job failed' })
      return null
    }
  }

  const handleGlobalAnalysis = (event) => {
    event.preventDefault()
    unlockCompletionAudio(audioContextRef)
    const currentAnalyzed = Number(analysisCounts?.analyzed_fens || 0)
    const targetFens = Number(globalJob.totalFens)
    enqueueJob({
      key: 'global',
      path: '/analysis/run_job',
      body: {
        total_fens_to_process: Number(globalJob.totalFens),
        batch_size: Number(globalJob.batchSize),
        nodes_limit: DEFAULT_ANALYSIS_NODES
      },
      meta: {
        startAnalyzed: currentAnalyzed,
        targetFens,
        progress: {
          analyzed: 0,
          target: Math.max(1, targetFens),
          percent: 0
        }
      }
    })
  }

  const handlePlayerAnalysis = (event) => {
    event.preventDefault()
    unlockCompletionAudio(audioContextRef)
    const playerName = String(playerJob.playerName || '').trim().toLowerCase()
    if (!playerName) {
      updateJobState('player', { loading: false, error: 'Player name is required.', payload: null })
      return
    }

    enqueueJob({
      key: 'player',
      path: '/analysis/run_player_job',
      body: {
        player_name: playerName,
        total_fens_to_process: Number(playerJob.totalFens),
        batch_size: Number(playerJob.batchSize),
        nodes_limit: DEFAULT_ANALYSIS_NODES
      },
      meta: {
        targetPlayerFens: Number(playerJob.totalFens),
        progress: {
          analyzed: 0,
          failed: 0,
          target: Math.max(1, Number(playerJob.totalFens)),
          percent: 0
        }
      }
    })
  }

  const handleAnalysisLoop = (event) => {
    event.preventDefault()
    unlockCompletionAudio(audioContextRef)
    const playerName = String(loopJob.playerName || '').trim().toLowerCase()
    const queueKey = isTrackedJobActive(jobState.loop) ? 'loopNext' : 'loop'
    if (loopJob.scope === 'player' && !playerName) {
      updateJobState(queueKey, { loading: false, error: 'Player name is required for player loops.', payload: null })
      return
    }

    const positionsPerRun = Math.max(1, Number(loopJob.positionsPerRun) || 1)
    const runs = Math.max(1, Number(loopJob.runs) || 1)
    const targetFens = positionsPerRun * runs
    enqueueJob({
      key: queueKey,
      path: '/analysis/run_loop_job',
      body: {
        scope: loopJob.scope,
        player_name: loopJob.scope === 'player' ? playerName : null,
        positions_per_run: positionsPerRun,
        runs,
        batches: Number(loopJob.batchSize),
        cool_off: Math.max(0, Number(loopJob.coolOff) || 0),
        nodes_limit: DEFAULT_ANALYSIS_NODES
      },
      meta: {
        targetFens,
        loopScope: loopJob.scope,
        loopPlayerName: playerName,
        progress: {
          analyzed: 0,
          failed: 0,
          target: targetFens,
          percent: 0,
          phase: 'queued',
          detail: `waiting to start run 1/${runs}`
        }
      }
    })
  }

  const playerGameScopePayload = () => ({
    player_name: String(playerGameAnalysis.playerName || '').trim().toLowerCase(),
    selection_mode: playerGameAnalysis.selectionMode,
    date_from: playerGameAnalysis.selectionMode === 'range' ? playerGameAnalysis.dateFrom || null : null,
    date_to: playerGameAnalysis.selectionMode === 'range' ? playerGameAnalysis.dateTo || null : null
  })

  const handleInspectPlayerGameScope = async (event) => {
    event.preventDefault()
    const payload = playerGameScopePayload()
    if (!payload.player_name) {
      setPlayerGameAnalysis((current) => ({ ...current, error: 'Player name is required.' }))
      return
    }
    if (payload.selection_mode === 'range' && (!payload.date_from || !payload.date_to)) {
      setPlayerGameAnalysis((current) => ({ ...current, error: 'Choose both dates for the range.' }))
      return
    }

    setPlayerGameAnalysis((current) => ({
      ...current,
      loadingScope: true,
      error: '',
      scope: null,
      preview: null
    }))
    try {
      const scope = await postJson('/analysis/player_games/scope', payload)
      setPlayerGameAnalysis((current) => ({ ...current, loadingScope: false, scope, error: '' }))
    } catch (err) {
      setPlayerGameAnalysis((current) => ({
        ...current,
        loadingScope: false,
        error: err instanceof Error ? err.message : 'Unable to inspect player games.'
      }))
    }
  }

  const handlePreviewPlayerGameAnalysis = async () => {
    if (!playerGameAnalysis.scope) {
      setPlayerGameAnalysis((current) => ({ ...current, error: 'Inspect the game selection first.' }))
      return
    }
    const useAll = playerGameAnalysis.selectionMode === 'range' && playerGameAnalysis.useAll
    const gameLimit = Math.max(1, Number(playerGameAnalysis.gameLimit) || 1)
    setPlayerGameAnalysis((current) => ({ ...current, loadingPreview: true, preview: null, error: '' }))
    try {
      const preview = await postJson('/analysis/player_games/preview', {
        ...playerGameScopePayload(),
        game_limit: useAll ? null : gameLimit,
        use_all: useAll,
        range_order: playerGameAnalysis.rangeOrder
      })
      setPlayerGameAnalysis((current) => ({ ...current, loadingPreview: false, preview, error: '' }))
    } catch (err) {
      setPlayerGameAnalysis((current) => ({
        ...current,
        loadingPreview: false,
        error: err instanceof Error ? err.message : 'Unable to calculate the FEN workload.'
      }))
    }
  }

  const handleConfirmPlayerGameAnalysis = async () => {
    const preview = playerGameAnalysis.preview
    if (!preview?.plan_id) return
    unlockCompletionAudio(audioContextRef)
    const payload = await enqueueJob({
      key: 'gameCompletion',
      path: '/analysis/player_games/run_job',
      body: {
        plan_id: preview.plan_id,
        batch_size: Number(playerGameAnalysis.batchSize),
        cool_off: Math.max(0, Number(playerGameAnalysis.coolOff) || 0),
        nodes_limit: DEFAULT_ANALYSIS_NODES
      },
      meta: {
        targetFens: Number(preview.fens_to_analyze || 0),
        progress: {
          analyzed: 0,
          failed: 0,
          target: Math.max(1, Number(preview.fens_to_analyze || 0)),
          percent: 0,
          phase: 'queued',
          detail: `${formatNumber(preview.selected_games)} frozen games for ${preview.player_name}`
        }
      }
    })
    if (payload) {
      setPlayerGameAnalysis((current) => ({ ...current, preview: null }))
      await loadAnalysisProcesses()
    }
  }

  const handleInspectPlayer = async () => {
    const playerName = String(playerJob.playerName || '').trim().toLowerCase()
    if (!playerName) {
      setPlayerInspection({ loading: false, error: 'Player name is required.', data: null })
      return
    }

    setPlayerInspection({ loading: true, error: '', data: null })
    try {
      const payload = await fetchJson(`/fens/players/${encodeURIComponent(playerName)}/analysis_counts`)
      setPlayerInspection({ loading: false, error: '', data: payload })
    } catch (err) {
      setPlayerInspection({
        loading: false,
        error: err instanceof Error ? err.message : 'Unable to inspect player positions.',
        data: null
      })
    }
  }

  const handleDeleteQueuedAnalysis = async (key) => {
    const state = jobState[key]
    const jobId = state?.jobId
    const phase = getTrackedJobPhase(state)
    if (!jobId || (phase !== 'queued' && phase !== 'deferred')) return

    const confirmed = window.confirm('Delete this queued analysis? The currently running analysis will not be affected.')
    if (!confirmed) return

    updateJobState(key, { deleting: true, error: '' })
    try {
      await deleteJson(`/jobs/${encodeURIComponent(jobId)}/queued`)
      cancelJobDismissTimers(key)
      clearJobState(key)
      await loadAnalysisProcesses()
    } catch (err) {
      updateJobState(key, {
        deleting: false,
        error: err instanceof Error ? err.message : 'Unable to delete the queued analysis.'
      })
    }
  }

  const handleDeleteAnalysisProcess = async (process) => {
    if (!process?.job_id || !process.isQueued) return
    const confirmed = window.confirm('Delete this queued analysis? Running analyses will not be affected.')
    if (!confirmed) return

    setDeletingAnalysisJobIds((current) => new Set(current).add(process.job_id))
    try {
      await deleteJson(`/jobs/${encodeURIComponent(process.job_id)}/queued`)
      setJobState((current) => {
        const next = { ...current }
        Object.entries(next).forEach(([key, state]) => {
          if (state?.jobId === process.job_id) delete next[key]
        })
        storeJobState(next)
        return next
      })
      await loadAnalysisProcesses()
    } catch (err) {
      setAnalysisProcessesError(err instanceof Error ? err.message : 'Unable to delete the queued analysis.')
    } finally {
      setDeletingAnalysisJobIds((current) => {
        const next = new Set(current)
        next.delete(process.job_id)
        return next
      })
    }
  }

  const renderEstimatedTime = (jobId, progress, status) => {
    const phase = String(progress?.phase || status || '').toLowerCase()
    if (
      !jobId ||
      phase === 'queued' ||
      phase === 'deferred' ||
      phase === 'complete' ||
      phase === 'failed' ||
      phase === 'not_found'
    ) return null

    const snapshot = getProgressSnapshot(progress)
    if (!snapshot || snapshot.processed >= snapshot.total) return null

    const estimate = etaEstimatesRef.current.get(String(jobId))
    const hasRate = Number.isFinite(estimate?.rate) && estimate.rate > 0
    let countdown = null
    if (hasRate) {
      const remainingAtSample = Math.max(0, estimate.total - estimate.processed) / estimate.rate
      const secondsSinceSample = Math.max(0, (etaClockMs / 1000) - estimate.sampleAt)
      countdown = Math.max(1, remainingAtSample - secondsSinceSample)
    }

    return (
      <div className={`job-progress-eta ${hasRate ? '' : 'is-calculating'}`}>
        <span>Estimated time left</span>
        <strong>{hasRate ? formatCountdown(countdown) : 'Calculating…'}</strong>
      </div>
    )
  }

  const renderJobResult = (key) => {
    const state = jobState[key] || {}
    if (state.error) return <div className="status-banner warn">{state.error}</div>
    if (!state.payload) return null
    const status = getTrackedJobPhase(state)
    const info = state.status?.info
    const resultInfo = state.status?.result
    const functionName = info?.function || resultInfo?.queue_name || state.status?.queue_name || 'job'
    const canDeleteQueuedAnalysis = (
      isAnalysisJobKey(key) &&
      (status === 'queued' || status === 'deferred')
    )

    return (
      <div
        className={`job-result ${state.fading ? 'job-result-fading' : ''}`}
        ref={setJobCardRef(key)}
      >
        <span>{status.replace('_', ' ')}</span>
        <small>{functionName}</small>
        {isLoopJobKey(key) ? (
          <strong className="job-scope-label">Running scope: {getLoopScopeLabel(state)}</strong>
        ) : null}
        {key === 'gameCompletion' ? (
          <strong className="job-scope-label">
            Completing games for {state.status?.info?.kwargs?.player_name || state.payload?.player_name || 'player'}
          </strong>
        ) : null}
        {canDeleteQueuedAnalysis ? (
          <button
            className="btn job-delete-queued"
            type="button"
            disabled={state.deleting}
            onClick={() => handleDeleteQueuedAnalysis(key)}
          >
            {state.deleting ? 'Deleting' : 'Delete queued analysis'}
          </button>
        ) : null}
        {key === 'fen' && state.progress ? (
          <div className="job-progress">
            <div className="job-progress-head">
              <span>{formatNumber(state.progress.extracted)} / {formatNumber(state.progress.target)}</span>
              <strong>{state.progress.percent}%</strong>
            </div>
            <div className="job-progress-track">
              <div className="job-progress-fill" style={{ width: `${state.progress.percent}%` }} />
            </div>
            {state.progress.phase ? (
              <small>{state.progress.phase}{state.progress.detail ? ` | ${state.progress.detail}` : ''}</small>
            ) : null}
            {renderEstimatedTime(
              state.jobId,
              state.status?.progress || state.progress,
              state.status?.status || status
            )}
          </div>
        ) : null}
        {(isAnalysisJobKey(key) || key === 'tablebase') && state.progress ? (
          <div className="job-progress">
            <div className="job-progress-head">
              <span>{formatNumber(state.progress.analyzed)} / {formatNumber(state.progress.target)}</span>
              <strong>{state.progress.percent}%</strong>
            </div>
            <div className="job-progress-track">
              <div className="job-progress-fill" style={{ width: `${state.progress.percent}%` }} />
            </div>
            {state.progress.phase ? (
              <small>{state.progress.phase}{state.progress.detail ? ` | ${state.progress.detail}` : ''}</small>
            ) : null}
            {renderEstimatedTime(
              state.jobId,
              state.status?.progress || state.progress,
              state.status?.status || status
            )}
          </div>
        ) : null}
      </div>
    )
  }

  const handleAnalyze = async (event) => {
    event.preventDefault()
    if (!validation.isValid) {
      setError(validation.error)
      return
    }

    setLoading(true)
    setError('')
    setResult(null)

    try {
      const payload = await analyzeFen({
        fen: validation.fen,
        nodesLimit,
        multipv
      })
      setResult(payload)
      if (payload && payload.is_valid === false) {
        setError('Stockfish rejected this FEN.')
      }
    } catch (err) {
      setError(err.message || 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="positions-main">
          <section className="coverage-bar-chart" aria-label="Position coverage">
            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}

            {coverageBarItems.map((item) => (
              <div className={`coverage-bar-row ${item.capped ? 'is-capped' : ''}`} key={item.key}>
                <div className="coverage-bar-meta">
                  <span>{item.label}</span>
                  <strong>{item.ready ? formatNumber(item.value) : '-'}</strong>
                </div>
                <div className="coverage-bar-track" aria-hidden="true">
                  <div
                    className={`coverage-bar-fill coverage-bar-fill-${item.key}`}
                    style={{ width: `${item.percent}%` }}
                  />
                </div>
              </div>
            ))}
          </section>

          <div className={`pipeline-grid ${playerInspection.data || playerInspection.error ? 'has-player-inspection' : ''}`}>
            <section className="pipeline-card" aria-live="polite">
              <div>
                <p className="eyebrow">AUTOMATIC FEN EXTRACTION</p>
              </div>
              <div className="pipeline-pending">
                <span>Games pending</span>
                <strong>{remainingFenGames === null ? '-' : formatNumber(remainingFenGames)}</strong>
              </div>
              <div className="status-banner">
                {isTrackedJobActive(jobState.tablebase)
                  ? 'Caching exact endgame results automatically.'
                  : isTrackedJobActive(jobState.fen)
                    ? 'Extracting positions automatically.'
                  : remainingFenGames > 0
                    ? 'Pending games are queued for automatic extraction.'
                    : 'New games are extracted automatically, then eligible endgames are solved with Syzygy.'}
              </div>
              {renderJobResult('fen')}
              {renderJobResult('tablebase')}
            </section>

            <form className="pipeline-card" onSubmit={handleGlobalAnalysis}>
              <div>
                <p className="eyebrow">ANALYZE ALL</p>
              </div>
              <div className="pipeline-summary-grid">
                <div className="pipeline-pending">
                  <span>Positions pending</span>
                  <strong>{analysisCounts || coverage ? formatNumber(pendingPositions) : '-'}</strong>
                </div>
                <div className="pipeline-pending">
                  <span>Scored positions</span>
                  <strong>{analysisCounts ? formatNumber(scoredPositions) : '-'}</strong>
                </div>
              </div>
              <div className="pipeline-inline pipeline-inline-compact">
                <label>
                  <span className="field-label">Positions</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={globalJob.totalFens}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setGlobalJob((current) => ({ ...current, totalFens: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_ANALYSIS_BATCH_SIZE}
                    value={globalJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setGlobalJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value)
                    }))}
                  />
                </label>
              </div>
              <button className="btn btn-primary" type="submit" disabled={jobState.global?.loading || isTrackedJobActive(jobState.global)}>
                {jobState.global?.loading ? 'Queueing' : isTrackedJobActive(jobState.global) ? 'Analyzing' : 'Analyze'}
              </button>
              {renderJobResult('global')}
            </form>

            <form className="pipeline-card" onSubmit={handlePlayerAnalysis}>
              <div>
                <p className="eyebrow">ANALYZE PLAYER</p>
              </div>
              <label className="pipeline-player-name-row">
                <div className="input-action-row">
                  <input
                    className="text-input"
                    type="text"
                    value={playerJob.playerName}
                    onChange={(event) => {
                      setPlayerJob((current) => ({ ...current, playerName: event.target.value }))
                      setPlayerInspection((current) => ({ ...current, error: '', data: null }))
                    }}
                    onKeyDown={(event) => {
                      if (event.key !== 'Enter') return
                      event.preventDefault()
                      handleInspectPlayer()
                    }}
                    placeholder="...chess.com nickname..."
                  />
                  <button
                    className="btn btn-secondary btn-inline"
                    type="button"
                    onClick={handleInspectPlayer}
                    disabled={playerInspection.loading}
                  >
                    {playerInspection.loading ? 'inspecting' : 'inspect'}
                  </button>
                </div>
              </label>
              {playerInspection.error ? <div className="status-banner warn">{playerInspection.error}</div> : null}
              {playerInspection.data ? (
                <div className="player-inspection-card">
                  <div>
                    <span>Total Positions</span>
                    <strong>{formatNumber(playerInspection.data.total_positions)}</strong>
                  </div>
                  <div>
                    <span>Analyzed</span>
                    <strong>{formatNumber(playerInspection.data.analyzed_positions)}</strong>
                  </div>
                </div>
              ) : null}
              <div className="pipeline-inline">
                <label>
                  <span className="field-label">Positions</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={playerJob.totalFens}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setPlayerJob((current) => ({ ...current, totalFens: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_ANALYSIS_BATCH_SIZE}
                    value={playerJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setPlayerJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value)
                    }))}
                  />
                </label>
              </div>
              <button className="btn btn-primary" type="submit" disabled={jobState.player?.loading || isTrackedJobActive(jobState.player)}>
                {jobState.player?.loading ? 'Queueing' : isTrackedJobActive(jobState.player) ? 'Analyzing' : 'Analyze'}
              </button>
              {renderJobResult('player')}
            </form>

            <form className="pipeline-card analysis-loop-card" onSubmit={handleAnalysisLoop}>
              <div className="analysis-loop-heading">
                <div>
                  <p className="eyebrow">ANALYSIS LOOPS</p>
                  <small>Run sequential Stockfish passes with a cooling pause between them.</small>
                </div>
                <span className="stat-chip">
                  {formatNumber(Number(loopJob.positionsPerRun || 0) * Number(loopJob.runs || 0))} total positions
                </span>
              </div>

              <div className="analysis-loop-scope" role="radiogroup" aria-label="Analysis loop scope">
                <label className={`analysis-loop-scope-option ${loopJob.scope === 'all' ? 'selected' : ''}`}>
                  <input
                    type="radio"
                    name="analysis-loop-scope"
                    value="all"
                    checked={loopJob.scope === 'all'}
                    onChange={() => setLoopJob((current) => ({ ...current, scope: 'all' }))}
                  />
                  <span>
                    <strong>Analyze all</strong>
                    <small>Most repeated positions across the database</small>
                  </span>
                </label>
                <label className={`analysis-loop-scope-option ${loopJob.scope === 'player' ? 'selected' : ''}`}>
                  <input
                    type="radio"
                    name="analysis-loop-scope"
                    value="player"
                    checked={loopJob.scope === 'player'}
                    onChange={() => setLoopJob((current) => ({ ...current, scope: 'player' }))}
                  />
                  <span>
                    <strong>Analyze player</strong>
                    <small>Most repeated positions belonging to one player</small>
                  </span>
                </label>
              </div>

              <p className="analysis-loop-selection" aria-live="polite">
                Selected scope: <strong>{loopJob.scope === 'player' ? 'one player' : 'all database positions'}</strong>
              </p>

              {loopJob.scope === 'player' ? (
                <label>
                  <span className="field-label">Chess.com nickname</span>
                  <input
                    className="text-input"
                    type="text"
                    value={loopJob.playerName}
                    onChange={(event) => setLoopJob((current) => ({ ...current, playerName: event.target.value }))}
                    placeholder="...Chess.com nickname..."
                  />
                </label>
              ) : null}

              <div className="analysis-loop-fields">
                <label>
                  <span className="field-label">Positions / run</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={loopJob.positionsPerRun}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, positionsPerRun: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Runs</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max="100"
                    value={loopJob.runs}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, runs: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_LOOP_ANALYSIS_BATCH_SIZE}
                    value={loopJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value, MAX_LOOP_ANALYSIS_BATCH_SIZE)
                    }))}
                  />
                </label>
                <label>
                  <span className="field-label">Cool-off seconds</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="0"
                    max="3600"
                    value={loopJob.coolOff}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, coolOff: event.target.value }))}
                  />
                </label>
              </div>

              <p className="analysis-loop-note">
                Batch 500 is recommended because it commits progress more frequently while keeping all four engines busy. The maximum is 1,000.
                {' '}When a schedule is already running, the next one waits in the queue and starts automatically afterward.
              </p>
              <button className="btn btn-primary" type="submit" disabled={loopJobIsQueueing || activeLoopJobCount >= 2}>
                {loopJobIsQueueing
                  ? 'Queueing'
                  : activeLoopJobCount >= 2
                    ? 'Next schedule queued'
                    : activeLoopJobCount === 1
                      ? 'Queue next schedule'
                    : loopJob.scope === 'player' ? 'Start player loops' : 'Start all-position loops'}
              </button>
              {renderJobResult('loop')}
              {renderJobResult('loopNext')}
            </form>

            <section className="pipeline-card player-game-analysis-card">
              <div className="analysis-processes-heading">
                <div>
                  <p className="eyebrow">COMPLETE PLAYER GAMES</p>
                  <small>Select exact incomplete games, preview their unique missing FENs, then confirm or cancel.</small>
                </div>
                {playerGameAnalysis.scope ? (
                  <span className="stat-chip">
                    {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games
                  </span>
                ) : null}
              </div>

              <form className="player-game-scope-form" onSubmit={handleInspectPlayerGameScope}>
                <label>
                  <span className="field-label">Chess.com nickname</span>
                  <input
                    className="text-input"
                    type="text"
                    value={playerGameAnalysis.playerName}
                    placeholder="hikaru"
                    onChange={(event) => setPlayerGameAnalysis((current) => ({
                      ...current,
                      playerName: event.target.value,
                      scope: null,
                      preview: null,
                      error: ''
                    }))}
                  />
                </label>

                <div className="player-game-mode" role="radiogroup" aria-label="Game selection method">
                  {[
                    ['latest', 'Latest games', 'Newest incomplete games first'],
                    ['oldest', 'Oldest games', 'Oldest incomplete games first'],
                    ['range', 'Date range', 'Inspect all games between two dates'],
                    ['fair_range', 'Fair range', 'Balanced samples across every available month']
                  ].map(([value, label, description]) => (
                    <label
                      className={`analysis-loop-scope-option ${playerGameAnalysis.selectionMode === value ? 'selected' : ''}`}
                      key={value}
                    >
                      <input
                        type="radio"
                        name="player-game-selection-mode"
                        value={value}
                        checked={playerGameAnalysis.selectionMode === value}
                        onChange={() => setPlayerGameAnalysis((current) => ({
                          ...current,
                          selectionMode: value,
                          useAll: false,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                      <span><strong>{label}</strong><small>{description}</small></span>
                    </label>
                  ))}
                </div>

                {playerGameAnalysis.selectionMode === 'range' ? (
                  <div className="player-game-date-fields">
                    <label>
                      <span className="field-label">Start date</span>
                      <input
                        className="text-input"
                        type="date"
                        value={playerGameAnalysis.dateFrom}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          dateFrom: event.target.value,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                    </label>
                    <label>
                      <span className="field-label">End date</span>
                      <input
                        className="text-input"
                        type="date"
                        value={playerGameAnalysis.dateTo}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          dateTo: event.target.value,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                    </label>
                  </div>
                ) : null}

                <button className="btn btn-secondary" type="submit" disabled={playerGameAnalysis.loadingScope}>
                  {playerGameAnalysis.loadingScope ? 'Inspecting games' : 'Inspect games'}
                </button>
              </form>

              {playerGameAnalysis.error ? <div className="status-banner warn">{playerGameAnalysis.error}</div> : null}

              {playerGameAnalysis.scope ? (
                <div className="player-game-scope-results">
                  <div><small>Games with FENs</small><strong>{formatNumber(playerGameAnalysis.scope.games_with_fens)}</strong></div>
                  <div><small>Already complete</small><strong>{formatNumber(playerGameAnalysis.scope.complete_games)}</strong></div>
                  <div><small>Incomplete available</small><strong>{formatNumber(playerGameAnalysis.scope.incomplete_games)}</strong></div>
                  <div>
                    <small>Available dates</small>
                    <strong>
                      {playerGameAnalysis.scope.earliest_game
                        ? `${String(playerGameAnalysis.scope.earliest_game).slice(0, 10)} → ${String(playerGameAnalysis.scope.latest_game).slice(0, 10)}`
                        : '--'}
                    </strong>
                  </div>
                </div>
              ) : null}

              {playerGameAnalysis.scope && Number(playerGameAnalysis.scope.incomplete_games || 0) > 0 ? (
                <div className="player-game-selection-controls">
                  {playerGameAnalysis.selectionMode === 'range' ? (
                    <div className="player-game-range-choice">
                      <label className="player-game-check-option">
                        <input
                          type="checkbox"
                          checked={playerGameAnalysis.useAll}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            useAll: event.target.checked,
                            preview: null
                          }))}
                        />
                        <span>Use all {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games in this range</span>
                      </label>
                    </div>
                  ) : null}

                  <div className="player-game-selection-fields">
                    {(playerGameAnalysis.selectionMode !== 'range' || !playerGameAnalysis.useAll) ? (
                      <label>
                        <span className="field-label">Number of games</span>
                        <input
                          className="text-input number-input-clean"
                          type="number"
                          min="1"
                          max={playerGameAnalysis.scope.incomplete_games}
                          value={playerGameAnalysis.gameLimit}
                          onWheel={(event) => event.currentTarget.blur()}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            gameLimit: event.target.value,
                            preview: null
                          }))}
                        />
                      </label>
                    ) : null}
                    {playerGameAnalysis.selectionMode === 'range' ? (
                      <label>
                        <span className="field-label">Take from range</span>
                        <select
                          className="text-input"
                          value={playerGameAnalysis.rangeOrder}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            rangeOrder: event.target.value,
                            preview: null
                          }))}
                        >
                          <option value="latest">Latest first</option>
                          <option value="oldest">Oldest first</option>
                        </select>
                      </label>
                    ) : null}
                  </div>

                  <button
                    className="btn btn-primary"
                    type="button"
                    disabled={playerGameAnalysis.loadingPreview}
                    onClick={handlePreviewPlayerGameAnalysis}
                  >
                    {playerGameAnalysis.loadingPreview ? 'Calculating FENs' : 'Calculate FEN workload'}
                  </button>
                </div>
              ) : null}

              {playerGamePreview ? (
                <div className="player-game-confirmation">
                  <div className="player-game-confirmation-heading">
                    <div>
                      <span>CONFIRM FROZEN ANALYSIS PLAN</span>
                      <strong>{playerGamePreview.player_name}</strong>
                    </div>
                    <span className="analysis-process-phase queued">Awaiting confirmation</span>
                  </div>

                  <div className="player-game-preview-grid">
                    <div><small>Games selected</small><strong>{formatNumber(playerGamePreview.selected_games)}</strong></div>
                    <div><small>Position occurrences</small><strong>{formatNumber(playerGamePreview.position_occurrences)}</strong></div>
                    <div><small>Unique FENs</small><strong>{formatNumber(playerGamePreview.unique_fens)}</strong></div>
                    <div><small>Already analyzed</small><strong>{formatNumber(playerGamePreview.analyzed_unique_fens)}</strong></div>
                    <div><small>Syzygy exact FENs</small><strong>{formatNumber(playerGamePreview.tablebase_fens)}</strong></div>
                    <div className="highlight">
                      <small>Estimated FENs for Stockfish</small>
                      <strong>{formatNumber(Math.max(0, Number(playerGamePreview.fens_to_analyze || 0) - Number(playerGamePreview.tablebase_fens || 0)))}</strong>
                    </div>
                    <div><small>Expected complete games</small><strong>{formatNumber(playerGamePreview.selected_games)}</strong></div>
                    {playerGamePreview.selection_mode === 'fair_range' ? (
                      <div>
                        <small>Months sampled</small>
                        <strong>
                          {formatNumber(playerGamePreview.sampled_periods)} / {formatNumber(playerGamePreview.available_periods)}
                        </strong>
                      </div>
                    ) : null}
                    <div>
                      <small>Selection order</small>
                      <strong>{getPlayerGameSelectionLabel(playerGamePreview.selection_order)}</strong>
                    </div>
                    <div><small>Estimated total time</small><strong>~{formatDuration(playerGamePreviewTotalSeconds)}</strong></div>
                  </div>

                  <div className="player-game-runtime-fields">
                    <label>
                      <span className="field-label">Batch</span>
                      <input
                        className="text-input number-input-clean"
                        type="number"
                        min="1"
                        max={MAX_LOOP_ANALYSIS_BATCH_SIZE}
                        value={playerGameAnalysis.batchSize}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          batchSize: clampAnalysisBatchInput(event.target.value, MAX_LOOP_ANALYSIS_BATCH_SIZE)
                        }))}
                      />
                    </label>
                    <label>
                      <span className="field-label">Cool-off every 5,000 FENs</span>
                      <input
                        className="text-input number-input-clean"
                        type="number"
                        min="0"
                        max="3600"
                        value={playerGameAnalysis.coolOff}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({ ...current, coolOff: event.target.value }))}
                      />
                    </label>
                  </div>

                  <p>
                    The exact {formatNumber(playerGamePreview.selected_games)} game IDs are frozen. Shared FENs are analyzed once,
                    and the chosen game order is prioritized.
                  </p>
                  <div className="player-game-confirm-actions">
                    <button
                      className="btn btn-secondary"
                      type="button"
                      onClick={() => setPlayerGameAnalysis((current) => ({ ...current, preview: null }))}
                    >
                      Cancel
                    </button>
                    <button
                      className="btn btn-primary"
                      type="button"
                      disabled={jobState.gameCompletion?.loading}
                      onClick={handleConfirmPlayerGameAnalysis}
                    >
                      {jobState.gameCompletion?.loading ? 'Queueing' : 'Yes, queue analysis'}
                    </button>
                  </div>
                </div>
              ) : null}

              {renderJobResult('gameCompletion')}
            </section>

            <section className="pipeline-card analysis-processes-card" aria-live="polite">
              <div className="analysis-processes-heading">
                <div>
                  <p className="eyebrow">ACTIVE ANALYSIS PROCESSES</p>
                  <small>Server-side Stockfish jobs currently running or waiting in the queue.</small>
                </div>
                <span className="stat-chip">{analysisProcessViews.length} processes</span>
              </div>

              {analysisProcessesError ? <div className="status-banner warn">{analysisProcessesError}</div> : null}
              {!analysisProcessesError && analysisProcessViews.length === 0 ? (
                <div className="analysis-process-empty">No running or queued Stockfish analysis.</div>
              ) : null}

              <div className="analysis-process-list">
                {analysisProcessViews.map((process) => (
                  <article className="analysis-process-card" key={process.job_id}>
                    <div className="analysis-process-title">
                      <div>
                        <strong>{process.scopeLabel}</strong>
                        <small>{String(process.job_id).slice(0, 12)}</small>
                      </div>
                      <span className={`analysis-process-phase ${process.phase}`}>{process.phase}</span>
                    </div>

                    <div className="analysis-process-parameters">
                      <span><small>Positions/run</small><strong>{formatNumber(process.positionsPerRun)}</strong></span>
                      <span><small>Runs</small><strong>{formatNumber(process.runs)}</strong></span>
                      <span><small>Batch</small><strong>{formatNumber(process.batchSize)}</strong></span>
                      <span><small>Cool-off</small><strong>{formatNumber(process.coolOff)}s</strong></span>
                    </div>

                    <div className="job-progress">
                      <div className="job-progress-head">
                        <span>{formatNumber(process.processed)} / {formatNumber(process.total)}</span>
                        <strong>{process.percent}%</strong>
                      </div>
                      <div className="job-progress-track">
                        <div className="job-progress-fill" style={{ width: `${process.percent}%` }} />
                      </div>
                      {process.progress?.detail ? <small>{process.progress.detail}</small> : null}
                      {renderEstimatedTime(
                        process.job_id,
                        process.progress,
                        process.status
                      )}
                    </div>

                    {process.isQueued ? (
                      <button
                        className="btn job-delete-queued"
                        type="button"
                        disabled={deletingAnalysisJobIds.has(process.job_id)}
                        onClick={() => handleDeleteAnalysisProcess(process)}
                      >
                        {deletingAnalysisJobIds.has(process.job_id) ? 'Deleting' : 'Delete queued analysis'}
                      </button>
                    ) : null}
                  </article>
                ))}
              </div>
            </section>
          </div>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Positions
