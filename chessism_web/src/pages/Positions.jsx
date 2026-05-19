import { useEffect, useMemo, useRef, useState } from 'react'
import { Chess } from 'chess.js'
import { Chessboard } from 'react-chessboard'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const START_FEN = new Chess().fen()
const DEFAULT_ANALYSIS_NODES = 1_000_000
const POSITION_JOBS_STORAGE_KEY = 'chessism:positions:jobs'
const COMPLETED_JOB_VISIBLE_MS = 5000
const COMPLETED_JOB_FADE_MS = 700

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US')
}

const normalizeFen = (value) => String(value || '').trim().replace(/\s+/g, ' ')

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
  const [fenJob, setFenJob] = useState({ totalGames: 1000, batchSize: 1000 })
  const [globalJob, setGlobalJob] = useState({ totalFens: 100, batchSize: 100 })
  const [playerJob, setPlayerJob] = useState({ playerName: '', totalFens: 100, batchSize: 100 })
  const [playerInspection, setPlayerInspection] = useState({ loading: false, error: '', data: null })
  const [jobState, setJobState] = useState(loadStoredJobState)
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
  const canGenerateFens = remainingFenGames !== null && remainingFenGames > 0 && !jobState.fen?.loading && !isTrackedJobActive(jobState.fen)

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

  const hydrateActiveJobs = async () => {
    const payload = await fetchJson('/jobs/active')
    const jobs = Array.isArray(payload.jobs) ? payload.jobs : []
    jobs.forEach((job) => {
      const progress = job.progress || {}
      const kind = progress.kind || progress.job_kind
      const key = kind === 'character_repeated' || kind === 'player' ? 'player' : 'global'
      setJobState((current) => {
        if (isTrackedJobActive(current[key])) return current
        const target = Math.max(1, Number(progress.total || 1))
        const processed = Number(progress.processed || 0)
        const next = {
          ...current,
          [key]: {
            ...(current[key] || {}),
            payload: { job_id: job.job_id },
            jobId: job.job_id,
            loading: false,
            error: '',
            status: job,
            targetFens: target,
            progress: {
              analyzed: Math.min(target, processed),
              failed: Number(progress.failed || 0),
              target,
              percent: Math.min(100, Math.round((Math.min(target, processed) / target) * 100)),
              phase: progress.phase,
              detail: progress.detail
            }
          }
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
              const startRemaining = Number(state.startRemaining ?? remaining)
              const targetGames = Math.max(1, Number(state.targetGames || 1))
              const extracted = Math.max(0, startRemaining - remaining)
              patch.progress = {
                remaining,
                extracted: Math.min(targetGames, extracted),
                target: targetGames,
                percent: Math.min(100, Math.round((Math.min(targetGames, extracted) / targetGames) * 100))
              }
            }

            if ((key === 'global' || key === 'player') && status.progress) {
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
    } catch (err) {
      updateJobState(key, { loading: false, error: err.message || 'Job failed' })
    }
  }

  const handleFenGeneration = (event) => {
    event.preventDefault()
    unlockCompletionAudio(audioContextRef)

    const currentRemaining = Number(remainingFenGames ?? fenJob.totalGames)
    const requestedGames = Number(fenJob.totalGames)
    enqueueJob({
      key: 'fen',
      path: '/fens/generate',
      body: {
        total_games_to_process: Number(fenJob.totalGames),
        batch_size: Number(fenJob.batchSize)
      },
      meta: {
        startRemaining: currentRemaining,
        targetGames: Math.max(1, Math.min(requestedGames, currentRemaining)),
        progress: {
          remaining: currentRemaining,
          extracted: 0,
          target: Math.max(1, Math.min(requestedGames, currentRemaining)),
          percent: 0
        }
      }
    })
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

  const renderJobResult = (key) => {
    const state = jobState[key] || {}
    if (state.error) return <div className="status-banner warn">{state.error}</div>
    if (!state.payload) return null
    const status = getTrackedJobPhase(state)
    const info = state.status?.info
    const resultInfo = state.status?.result
    const functionName = info?.function || resultInfo?.queue_name || state.status?.queue_name || 'job'

    return (
      <div
        className={`job-result ${state.fading ? 'job-result-fading' : ''}`}
        ref={setJobCardRef(key)}
      >
        <span>{status.replace('_', ' ')}</span>
        <small>{functionName}</small>
        {key === 'fen' && state.progress ? (
          <div className="job-progress">
            <div className="job-progress-head">
              <span>{formatNumber(state.progress.extracted)} / {formatNumber(state.progress.target)}</span>
              <strong>{state.progress.percent}%</strong>
            </div>
            <div className="job-progress-track">
              <div className="job-progress-fill" style={{ width: `${state.progress.percent}%` }} />
            </div>
          </div>
        ) : null}
        {(key === 'global' || key === 'player') && state.progress ? (
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
            <form className="pipeline-card" onSubmit={handleFenGeneration}>
              <div>
                <p className="eyebrow">GENERATE FENS</p>
              </div>
              <div className="pipeline-pending">
                <span>Games pending</span>
                <strong>{remainingFenGames === null ? '-' : formatNumber(remainingFenGames)}</strong>
              </div>
              <div className="pipeline-inline pipeline-inline-compact">
                <label>
                  <span className="field-label">Games</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={fenJob.totalGames}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setFenJob((current) => ({ ...current, totalGames: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={fenJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setFenJob((current) => ({ ...current, batchSize: event.target.value }))}
                  />
                </label>
              </div>
              <button
                className="btn btn-primary"
                type="submit"
                disabled={!canGenerateFens}
              >
                {jobState.fen?.loading ? 'Queueing' : remainingFenGames === 0 ? 'No games pending' : 'Generate'}
              </button>
              {renderJobResult('fen')}
            </form>

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
                    value={globalJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setGlobalJob((current) => ({ ...current, batchSize: event.target.value }))}
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
                    value={playerJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setPlayerJob((current) => ({ ...current, batchSize: event.target.value }))}
                  />
                </label>
              </div>
              <button className="btn btn-primary" type="submit" disabled={jobState.player?.loading || isTrackedJobActive(jobState.player)}>
                {jobState.player?.loading ? 'Queueing' : isTrackedJobActive(jobState.player) ? 'Analyzing' : 'Analyze'}
              </button>
              {renderJobResult('player')}
            </form>
          </div>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Positions
