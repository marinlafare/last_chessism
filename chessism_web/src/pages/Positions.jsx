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
  const status = state?.status?.status
  return status !== 'complete' && status !== 'not_found'
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

const playCompletionSound = (audioContextRef) => {
  if (typeof window === 'undefined') return
  const context = audioContextRef.current || getAudioContext()
  if (!context) return
  audioContextRef.current = context

  const now = context.currentTime
  const notes = [
    { frequency: 246.94, start: 0, duration: 0.16 },
    { frequency: 293.66, start: 0.17, duration: 0.22 }
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
  const [fenJob, setFenJob] = useState({ totalGames: 100000, batchSize: 1000, workers: 3 })
  const [globalJob, setGlobalJob] = useState({ totalFens: 10000, batchSize: 100 })
  const [playerJob, setPlayerJob] = useState({ playerName: '', totalFens: 1000, batchSize: 50 })
  const [jobState, setJobState] = useState(loadStoredJobState)
  const boardWrapRef = useRef(null)
  const audioContextRef = useRef(null)
  const completedSoundJobsRef = useRef(new Set())

  const validation = useMemo(() => validateFen(fenInput), [fenInput])
  const analysisLines = useMemo(() => getAnalysisLines(result), [result])
  const bestLine = analysisLines[0]
  const turnLabel = validation.game?.turn() === 'b' ? 'Black' : 'White'
  const unscoredPositions = Number(analysisCounts?.unscored_fens ?? Math.max(0, Number(coverage?.n_positions || 0) - Number(coverage?.scored_fens || 0)))
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

  useEffect(() => {
    const activeJobs = Object.entries(jobState).filter(([, state]) => {
      return isTrackedJobActive(state)
    })

    if (!activeJobs.length) return undefined

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

            if (status.status === 'complete') {
              await loadCoverage()
              await loadAnalysisCounts()
              if (
                key === 'fen' &&
                status.result?.success !== false &&
                !completedSoundJobsRef.current.has(state.jobId)
              ) {
                completedSoundJobsRef.current.add(state.jobId)
                playCompletionSound(audioContextRef)
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
            updateJobState(key, patch)
          } catch (err) {
            updateJobState(key, { error: err.message || 'Job status unavailable' })
          }
        })
      )
    }

    const timer = window.setInterval(pollJobs, 3000)
    return () => window.clearInterval(timer)
  }, [jobState])

  const enqueueJob = async ({ key, path, body, meta = {} }) => {
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
    const context = audioContextRef.current || getAudioContext()
    if (context) {
      audioContextRef.current = context
      context.resume?.()
    }

    const currentRemaining = Number(remainingFenGames ?? fenJob.totalGames)
    const requestedGames = Number(fenJob.totalGames)
    enqueueJob({
      key: 'fen',
      path: '/fens/generate',
      body: {
        total_games_to_process: Number(fenJob.totalGames),
        batch_size: Number(fenJob.batchSize),
        num_workers: Number(fenJob.workers)
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

  const renderJobResult = (key) => {
    const state = jobState[key] || {}
    if (state.error) return <div className="status-banner warn">{state.error}</div>
    if (!state.payload) return null
    const status = state.status?.status || 'queued'
    const info = state.status?.info
    const resultInfo = state.status?.result
    const functionName = info?.function || resultInfo?.queue_name || state.status?.queue_name || 'job'

    return (
      <div className="job-result">
        <span>{status.replace('_', ' ')}</span>
        <strong>{state.payload.job_id || '--'}</strong>
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
          <section className="position-ops">
            <div className="section-head">
              <div>
                <p className="eyebrow">Pipeline</p>
                <h1>Position Coverage</h1>
              </div>
              <button className="btn btn-secondary btn-inline" type="button" onClick={loadCoverage}>
                Refresh
              </button>
            </div>

            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}

            <div className="position-coverage-grid">
              <article className="metric-card">
                <span>Games</span>
                <strong>{coverage ? formatNumber(coverage.n_games_in_db) : '-'}</strong>
              </article>
              <article className="metric-card">
                <span>Positions</span>
                <strong>{coverage ? formatNumber(coverage.n_positions) : '-'}</strong>
              </article>
              <article className="metric-card">
                <span>Analyzed FENs</span>
                <strong>{analysisCounts ? formatNumber(analysisCounts.analyzed_fens) : '-'}</strong>
              </article>
              <article className="metric-card">
                <span>Unscored FENs</span>
                <strong>{coverage ? formatNumber(unscoredPositions) : '-'}</strong>
              </article>
            </div>

            <div className="pipeline-grid">
              <form className="pipeline-card" onSubmit={handleFenGeneration}>
                <div>
                  <p className="eyebrow">Create</p>
                  <h2>Generate FENs</h2>
                </div>
                <div className="pipeline-pending">
                  <span>Games pending FEN extraction</span>
                  <strong>{remainingFenGames === null ? '-' : formatNumber(remainingFenGames)}</strong>
                </div>
                <label>
                  <span className="field-label">Games</span>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    value={fenJob.totalGames}
                    onChange={(event) => setFenJob((current) => ({ ...current, totalGames: event.target.value }))}
                  />
                </label>
                <div className="pipeline-inline">
                  <label>
                    <span className="field-label">Batch</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      value={fenJob.batchSize}
                      onChange={(event) => setFenJob((current) => ({ ...current, batchSize: event.target.value }))}
                    />
                  </label>
                  <label>
                    <span className="field-label">Workers</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      max="16"
                      value={fenJob.workers}
                      onChange={(event) => setFenJob((current) => ({ ...current, workers: event.target.value }))}
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
                  <p className="eyebrow">Analyze</p>
                  <h2>Most Repeated</h2>
                </div>
                <label>
                  <span className="field-label">Positions</span>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    value={globalJob.totalFens}
                    onChange={(event) => setGlobalJob((current) => ({ ...current, totalFens: event.target.value }))}
                  />
                </label>
                <div className="pipeline-inline">
                  <label>
                    <span className="field-label">Batch</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      value={globalJob.batchSize}
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
                  <p className="eyebrow">Analyze</p>
                  <h2>Character Repeated</h2>
                </div>
                <label>
                  <span className="field-label">Character</span>
                  <input
                    className="text-input"
                    type="text"
                    value={playerJob.playerName}
                    onChange={(event) => setPlayerJob((current) => ({ ...current, playerName: event.target.value }))}
                    placeholder="hikaru"
                  />
                </label>
                <div className="pipeline-inline">
                  <label>
                    <span className="field-label">Positions</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      value={playerJob.totalFens}
                      onChange={(event) => setPlayerJob((current) => ({ ...current, totalFens: event.target.value }))}
                    />
                  </label>
                  <label>
                    <span className="field-label">Batch</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      value={playerJob.batchSize}
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
          </section>

          <section className="positions-workbench">
            <form className="position-input-panel" onSubmit={handleAnalyze}>
              <div className="section-head">
                <div>
                  <p className="eyebrow">Stockfish</p>
                  <h1>Position Analysis</h1>
                </div>
                <button className="btn btn-secondary btn-inline" type="button" onClick={() => setFenInput(START_FEN)}>
                  Start FEN
                </button>
              </div>

              <label className="field-label" htmlFor="fen-input">FEN</label>
              <textarea
                id="fen-input"
                className="text-input fen-textarea"
                value={fenInput}
                onChange={(event) => setFenInput(event.target.value)}
                spellCheck="false"
              />

              <div className="position-control-grid">
                <label>
                  <span className="field-label">Nodes</span>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    max="100000000"
                    step="100000"
                    value={nodesLimit}
                    onChange={(event) => setNodesLimit(event.target.value)}
                  />
                </label>
                <label>
                  <span className="field-label">MultiPV</span>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    max="10"
                    value={multipv}
                    onChange={(event) => setMultipv(event.target.value)}
                  />
                </label>
                <button className="btn btn-primary analyze-btn" type="submit" disabled={loading || !validation.isValid}>
                  {loading ? 'Analyzing' : 'Analyze'}
                </button>
              </div>

              {!validation.isValid ? <p className="result-line warn-text">{validation.error}</p> : null}
              {error ? <div className="status-banner warn">{error}</div> : null}
            </form>

            <section className="position-board-panel" aria-label="Chess position">
              <div className="position-board-shell" ref={boardWrapRef}>
                <Chessboard
                  position={validation.isValid ? validation.fen : START_FEN}
                  boardWidth={boardWidth}
                  arePiecesDraggable={false}
                />
              </div>
            </section>

            <section className="engine-panel" aria-label="Engine analysis">
              <div className="section-head">
                <div>
                  <p className="eyebrow">Evaluation</p>
                  <h2>{getScoreLabel(bestLine?.score)}</h2>
                </div>
                <span className="stat-chip">{turnLabel} to move</span>
              </div>

              <div className="engine-summary-grid">
                <div className="profile-item">
                  <span>Best move</span>
                  <strong>{moveToSan(validation.fen, bestLine?.pv?.[0])}</strong>
                </div>
                <div className="profile-item">
                  <span>Depth</span>
                  <strong>{formatNumber(bestLine?.depth)}</strong>
                </div>
                <div className="profile-item">
                  <span>Nodes</span>
                  <strong>{formatNumber(bestLine?.nodes)}</strong>
                </div>
              </div>

              <div className="engine-lines">
                {analysisLines.length ? (
                  analysisLines.map((line, index) => {
                    const pv = Array.isArray(line?.pv) ? line.pv : []
                    return (
                      <div className="engine-line" key={`${line?.score}-${index}`}>
                        <span className="engine-line-rank">#{index + 1}</span>
                        <strong>{getScoreLabel(line?.score)}</strong>
                        <p>{pv.map((move) => moveToSan(validation.fen, move)).join(' ') || '--'}</p>
                      </div>
                    )
                  })
                ) : (
                  <p className="result-line">No analysis loaded.</p>
                )}
              </div>
            </section>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Positions
