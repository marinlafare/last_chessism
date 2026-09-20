import { useEffect, useMemo, useRef, useState } from 'react'
import {
  COMPLETED_JOB_FADE_MS, COMPLETED_JOB_VISIBLE_MS, DEFAULT_ANALYSIS_NODES,
  ETA_RATE_SMOOTHING, MAX_ANALYSIS_BATCH_SIZE, START_FEN, analyzeFen, deleteJson,
  fetchJson, getAnalysisLines, getAnalysisProcessView, getPositionJobKey,
  getProgressSnapshot, getTrackedJobPhase, isTrackedJobActive, isTrackedJobComplete,
  loadStoredJobState, pageHasAttention, playCompletionSound, postJson, storeJobState,
  unlockCompletionAudio, validateFen,
} from './positionPageSupport'

export function usePositionsPage() {
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
      const payload = await analyzeFen({ fen: validation.fen, nodesLimit, multipv })
      setResult(payload)
      if (payload && payload.is_valid === false) setError('Stockfish rejected this FEN.')
    } catch (err) {
      setError(err.message || 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }

  return {
    activeLoopJobCount, analysisLines, analysisProcessViews, analysisProcessesError,
    analysisCounts, bestLine, boardWidth, boardWrapRef, coverage, coverageBarItems, coverageError,
    deletingAnalysisJobIds, error, etaClockMs, etaEstimatesRef, fenInput, globalJob,
    handleAnalysisLoop, handleAnalyze, handleConfirmPlayerGameAnalysis,
    handleDeleteAnalysisProcess, handleDeleteQueuedAnalysis, handleGlobalAnalysis,
    handleInspectPlayer, handleInspectPlayerGameScope, handlePlayerAnalysis,
    handlePreviewPlayerGameAnalysis, jobState, loading, loopJob, loopJobIsQueueing,
    multipv, nodesLimit, pendingPositions, playerGameAnalysis, playerGamePreview,
    playerGamePreviewTotalSeconds, playerInspection, playerJob, remainingFenGames,
    result, scoredPositions, setFenInput, setGlobalJob, setJobCardRef, setLoopJob,
    setMultipv, setNodesLimit, setPlayerGameAnalysis, setPlayerJob, turnLabel,
    validation,
  }
}
