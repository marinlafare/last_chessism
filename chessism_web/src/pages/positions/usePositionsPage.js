import { useJobCompletionPresentation } from './useJobCompletionPresentation.js'
import { usePlayerPositionInspection } from './usePlayerPositionInspection.js'
import { usePlayerGameSelection } from './usePlayerGameSelection.js'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  DEFAULT_ANALYSIS_NODES,
  ETA_RATE_SMOOTHING, MAX_ANALYSIS_BATCH_SIZE,
  formatNumber, getAnalysisJobProgress, getAnalysisProcessView, getPositionJobKey,
  getProgressSnapshot, getTrackedJobPhase, getTrackedJobFailureDetail, isAnalysisJobKey, isTrackedJobActive,
  loadStoredJobState, parseTimestampSeconds,
  storeJobState,
} from './positionPageSupport'
import { unlockCompletionAudio } from '../../utils/completionAudio'
import { usePositionJobPolling, usePositionStatusFeed } from './usePositionStatusPolling.js'
import { obsoletePositionStatus } from './positionStatusFreshness.js'
import {
  deleteQueuedAnalysisJob,
  fetchActiveJobs,
  fetchAnalysisCounts,
  fetchAnalysisProcesses,
  fetchCoverage,
  fetchGameAnalysisOverview,
  fetchLatestIngestionTiming,
  queueAnalysisLoop,
  queueGlobalAnalysis,
  queuePlayerAnalysis,
  queuePlayerGameAnalysis,
} from './positionsApi'

const AUTOMATIC_PIPELINE_JOB_KEYS = new Set(['gameParsing', 'fen', 'tablebase'])

export function usePositionsPage() {
  const [coverage, setCoverage] = useState(null)
  const [coverageError, setCoverageError] = useState('')
  const [gameAnalysisOverview, setGameAnalysisOverview] = useState(null)
  const [analysisCounts, setAnalysisCounts] = useState(null)
  const [ingestionTiming, setIngestionTiming] = useState(null)
  const [globalJob, setGlobalJob] = useState({ totalFens: 100, batchSize: MAX_ANALYSIS_BATCH_SIZE })
  const { playerJob, setPlayerJob, playerInspection, setPlayerInspection,
    handleInspectPlayer } = usePlayerPositionInspection()
  const [loopJob, setLoopJob] = useState({
    scope: 'all',
    playerName: '',
    positionsPerRun: 20_000,
    runs: 4,
    batchSize: MAX_ANALYSIS_BATCH_SIZE,
    coolOff: 300
  })
  const { playerGameAnalysis, setPlayerGameAnalysis, handleInspectPlayerGameScope,
    handlePreviewPlayerGameAnalysis, takePlan } = usePlayerGameSelection()
  const [jobState, setJobState] = useState(loadStoredJobState)
  const latestJobs = useRef(jobState)
  latestJobs.current = jobState
  const discoverJobs = useCallback(async options => {
    const observed = latestJobs.current
    const payload = await fetchActiveJobs(options)
    return { payload, observed }
  }, [])
  const [analysisProcesses, setAnalysisProcesses] = useState([])
  const [analysisProcessesError, setAnalysisProcessesError] = useState('')
  const [deletingAnalysisJobIds, setDeletingAnalysisJobIds] = useState(() => new Set())
  const [etaClockMs, setEtaClockMs] = useState(Date.now())
  const [, setEtaRevision] = useState(0)
  const etaEstimatesRef = useRef(new Map())

  const scoredPositions = Number(analysisCounts?.analyzed_fens || 0)
  const pendingPositions = Number(
    analysisCounts?.unscored_fens ?? Math.max(0, Number(coverage?.n_positions || 0) - scoredPositions)
  )
  const coverageBarItems = useMemo(() => {
    const gamesValue = Number(coverage?.n_games_in_db || 0)
    const analyzedGamesValue = Number(gameAnalysisOverview?.fully_analyzed_games || 0)
    const positionsValue = Number(coverage?.n_positions || 0)
    const analyzedValue = scoredPositions
    const items = [
      { key: 'games', label: 'Games', value: gamesValue, ready: Boolean(coverage) },
      {
        key: 'games-analyzed',
        label: 'Games analyzed',
        value: analyzedGamesValue,
        ready: Boolean(gameAnalysisOverview),
      },
      {
        key: 'positions',
        label: 'Positions',
        value: positionsValue,
        ready: Boolean(coverage),
        displayInMillions: true,
      },
      {
        key: 'analyzed',
        label: 'Scored Positions',
        value: analyzedValue,
        ready: Boolean(analysisCounts),
        displayInMillions: true,
      }
    ]

    return items.map((item) => {
      let percent = 0

      if (item.key === 'games') {
        percent = item.ready && gamesValue > 0 ? 100 : 0
      } else if (item.key === 'games-analyzed') {
        percent = item.ready && gamesValue > 0
          ? Math.min(100, (item.value / gamesValue) * 100)
          : 0
      } else if (item.key === 'positions') {
        percent = item.ready && positionsValue > 0 ? 100 : 0
      } else if (item.key === 'analyzed') {
        percent = item.ready && positionsValue > 0
          ? Math.min(100, (item.value / positionsValue) * 100)
          : 0
      }

      return {
        ...item,
        percent,
      }
    })
  }, [analysisCounts, coverage, gameAnalysisOverview, scoredPositions])
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
      if (observation.status === 'queued' || observation.status === 'deferred') {
        changed = etaEstimatesRef.current.delete(jobId) || changed
        return
      }
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

  const loadCoverage = async () => {
    try {
      const payload = await fetchCoverage()
      setCoverage(payload)
      setCoverageError('')
      return payload
    } catch (err) {
      setCoverageError(err.message || 'Coverage unavailable')
      return null
    }
  }

  const loadAnalysisCounts = async () => {
    const payload = await fetchAnalysisCounts()
    setAnalysisCounts(payload)
    return payload
  }

  const loadGameAnalysisOverview = async () => {
    const payload = await fetchGameAnalysisOverview()
    setGameAnalysisOverview(payload)
    return payload
  }

  usePositionStatusFeed(fetchLatestIngestionTiming, payload => {
    setIngestionTiming(payload?.run || null)
  })

  const loadAnalysisProcesses = usePositionStatusFeed(fetchAnalysisProcesses,
    payload => {
      setAnalysisProcesses(Array.isArray(payload.jobs) ? payload.jobs : [])
      setAnalysisProcessesError('')
    }, err => {
      setAnalysisProcessesError(err instanceof Error ? err.message : 'Unable to load analysis processes.')
    })

  usePositionStatusFeed(discoverJobs, ({ payload, observed }) => {
    const jobs = Array.isArray(payload.jobs) ? payload.jobs : []
    jobs.forEach((job) => {
      const progress = job.progress || {}
      const key = getPositionJobKey(job)
      if (!key) return

      setJobState((current) => {
        // A discovery read begun before replacement/dismissal cannot restore
        // that previous selection, even after the new job has finished.
        if (current[key]?.jobId !== observed[key]?.jobId) return current
        if (current[key]?.jobId && !isTrackedJobActive(current[key]) && isTrackedJobActive(observed[key])) return current
        if (current[key]?.loading || obsoletePositionStatus(current[key], job)) return current
        const next = { ...current }
        Object.entries(next).forEach(([trackedKey, trackedState]) => {
          if (trackedKey !== key && trackedState?.jobId === job.job_id) {
            delete next[trackedKey]
          }
        })

        if (isTrackedJobActive(next[key]) && next[key]?.jobId !== job.job_id) {
          const incomingUpdatedAt = Number(progress.updated_at || 0)
          const trackedProgress = next[key]?.status?.progress || next[key]?.progress || {}
          const trackedUpdatedAt = Number(trackedProgress.updated_at || 0)
          const newerAutomaticJob = (
            AUTOMATIC_PIPELINE_JOB_KEYS.has(key) &&
            incomingUpdatedAt > trackedUpdatedAt
          )

          if (!newerAutomaticJob) {
            storeJobState(next)
            return next
          }
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
          error: getTrackedJobPhase({ status: job }) === 'failed' ? getTrackedJobFailureDetail(job) : '',
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
  })

  useEffect(() => {
    loadCoverage()
    loadGameAnalysisOverview().catch(() => {})
    loadAnalysisCounts().catch(() => {})
  }, [])

  const updateJobState = (key, patch, expectedJobId) => {
    setJobState((current) => {
      if (expectedJobId && current[key]?.jobId !== expectedJobId) return current
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

  const clearJobState = (key, expectedJobId) => {
    setJobState((current) => {
      if (!current[key]) return current
      if (expectedJobId && current[key].jobId !== expectedJobId) return current
      const next = { ...current }
      delete next[key]
      storeJobState(next)
      return next
    })
  }

  const { audioContextRef, setJobCardRef, cancelJobDismissTimers, handleCompletedJob } =
    useJobCompletionPresentation(jobState, ingestionTiming, updateJobState, clearJobState)

  usePositionJobPolling(jobState, (key, state, status) => {
    const patch = { status, error: '', completedAt: null, fading: false }
    if (key === 'fen' && status.progress) {
      const { total, processed, phase, detail } = status.progress
      const target = Math.max(1, Number(total || 1))
      const extracted = Math.min(target, Math.max(0, Number(processed || 0)))
      patch.progress = { extracted, target, percent: Math.min(100, Math.round(extracted / target * 100)), phase, detail }
    }
    if (isAnalysisJobKey(key)) patch.progress = getAnalysisJobProgress({ ...state, status })
    const phase = getTrackedJobPhase({ status })
    if (phase === 'not_found') {
      clearJobState(key, state.jobId)
      return
    }
    if (phase === 'complete') {
      // Totals refresh independently; an unavailable summary must not hide a
      // completed job or delay polling other jobs.
      Promise.allSettled([loadCoverage(), loadAnalysisCounts(), loadGameAnalysisOverview()])
      if (key === 'gameParsing') {
        clearJobState(key, state.jobId)
        return
      }
      handleCompletedJob(key, state, status, patch)
    } else if (phase === 'failed' || phase === 'unavailable' || phase === 'cancelled') {
      patch.error = getTrackedJobFailureDetail(status)
      patch.fading = false
    }
    updateJobState(key, patch, state.jobId)
  }, (key, state, error) => {
    updateJobState(key, { loading: false, error: error?.message || 'Job status unavailable' }, state.jobId)
  })

  const enqueueJob = async ({ key, request, body, meta = {} }) => {
    cancelJobDismissTimers(key)
    updateJobState(key, { loading: true, error: '', payload: null, status: null, jobId: null, ...meta })
    try {
      const payload = await request(body)
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
    const targetFens = Number(globalJob.totalFens)
    enqueueJob({
      key: 'global',
      request: queueGlobalAnalysis,
      body: {
        total_fens_to_process: Number(globalJob.totalFens),
        batch_size: Number(globalJob.batchSize),
        nodes_limit: DEFAULT_ANALYSIS_NODES
      },
      meta: {
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
      request: queuePlayerAnalysis,
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
      request: queueAnalysisLoop,
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

  const handleConfirmPlayerGameAnalysis = async () => {
    const plan = takePlan()
    if (!plan) return
    const { preview } = plan
    unlockCompletionAudio(audioContextRef)
    const payload = await enqueueJob({
      key: 'gameCompletion',
      request: queuePlayerGameAnalysis,
      body: {
        plan_id: preview.plan_id,
        batch_size: plan.batchSize,
        cool_off: plan.coolOff,
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
      await loadAnalysisProcesses()
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
      await deleteQueuedAnalysisJob(jobId)
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
      await deleteQueuedAnalysisJob(process.job_id)
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

  return {
    activeLoopJobCount, analysisProcessViews, analysisProcessesError,
    analysisCounts, coverage, coverageBarItems, coverageError,
    deletingAnalysisJobIds, etaClockMs, etaEstimatesRef, globalJob,
    handleAnalysisLoop, handleConfirmPlayerGameAnalysis,
    handleDeleteAnalysisProcess, handleDeleteQueuedAnalysis, handleGlobalAnalysis,
    handleInspectPlayer, handleInspectPlayerGameScope, handlePlayerAnalysis,
    handlePreviewPlayerGameAnalysis, jobState, loopJob, loopJobIsQueueing,
    ingestionTiming,
    pendingPositions, playerGameAnalysis, playerGamePreview,
    playerGamePreviewTotalSeconds, playerInspection, playerJob,
    scoredPositions, setGlobalJob, setJobCardRef, setLoopJob,
    setPlayerGameAnalysis, setPlayerInspection, setPlayerJob,
    refreshCloudProgress: () => Promise.allSettled([loadAnalysisCounts(), loadCoverage(), loadGameAnalysisOverview()]),
  }
}
