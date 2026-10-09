import { useEffect, useMemo, useRef, useState } from 'react'
import {
  formatNumber,
  getTrackedJobPhase,
  isTrackedJobActive,
} from './positionPageSupport'

const FADE_TO_IDLE_MS = 10_000
const STAGES = [
  { key: 'gameParsing', label: 'Parsing games', source: 'gameParsing' },
  { key: 'fenExtraction', label: 'FEN extraction', source: 'fen' },
  { key: 'savingPositions', label: 'Saving positions', source: 'fen' },
  { key: 'linkingGames', label: 'Linking positions', source: 'fen' },
  { key: 'updatingSummaries', label: 'Updating summaries', source: 'fen' },
  { key: 'tablebase', label: 'Tablebase marking', source: 'tablebase' },
]

const emptyStageProgress = () => Object.fromEntries(
  STAGES.map(({ key }) => [key, { value: 0, total: 0 }])
)

function phaseOf(state) {
  return String(getTrackedJobPhase(state) || '').toLowerCase()
}

function stageIndexForJob(source, state) {
  if (source === 'gameParsing') return 0
  if (source === 'tablebase') return 5

  switch (phaseOf(state)) {
    case 'saving_fens':
      return 2
    case 'saving_games':
      return 3
    case 'refreshing_statistics':
      return 4
    case 'discovering_tablebase':
      return 5
    default:
      return 1
  }
}

function jobUpdatedAt(state) {
  const progress = state?.status?.progress || state?.progress
  return Number(progress?.updated_at || 0)
}

function readCurrentJobProgress(state) {
  if (phaseOf(state) === 'discovering_tablebase') return null
  const progress = state?.status?.progress || state?.progress
  if (!progress) return null

  const total = Number(progress.total ?? progress.target ?? 0)
  const value = Number(
    progress.processed ?? progress.extracted ?? progress.analyzed ?? 0
  )

  if (!Number.isFinite(total) || total <= 0 || !Number.isFinite(value)) return null

  return {
    total: Math.max(0, total),
    value: Math.min(total, Math.max(0, value)),
  }
}

function PipelineStage({
  label,
  value,
  total,
  state,
  pipelineActive,
  stageComplete,
  stageFailed,
  stageWorking,
}) {
  const safeTotal = Number.isFinite(total) ? Math.max(0, total) : 0
  const recordedValue = Number.isFinite(value)
    ? Math.min(safeTotal, Math.max(0, value))
    : 0
  const safeValue = stageComplete && safeTotal > 0 ? safeTotal : recordedValue
  const percent = safeTotal > 0 ? Math.min(100, (safeValue / safeTotal) * 100) : 0
  const phase = phaseOf(state).replaceAll('_', ' ')
  const stageTracked = isTrackedJobActive(state)
  const lifecycle = stageFailed
    ? 'failed'
    : stageWorking
      ? 'working'
      : stageComplete
        ? 'complete'
        : 'waiting'
  const statusLabel = stageFailed
    ? 'failed'
    : stageWorking
      ? phase
      : stageComplete
        ? 'complete'
        : stageTracked
          ? phase
          : pipelineActive
            ? 'waiting'
            : 'up to date'

  return (
    <article className={`fen-pipeline-stage is-${lifecycle}`}>
      <div className="fen-pipeline-stage-head">
        <span>{label}</span>
        <i
          className="fen-pipeline-live-light"
          role="status"
          aria-label={`${label} is ${lifecycle}`}
          title={`${label} is ${lifecycle}`}
        />
      </div>
      <strong>{`${formatNumber(safeValue)} / ${formatNumber(safeTotal)}`}</strong>
      <div
        className="fen-pipeline-stage-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin="0"
        aria-valuemax={safeTotal}
        aria-valuenow={safeValue}
      >
        <div style={{ width: `${percent}%` }} />
      </div>
      <small>{statusLabel}</small>
    </article>
  )
}

export default function FenPipelineStages({ jobState }) {
  const candidates = useMemo(() => (
    ['gameParsing', 'fen', 'tablebase']
      .map((source) => ({
        source,
        state: jobState[source],
        stageIndex: stageIndexForJob(source, jobState[source]),
      }))
      .filter(({ state }) => state?.jobId)
  ), [jobState.gameParsing, jobState.fen, jobState.tablebase])
  const activeCandidates = candidates.filter(({ state }) => isTrackedJobActive(state))
  const runningCandidates = activeCandidates.filter(({ state }) => {
    const phase = phaseOf(state)
    return !['queued', 'deferred', 'discovering_tablebase'].includes(phase)
  })
  const currentCandidate = runningCandidates.at(-1) || activeCandidates.at(-1) || null
  const currentStageIndex = currentCandidate?.stageIndex ?? -1
  const workingStageIndex = runningCandidates.at(-1)?.stageIndex ?? -1
  const pipelineActive = activeCandidates.length > 0
  const failedCandidate = candidates
    .filter(({ state }) => ['failed', 'unavailable'].includes(phaseOf(state)))
    .sort((left, right) => jobUpdatedAt(right.state) - jobUpdatedAt(left.state))[0] || null
  const pipelineFailed = !pipelineActive && Boolean(failedCandidate)
  const failedStageIndex = pipelineFailed ? failedCandidate.stageIndex : -1
  const wasActiveRef = useRef(false)
  const fadeTimerRef = useRef(null)
  const [fadingToIdle, setFadingToIdle] = useState(false)
  const [stageProgress, setStageProgress] = useState(emptyStageProgress)

  useEffect(() => {
    if (pipelineActive) {
      const continuesPreviousStage = Boolean(fadeTimerRef.current)
      if (fadeTimerRef.current) window.clearTimeout(fadeTimerRef.current)
      fadeTimerRef.current = null
      setFadingToIdle(false)

      if (!wasActiveRef.current && !continuesPreviousStage) {
        setStageProgress(emptyStageProgress())
      }
    } else if (pipelineFailed) {
      if (fadeTimerRef.current) window.clearTimeout(fadeTimerRef.current)
      fadeTimerRef.current = null
      setFadingToIdle(false)
    } else if (wasActiveRef.current) {
      setFadingToIdle(true)
      fadeTimerRef.current = window.setTimeout(() => {
        fadeTimerRef.current = null
        setFadingToIdle(false)
        setStageProgress(emptyStageProgress())
      }, FADE_TO_IDLE_MS)
    }

    wasActiveRef.current = pipelineActive
  }, [pipelineActive, pipelineFailed])

  useEffect(() => {
    const visibleCandidate = currentCandidate || failedCandidate
    if (!visibleCandidate) return

    const progress = readCurrentJobProgress(visibleCandidate.state)
    if (!progress) return

    setStageProgress((current) => {
      const next = { ...current }
      const { source, stageIndex } = visibleCandidate
      const firstRelatedIndex = source === 'fen' ? 1 : stageIndex

      for (let index = firstRelatedIndex; index < stageIndex; index += 1) {
        const key = STAGES[index].key
        if (Number(next[key]?.total || 0) <= 0) {
          next[key] = { value: progress.total, total: progress.total }
        }
      }

      const currentKey = STAGES[stageIndex].key
      next[currentKey] = progress
      return next
    })
  }, [currentCandidate, failedCandidate])

  useEffect(() => () => {
    if (fadeTimerRef.current) window.clearTimeout(fadeTimerRef.current)
  }, [])

  const lifecycleClass = pipelineActive
    ? 'is-active'
    : pipelineFailed
      ? 'is-failed'
      : fadingToIdle
        ? 'is-fading-to-idle'
        : 'is-idle'

  return (
    <div className={`fen-pipeline-stages ${lifecycleClass}`}>
      {STAGES.map(({ key, label, source }, index) => {
        const state = index === 5 && currentCandidate?.stageIndex === 5
          ? currentCandidate.state
          : index === failedStageIndex
            ? failedCandidate?.state
            : jobState[source]

        return (
          <PipelineStage
            key={key}
            label={label}
            {...stageProgress[key]}
            state={state}
            pipelineActive={pipelineActive}
            stageComplete={pipelineActive && index < currentStageIndex}
            stageFailed={index === failedStageIndex}
            stageWorking={pipelineActive && index === workingStageIndex}
          />
        )
      })}
    </div>
  )
}
