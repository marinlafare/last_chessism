import {
  formatCountdown,
  formatNumber,
  getLoopScopeLabel,
  getProgressSnapshot,
  getTrackedJobPhase,
  isAnalysisJobKey,
  isLoopJobKey,
} from './positionPageSupport'

export function EstimatedTime({ jobId, progress, status, etaClockMs, etaEstimatesRef }) {
  const phase = String(progress?.phase || status || '').toLowerCase()
  if (
    !jobId || phase === 'queued' || phase === 'deferred' || phase === 'complete' ||
    phase === 'failed' || phase === 'not_found'
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

function JobProgress({ mode, state, status, etaClockMs, etaEstimatesRef }) {
  const progress = state.progress
  const processed = mode === 'fen' ? progress.extracted : progress.analyzed

  return (
    <div className="job-progress">
      <div className="job-progress-head">
        <span>{formatNumber(processed)} / {formatNumber(progress.target)}</span>
        <strong>{progress.percent}%</strong>
      </div>
      <div className="job-progress-track">
        <div className="job-progress-fill" style={{ width: `${progress.percent}%` }} />
      </div>
      {progress.phase ? (
        <small>{progress.phase}{progress.detail ? ` | ${progress.detail}` : ''}</small>
      ) : null}
      <EstimatedTime
        jobId={state.jobId}
        progress={state.status?.progress || progress}
        status={state.status?.status || status}
        etaClockMs={etaClockMs}
        etaEstimatesRef={etaEstimatesRef}
      />
    </div>
  )
}

export function JobStatus({ jobKey, page }) {
  const state = page.jobState[jobKey] || {}
  if (state.error) return <div className="status-banner warn">{state.error}</div>
  if (!state.payload) return null

  const status = getTrackedJobPhase(state)
  const info = state.status?.info
  const resultInfo = state.status?.result
  const functionName = info?.function || resultInfo?.queue_name || state.status?.queue_name || 'job'
  const canDelete = isAnalysisJobKey(jobKey) && (status === 'queued' || status === 'deferred')

  return (
    <div
      className={`job-result ${state.fading ? 'job-result-fading' : ''}`}
      ref={page.setJobCardRef(jobKey)}
    >
      <span>{status.replace('_', ' ')}</span>
      <small>{functionName}</small>
      {isLoopJobKey(jobKey) ? (
        <strong className="job-scope-label">Running scope: {getLoopScopeLabel(state)}</strong>
      ) : null}
      {jobKey === 'gameCompletion' ? (
        <strong className="job-scope-label">
          Completing games for {state.status?.info?.kwargs?.player_name || state.payload?.player_name || 'player'}
        </strong>
      ) : null}
      {canDelete ? (
        <button
          className="btn job-delete-queued"
          type="button"
          disabled={state.deleting}
          onClick={() => page.handleDeleteQueuedAnalysis(jobKey)}
        >
          {state.deleting ? 'Deleting' : 'Delete queued analysis'}
        </button>
      ) : null}
      {jobKey === 'fen' && state.progress ? (
        <JobProgress mode="fen" state={state} status={status} {...page} />
      ) : null}
      {(isAnalysisJobKey(jobKey) || jobKey === 'tablebase') && state.progress ? (
        <JobProgress mode="analysis" state={state} status={status} {...page} />
      ) : null}
    </div>
  )
}
