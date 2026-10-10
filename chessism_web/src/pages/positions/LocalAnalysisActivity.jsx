import { EstimatedTime } from './JobStatus'
import FenPipelineStages from './FenPipelineStages'
import { formatDuration, formatNumber } from './positionPageSupport'

export default function LocalAnalysisActivity({ page }) {
  const { analysisProcessViews, analysisProcessesError, deletingAnalysisJobIds, etaClockMs, etaEstimatesRef, handleDeleteAnalysisProcess, ingestionTiming, jobState } = page
  return (
    <details className="local-activity">
      <summary>Local activity and ingestion · {analysisProcessViews.length} analysis processes</summary>
    <section className="local-ingestion-status" aria-live="polite">
      <div>
        <p className="eyebrow">INGESTION PIPELINE</p>
      </div>
      <FenPipelineStages jobState={jobState} />
      {ingestionTiming ? (
        <div className="fen-pipeline-timing" aria-label="Latest ingestion timing">
          <strong>
            Latest run
            {ingestionTiming.player_name ? ` · ${ingestionTiming.player_name}` : ''}
            {ingestionTiming.elapsed_ms != null
              ? ` · ${formatPipelineTime(ingestionTiming.elapsed_ms)}`
              : ' · running'}
          </strong>
          <div>
            {(ingestionTiming.stages || []).map((stage) => (
              <span key={stage.stage}>
                {formatStageName(stage.stage)} · {formatPipelineTime(stage.elapsed_ms)}
              </span>
            ))}
          </div>
        </div>
      ) : null}
      {['gameParsing', 'fen', 'tablebase'].map((key) => (
        jobState[key]?.error
          ? <div className="status-banner warn" key={key}>{jobState[key].error}</div>
          : null
      ))}
    </section>
    <section className="local-process-status" aria-live="polite">
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
              <EstimatedTime
                jobId={process.job_id}
                progress={process.progress}
                status={process.status}
                etaClockMs={etaClockMs}
                etaEstimatesRef={etaEstimatesRef}
              />
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
    </details>
  )
}

function formatStageName(value) {
  return String(value || '').replaceAll('_', ' ')
}

function formatPipelineTime(milliseconds) {
  const seconds = Math.max(0, Number(milliseconds || 0) / 1000)
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  return formatDuration(seconds)
}
