import { pageStyleTarget } from '../../appearance/styleCore.js'
import { EstimatedTime } from './JobStatus'
import AnalysisHistory from './AnalysisHistory'
import { formatNumber } from './positionPageSupport'

export default function LocalAnalysisActivity({ page }) {
  const { analysisProcessViews, analysisProcessesError, deletingAnalysisJobIds, etaClockMs, etaEstimatesRef, handleDeleteAnalysisProcess } = page
  return (
    <>
    {(analysisProcessViews.length > 0 || analysisProcessesError) && <section {...pageStyleTarget('panel')} className="local-process-status" aria-live="polite">
      <div className="analysis-processes-heading">
        <div>
          <p {...pageStyleTarget('text')} className="eyebrow">ACTIVE ANALYSIS PROCESSES</p>
          <small {...pageStyleTarget('text')}>Server-side Stockfish jobs currently running or waiting in the queue.</small>
        </div>
        <span className="stat-chip">{analysisProcessViews.length} processes</span>
      </div>

      {analysisProcessesError ? <div className="status-banner warn">{analysisProcessesError}</div> : null}
      <div className="analysis-process-list">
        {analysisProcessViews.map((process) => (
          <article {...pageStyleTarget('card')} className="analysis-process-card" key={process.job_id}>
            <div className="analysis-process-title">
              <div>
                <strong {...pageStyleTarget('value')}>{process.scopeLabel}</strong>
                <small {...pageStyleTarget('text')}>{String(process.job_id).slice(0, 12)}</small>
              </div>
              <span className={`analysis-process-phase ${process.phase}`}>{process.phase}</span>
            </div>

            <div className="analysis-process-parameters">
              <span><small {...pageStyleTarget('text')}>Positions/run</small><strong {...pageStyleTarget('value')}>{formatNumber(process.positionsPerRun)}</strong></span>
              <span><small {...pageStyleTarget('text')}>Runs</small><strong {...pageStyleTarget('value')}>{formatNumber(process.runs)}</strong></span>
              <span><small {...pageStyleTarget('text')}>Batch</small><strong {...pageStyleTarget('value')}>{formatNumber(process.batchSize)}</strong></span>
              <span><small {...pageStyleTarget('text')}>Cool-off</small><strong {...pageStyleTarget('value')}>{formatNumber(process.coolOff)}s</strong></span>
            </div>

            <div className="job-progress">
              <div className="job-progress-head">
                <span>{formatNumber(process.processed)} / {formatNumber(process.total)}</span>
                <strong {...pageStyleTarget('value')}>{process.percent}%</strong>
              </div>
              <div className="job-progress-track">
                <div className="job-progress-fill" style={{ width: `${process.percent}%` }} />
              </div>
              {process.progress?.detail ? <small {...pageStyleTarget('text')}>{process.progress.detail}</small> : null}
              <EstimatedTime
                jobId={process.job_id}
                progress={process.progress}
                status={process.status}
                etaClockMs={etaClockMs}
                etaEstimatesRef={etaEstimatesRef}
              />
            </div>

            {process.isQueued ? (
              <button {...pageStyleTarget('button')}
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
    </section>}
    <AnalysisHistory backend="local" revision={analysisProcessViews.map(process => `${process.job_id}:${process.status}`).join('|')} />
    </>
  )
}
