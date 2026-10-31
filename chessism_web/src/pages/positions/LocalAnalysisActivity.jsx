import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter.jsx'
import { EstimatedTime } from './JobStatus'
import AnalysisHistory from './AnalysisHistory'
import { formatNumber } from './positionPageSupport'
import ProcessTiming from './ProcessTiming.jsx'

export default function LocalAnalysisActivity({ page }) {
  const { analysisProcessViews, analysisProcessesError, deletingAnalysisJobIds, etaClockMs, etaEstimatesRef, handleDeleteAnalysisProcess } = page
  return (
    <>
    {(analysisProcessViews.length > 0 || analysisProcessesError) && <section {...pageStyleTarget('local.activity.panel')} className="local-process-status" aria-live="polite">
      <div {...pageStyleTarget('local.activity.heading')} className="analysis-processes-heading">
        <div>
          <p {...pageStyleTarget('local.activity.title')} className="eyebrow">ACTIVE ANALYSIS PROCESSES</p>
          <small {...pageStyleTarget('local.activity.note')}>Server-side Stockfish jobs currently running or waiting in the queue.</small>
        </div>
        <span {...pageStyleTarget('local.activity.count')} className="stat-chip">{analysisProcessViews.length} processes</span>
      </div>

      {analysisProcessesError ? <div className="status-banner warn">{analysisProcessesError}</div> : null}
      <div {...pageStyleTarget('local.activity.list')} className="analysis-process-list">
        {analysisProcessViews.map((process) => (
          <article {...pageStyleTarget('local.activity.card')} className="analysis-process-card" key={process.job_id}>
            <div {...pageStyleTarget('local.activity.header')} className="analysis-process-title">
              <div>
                <strong {...pageStyleTarget('local.activity.scope')}>{process.scopeLabel}</strong>
                <small {...pageStyleTarget('local.activity.job-id')}>{String(process.job_id).slice(0, 12)}</small>
              </div>
              <span className={`analysis-process-phase ${process.phase}`}>{process.phase}</span>
            </div>

            <div {...pageStyleTarget('local.activity.parameters')} className="analysis-process-parameters">
              <span><small {...pageStyleTarget('local.activity.positions.label')}>Positions/run</small><strong {...pageStyleTarget('local.activity.positions.value')}>{formatNumber(process.positionsPerRun)}</strong></span>
              <span><small {...pageStyleTarget('local.activity.runs.label')}>Runs</small><strong {...pageStyleTarget('local.activity.runs.value')}>{formatNumber(process.runs)}</strong></span>
              <span><small {...pageStyleTarget('local.activity.batch.label')}>Batch</small><strong {...pageStyleTarget('local.activity.batch.value')}>{formatNumber(process.batchSize)}</strong></span>
              <span><small {...pageStyleTarget('local.activity.cool-off.label')}>Cool-off</small><strong {...pageStyleTarget('local.activity.cool-off.value')}>{formatNumber(process.coolOff)}s</strong></span>
            </div>

            <div {...pageStyleTarget('local.activity.progress')} className="job-progress">
              <ProcessTiming {...pageStyleTarget('local.activity.detail')}
                started={process.result?.start_time || process.progress?.started_at}
                finished={process.result?.finish_time} active={!process.isQueued && !process.result?.finish_time}
                pending={process.isQueued} />
              <div {...pageStyleTarget('local.activity.progress-heading')} className="job-progress-head">
                <span>{formatNumber(process.processed)} / {formatNumber(process.total)}</span>
                <strong {...pageStyleTarget('local.activity.percent')}>{process.percent}%</strong>
              </div>
              <ProgressMeter {...pageStyleTarget('local.activity.track')} className="job-progress-track"
                fillClassName="job-progress-fill" value={process.percent} aria-label={process.scopeLabel}
                aria-valuetext={`${formatNumber(process.processed)} of ${formatNumber(process.total)}, ${process.percent}%`} />
              {process.progress?.detail ? <small {...pageStyleTarget('local.activity.detail')}>{process.progress.detail}</small> : null}
              <EstimatedTime
                jobId={process.job_id}
                progress={process.progress}
                status={process.status}
                etaClockMs={etaClockMs}
                etaEstimatesRef={etaEstimatesRef}
              />
            </div>

            {process.isQueued ? (
              <button {...pageStyleTarget('local.activity.delete')}
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
