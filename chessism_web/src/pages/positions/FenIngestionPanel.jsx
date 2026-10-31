import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressReviewButton from '../../components/progress/ProgressReviewButton.jsx'
import FenPipelineStages from './FenPipelineStages'
import IngestionHistory from './IngestionHistory'
import { isTrackedJobActive } from './positionPageSupport'

export default function FenIngestionPanel({
  jobState = {}, ingestionTiming, loading = false, error = '', timingError = '',
  className = '', title = 'Ingestion',
  completedRuns = [], onDismiss,
}) {
  const active = ingestionTiming
    ? ingestionTiming.status === 'running'
    : ['gameParsing', 'fen', 'tablebase'].some(key => isTrackedJobActive(jobState[key]))
  return (
    <section {...pageStyleTarget('panel')} className={`fen-ingestion-panel ${className}`} aria-label={title} aria-live="polite">
      <h2 {...pageStyleTarget('title')} className="panel-title">{title}</h2>
      {loading ? <p role="status">Loading automatic pipeline status…</p> : null}
      {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
      {!loading && active ? <FenPipelineStages jobState={jobState} ingestionTiming={ingestionTiming} /> : null}
      {timingError ? <div className="status-banner warn">{timingError}</div> : null}
      {active && ['gameParsing', 'fen', 'tablebase'].map((key) => (
        jobState[key]?.error
          ? <div className="status-banner warn" role="alert" key={key}>{jobState[key].error}</div> : null
      ))}
      {completedRuns.map(({ run, key }) => (
        <div className="fen-ingestion-result" key={key}>
          <div className="fen-ingestion-result-heading">
            <strong {...pageStyleTarget('title')}>{run.player_name || 'Automatic ingestion'}</strong>
            <span {...pageStyleTarget('text')}>{run.status === 'complete' ? 'Completed' : run.status}</span>
          </div>
          {run.last_error && <div className="status-banner warn" role="alert">{run.last_error}</div>}
          <FenPipelineStages jobState={{}} ingestionTiming={run} />
          <ProgressReviewButton {...pageStyleTarget('button')} className="fen-ingestion-dismiss"
            title="Hide this progress result. Downloaded games, positions and ingestion history are kept."
            onClick={() => onDismiss(key)} />
        </div>
      ))}
      <IngestionHistory revision={`${ingestionTiming?.run_id || ''}:${ingestionTiming?.status || ''}`} />
    </section>
  )
}
