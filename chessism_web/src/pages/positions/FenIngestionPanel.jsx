import { pageStyleTarget } from '../../appearance/styleCore.js'
import FenPipelineStages from './FenPipelineStages'
import IngestionHistory from './IngestionHistory'
import { isTrackedJobActive } from './positionPageSupport'

export default function FenIngestionPanel({
  jobState = {}, ingestionTiming, loading = false, error = '', timingError = '',
  className = '', title = 'Ingestion',
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
      <IngestionHistory revision={`${ingestionTiming?.run_id || ''}:${ingestionTiming?.status || ''}`} />
    </section>
  )
}
