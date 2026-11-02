import { useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressReviewButton from '../../components/progress/ProgressReviewButton.jsx'
import FenPipelineStages from './FenPipelineStages'
import IngestionHistory from './IngestionHistory'
import { isTrackedJobActive } from './positionPageSupport'
import { graphIngestionActive } from './pipelineProgress.js'

export default function FenIngestionPanel({
  jobState = {}, ingestionTiming, loading = false, error = '', timingError = '',
  className = '', title = 'Ingestion',
  completedRuns = [], onDismiss,
}) {
  const [olderOpen, setOlderOpen] = useState(false)
  const active = ingestionTiming
    ? ingestionTiming.status === 'running' || graphIngestionActive(ingestionTiming)
    : ['gameParsing', 'fen', 'tablebase'].some(key => isTrackedJobActive(jobState[key]))
  // Retained results belong to older runs, but their enabled review buttons can
  // look like permission to dismiss the live pipeline above them. Also cover
  // the polling gap where a new job is visible before its timing record arrives.
  const dismissalBlocked = active || loading || Boolean(error || timingError)
    || ['gameParsing', 'fen', 'tablebase'].some(key => isTrackedJobActive(jobState[key]))
  const latest = !active ? completedRuns.find(item => item.run.run_id === ingestionTiming?.run_id) || completedRuns.at(-1) : null
  const older = completedRuns.filter(item => item !== latest)
  const result = ({ run, key }, showProgress) => (
    <div className="fen-ingestion-result" key={key}>
      <div className="fen-ingestion-result-heading">
        <strong {...pageStyleTarget('title')}>{run.player_name || 'Automatic ingestion'}</strong>
        <span {...pageStyleTarget('text')}>{run.status === 'complete' ? 'Completed' : run.status}</span>
      </div>
      {run.last_error && <div className="status-banner warn" role="alert">{run.last_error}</div>}
      {showProgress && <FenPipelineStages jobState={{}} ingestionTiming={run} />}
      <ProgressReviewButton {...pageStyleTarget('button')} className="fen-ingestion-dismiss"
        disabled={dismissalBlocked}
        title={dismissalBlocked
          ? `This result belongs to ${run.player_name || 'an earlier ingestion'}. Review is available after active ingestion and Neo4j synchronization finish and their status is confirmed.`
          : `Hide the progress result for ${run.player_name || 'this ingestion'}. Downloaded games, positions and ingestion history are kept.`}
        onClick={() => { if (!dismissalBlocked) onDismiss(key) }} />
    </div>
  )
  return (
    <section {...pageStyleTarget('panel')} className={`fen-ingestion-panel ${className}`} aria-label={title} aria-live="polite">
      <h2 {...pageStyleTarget('title')} className="panel-title">{title}</h2>
      {loading ? <p role="status">Loading automatic pipeline status…</p> : null}
      {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
      {active && ingestionTiming?.player_name && <strong {...pageStyleTarget('ingestion.player')}>{ingestionTiming.player_name}</strong>}
      {!loading && active ? <FenPipelineStages jobState={jobState} ingestionTiming={ingestionTiming} /> : null}
      {timingError ? <div className="status-banner warn">{timingError}</div> : null}
      {active && ['gameParsing', 'fen', 'tablebase'].map((key) => (
        jobState[key]?.error
          ? <div className="status-banner warn" role="alert" key={key}>{jobState[key].error}</div> : null
      ))}
      {latest && result(latest, true)}
      {older.length > 0 && <details {...pageStyleTarget('ingestion.previous')} className="fen-ingestion-previous"
        open={olderOpen} onToggle={event => setOlderOpen(event.currentTarget.open)}>
        <summary {...pageStyleTarget('ingestion.previous.toggle')}>Previous results ({older.length})</summary>
        {olderOpen && older.map(item => result(item, false))}
      </details>}
      <IngestionHistory revision={`${ingestionTiming?.run_id || ''}:${ingestionTiming?.status || ''}`} />
    </section>
  )
}
