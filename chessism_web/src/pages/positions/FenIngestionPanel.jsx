import { pageStyleTarget } from '../../appearance/styleCore.js'
import FenPipelineStages from './FenPipelineStages'
import { formatDuration } from './positionPageSupport'

export default function FenIngestionPanel({
  jobState = {}, ingestionTiming, loading = false, error = '', timingError = '',
  className = '', title = 'Automatic FEN creation and endgame analysis',
}) {
  return (
    <section {...pageStyleTarget('panel')} className={`fen-ingestion-panel ${className}`} aria-label={title} aria-live="polite">
      <h2 {...pageStyleTarget('title')} className="panel-title">{title}</h2>
      {loading ? <p role="status">Loading automatic pipeline status…</p> : null}
      {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
      {!loading ? <FenPipelineStages jobState={jobState} ingestionTiming={ingestionTiming} /> : null}
      {ingestionTiming ? (
        <div className="fen-pipeline-timing" aria-label="Latest ingestion timing">
          <strong {...pageStyleTarget('value')}>
            Latest run
            {ingestionTiming.player_name ? ` · ${ingestionTiming.player_name}` : ''}
            {ingestionTiming.elapsed_ms != null
              ? ` · ${formatPipelineTime(ingestionTiming.elapsed_ms)}` : ' · running'}
          </strong>
          <div>
            {(ingestionTiming.stages || []).map((stage) => (
              <span key={stage.stage}>
                {String(stage.stage || '').replaceAll('_', ' ')} · {formatPipelineTime(stage.elapsed_ms)}
              </span>
            ))}
          </div>
        </div>
      ) : null}
      {timingError ? <div className="status-banner warn">{timingError}</div> : null}
      {['gameParsing', 'fen', 'tablebase'].map((key) => (
        jobState[key]?.error
          ? <div className="status-banner warn" role="alert" key={key}>{jobState[key].error}</div> : null
      ))}
    </section>
  )
}

function formatPipelineTime(milliseconds) {
  const seconds = Math.max(0, Number(milliseconds || 0) / 1000)
  return seconds < 60 ? `${seconds.toFixed(1)}s` : formatDuration(seconds)
}
