import { pageStyleTarget } from '../../appearance/styleCore.js'
import { fenProgress } from './cloudJobState'
import { cloudJobProgress } from './cloudJobProgress'
import CloudJobIndicator from './CloudJobIndicator'

export default function CloudFenProgress({ job, label = 'Overall progress', variant = 'overall', showCompletionStatus = true }) {
  const { target, imported, percent } = fenProgress(job)
  const { done, label: currentProcess } = cloudJobProgress(job)
  return (
    <div className={`cloud-fen-progress cloud-fen-progress--${variant}`} aria-live="polite">
      <strong {...pageStyleTarget('label')}>{label}</strong>
      <div className="cloud-fen-count">
        <span className="cloud-fen-label"><CloudJobIndicator job={job} />
          <span>{imported.toLocaleString()} / {target.toLocaleString()} FENs saved locally</span>
        </span>
        <strong {...pageStyleTarget('value')}>{percent}%</strong>
      </div>
      <progress value={imported} max={target || 1} aria-label={`${label} — FENs saved locally`}
        aria-valuetext={`${imported} of ${target} FENs saved locally, ${percent}%`} />
      {showCompletionStatus && (done || percent === 100 || job.stop_cleanup) && <small {...pageStyleTarget('text')}>{done ? 'Analysis, import and cleanup complete.'
        : currentProcess}</small>}
    </div>
  )
}
