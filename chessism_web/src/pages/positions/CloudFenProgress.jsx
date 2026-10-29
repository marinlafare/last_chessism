import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter.jsx'
import { fenProgress } from './cloudJobState'
import { cloudJobProgress } from './cloudJobProgress'
import CloudJobIndicator from './CloudJobIndicator'

export default function CloudFenProgress({ job, label = 'Overall progress', variant = 'overall', showCompletionStatus = true }) {
  const { target, imported, percent } = fenProgress(job)
  const { done, label: currentProcess } = cloudJobProgress(job)
  const style = `cloud.progress.${variant === 'cycle' ? 'cycle' : 'overall'}`
  return (
    <div {...pageStyleTarget(style)} className={`cloud-fen-progress cloud-fen-progress--${variant}`} aria-live="polite">
      <strong {...pageStyleTarget(`${style}.title`)}>{label}</strong>
      <div className="cloud-fen-count">
        <span className="cloud-fen-label"><CloudJobIndicator job={job} />
          <span {...pageStyleTarget(`${style}.count`)}>{imported.toLocaleString()} / {target.toLocaleString()} FENs saved locally</span>
        </span>
        <strong {...pageStyleTarget(`${style}.percentage`)}>{percent}%</strong>
      </div>
      <ProgressMeter native {...pageStyleTarget(`${style}.bar`)} value={imported} max={target || 1} aria-label={`${label} — FENs saved locally`}
        aria-valuetext={`${imported} of ${target} FENs saved locally, ${percent}%`} />
      {showCompletionStatus && (done || percent === 100 || job.stop_cleanup) && <small {...pageStyleTarget(`${style}.status`)}>{done ? 'Analysis, import and cleanup complete.'
        : currentProcess}</small>}
    </div>
  )
}
