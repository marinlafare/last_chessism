import { fenProgress } from './cloudJobState'
import { cloudJobDone } from './cloudJobProgress'
import CloudJobIndicator from './CloudJobIndicator'

export default function CloudFenProgress({ job }) {
  const { target, imported, percent } = fenProgress(job)
  return (
    <div className="cloud-fen-progress" aria-live="polite">
      <div className="cloud-fen-count">
        <span className="cloud-fen-label"><CloudJobIndicator job={job} />
          <span>{imported.toLocaleString()} / {target.toLocaleString()} FENs saved locally</span>
        </span>
        <strong>{percent}%</strong>
      </div>
      <progress value={imported} max={target || 1} aria-label="Cloud FEN progress"
        aria-valuetext={`${imported} of ${target} FENs saved locally, ${percent}%`} />
      <small>{cloudJobDone(job) ? 'Analysis, import and cleanup complete.'
        : percent === 100 ? 'All FEN results are saved. Job is not done: final verification, cleanup or database totals are still pending.'
          : 'Updates when result batches are imported (up to 500 FENs per batch).'}</small>
    </div>
  )
}
