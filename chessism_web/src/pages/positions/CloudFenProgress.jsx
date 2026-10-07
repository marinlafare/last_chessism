import { fenProgress } from './cloudJobState'

export default function CloudFenProgress({ job }) {
  const { target, imported, percent } = fenProgress(job)
  return (
    <div className="cloud-fen-progress" aria-live="polite">
      <div className="cloud-fen-count">
        <span>{imported.toLocaleString()} / {target.toLocaleString()} FENs saved locally</span>
        <strong>{percent}%</strong>
      </div>
      <progress value={imported} max={target || 1} aria-label="Cloud FEN progress"
        aria-valuetext={`${imported} of ${target} FENs saved locally, ${percent}%`} />
      <small>{job.status === 'complete' ? 'Analysis, import and cleanup complete.'
        : percent === 100 ? 'All selected results are saved; finalization/cleanup may still be running.'
          : 'Updates when result batches are imported (up to 500 FENs per batch).'}</small>
    </div>
  )
}
